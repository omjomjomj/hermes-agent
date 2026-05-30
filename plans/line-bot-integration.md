# LINE Bot Integration — Design Doc

## Implementation Status (2026-05-25)

**Status: v1 implemented and unit-tested.** Plugin lives in
`plugins/platforms/line/` (`plugin.yaml`, `__init__.py`, `adapter.py`).

Two deviations from the original design, both validated against the codebase:
- **No `line-bot-sdk`.** Uses `httpx` (already a core dep) for the REST API and
  stdlib `hmac` for signature verification. Eliminates the "v3 async churn" risk
  hotspot and adds zero packages.
- **`aiohttp.web`** for the webhook listener, matching `gateway/platforms/webhook.py`.

Verified end-to-end:
- Plugin auto-discovers and registers (`platform_registry.get("line")`); bundled
  `kind: platform` plugins auto-load.
- Authorization auto-wires — `gateway/run.py:_is_user_authorized` reads the
  plugin's `LINE_ALLOWED_USERS` / `LINE_ALLOW_ALL_USERS` from the registry
  (run.py:4193-4204).
- `send_message` tool (tools/send_message_tool.py:212-217) and cron delivery
  (cron/scheduler.py:411-414) both route plugin platforms via `Platform("line")`
  → live gateway adapter. **Step 12 fallback (`_send_line()`) is NOT needed.**
- Tests: `tests/gateway/test_line_adapter.py` — 40 passing (signature verify,
  chunking, reply-token cache, inbound mapping, reply/push fallback, send_typing,
  chat_info, media hosting, registration). No regression in sibling adapter tests.
- Docs: `website/docs/user-guide/messaging/line.md`.

Done: steps 1-14 of the plan below (group/room supported; stickers inbound as
placeholder). Not yet built (future): Flex Messages, rich menu, sticker output,
quick replies, voice auto-transcription, live integration test (`-m line_live`).

Steps for the next session: live smoke test against a real LINE channel; wire
LINE into README / AGENTS.md platform tables and the docs index/env-var reference.

---

## Goal
Add LINE Messaging API support to Hermes Agent so users can chat with Hermes through LINE (1-on-1, group, room). The agent should receive incoming user messages via webhook and reply via the LINE Reply / Push API. Cron-scheduled jobs and the `send_message` tool should also be able to deliver to LINE.

## Architecture Decision

**Path: Plugin** (`plugins/platforms/line/`) — chosen over the built-in path.

Rationale:
- LINE is fundamentally a webhook + REST adapter, not a streaming protocol; the `BasePlatformAdapter` interface fits cleanly.
- Plugin path needs **zero changes** to core Hermes code (per `gateway/platforms/ADDING_A_PLATFORM.md` §Plugin Path). The `register_platform()` ctx call automatically wires authorization, cron delivery, send_message routing, system-prompt hints, status display, and the gateway setup wizard.
- IRC plugin (`plugins/platforms/irc/adapter.py`, 686 lines) is the closest reference: stdlib-only protocol, single-file adapter, `register(ctx)` entry point. We mirror its layout, swapping the IRC socket loop for an aiohttp webhook listener.
- Built-in path requires touching ~16 files (see `ADDING_A_PLATFORM.md`); reserved for upstream contribution if the plugin proves stable.

Trade-off:
- Built-in path lets us use `Platform.LINE` enum directly. Plugin path uses `Platform("line")` constructed at runtime, which the plugin loader registers into the enum — equivalent capability, just one indirection.
- If we later upstream this, the diff to convert plugin → built-in is mechanical (move file, add the 16 integration points).

## LINE Messaging API — what matters for us

| Concern | Behavior |
|---------|----------|
| Inbound transport | Webhook POST to `https://your-host/<path>`, JSON body with `events: [...]` |
| Auth on inbound | `X-Line-Signature` header — HMAC-SHA256(channel_secret, raw_body), base64-encoded |
| Outbound reply | `POST /v2/bot/message/reply` with `replyToken` from event (free, ~1 min TTL, single use) |
| Outbound push | `POST /v2/bot/message/push` with `to` = userId/groupId/roomId (counted against monthly quota) |
| Message types in | text, image, video, audio, file, location, sticker, postback, follow, unfollow, join, leave |
| Message types out | text (max 5000 chars), image (URL), video, audio, sticker, location, flex, template, imagemap |
| Identifiers | `userId` (33 chars, `U…`), `groupId` (`C…`), `roomId` (`R…`) — all opaque |
| Display name lookup | `GET /v2/bot/profile/{userId}` → `displayName` |
| Loading indicator | `POST /v2/bot/chat/loading/start` (5–60s shown spinner, 1-on-1 only) |
| Edit / delete | Not supported (LINE has no message edit API) |
| Markdown | Not rendered — plain text only |
| Rate limits | 2000 msgs/sec push; reply API not rate-limited per token |
| SDK | `line-bot-sdk>=3.13` — has async `AsyncMessagingApi`, `AsyncMessagingApiBlob` for media |

## Files to Create

```
plugins/platforms/line/
├── plugin.yaml          # plugin manifest
├── __init__.py          # re-exports register
└── adapter.py           # LineAdapter + webhook receiver + register()
```

Optionally (if it grows past ~800 lines we split):
```
plugins/platforms/line/
├── adapter.py           # LineAdapter (BasePlatformAdapter subclass)
├── webhook.py           # aiohttp app: signature verify + event dispatch
├── outbound.py          # reply/push API wrappers + media upload
└── setup.py             # interactive_setup() wizard
```

Initial implementation: single `adapter.py`. Split only if needed.

## Module Design

### `LineAdapter(BasePlatformAdapter)`

Constructor:
```python
def __init__(self, config, **kwargs):
    super().__init__(config=config, platform=Platform("line"))
    extra = getattr(config, "extra", {}) or {}
    self.channel_access_token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or extra.get("channel_access_token", "")
    self.channel_secret      = os.getenv("LINE_CHANNEL_SECRET")        or extra.get("channel_secret", "")
    self.host                = os.getenv("LINE_WEBHOOK_HOST") or extra.get("host", "0.0.0.0")
    self.port                = int(os.getenv("LINE_WEBHOOK_PORT") or extra.get("port", 8645))
    self.webhook_path        = extra.get("webhook_path", "/line/webhook")
    self.allowed_users       = extra.get("allowed_users", [])
    self.max_message_length  = int(extra.get("max_message_length", 5000))
    # SDK clients lazy-init in connect()
    self._api: AsyncMessagingApi | None = None
    self._blob_api: AsyncMessagingApiBlob | None = None
    self._http: aiohttp.ClientSession | None = None
    self._runner: web.AppRunner | None = None
    # Reply-token cache: chat_id → (reply_token, deadline_ts)
    self._reply_tokens: dict[str, tuple[str, float]] = {}
```

Required overrides (match `base.py` signatures already audited at lines 1410, 1419, 1424, 1616, 1690, 3164, 3202):

| Method | Implementation strategy |
|--------|-------------------------|
| `connect() -> bool` | Validate token+secret, init SDK clients, start aiohttp app on host:port, register webhook URL hint in logs. Return `False` with `_set_fatal_error("config_missing", …, retryable=False)` if creds missing. |
| `disconnect()` | `runner.cleanup()`, close ApiClient + http session, cancel any background tasks. |
| `send(chat_id, content, reply_to=None, metadata=None)` | Try Reply API first if a fresh `reply_token` is cached for `chat_id`; fall back to Push API. Split content into ≤5000-char chunks. Return `SendResult(success, message_id, error, retryable)`. |
| `send_typing(chat_id)` | `chat/loading/start` with `loadingSeconds=20` — only for 1-on-1 (`chat_id` starts with `U`). Silently skip for groups/rooms. |
| `send_image(chat_id, image_url, caption)` | LINE requires both `originalContentUrl` and `previewImageUrl`. If `caption` is set, send as separate text message after the image. Reject non-HTTPS URLs. |
| `send_image_file(chat_id, path, caption)` | Upload via `AsyncMessagingApiBlob` is for **inbound** content download only — for outbound we need a public URL. Strategy: lazy-host local files via the same aiohttp app at `/line/media/<token>` (token is HMAC of path+expiry); generate URL, send as image message, expire token after 1 hour. |
| `send_voice(chat_id, path)` | LINE audio messages need `originalContentUrl` (m4a) + duration. Same media-hosting trick. |
| `send_video(chat_id, path, caption)` | Same as image, plus required `previewImageUrl` (poster frame). If no poster, generate via ffmpeg or fall back to `send_document` (LINE has no document type — fall back to public URL in a text message). |
| `send_document(chat_id, path, caption)` | LINE has no native file message. Send as URL+caption text message (or skip with warning). |
| `send_animation(chat_id, path, caption)` | Same as video; LINE renders animated GIF as image with autoplay. |
| `get_chat_info(chat_id) -> {name, type, chat_id}` | If `U…` → `bot/profile/{userId}` → displayName, type="dm". If `C…` → `bot/group/{groupId}/summary` → groupName, type="group". If `R…` → can't query name, type="room". |

Optional but useful:
- `delete_message(...)` → return `SendResult(success=False, error="not supported")` (default).
- `edit_message(...)` → same.

### Webhook receiver

Single aiohttp `web.Application` started in `connect()`:

```python
app = web.Application()
app.router.add_post(self.webhook_path, self._handle_webhook)
app.router.add_get("/line/media/{token}/{filename}", self._serve_media)
runner = web.AppRunner(app)
await runner.setup()
site = web.TCPSite(runner, self.host, self.port)
await site.start()
```

`_handle_webhook(request)`:
1. Read raw body bytes (need bytes for HMAC; do NOT decode first).
2. Compute `expected = base64(hmac_sha256(channel_secret, body))`.
3. Compare to `request.headers["X-Line-Signature"]` with `hmac.compare_digest`.
4. On mismatch → 401, log redacted info (don't log the secret or body).
5. Parse JSON → loop `events`. For each event:
   - Extract `type` (`message`, `follow`, `join`, `postback`, …).
   - Build `MessageEvent` (see mapping table below).
   - Cache `replyToken` in `self._reply_tokens[chat_id]` with 50s deadline.
   - `await self.handle_message(event)` to dispatch to gateway.
6. Always return 200 within ~5s — LINE retries on timeout.

Defense against replay & flooding:
- Idempotency: track `(channel_id, event.webhookEventId)` in an LRU set, drop duplicates. (LINE retries on 5xx for ~1 hour.)
- Reject bodies > 1 MB.
- Per-source rate limit (use the same fixed-window pattern from `webhook.py` if needed, ~30 events/min/user).

### Inbound message-type mapping

| LINE event | `MessageType` | Notes |
|-----------|---------------|-------|
| `message` / `type=text` | `TEXT` (or `COMMAND` if starts with `/`) | Use `is_command()` from base. |
| `message` / `type=image` | `PHOTO` | Download blob via `AsyncMessagingApiBlob.get_message_content`, write to media cache via `cache_image_from_bytes`, append path to `media_urls`. |
| `message` / `type=video` | `VIDEO` | Same blob download. |
| `message` / `type=audio` | `AUDIO` (or `VOICE` if `duration` present and < 30s? — start with AUDIO) | Same blob download. |
| `message` / `type=file` | `DOCUMENT` | `cache_document_from_bytes`. |
| `message` / `type=location` | `LOCATION` | Fill `MessageEvent.text` with `f"📍 {title}\n{address}\nlat={lat} lon={lon}"`. |
| `message` / `type=sticker` | `STICKER` | Sticker has `packageId` + `stickerId`; stuff into `text` as `[Sticker {packageId}/{stickerId}]` for now. (Future: render actual sticker via `sticker_cache`.) |
| `postback` | `TEXT` (with `data` as content) | For rich-menu / quick-reply later. |
| `follow` / `unfollow` / `join` / `leave` | _(skip)_ | Log only; no agent dispatch in v1. |

Building the `SessionSource` via `self.build_source(...)`:
- 1-on-1 (`source.type=user`): `chat_id=userId`, `chat_type="dm"`, `user_id=userId`, `user_name=<from profile lookup, cached>`.
- Group (`source.type=group`): `chat_id=groupId`, `chat_type="group"`, `user_id=userId`, `user_name=<group member profile>`.
- Room (`source.type=room`): same as group with `chat_type="group"` (or add new type — start with "group").

### Outbound send strategy

```
                ┌──────────────────────────────────┐
send(chat_id, ─▶│ has fresh reply_token (≤50s old)?│
     content)   └──────────────┬───────────────────┘
                               │
                  yes ◀────────┴────────▶ no
                   │                       │
        ┌──────────▼──────────┐  ┌─────────▼──────────┐
        │ Reply API           │  │ Push API           │
        │ (free, single use)  │  │ (counts vs quota)  │
        └─────────────────────┘  └────────────────────┘
```

Token used → drop from cache. If Reply API returns 400 "Invalid reply token" (token expired/used), retry once via Push.

### `register(ctx)` entry point

```python
def register(ctx):
    ctx.register_platform(
        name="line",
        label="LINE",
        adapter_factory=lambda cfg: LineAdapter(cfg),
        check_fn=check_requirements,         # returns line-bot-sdk + aiohttp installed
        validate_config=validate_config,     # token + secret present
        is_connected=is_connected,           # bool(token and secret)
        required_env=["LINE_CHANNEL_ACCESS_TOKEN", "LINE_CHANNEL_SECRET"],
        install_hint="pip install 'line-bot-sdk>=3.13'",
        setup_fn=interactive_setup,          # wizard for `hermes gateway setup`
        allowed_users_env="LINE_ALLOWED_USERS",
        allow_all_env="LINE_ALLOW_ALL_USERS",
        max_message_length=5000,
        emoji="💚",
        pii_safe=False,                      # LINE userIds are stable identifiers
        allow_update_command=True,
        platform_hint=(
            "You are chatting via LINE. LINE does not render markdown — use plain "
            "text only (bold/italic/code blocks will appear as raw asterisks/backticks). "
            "Messages are limited to 5000 characters per message; long replies are "
            "split automatically. You cannot edit or delete messages once sent. "
            "Stickers and rich Flex Messages are not yet supported in this adapter."
        ),
    )
```

## Configuration

### Environment variables (preferred, mirror IRC pattern)

| Var | Required | Description |
|-----|----------|-------------|
| `LINE_CHANNEL_ACCESS_TOKEN` | yes | Long-lived channel access token (LINE Developers console → Messaging API → Channel access token) |
| `LINE_CHANNEL_SECRET` | yes | Channel secret (same console → Basic settings) |
| `LINE_WEBHOOK_HOST` | no | Bind address, default `0.0.0.0` |
| `LINE_WEBHOOK_PORT` | no | Bind port, default `8645` (avoid clashing with webhook.py's 8644) |
| `LINE_WEBHOOK_PATH` | no | URL path, default `/line/webhook` |
| `LINE_ALLOWED_USERS` | no | Comma-separated `U…` IDs allowed to talk to the bot |
| `LINE_ALLOW_ALL_USERS` | no | `true` to allow anyone (use with care for public bots) |
| `LINE_PUBLIC_BASE_URL` | recommended | Public HTTPS URL of the webhook host, e.g. `https://hermes.example.com`. Used to construct media URLs sent in outbound messages. |

### `config.yaml` fallback (per IRC convention)

```yaml
gateway:
  platforms:
    line:
      enabled: true
      extra:
        channel_access_token: "..."
        channel_secret: "..."
        host: 0.0.0.0
        port: 8645
        webhook_path: /line/webhook
        public_base_url: https://hermes.example.com
        allowed_users: ["U1234..."]
        max_message_length: 5000
```

## Deployment

LINE requires the webhook URL to be **publicly reachable HTTPS**. Three deploy modes:

1. **VPS + reverse proxy** (production): Caddy/nginx terminates TLS at `:443`, proxies `/line/webhook` → `127.0.0.1:8645`. Simplest if user already has a server.
2. **Cloudflare Tunnel** (recommended for hobby): `cloudflared tunnel --url http://localhost:8645` gives a stable HTTPS URL without opening ports.
3. **ngrok** (development): `ngrok http 8645`. URL changes each restart; user must update LINE console webhook URL.

`interactive_setup()` should:
- Prompt for token + secret (password-masked input).
- Probe `/v2/bot/info` with the token; on 200 echo bot name. On 401 abort with hint.
- Suggest tunnel options (don't auto-launch — too magical).
- Print final webhook URL the user must paste into LINE Developers console.

The plugin does NOT manage the tunnel itself — that's a deployment concern, not adapter logic. `LINE_PUBLIC_BASE_URL` only matters for the media-hosting subroute; if not set, fall back to sending public URLs the user provides via `send_image(image_url=...)` and skipping local-file image sends with a warning.

## Trade-offs and Limitations

| Limitation | Why | Mitigation |
|-----------|-----|------------|
| No markdown rendering | LINE platform | `platform_hint` tells LLM to use plain text |
| No edit/delete | LINE platform | `edit_message` returns success=False; `stream_consumer.py` already handles this fallback |
| Local file → image/video needs public URL | LINE platform | Lazy-host via aiohttp subroute `/line/media/{token}` |
| No native document type | LINE platform | Send as text message with link |
| Reply token has 1-min TTL and single use | LINE platform | Cache with 50s deadline; fall back to Push API |
| Push API counts against monthly quota | LINE pricing | Prefer Reply; cron-delivered messages always use Push |
| Loading animation 1-on-1 only | LINE platform | `send_typing` no-op for groups/rooms |
| Webhook URL must be HTTPS | LINE platform | Document tunnel options in setup wizard |
| Group `userId` not always available | LINE privacy | Some events lack `userId`; degrade gracefully (`user_name="LINE user"`) |

## Implementation Steps

1. **Skeleton** — `plugins/platforms/line/{plugin.yaml, __init__.py, adapter.py}`. Adapter has stub `connect/disconnect/send/get_chat_info`. `register(ctx)` works. Plugin loads cleanly.
2. **Webhook receiver** — aiohttp app, signature verification, JSON parsing. Verify with a captured sample event from LINE Developers console "Verify" button.
3. **Inbound text** — map `message/text` → `MessageEvent(TEXT)`, dispatch via `handle_message`. Confirm agent reply lands in `send()`.
4. **Outbound text** — Reply API + Push API path, token caching, chunking at 5000 chars. Verify reply lands in LINE app.
5. **Allowlist + auth** — wire `LINE_ALLOWED_USERS` / `LINE_ALLOW_ALL_USERS` via the `register_platform` env keys; verify `_is_user_authorized()` blocks non-allowed users.
6. **Media inbound** — image/video/audio/file blob download → cache helpers → `media_urls`. Verify the agent's vision tool can read attachments.
7. **Media outbound** — image URL send via Reply/Push. Local-file media subroute (`/line/media/{token}`). Test `send_image_file`.
8. **Setup wizard** — `interactive_setup()` for `hermes gateway setup`. Test on Termux + WSL2.
9. **Loading indicator** — `send_typing` for 1-on-1.
10. **Group/room support** — group/room source mapping, `get_chat_info` for groups.
11. **Stickers (basic)** — inbound stickers as text placeholder; outbound not in v1.
12. **Cron delivery + send_message tool** — verify the plugin system auto-wires these (per `ADDING_A_PLATFORM.md` plugin path claim). If not, fall back to writing a `_send_line()` function and registering it.
13. **Tests** — `tests/plugins/test_line_adapter.py`: signature verification, message mapping, allowlist, send chunking. Mock `AsyncMessagingApi`.
14. **Docs** — `website/docs/user-guide/messaging/line.md` with setup steps + tunnel guide.

## Testing Strategy

Unit tests (no LINE network calls):
- HMAC signature verify: known body + secret → known signature; tampered body fails.
- Inbound mapping: each LINE event JSON → expected `MessageEvent` shape (text, photo, location, sticker).
- Outbound chunking: 12000-char string → 3 messages, each ≤5000 chars, no mid-grapheme split.
- Reply-token cache: TTL expiry, single-use eviction.
- Allowlist: user not in `LINE_ALLOWED_USERS` → message dropped.
- `get_chat_info`: 1-on-1, group, room paths.

Integration test (requires real channel; gated behind env var):
- `pytest -m line_live` runs against a test channel.
- Send "ping" via httpx to webhook with valid signature → assert agent received `MessageEvent` with text="ping".

Manual smoke test (documented in setup):
- `hermes gateway setup` → choose LINE → paste creds.
- Tunnel up, paste URL into LINE console "Verify" → expect green check.
- Send "hi" from LINE app to bot → expect agent reply.

## Future Extensions (out of scope for v1)

- **Flex Messages / Templates** — rich-card replies via `tools/line_flex_tool.py`. Useful for menus, carousels, quick-replies.
- **Rich Menu** — programmable menu attached to chat. Configured via `tools/line_rich_menu_tool.py`.
- **Sticker output** — render sticker IDs as actual stickers (need a sticker pack lookup table).
- **Quick Reply buttons** — attach `quickReply` to outbound messages for inline action buttons.
- **LINE Login** — OAuth identity for cross-platform user linking.
- **Audience push** — broadcast/multicast/narrowcast for fan-out scenarios.
- **Webhook fallback** — if local aiohttp port is firewalled, support running the receiver behind the existing `gateway/platforms/webhook.py` shared HTTP server (single port for all webhook adapters).

## Open Questions for User

1. **Deployment target**: VPS + nginx? Cloudflare Tunnel? ngrok? — affects what we put in setup wizard hints.
2. **LINE channel type**: Personal LINE Developers account, or a LINE Official Account (LOA) with verified status? — affects which APIs are available (rich menu / narrowcast need LOA).
3. **Group support priority**: v1 ships group-aware? Or 1-on-1 first and groups in v1.1?
4. **Voice memo handling**: Hermes has voice transcription; should LINE audio messages auto-transcribe like Telegram does? (`agent/voice_transcribe.py`)
5. **Quota-aware push**: When reply token unavailable AND we're near monthly Push quota, what's the fallback? Drop with warning, or send anyway? (Default: send anyway; surface quota in `/status`.)

---

**Estimated scope**: ~700–900 lines in `adapter.py`, ~150 lines tests, ~100 lines docs. 1–2 days of focused work for v1 (steps 1–8). Steps 9–14 incremental.

**Risk hotspots**: (a) media outbound URL hosting — first time Hermes serves outbound media URLs over HTTP; needs careful token expiry. (b) LINE SDK v3 async API churn — pin to a tested version. (c) Verifying the plugin auto-wires cron + send_message tool routing (the ADDING_A_PLATFORM.md docs claim it does; if not, falls into step 12 fallback).
