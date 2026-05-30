---
sidebar_position: 17
sidebar_label: "LINE"
title: "LINE"
description: "Set up Hermes Agent as a LINE bot via the LINE Messaging API"
---

# LINE Setup

Hermes connects to [LINE](https://line.me/) through the **Messaging API**. People
chat with your bot in a 1-on-1 conversation, group, or room and get AI responses
back — the same conversational experience as Telegram or Discord.

LINE is a **bundled platform plugin** (`plugins/platforms/line/`). It uses an
aiohttp webhook listener for inbound messages and the LINE Reply / Push API for
replies. No `line-bot-sdk` is required — it talks to LINE over `httpx` and
verifies webhook signatures with the standard library.

---

## Prerequisites

- **A LINE Messaging API channel** — create one in the
  [LINE Developers console](https://developers.line.biz/console/)
- **A publicly reachable HTTPS URL** — LINE delivers events to your webhook over
  HTTPS only (see [Exposing the webhook](#exposing-the-webhook))
- **aiohttp** — `pip install 'hermes-agent[messaging]'`

---

## Step 1: Create a Messaging API channel

1. Open the [LINE Developers console](https://developers.line.biz/console/) and
   create (or pick) a **Provider**.
2. Create a new **Messaging API** channel under that provider.
3. In **Basic settings**, copy the **Channel secret**.
4. In the **Messaging API** tab, issue and copy a **Channel access token**
   (long-lived).

---

## Step 2: Configure Hermes

### Interactive setup (recommended)

```bash
hermes gateway setup
```

Select **LINE** from the platform list. The wizard prompts for your token, secret,
webhook port, public base URL, and access-control policy, then prints the exact
webhook URL to paste into the LINE console.

### Manual setup

Add to `~/.hermes/.env`:

```bash
LINE_CHANNEL_ACCESS_TOKEN=your_long_lived_channel_access_token
LINE_CHANNEL_SECRET=your_channel_secret

# Public HTTPS base URL of this host — required to send images/video/audio
# back to LINE (LINE accepts media only as a public URL, not raw bytes).
LINE_PUBLIC_BASE_URL=https://hermes.example.com

# Security: restrict who can message the bot (recommended)
LINE_ALLOWED_USERS=U1234567890abcdef...,Ufedcba0987654321...

# Optional overrides
LINE_WEBHOOK_HOST=0.0.0.0          # bind address (default 0.0.0.0)
LINE_WEBHOOK_PORT=8645             # bind port (default 8645)
LINE_WEBHOOK_PATH=/line/webhook    # URL path (default /line/webhook)
# LINE_ALLOW_ALL_USERS=true        # allow anyone (public bots only — use with care)
```

Or in `~/.hermes/config.yaml`:

```yaml
gateway:
  platforms:
    line:
      enabled: true
      extra:
        channel_access_token: "..."
        channel_secret: "..."
        public_base_url: https://hermes.example.com
        host: 0.0.0.0
        port: 8645
        webhook_path: /line/webhook
        allowed_users: ["U1234..."]
```

Environment variables take precedence over `config.yaml`.

---

## Exposing the webhook

LINE only delivers events to a public **HTTPS** URL. Pick whichever fits your
deployment — the plugin itself does **not** manage the tunnel:

| Mode | Command | Notes |
|------|---------|-------|
| **Cloudflare Tunnel** (recommended for hobby) | `cloudflared tunnel --url http://localhost:8645` | Stable HTTPS URL, no open ports |
| **ngrok** (development) | `ngrok http 8645` | URL changes each restart — re-paste into LINE console |
| **VPS + reverse proxy** (production) | Caddy/nginx terminates TLS on `:443`, proxies `/line/webhook` → `127.0.0.1:8645` | Simplest if you already run a server |

Set `LINE_PUBLIC_BASE_URL` to the public origin (e.g. `https://hermes.example.com`)
so outbound media links resolve.

---

## Step 3: Point LINE at your webhook

1. In the channel's **Messaging API** tab, set the **Webhook URL** to
   `https://<your-host>/line/webhook`.
2. Enable **Use webhook**.
3. Click **Verify** — you should get a green check.
4. Disable **Auto-reply messages** and **Greeting messages** (under the LINE
   Official Account Manager) so they don't compete with the agent.

Start the gateway:

```bash
hermes gateway restart
```

Add the bot as a friend (scan the QR in the **Messaging API** tab) and send it a
message.

---

## Capabilities & limitations

| Area | Behavior |
|------|----------|
| Formatting | **Plain text only** — LINE renders no markdown. Long replies are split at 5000 characters automatically. |
| Reply vs Push | Replies use the free, single-use **reply token** when available (~1 min TTL) and fall back to the **Push API** (counts against your monthly quota) otherwise. Cron-delivered messages always use Push. |
| Inbound media | Images, video, audio, and files are downloaded and made available to the agent (e.g. for vision). |
| Outbound media | Images/video/audio require `LINE_PUBLIC_BASE_URL`; the file is hosted on a short-lived `/line/media/<token>` URL. Documents are sent as a link (LINE has no file message type). |
| Typing indicator | Shown in 1-on-1 chats only (LINE limitation). |
| Edit / delete | Not supported by LINE — once sent, messages stay. |
| Groups & rooms | Supported. The bot must be invited; some group events omit the sender's user ID. |
| Stickers | Inbound stickers arrive as a `[Sticker <pkg>/<id>]` text placeholder. Outbound stickers and rich Flex Messages are not yet supported. |

---

## Access control

By default the bot only answers users listed in `LINE_ALLOWED_USERS` (comma-separated
LINE user IDs, which look like `U1234...`). To find a user's ID, message the bot once
and check `gateway.log`. Set `LINE_ALLOW_ALL_USERS=true` only for intentionally public
bots.

---

## Troubleshooting

- **"Verify" fails in the LINE console** — confirm the gateway is running, the
  tunnel is up, and the webhook URL ends in `/line/webhook`. Signature failures
  return `401`; double-check `LINE_CHANNEL_SECRET`.
- **Bot receives messages but never replies** — the sender's user ID is probably
  not in `LINE_ALLOWED_USERS`. Check `gateway.log` for the dropped message.
- **Images don't send** — set `LINE_PUBLIC_BASE_URL` to a reachable HTTPS origin;
  local files are served from that base.
- **Port already in use** — change `LINE_WEBHOOK_PORT` (the generic webhook
  adapter defaults to `8644`, so LINE uses `8645`).
