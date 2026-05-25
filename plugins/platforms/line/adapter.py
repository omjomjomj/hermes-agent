"""
LINE Messaging API platform adapter for Hermes Agent.

A plugin-based gateway adapter that receives inbound LINE messages over an
aiohttp webhook listener and replies through the LINE Reply / Push API.

Design notes
------------
* **No line-bot-sdk dependency.**  The LINE Messaging API is a plain REST +
  HMAC surface, so we talk to it with :mod:`httpx` (already a core Hermes
  dependency) and verify signatures with the stdlib :mod:`hmac`.  This avoids
  the line-bot-sdk v3 async-API churn and adds zero new packages.
* **aiohttp webhook listener.**  Mirrors ``gateway/platforms/webhook.py`` —
  HMAC-verify the raw body first, then dispatch.  Ships with the ``messaging``
  extra.
* **Reply-then-Push.**  Every inbound event carries a single-use ``replyToken``
  (~1 min TTL).  We cache it and prefer the (free) Reply API; if it is missing
  or rejected we fall back to the Push API (counts against the monthly quota).

Configuration in config.yaml::

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

Or via environment variables (override config.yaml)::

    LINE_CHANNEL_ACCESS_TOKEN, LINE_CHANNEL_SECRET, LINE_WEBHOOK_HOST,
    LINE_WEBHOOK_PORT, LINE_WEBHOOK_PATH, LINE_PUBLIC_BASE_URL,
    LINE_ALLOWED_USERS, LINE_ALLOW_ALL_USERS
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import mimetypes
import os
import secrets
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    from aiohttp import web

    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only when extra missing
    AIOHTTP_AVAILABLE = False
    web = None  # type: ignore[assignment]

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_audio_from_bytes,
    cache_document_from_bytes,
    cache_image_from_bytes,
)
from gateway.config import Platform

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LINE_API_BASE = "https://api.line.me"
LINE_DATA_API_BASE = "https://api-data.line.me"

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8645  # avoid clashing with webhook.py's 8644
DEFAULT_WEBHOOK_PATH = "/line/webhook"
MEDIA_ROUTE = "/line/media/{token}/{filename}"

MAX_TEXT_LEN = 5000          # LINE hard limit per text message object
MAX_MESSAGES_PER_REQUEST = 5  # LINE reply/push accept up to 5 message objects
REPLY_TOKEN_TTL = 50.0        # seconds — LINE reply tokens live ~1 min; stay under
MEDIA_TOKEN_TTL = 3600.0      # seconds — outbound local-file hosting window
NAME_CACHE_TTL = 3600.0       # seconds — displayName cache
IDEMPOTENCY_TTL = 3600.0      # seconds — LINE retries 5xx for up to ~1 hour
MAX_BODY_BYTES = 1_048_576    # 1 MB — reject oversized webhook bodies
LOADING_SECONDS = 20          # multiple of 5 in [5, 60]


def check_requirements() -> bool:
    """Return True when the adapter's runtime dependencies are importable.

    httpx is a core Hermes dependency; aiohttp ships with the ``messaging``
    extra.  Only the latter can realistically be missing.
    """
    return AIOHTTP_AVAILABLE


# ---------------------------------------------------------------------------
# LINE Adapter
# ---------------------------------------------------------------------------


class LineAdapter(BasePlatformAdapter):
    """Async LINE adapter implementing the BasePlatformAdapter interface."""

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("line"))

        extra = getattr(config, "extra", {}) or {}

        # Credentials (env wins over config.yaml)
        self.channel_access_token = (
            os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
            or extra.get("channel_access_token", "")
        ).strip()
        self.channel_secret = (
            os.getenv("LINE_CHANNEL_SECRET") or extra.get("channel_secret", "")
        ).strip()

        # Webhook listener
        self.host = os.getenv("LINE_WEBHOOK_HOST") or extra.get("host", DEFAULT_HOST)
        self.port = int(os.getenv("LINE_WEBHOOK_PORT") or extra.get("port", DEFAULT_PORT))
        self.webhook_path = (
            os.getenv("LINE_WEBHOOK_PATH")
            or extra.get("webhook_path", DEFAULT_WEBHOOK_PATH)
        )

        # Public base URL — required only for hosting outbound local media.
        base = os.getenv("LINE_PUBLIC_BASE_URL") or extra.get("public_base_url", "")
        self.public_base_url = base.rstrip("/") if base else ""

        # Limits
        self.max_message_length = int(extra.get("max_message_length", MAX_TEXT_LEN))

        # Runtime clients/state (lazy-init in connect()).  Authorization is
        # wired by register_platform()'s allowed_users_env / allow_all_env and
        # enforced centrally by the gateway's _is_user_authorized().
        self._http = None  # httpx.AsyncClient
        self._runner = None  # web.AppRunner
        self._bot_user_id: str = ""

        # chat_id -> (reply_token, deadline_ts)
        self._reply_tokens: Dict[str, Tuple[str, float]] = {}
        # webhookEventId -> seen_ts (idempotency against LINE retries)
        self._seen_events: Dict[str, float] = {}
        # (scope, user_id) -> (display_name, ts)
        self._name_cache: Dict[Tuple[str, str], Tuple[str, float]] = {}
        # media token -> (abs_path, expiry_ts) for outbound local-file hosting
        self._media_tokens: Dict[str, Tuple[str, float]] = {}

    @property
    def name(self) -> str:
        return "LINE"

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self) -> bool:
        if not self.channel_access_token or not self.channel_secret:
            logger.error("LINE: channel access token and secret must be configured")
            self._set_fatal_error(
                "config_missing",
                "LINE_CHANNEL_ACCESS_TOKEN and LINE_CHANNEL_SECRET must be set",
                retryable=False,
            )
            return False

        import httpx

        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=20),
            headers={"Authorization": f"Bearer {self.channel_access_token}"},
        )

        # Validate the token up front; a bad token is a fatal, non-retryable
        # misconfiguration rather than a transient connection failure.
        try:
            resp = await self._http.get(f"{LINE_API_BASE}/v2/bot/info")
            if resp.status_code == 401:
                logger.error("LINE: channel access token rejected (401)")
                self._set_fatal_error(
                    "auth_failed",
                    "LINE channel access token rejected (401)",
                    retryable=False,
                )
                await self._close_http()
                return False
            if resp.status_code == 200:
                info = resp.json()
                self._bot_user_id = info.get("userId", "")
                logger.info(
                    "LINE: authenticated as %s (%s)",
                    info.get("displayName", "?"),
                    info.get("basicId", ""),
                )
        except Exception as e:
            # Network hiccup during validation — treat as retryable.
            logger.error("LINE: failed to reach LINE API during connect — %s", e)
            self._set_fatal_error("connect_failed", str(e), retryable=True)
            await self._close_http()
            return False

        # Fail fast if the webhook port is already taken.
        import socket as _socket

        try:
            with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as _s:
                _s.settimeout(1)
                _s.connect(("127.0.0.1", self.port))
            logger.error(
                "LINE: port %d already in use — set LINE_WEBHOOK_PORT", self.port
            )
            self._set_fatal_error(
                "port_in_use", f"LINE webhook port {self.port} already in use", retryable=False
            )
            await self._close_http()
            return False
        except (ConnectionRefusedError, OSError):
            pass  # port is free

        app = web.Application()
        app.router.add_post(self.webhook_path, self._handle_webhook)
        app.router.add_get(MEDIA_ROUTE, self._serve_media)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()

        self._mark_connected()
        public = self.public_base_url or f"http://{self.host}:{self.port}"
        logger.info(
            "LINE: listening on %s:%d — point the LINE console webhook URL at "
            "%s%s",
            self.host,
            self.port,
            public,
            self.webhook_path,
        )
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()
        if self._runner:
            try:
                await self._runner.cleanup()
            except Exception:
                logger.debug("LINE: error during webhook runner cleanup", exc_info=True)
            self._runner = None
        await self._close_http()
        # Cancel any in-flight inbound-processing tasks.
        for task in list(self._background_tasks):
            if not task.done():
                task.cancel()
        logger.info("LINE: disconnected")

    async def _close_http(self) -> None:
        if self._http is not None:
            try:
                await self._http.aclose()
            except Exception:
                logger.debug("LINE: error closing httpx client", exc_info=True)
            self._http = None

    # ── Outbound: text ────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not self._http:
            return SendResult(success=False, error="Not connected", retryable=True)

        chunks = self._chunk_text(content)
        messages = [{"type": "text", "text": c} for c in chunks if c]
        if not messages:
            return SendResult(success=True)
        return await self._deliver(chat_id, messages)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Show the LINE loading animation (1-on-1 chats only)."""
        if not self._http or not chat_id.startswith("U"):
            return  # loading API is only valid for 1-on-1 user chats
        try:
            await self._http.post(
                f"{LINE_API_BASE}/v2/bot/chat/loading/start",
                json={"chatId": chat_id, "loadingSeconds": LOADING_SECONDS},
            )
        except Exception as e:
            logger.debug("LINE: loading indicator failed: %s", e)

    # ── Outbound: media ───────────────────────────────────────────────────

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not image_url.lower().startswith("https://"):
            logger.warning("LINE: image URL must be HTTPS, got %s", image_url[:60])
            return await self.send(chat_id, caption or image_url, reply_to=reply_to)
        messages: List[dict] = [
            {"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url}
        ]
        if caption:
            messages.extend({"type": "text", "text": c} for c in self._chunk_text(caption))
        return await self._deliver(chat_id, messages)

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        url = self._host_local_file(image_path)
        if not url:
            return SendResult(
                success=False,
                error="LINE_PUBLIC_BASE_URL not set — cannot host local image for LINE",
            )
        return await self.send_image(chat_id, url, caption=caption, reply_to=reply_to)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        url = self._host_local_file(video_path)
        if not url:
            return SendResult(
                success=False,
                error="LINE_PUBLIC_BASE_URL not set — cannot host local video for LINE",
            )
        # LINE requires a preview image; we have no poster frame, so reuse the
        # video URL (LINE tolerates this and renders a generic thumbnail).
        messages: List[dict] = [
            {"type": "video", "originalContentUrl": url, "previewImageUrl": url}
        ]
        if caption:
            messages.extend({"type": "text", "text": c} for c in self._chunk_text(caption))
        return await self._deliver(chat_id, messages)

    async def send_animation(
        self,
        chat_id: str,
        animation_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        # LINE renders animated GIFs as autoplay images.
        return await self.send_image(
            chat_id, animation_url, caption=caption, reply_to=reply_to, metadata=metadata
        )

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        url = self._host_local_file(audio_path)
        if not url:
            return SendResult(
                success=False,
                error="LINE_PUBLIC_BASE_URL not set — cannot host local audio for LINE",
            )
        # LINE requires duration (ms); we cannot probe it without a codec dep,
        # so send a conservative placeholder — it only affects the UI length bar.
        messages: List[dict] = [
            {"type": "audio", "originalContentUrl": url, "duration": 60000}
        ]
        if caption:
            messages.extend({"type": "text", "text": c} for c in self._chunk_text(caption))
        return await self._deliver(chat_id, messages)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        # LINE has no native document message — send a download link as text.
        url = self._host_local_file(file_path)
        label = file_name or os.path.basename(file_path)
        if url:
            body = f"{caption}\n{url}" if caption else url
        else:
            body = caption or f"📎 {label}"
            logger.warning(
                "LINE: no LINE_PUBLIC_BASE_URL — cannot share local file %s", label
            )
        return await self.send(chat_id, body, reply_to=reply_to)

    # ── Chat info ─────────────────────────────────────────────────────────

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        if chat_id.startswith("U"):
            name = await self._lookup_display_name("user", chat_id, chat_id)
            return {"name": name or "LINE user", "type": "dm", "chat_id": chat_id}
        if chat_id.startswith("C"):
            name = await self._lookup_group_name(chat_id)
            return {"name": name or "LINE group", "type": "group", "chat_id": chat_id}
        if chat_id.startswith("R"):
            return {"name": "LINE room", "type": "group", "chat_id": chat_id}
        return {"name": chat_id, "type": "group", "chat_id": chat_id}

    # ── Webhook receiver ──────────────────────────────────────────────────

    async def _handle_webhook(self, request: "web.Request") -> "web.Response":
        # Reject oversized bodies before reading them.
        if (request.content_length or 0) > MAX_BODY_BYTES:
            return web.Response(status=413, text="payload too large")

        try:
            raw_body = await request.read()
        except Exception:
            return web.Response(status=400, text="bad request")

        # HMAC-SHA256 over the *raw* bytes, base64-encoded — verify FIRST.
        signature = request.headers.get("X-Line-Signature", "")
        if not self._verify_signature(raw_body, signature):
            logger.warning("LINE: webhook signature verification failed")
            return web.Response(status=401, text="invalid signature")

        try:
            payload = json.loads(raw_body)
            events = payload.get("events", [])
        except (json.JSONDecodeError, AttributeError):
            return web.Response(status=400, text="bad json")

        now = time.time()
        self._prune(now)
        for event in events:
            # Idempotency: LINE retries delivery for ~1h; drop duplicates.
            event_id = event.get("webhookEventId")
            if event_id:
                if event_id in self._seen_events:
                    continue
                self._seen_events[event_id] = now
            # Cache the single-use reply token before dispatching.
            self._stash_reply_token(event, now)
            # Process out-of-band so we always return 200 within LINE's window.
            task = asyncio.create_task(self._process_event(event))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

        return web.Response(status=200, text="OK")

    def _verify_signature(self, body: bytes, signature: str) -> bool:
        if not signature:
            return False
        digest = hmac.new(self.channel_secret.encode("utf-8"), body, hashlib.sha256).digest()
        expected = base64.b64encode(digest).decode("utf-8")
        return hmac.compare_digest(expected, signature)

    def _stash_reply_token(self, event: dict, now: float) -> None:
        token = event.get("replyToken")
        chat_id = self._chat_id_of(event.get("source", {}))
        if token and chat_id:
            self._reply_tokens[chat_id] = (token, now + REPLY_TOKEN_TTL)

    async def _process_event(self, event: dict) -> None:
        """Map a single LINE event to a MessageEvent and dispatch it."""
        try:
            etype = event.get("type")
            if etype == "message":
                await self._process_message(event)
            elif etype == "postback":
                await self._process_postback(event)
            elif etype in ("follow", "unfollow", "join", "leave", "memberJoined", "memberLeft"):
                logger.info("LINE: %s event (no agent dispatch)", etype)
            else:
                logger.debug("LINE: ignoring event type %s", etype)
        except Exception:
            logger.exception("LINE: error processing event")

    async def _process_message(self, event: dict) -> None:
        message = event.get("message", {})
        mtype = message.get("type")
        source = event.get("source", {})

        text = ""
        media_urls: List[str] = []
        media_types: List[str] = []
        msg_type = MessageType.TEXT

        if mtype == "text":
            text = message.get("text", "")
            msg_type = MessageType.COMMAND if text.startswith("/") else MessageType.TEXT
        elif mtype in ("image", "video", "audio", "file"):
            path = await self._download_media(message)
            if path:
                media_urls.append(path)
                media_types.append(mtype)
            msg_type = {
                "image": MessageType.PHOTO,
                "video": MessageType.VIDEO,
                "audio": MessageType.AUDIO,
                "file": MessageType.DOCUMENT,
            }[mtype]
            if mtype == "file":
                text = f"[File: {message.get('fileName', 'file')}]"
        elif mtype == "location":
            title = message.get("title") or "Location"
            address = message.get("address", "")
            lat = message.get("latitude")
            lon = message.get("longitude")
            text = f"📍 {title}\n{address}\nlat={lat} lon={lon}".strip()
            msg_type = MessageType.LOCATION
        elif mtype == "sticker":
            pkg = message.get("packageId", "?")
            sid = message.get("stickerId", "?")
            keywords = ", ".join(message.get("keywords", []) or [])
            text = f"[Sticker {pkg}/{sid}]" + (f" ({keywords})" if keywords else "")
            msg_type = MessageType.STICKER
        else:
            logger.debug("LINE: unsupported message type %s", mtype)
            return

        await self._dispatch(
            text=text,
            source=source,
            message_id=message.get("id"),
            message_type=msg_type,
            media_urls=media_urls,
            media_types=media_types,
            timestamp_ms=event.get("timestamp"),
            raw=event,
        )

    async def _process_postback(self, event: dict) -> None:
        data = (event.get("postback") or {}).get("data", "")
        await self._dispatch(
            text=data,
            source=event.get("source", {}),
            message_id=event.get("webhookEventId"),
            message_type=MessageType.TEXT,
            media_urls=[],
            media_types=[],
            timestamp_ms=event.get("timestamp"),
            raw=event,
        )

    async def _dispatch(
        self,
        *,
        text: str,
        source: dict,
        message_id: Optional[str],
        message_type: MessageType,
        media_urls: List[str],
        media_types: List[str],
        timestamp_ms: Optional[int],
        raw: Any,
    ) -> None:
        chat_id = self._chat_id_of(source)
        if not chat_id:
            logger.debug("LINE: event has no resolvable chat id, skipping")
            return

        src_type = source.get("type")  # user | group | room
        user_id = source.get("userId")
        chat_type = "dm" if src_type == "user" else "group"

        # Resolve a friendly display name (cached); never block on failure.
        user_name = "LINE user"
        if user_id:
            scope = src_type if src_type in ("group", "room") else "user"
            scope_id = chat_id if src_type in ("group", "room") else user_id
            looked = await self._lookup_display_name(scope, scope_id, user_id)
            if looked:
                user_name = looked

        chat_name = None
        if src_type == "group":
            chat_name = await self._lookup_group_name(chat_id)

        session_source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_name or chat_id,
            chat_type=chat_type,
            user_id=user_id or chat_id,
            user_name=user_name,
        )

        ts = (
            datetime.fromtimestamp(timestamp_ms / 1000)
            if isinstance(timestamp_ms, (int, float))
            else datetime.now()
        )
        msg_event = MessageEvent(
            text=text,
            message_type=message_type,
            source=session_source,
            raw_message=raw,
            message_id=str(message_id) if message_id else None,
            media_urls=media_urls,
            media_types=media_types,
            timestamp=ts,
        )
        await self.handle_message(msg_event)

    # ── Inbound media download ────────────────────────────────────────────

    async def _download_media(self, message: dict) -> Optional[str]:
        """Download an inbound media blob and cache it; return local path."""
        if not self._http:
            return None
        mtype = message.get("type")
        provider = message.get("contentProvider", {}) or {}
        try:
            if provider.get("type") == "external":
                url = provider.get("originalContentUrl")
                if not url:
                    return None
                resp = await self._http.get(url)
            else:
                msg_id = message.get("id")
                if not msg_id:
                    return None
                resp = await self._http.get(
                    f"{LINE_DATA_API_BASE}/v2/bot/message/{msg_id}/content"
                )
            resp.raise_for_status()
            data = resp.content
        except Exception as e:
            logger.warning("LINE: failed to download %s content: %s", mtype, e)
            return None

        try:
            if mtype == "image":
                return cache_image_from_bytes(data, ext=".jpg")
            if mtype == "audio":
                return cache_audio_from_bytes(data, ext=".m4a")
            if mtype == "video":
                return cache_document_from_bytes(data, "video.mp4")
            if mtype == "file":
                fname = message.get("fileName") or "file.bin"
                return cache_document_from_bytes(data, fname)
        except Exception as e:
            logger.warning("LINE: failed to cache %s content: %s", mtype, e)
        return None

    # ── Outbound delivery (reply-then-push) ───────────────────────────────

    async def _deliver(self, chat_id: str, messages: List[dict]) -> SendResult:
        """Send LINE message objects, preferring the single-use Reply API."""
        if not messages:
            return SendResult(success=True)

        reply_token = self._take_reply_token(chat_id)
        if reply_token:
            first = messages[:MAX_MESSAGES_PER_REQUEST]
            result = await self._api_send(
                "/v2/bot/message/reply",
                {"replyToken": reply_token, "messages": first},
            )
            if result.success:
                rest = messages[MAX_MESSAGES_PER_REQUEST:]
                if rest:
                    push_result = await self._push_batched(chat_id, rest)
                    if not push_result.success:
                        return push_result
                return result
            # Reply token expired/used — fall back to Push for everything.
            logger.info("LINE: reply API failed (%s), falling back to push", result.error)

        return await self._push_batched(chat_id, messages)

    async def _push_batched(self, chat_id: str, messages: List[dict]) -> SendResult:
        last = SendResult(success=True)
        for i in range(0, len(messages), MAX_MESSAGES_PER_REQUEST):
            batch = messages[i : i + MAX_MESSAGES_PER_REQUEST]
            last = await self._api_send(
                "/v2/bot/message/push", {"to": chat_id, "messages": batch}
            )
            if not last.success:
                return last
        return last

    async def _api_send(self, path: str, body: dict) -> SendResult:
        if not self._http:
            return SendResult(success=False, error="Not connected", retryable=True)
        try:
            resp = await self._http.post(f"{LINE_API_BASE}{path}", json=body)
        except Exception as e:
            # Connection-level failure — let the base retry.
            return SendResult(success=False, error=str(e), retryable=True)

        if resp.status_code == 200:
            msg_id = None
            try:
                sent = resp.json().get("sentMessages") or []
                if sent:
                    msg_id = sent[0].get("id")
            except Exception:
                pass
            return SendResult(success=True, message_id=msg_id or str(int(time.time() * 1000)))

        # 429 / 5xx are transient; 4xx (bad token, bad request) are not.
        retryable = resp.status_code == 429 or resp.status_code >= 500
        detail = resp.text[:200]
        logger.warning("LINE: %s -> %d %s", path, resp.status_code, detail)
        return SendResult(
            success=False, error=f"HTTP {resp.status_code}: {detail}", retryable=retryable
        )

    def _take_reply_token(self, chat_id: str) -> Optional[str]:
        entry = self._reply_tokens.pop(chat_id, None)
        if not entry:
            return None
        token, deadline = entry
        return token if time.time() < deadline else None

    # ── Profile / group name lookups (cached) ─────────────────────────────

    async def _lookup_display_name(
        self, scope: str, scope_id: str, user_id: str
    ) -> Optional[str]:
        key = (scope, user_id)
        cached = self._name_cache.get(key)
        if cached and time.time() - cached[1] < NAME_CACHE_TTL:
            return cached[0]
        if not self._http:
            return None
        if scope == "group":
            url = f"{LINE_API_BASE}/v2/bot/group/{scope_id}/member/{user_id}"
        elif scope == "room":
            url = f"{LINE_API_BASE}/v2/bot/room/{scope_id}/member/{user_id}"
        else:
            url = f"{LINE_API_BASE}/v2/bot/profile/{user_id}"
        try:
            resp = await self._http.get(url)
            if resp.status_code == 200:
                name = resp.json().get("displayName")
                if name:
                    self._name_cache[key] = (name, time.time())
                    return name
        except Exception as e:
            logger.debug("LINE: profile lookup failed: %s", e)
        return None

    async def _lookup_group_name(self, group_id: str) -> Optional[str]:
        key = ("group_summary", group_id)
        cached = self._name_cache.get(key)
        if cached and time.time() - cached[1] < NAME_CACHE_TTL:
            return cached[0]
        if not self._http:
            return None
        try:
            resp = await self._http.get(f"{LINE_API_BASE}/v2/bot/group/{group_id}/summary")
            if resp.status_code == 200:
                name = resp.json().get("groupName")
                if name:
                    self._name_cache[key] = (name, time.time())
                    return name
        except Exception as e:
            logger.debug("LINE: group summary lookup failed: %s", e)
        return None

    # ── Outbound local-file hosting ───────────────────────────────────────

    def _host_local_file(self, path: str) -> Optional[str]:
        """Register a local file for short-lived HTTPS hosting; return its URL.

        LINE image/video/audio messages need a public HTTPS URL — it cannot
        accept raw bytes.  We mint an unguessable token mapped to the file path
        and serve it from the same aiohttp app.  Requires LINE_PUBLIC_BASE_URL.
        """
        if not self.public_base_url or not os.path.isfile(path):
            return None
        token = secrets.token_urlsafe(16)
        self._media_tokens[token] = (os.path.abspath(path), time.time() + MEDIA_TOKEN_TTL)
        filename = os.path.basename(path) or "file"
        return f"{self.public_base_url}/line/media/{token}/{filename}"

    async def _serve_media(self, request: "web.Request") -> "web.Response":
        token = request.match_info.get("token", "")
        entry = self._media_tokens.get(token)
        if not entry:
            return web.Response(status=404, text="not found")
        path, expiry = entry
        if time.time() > expiry:
            self._media_tokens.pop(token, None)
            return web.Response(status=410, text="expired")
        if not os.path.isfile(path):
            return web.Response(status=404, text="not found")
        ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
        return web.FileResponse(path, headers={"Content-Type": ctype})

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _chat_id_of(source: dict) -> str:
        """Resolve the LINE conversation id from an event ``source`` object."""
        return (
            source.get("groupId")
            or source.get("roomId")
            or source.get("userId")
            or ""
        )

    def _chunk_text(self, content: str) -> List[str]:
        """Strip markdown LINE can't render, then split into ≤5000-char chunks."""
        content = self._strip_markdown(content or "")
        limit = min(self.max_message_length, MAX_TEXT_LEN)
        chunks: List[str] = []
        for paragraph in content.split("\n"):
            if not paragraph and not chunks:
                continue
            while len(paragraph) > limit:
                split_at = paragraph.rfind(" ", 0, limit)
                if split_at <= limit // 3:
                    split_at = limit
                chunks.append(paragraph[:split_at].rstrip())
                paragraph = paragraph[split_at:].lstrip()
            chunks.append(paragraph)
        # Re-join consecutive short lines back into single messages up to limit.
        merged: List[str] = []
        buf = ""
        for line in chunks:
            candidate = f"{buf}\n{line}" if buf else line
            if len(candidate) <= limit:
                buf = candidate
            else:
                if buf:
                    merged.append(buf)
                buf = line
        if buf:
            merged.append(buf)
        return [m for m in merged if m.strip()] or [content[:limit]]

    @staticmethod
    def _strip_markdown(text: str) -> str:
        """Convert basic markdown to plain text (LINE renders none)."""
        import re

        text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
        text = re.sub(r"__(.+?)__", r"\1", text)
        text = re.sub(r"\*(.+?)\*", r"\1", text)
        text = re.sub(r"(?<!\w)_(.+?)_(?!\w)", r"\1", text)
        text = re.sub(r"`(.+?)`", r"\1", text)   # inline code -> content
        text = re.sub(r"```\w*\n?", "", text)    # code-fence markers -> drop
        text = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", text)
        text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
        return text

    def _prune(self, now: float) -> None:
        """Drop expired idempotency, reply-token, name, and media entries."""
        self._seen_events = {
            k: t for k, t in self._seen_events.items() if now - t < IDEMPOTENCY_TTL
        }
        self._reply_tokens = {
            k: v for k, v in self._reply_tokens.items() if v[1] > now
        }
        self._media_tokens = {
            k: v for k, v in self._media_tokens.items() if v[1] > now
        }


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


def validate_config(config) -> bool:
    """Return True when token + secret are present (env or config.yaml)."""
    extra = getattr(config, "extra", {}) or {}
    token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or extra.get("channel_access_token", "")
    secret = os.getenv("LINE_CHANNEL_SECRET") or extra.get("channel_secret", "")
    return bool(token and secret)


def is_connected(config) -> bool:
    return validate_config(config)


def interactive_setup() -> None:
    """Interactive ``hermes gateway setup`` flow for the LINE platform."""
    from hermes_cli.setup import (
        prompt,
        prompt_yes_no,
        save_env_value,
        get_env_value,
        print_header,
        print_info,
        print_warning,
        print_success,
    )

    print_header("LINE")
    existing = get_env_value("LINE_CHANNEL_ACCESS_TOKEN")
    if existing:
        print_info("LINE: already configured")
        if not prompt_yes_no("Reconfigure LINE?", False):
            return

    print_info("Connect Hermes to LINE via the Messaging API.")
    print_info("   Create a Messaging API channel at https://developers.line.biz/console/")
    print_info("   Then copy the Channel access token and Channel secret below.")
    print()

    token = prompt("Channel access token", password=True)
    if not token:
        print_warning("Channel access token is required — skipping LINE setup")
        return
    save_env_value("LINE_CHANNEL_ACCESS_TOKEN", token.strip())

    secret = prompt("Channel secret", password=True)
    if not secret:
        print_warning("Channel secret is required — skipping LINE setup")
        return
    save_env_value("LINE_CHANNEL_SECRET", secret.strip())

    port = prompt(f"Webhook port (default {DEFAULT_PORT})", default=get_env_value("LINE_WEBHOOK_PORT") or "")
    if port:
        try:
            save_env_value("LINE_WEBHOOK_PORT", str(int(port)))
        except ValueError:
            print_warning(f"Invalid port — using default {DEFAULT_PORT}")

    print()
    print_info("🌐 LINE requires a public HTTPS webhook URL. Options:")
    print_info("   • Cloudflare Tunnel: cloudflared tunnel --url http://localhost:%d" % DEFAULT_PORT)
    print_info("   • ngrok:            ngrok http %d" % DEFAULT_PORT)
    print_info("   • VPS + reverse proxy terminating TLS to 127.0.0.1:%d" % DEFAULT_PORT)
    base = prompt(
        "Public base URL (e.g. https://hermes.example.com) — needed for outbound media",
        default=get_env_value("LINE_PUBLIC_BASE_URL") or "",
    )
    if base:
        save_env_value("LINE_PUBLIC_BASE_URL", base.strip().rstrip("/"))
        webhook_url = f"{base.strip().rstrip('/')}{DEFAULT_WEBHOOK_PATH}"
        print_success(f"Paste this webhook URL into the LINE console: {webhook_url}")

    print()
    print_info("🔒 Access control: restrict who can message the bot")
    allow_all = prompt_yes_no("Allow all LINE users to talk to the bot?", False)
    if allow_all:
        save_env_value("LINE_ALLOW_ALL_USERS", "true")
        save_env_value("LINE_ALLOWED_USERS", "")
        print_warning("⚠️  Open access — anyone who adds the bot can command it.")
    else:
        save_env_value("LINE_ALLOW_ALL_USERS", "false")
        allowed = prompt(
            "Allowed LINE user IDs (comma-separated U... ids, empty to deny everyone)",
            default=get_env_value("LINE_ALLOWED_USERS") or "",
        )
        save_env_value("LINE_ALLOWED_USERS", allowed.replace(" ", "") if allowed else "")

    print()
    print_success("LINE configuration saved to ~/.hermes/.env")
    print_info("Restart the gateway for changes to take effect: hermes gateway restart")


def register(ctx):
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="line",
        label="LINE",
        adapter_factory=lambda cfg: LineAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["LINE_CHANNEL_ACCESS_TOKEN", "LINE_CHANNEL_SECRET"],
        install_hint="Install the messaging extra: pip install 'hermes-agent[messaging]'",
        setup_fn=interactive_setup,
        allowed_users_env="LINE_ALLOWED_USERS",
        allow_all_env="LINE_ALLOW_ALL_USERS",
        max_message_length=MAX_TEXT_LEN,
        emoji="💚",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via LINE. LINE does not render markdown — use plain "
            "text only (bold/italic/code blocks appear as raw asterisks/backticks). "
            "Each message is limited to 5000 characters; long replies are split "
            "automatically. You cannot edit or delete messages once sent. Stickers "
            "and rich Flex Messages are not yet supported."
        ),
    )
