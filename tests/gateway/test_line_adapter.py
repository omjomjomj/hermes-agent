"""Tests for the LINE platform adapter plugin."""

import base64
import hashlib
import hmac
import json
import time

import pytest
from unittest.mock import AsyncMock, MagicMock

from tests.gateway._plugin_adapter_loader import load_plugin_adapter

# Load plugins/platforms/line/adapter.py under a unique module name so it
# cannot collide with sibling plugin adapters in the same xdist worker.
_line_mod = load_plugin_adapter("line")

LineAdapter = _line_mod.LineAdapter
check_requirements = _line_mod.check_requirements
validate_config = _line_mod.validate_config
is_connected = _line_mod.is_connected
register = _line_mod.register


# ── Test helpers ──────────────────────────────────────────────────────────


def _resp(status=200, json_data=None, content=b"", text=""):
    """Build a fake httpx.Response-like object."""
    r = MagicMock()
    r.status_code = status
    r.json = MagicMock(return_value=json_data or {})
    r.content = content
    r.text = text
    r.raise_for_status = MagicMock()
    return r


def _make_adapter(monkeypatch, **extra):
    for key in (
        "LINE_CHANNEL_ACCESS_TOKEN",
        "LINE_CHANNEL_SECRET",
        "LINE_WEBHOOK_HOST",
        "LINE_WEBHOOK_PORT",
        "LINE_WEBHOOK_PATH",
        "LINE_PUBLIC_BASE_URL",
    ):
        monkeypatch.delenv(key, raising=False)
    from gateway.config import PlatformConfig

    base = {"channel_access_token": "tok", "channel_secret": "shhh"}
    base.update(extra)
    return LineAdapter(PlatformConfig(enabled=True, extra=base))


def _signed(secret: str, body: bytes) -> str:
    return base64.b64encode(
        hmac.new(secret.encode(), body, hashlib.sha256).digest()
    ).decode()


# ── Init / config ──────────────────────────────────────────────────────────


class TestLineAdapterInit:
    def test_init_from_config_extra(self, monkeypatch):
        a = _make_adapter(
            monkeypatch,
            host="127.0.0.1",
            port=9000,
            webhook_path="/hook",
            public_base_url="https://x.example.com/",
        )
        assert a.channel_access_token == "tok"
        assert a.channel_secret == "shhh"
        assert a.host == "127.0.0.1"
        assert a.port == 9000
        assert a.webhook_path == "/hook"
        # trailing slash stripped
        assert a.public_base_url == "https://x.example.com"

    def test_defaults(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        assert a.port == _line_mod.DEFAULT_PORT
        assert a.webhook_path == _line_mod.DEFAULT_WEBHOOK_PATH
        assert a.public_base_url == ""
        assert a.max_message_length == _line_mod.MAX_TEXT_LEN

    def test_env_overrides_config(self, monkeypatch):
        monkeypatch.setenv("LINE_CHANNEL_ACCESS_TOKEN", "env-tok")
        monkeypatch.setenv("LINE_WEBHOOK_PORT", "7777")
        from gateway.config import PlatformConfig

        a = LineAdapter(
            PlatformConfig(extra={"channel_access_token": "cfg", "channel_secret": "s"})
        )
        assert a.channel_access_token == "env-tok"
        assert a.port == 7777


# ── Signature verification ───────────────────────────────────────────────


class TestSignatureVerification:
    def test_valid_signature(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        body = json.dumps({"events": []}).encode()
        assert a._verify_signature(body, _signed("shhh", body)) is True

    def test_tampered_body_fails(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        body = json.dumps({"events": []}).encode()
        sig = _signed("shhh", body)
        assert a._verify_signature(body + b"x", sig) is False

    def test_wrong_secret_fails(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        body = b"{}"
        assert a._verify_signature(body, _signed("other", body)) is False

    def test_empty_signature_fails(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        assert a._verify_signature(b"{}", "") is False


# ── Text chunking ──────────────────────────────────────────────────────────


class TestChunking:
    def test_long_message_split_under_limit(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        chunks = a._chunk_text("word " * 3000)  # ~15000 chars
        assert len(chunks) >= 3
        assert all(len(c) <= _line_mod.MAX_TEXT_LEN for c in chunks)

    def test_short_message_single_chunk(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        assert a._chunk_text("hello") == ["hello"]

    def test_markdown_stripped(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        out = a._chunk_text("**bold** and `code`")[0]
        assert "**" not in out and "`" not in out
        assert "bold" in out and "code" in out

    def test_strip_link(self, monkeypatch):
        assert (
            LineAdapter._strip_markdown("[click](https://e.com)")
            == "click (https://e.com)"
        )


# ── Reply-token cache ──────────────────────────────────────────────────────


class TestReplyTokenCache:
    def test_single_use(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        a._reply_tokens["U1"] = [("TOK", time.time() + 50)]
        assert a._take_reply_token("U1") == "TOK"
        assert a._take_reply_token("U1") is None  # consumed

    def test_expired_token_dropped(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        a._reply_tokens["U1"] = [("TOK", time.time() - 1)]
        assert a._take_reply_token("U1") is None

    def test_stash_from_event(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        a._stash_reply_token(
            {"replyToken": "RT", "source": {"type": "user", "userId": "Ume"}},
            time.time(),
        )
        assert a._reply_tokens["Ume"][0][0] == "RT"

    def test_multiple_events_preserve_tokens(self, monkeypatch):
        """Two events from the same chat must each keep their own token (FIFO)."""
        a = _make_adapter(monkeypatch)
        now = time.time()
        a._stash_reply_token(
            {"replyToken": "RT1", "source": {"type": "group", "groupId": "Cg"}}, now
        )
        a._stash_reply_token(
            {"replyToken": "RT2", "source": {"type": "group", "groupId": "Cg"}}, now
        )
        assert a._take_reply_token("Cg") == "RT1"
        assert a._take_reply_token("Cg") == "RT2"
        assert a._take_reply_token("Cg") is None

    def test_stash_drops_expired_and_caps(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        now = time.time()
        # one already-expired token should not survive a new stash
        a._reply_tokens["Cg"] = [("OLD", now - 1)]
        a._stash_reply_token(
            {"replyToken": "NEW", "source": {"type": "group", "groupId": "Cg"}}, now
        )
        assert [t[0] for t in a._reply_tokens["Cg"]] == ["NEW"]


# ── chat_id resolution ──────────────────────────────────────────────────────


class TestChatIdResolution:
    def test_group(self):
        assert (
            LineAdapter._chat_id_of({"type": "group", "groupId": "Cabc", "userId": "Uu"})
            == "Cabc"
        )

    def test_room(self):
        assert (
            LineAdapter._chat_id_of({"type": "room", "roomId": "Rabc", "userId": "Uu"})
            == "Rabc"
        )

    def test_user(self):
        assert LineAdapter._chat_id_of({"type": "user", "userId": "Uu"}) == "Uu"

    def test_empty(self):
        assert LineAdapter._chat_id_of({}) == ""


# ── Inbound message mapping ─────────────────────────────────────────────────


class TestInboundMapping:
    @pytest.fixture
    def adapter_capture(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        captured = []

        async def capture(**kwargs):
            captured.append(kwargs)

        a._dispatch = capture
        a._captured = captured
        return a

    @pytest.mark.asyncio
    async def test_text(self, adapter_capture):
        from gateway.platforms.base import MessageType

        a = adapter_capture
        await a._process_message(
            {
                "message": {"type": "text", "id": "1", "text": "hello"},
                "source": {"type": "user", "userId": "Ux"},
            }
        )
        assert len(a._captured) == 1
        assert a._captured[0]["text"] == "hello"
        assert a._captured[0]["message_type"] == MessageType.TEXT

    @pytest.mark.asyncio
    async def test_command(self, adapter_capture):
        from gateway.platforms.base import MessageType

        a = adapter_capture
        await a._process_message(
            {
                "message": {"type": "text", "id": "1", "text": "/reset"},
                "source": {"type": "user", "userId": "Ux"},
            }
        )
        assert a._captured[0]["message_type"] == MessageType.COMMAND

    @pytest.mark.asyncio
    async def test_location(self, adapter_capture):
        from gateway.platforms.base import MessageType

        a = adapter_capture
        await a._process_message(
            {
                "message": {
                    "type": "location",
                    "id": "1",
                    "title": "Office",
                    "address": "1 Main St",
                    "latitude": 35.6,
                    "longitude": 139.7,
                },
                "source": {"type": "user", "userId": "Ux"},
            }
        )
        assert a._captured[0]["message_type"] == MessageType.LOCATION
        assert "Office" in a._captured[0]["text"]
        assert "35.6" in a._captured[0]["text"]

    @pytest.mark.asyncio
    async def test_sticker(self, adapter_capture):
        from gateway.platforms.base import MessageType

        a = adapter_capture
        await a._process_message(
            {
                "message": {
                    "type": "sticker",
                    "id": "1",
                    "packageId": "11537",
                    "stickerId": "52002734",
                },
                "source": {"type": "group", "groupId": "Cg", "userId": "Uu"},
            }
        )
        assert a._captured[0]["message_type"] == MessageType.STICKER
        assert "11537/52002734" in a._captured[0]["text"]

    @pytest.mark.asyncio
    async def test_image_downloads_media(self, adapter_capture, monkeypatch):
        from gateway.platforms.base import MessageType

        a = adapter_capture
        a._download_media = AsyncMock(return_value="/cache/img.jpg")
        await a._process_message(
            {
                "message": {"type": "image", "id": "1", "contentProvider": {"type": "line"}},
                "source": {"type": "user", "userId": "Ux"},
            }
        )
        assert a._captured[0]["message_type"] == MessageType.PHOTO
        assert a._captured[0]["media_urls"] == ["/cache/img.jpg"]


# ── Outbound: reply-then-push ───────────────────────────────────────────────


class TestOutboundSend:
    @pytest.mark.asyncio
    async def test_reply_used_when_token_present(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        a._http = MagicMock()
        a._http.post = AsyncMock(return_value=_resp(200, {"sentMessages": [{"id": "9"}]}))
        a._reply_tokens["Ux"] = [("RT", time.time() + 50)]

        result = await a.send("Ux", "hi")
        assert result.success is True
        assert result.message_id == "9"
        url, kwargs = a._http.post.call_args[0][0], a._http.post.call_args[1]
        assert url.endswith("/v2/bot/message/reply")
        assert kwargs["json"]["replyToken"] == "RT"

    @pytest.mark.asyncio
    async def test_push_used_without_token(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        a._http = MagicMock()
        a._http.post = AsyncMock(return_value=_resp(200, {"sentMessages": [{"id": "1"}]}))

        result = await a.send("Ux", "hi")
        assert result.success is True
        url, kwargs = a._http.post.call_args[0][0], a._http.post.call_args[1]
        assert url.endswith("/v2/bot/message/push")
        assert kwargs["json"]["to"] == "Ux"

    @pytest.mark.asyncio
    async def test_reply_failure_falls_back_to_push(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        a._http = MagicMock()
        a._http.post = AsyncMock(
            side_effect=[
                _resp(400, text="Invalid reply token"),
                _resp(200, {"sentMessages": [{"id": "2"}]}),
            ]
        )
        a._reply_tokens["Ux"] = [("RT", time.time() + 50)]

        result = await a.send("Ux", "hi")
        assert result.success is True
        assert a._http.post.call_count == 2
        assert a._http.post.call_args_list[1][0][0].endswith("/v2/bot/message/push")

    @pytest.mark.asyncio
    async def test_send_not_connected(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        result = await a.send("Ux", "hi")
        assert result.success is False
        assert result.retryable is True

    @pytest.mark.asyncio
    async def test_server_error_is_retryable(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        a._http = MagicMock()
        a._http.post = AsyncMock(return_value=_resp(503, text="unavailable"))
        result = await a.send("Ux", "hi")
        assert result.success is False
        assert result.retryable is True


# ── Inbound media download: token safety ───────────────────────────────────


class TestInboundMediaTokenSafety:
    @pytest.mark.asyncio
    async def test_blob_download_sends_line_token(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        a._http = MagicMock()
        a._http.get = AsyncMock(return_value=_resp(200, content=b"data"))
        monkeypatch.setattr(_line_mod, "cache_document_from_bytes", lambda d, n: "/c/f.bin")

        path = await a._download_media(
            {"type": "file", "id": "123", "fileName": "r.pdf",
             "contentProvider": {"type": "line"}}
        )
        assert path == "/c/f.bin"
        url = a._http.get.call_args[0][0]
        headers = a._http.get.call_args[1].get("headers") or {}
        assert "api-data.line.me" in url
        assert headers.get("Authorization") == f"Bearer {a.channel_access_token}"

    @pytest.mark.asyncio
    async def test_external_download_omits_line_token(self, monkeypatch):
        """The LINE bearer token must never be sent to a third-party media host."""
        import tools.url_safety as url_safety

        a = _make_adapter(monkeypatch)
        a._http = MagicMock()
        a._http.get = AsyncMock(return_value=_resp(200, content=b"data"))
        monkeypatch.setattr(_line_mod, "cache_image_from_bytes", lambda d, ext=".jpg": "/c/i.jpg")
        monkeypatch.setattr(url_safety, "is_safe_url", lambda u: True)

        path = await a._download_media(
            {
                "type": "image",
                "contentProvider": {
                    "type": "external",
                    "originalContentUrl": "https://cdn.evil.example.com/x.jpg",
                },
            }
        )
        assert path == "/c/i.jpg"
        headers = a._http.get.call_args[1].get("headers") or {}
        assert "Authorization" not in headers

    @pytest.mark.asyncio
    async def test_external_download_blocked_when_unsafe(self, monkeypatch):
        import tools.url_safety as url_safety

        a = _make_adapter(monkeypatch)
        a._http = MagicMock()
        a._http.get = AsyncMock(return_value=_resp(200, content=b"data"))
        monkeypatch.setattr(url_safety, "is_safe_url", lambda u: False)

        path = await a._download_media(
            {
                "type": "image",
                "contentProvider": {
                    "type": "external",
                    "originalContentUrl": "http://169.254.169.254/latest/meta-data/",
                },
            }
        )
        assert path is None
        assert not a._http.get.called


# ── send_typing ──────────────────────────────────────────────────────────


class TestSendTyping:
    @pytest.mark.asyncio
    async def test_loading_for_dm(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        a._http = MagicMock()
        a._http.post = AsyncMock(return_value=_resp(202))
        await a.send_typing("Uabc")
        assert a._http.post.called
        assert a._http.post.call_args[0][0].endswith("/v2/bot/chat/loading/start")

    @pytest.mark.asyncio
    async def test_no_loading_for_group(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        a._http = MagicMock()
        a._http.post = AsyncMock(return_value=_resp(202))
        await a.send_typing("Cgroup")
        assert not a._http.post.called


# ── chat info ──────────────────────────────────────────────────────────────


class TestChatInfo:
    @pytest.mark.asyncio
    async def test_dm_profile(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        a._http = MagicMock()
        a._http.get = AsyncMock(return_value=_resp(200, {"displayName": "Alice"}))
        info = await a.get_chat_info("Uabc")
        assert info["type"] == "dm"
        assert info["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_group(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        a._http = MagicMock()
        a._http.get = AsyncMock(return_value=_resp(200, {"groupName": "Team"}))
        info = await a.get_chat_info("Cabc")
        assert info["type"] == "group"
        assert info["name"] == "Team"

    @pytest.mark.asyncio
    async def test_room(self, monkeypatch):
        a = _make_adapter(monkeypatch)
        info = await a.get_chat_info("Rabc")
        assert info["type"] == "group"


# ── Local-file hosting ──────────────────────────────────────────────────────


class TestMediaHosting:
    def test_no_base_url_returns_none(self, monkeypatch, tmp_path):
        a = _make_adapter(monkeypatch)
        f = tmp_path / "x.jpg"
        f.write_bytes(b"data")
        assert a._host_local_file(str(f)) is None

    def test_hosts_file_with_base_url(self, monkeypatch, tmp_path):
        a = _make_adapter(monkeypatch, public_base_url="https://h.example.com")
        f = tmp_path / "x.jpg"
        f.write_bytes(b"data")
        url = a._host_local_file(str(f))
        assert url.startswith("https://h.example.com/line/media/")
        assert url.endswith("/x.jpg")
        # token resolvable
        token = url.split("/line/media/")[1].split("/")[0]
        assert token in a._media_tokens

    @pytest.mark.asyncio
    async def test_send_image_file_without_base_url(self, monkeypatch, tmp_path):
        a = _make_adapter(monkeypatch)
        f = tmp_path / "x.jpg"
        f.write_bytes(b"data")
        result = await a.send_image_file("Ux", str(f))
        assert result.success is False
        assert "PUBLIC_BASE_URL" in result.error


# ── Requirements / validation / registration ───────────────────────────────


class TestRequirements:
    def test_check_requirements(self):
        # aiohttp is installed in the dev env (messaging extra)
        assert check_requirements() is True

    def test_validate_config_from_extra(self, monkeypatch):
        monkeypatch.delenv("LINE_CHANNEL_ACCESS_TOKEN", raising=False)
        monkeypatch.delenv("LINE_CHANNEL_SECRET", raising=False)
        from gateway.config import PlatformConfig

        cfg = PlatformConfig(extra={"channel_access_token": "t", "channel_secret": "s"})
        assert validate_config(cfg) is True
        assert is_connected(cfg) is True

    def test_validate_config_missing(self, monkeypatch):
        monkeypatch.delenv("LINE_CHANNEL_ACCESS_TOKEN", raising=False)
        monkeypatch.delenv("LINE_CHANNEL_SECRET", raising=False)
        from gateway.config import PlatformConfig

        assert validate_config(PlatformConfig(extra={})) is False


class TestRegistration:
    def test_register_passes_expected_kwargs(self):
        ctx = MagicMock()
        register(ctx)
        ctx.register_platform.assert_called_once()
        kwargs = ctx.register_platform.call_args[1]
        assert kwargs["name"] == "line"
        assert kwargs["label"] == "LINE"
        assert kwargs["allowed_users_env"] == "LINE_ALLOWED_USERS"
        assert kwargs["allow_all_env"] == "LINE_ALLOW_ALL_USERS"
        assert kwargs["max_message_length"] == _line_mod.MAX_TEXT_LEN
        assert "LINE_CHANNEL_ACCESS_TOKEN" in kwargs["required_env"]
