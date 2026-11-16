"""Feishu transport edge: lark SDK, mention parsing, reply transport."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Awaitable, Callable
from uuid import NAMESPACE_URL, uuid5

from lark_oapi import LogLevel
from lark_oapi.channel import (
    Events, FeishuChannel, FeishuChannelError, InboundConfig, InboundMessage, OutboundCard,
    PolicyConfig, SafetyConfig, SendOpts, TextBatchConfig, TextContent,
)

from feishu_gateway.config import valid_id
from feishu_gateway.feishu.replies import markdown_card

LOGGER = logging.getLogger("feishu-gateway")
LEADING_MENTION = re.compile(r"[ \t]*(@_user_[0-9]+)(?![0-9])")


class FeishuEdgeError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class FeishuInbound:
    message_id: str
    chat_id: str
    sender_id: str
    create_time: int | float
    chat_type: str
    text: str


class FeishuEdge:
    def __init__(self, channel: FeishuChannel) -> None:
        self._channel = channel
        self._handler: Callable[[FeishuInbound], Awaitable[None]] | None = None
        channel.on(Events.MESSAGE, self._on_message)
        channel.on(Events.RECONNECTING, lambda: LOGGER.info("飞书 WebSocket 正在重连。"))
        channel.on(Events.RECONNECTED, lambda: LOGGER.info("飞书 WebSocket 已重新连接。"))

    @classmethod
    def from_settings(cls, settings: dict) -> "FeishuEdge":
        app_id = settings.get("FEISHU_APP_ID")
        app_secret = settings.get("FEISHU_APP_SECRET")
        if not app_id or not app_secret:
            raise ValueError("请在项目 .env 中填写 FEISHU_APP_ID 和 FEISHU_APP_SECRET。")
        channel = FeishuChannel(
            app_id=app_id,
            app_secret=app_secret,
            transport="ws",
            log_level=LogLevel.CRITICAL,
            policy=PolicyConfig(require_mention=True, respond_to_mention_all=False),
            inbound=InboundConfig(expand_merge_forward=False, fetch_interactive_card=False),
            safety=SafetyConfig(text_batch=TextBatchConfig(delay_ms=0)),
            name_lookup=lambda _ids: {},
        )
        return cls(channel)

    def set_handler(self, handler: Callable[[FeishuInbound], Awaitable[None]]) -> None:
        self._handler = handler

    @property
    def bot_identity(self):
        return self._channel.bot_identity

    async def connect(self, *, timeout: float = 30) -> None:
        LOGGER.info("正在连接飞书 WebSocket。")
        try:
            await self._channel.connect_until_ready(timeout=timeout)
        except FeishuChannelError as error:
            raise FeishuEdgeError("Feishu WebSocket connection failed") from error
        LOGGER.info("飞书 WebSocket 已连接，正在等待消息。")
        if self._channel.bot_identity is None:
            LOGGER.warning("暂未取得机器人身份，群聊 @ 识别将在 SDK 重试成功后生效。")

    async def disconnect(self) -> None:
        await self._channel.disconnect()

    async def send(
        self, chat_id: str, message_id: str, text: str, phase: str | None = None,
        *, mention_open_id: str | None = None,
    ) -> None:
        reply_uuid = str(uuid5(NAMESPACE_URL, f"feishu-gateway:{message_id}:{phase}")) if phase else message_id
        card = OutboundCard(card=markdown_card(text, phase, mention_open_id=mention_open_id))
        opts = SendOpts(reply_to=message_id, reply_target_gone="fail", uuid=reply_uuid)
        last_error = None
        for attempt in range(2):
            try:
                result = await self._channel.send(chat_id, card, opts)
            except Exception as error:
                last_error = error
                if attempt == 0:
                    await asyncio.sleep(0.2)
                    continue
                raise
            if result.success:
                break
            last_error = RuntimeError("Feishu reply rejected")
            if attempt == 0:
                await asyncio.sleep(0.2)
                continue
            raise last_error
        if phase == "preview":
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
            LOGGER.info("飞书 Prompt Markdown 卡片已送达，正文摘要 %s。", digest)
        elif phase == "admin":
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
            LOGGER.info("飞书 Gateway 查询回执已送达，正文摘要 %s。", digest)
        else:
            LOGGER.info("飞书 Markdown 卡片已送达（%s）。", phase or "reply")

    async def send_fallback(
        self, chat_id: str, text: str, *, dedupe_key: str | None = None,
    ) -> None:
        """Send a fresh message when replying to the original is impossible."""
        identity = f"{chat_id}:{dedupe_key or text}"
        result = await self._channel.send(
            chat_id,
            OutboundCard(card=markdown_card(text, "fallback")),
            SendOpts(uuid=str(uuid5(NAMESPACE_URL, f"feishu-gateway-fallback:{identity}"))),
        )
        if not result.success:
            raise RuntimeError("Feishu fallback rejected")

    async def reply_simple(self, event: FeishuInbound, text: str) -> None:
        await self.send(event.chat_id, event.message_id, text, None)

    async def _on_message(self, message: InboundMessage) -> None:
        LOGGER.info(
            "飞书事件 chat_type=%s sender_bot=%s content_type=%s",
            getattr(message, "chat_type", None), getattr(message.sender, "is_bot", None),
            type(getattr(message, "content", None)).__name__,
        )
        try:
            text = self._original_group_text(message)
        except Exception:
            LOGGER.exception("飞书入站文本解析失败。")
            return
        if text is None:
            LOGGER.info("飞书事件被入站过滤丢弃（chat_type/mention/content）。")
            return
        event = FeishuInbound(
            message.message_id, message.chat_id, message.sender.open_id,
            message.create_time, message.chat_type, text,
        )
        if self._handler is None:
            return
        try:
            await self._handler(event)
        except Exception:
            LOGGER.exception("飞书消息处理失败，正在发送兜底回执。")
            try:
                await self.reply_simple(event, "消息处理失败，请稍后重试。")
            except Exception:
                try:
                    await self.send_fallback(event.chat_id, "消息处理失败，请稍后重试。")
                except Exception:
                    LOGGER.exception("飞书兜底回执发送失败。")

    def _original_group_text(self, message: InboundMessage) -> str | None:
        if (
            message.sender.is_bot
            or not isinstance(message.content, TextContent)
            or message.chat_type not in {"group", "p2p"}
            or not message.message_id
            or not valid_id(message.chat_id, "oc_")
            or not valid_id(message.sender.open_id, "ou_")
        ):
            return None
        identity = self._channel.bot_identity
        raw = message.content.raw.get("text")
        if not identity or not identity.open_id or not isinstance(raw, str):
            return None
        if message.chat_type == "p2p":
            return raw
        match = LEADING_MENTION.match(raw)
        if match is None or not any(
            mention.key == match[1] and mention.open_id == identity.open_id
            for mention in message.mentions
        ):
            return None
        return raw[match.end():].lstrip()
