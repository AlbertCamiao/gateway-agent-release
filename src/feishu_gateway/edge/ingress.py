"""Optional Feishu ingress component for the Gateway daemon."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from feishu_gateway import ingress_config
from feishu_gateway.application.dispatch_receipt import format_dispatch_target
from feishu_gateway.application.execution_brief import format_execution_brief
from feishu_gateway.application.presentation import render_result
from feishu_gateway.execution.delivery_store import DeliveryStore
from feishu_gateway.execution.task_runtime import format_task_prompt
from feishu_gateway.home import GatewayHome
from feishu_gateway.contracts import SessionRef

LOGGER = logging.getLogger("feishu-gateway")

if TYPE_CHECKING:
    from feishu_gateway.edge.feishu import FeishuInbound


class FeishuIngress:
    def __init__(
        self, home: GatewayHome, *, app_id: str, edge=None,
        ask: Callable[[str], Awaitable[Any]] | None = None,
    ) -> None:
        if ask is None:
            raise ValueError("ask callable is required")
        self.home = home
        self.app_id = app_id
        if edge is None:
            from feishu_gateway.edge.feishu import FeishuEdge
            edge = FeishuEdge.from_settings({
                "FEISHU_APP_ID": app_id, "FEISHU_APP_SECRET": "",
            })
        self._edge = edge
        self._ask = ask
        self._seen: list[str] = []
        self._seen_set: set[str] = set()
        self._bind_challenge_path = home.ingress_challenge_path
        self._binding_path = home.ingress_binding_path
        self._edge.set_handler(self._on_event)

    async def connect(self, *, timeout: float = 30) -> None:
        await self._edge.connect(timeout=timeout)

    async def close(self) -> None:
        await self._edge.disconnect()

    def issue_binding_code(self) -> str:
        code = ingress_config.generate_code()
        ingress_config.issue_challenge(self._bind_challenge_path, code)
        return code

    def _dedupe(self, message_id: str) -> bool:
        if message_id in self._seen_set:
            return True
        self._seen.append(message_id)
        self._seen_set.add(message_id)
        if len(self._seen) > 256:
            old = self._seen.pop(0)
            self._seen_set.discard(old)
        return False

    async def _on_event(self, event: FeishuInbound) -> None:
        if self._dedupe(event.message_id):
            return
        binding = ingress_config.read_binding(self._binding_path, app_id=self.app_id)
        if binding is None:
            await self._try_bind(event)
            return
        if event.chat_id != binding["chat_id"] or event.sender_id != binding["sender_id"]:
            return
        if event.chat_type != binding["chat_type"]:
            return
        if event.chat_type == "p2p" and binding.get("p2p_chat_id") != event.chat_id:
            return
        result = await self._ask(
            event.text, before_execute=lambda operation: self._preview(operation, event),
            request_id=event.message_id,
            delivery={
                "chat_id": event.chat_id,
                "sender_id": event.sender_id,
                "message_id": event.message_id,
            },
        )
        text = _format_operation_reply(result)
        await self._edge.send(event.chat_id, event.message_id, text)

    async def _preview(self, operation, event: FeishuInbound) -> None:
        if operation.name != "task.prompt":
            return
        args = operation.args
        host = args.get("host")
        workspace = args.get("workspace")
        session = args.get("session")
        text = args.get("text")
        if not all(isinstance(value, str) and value for value in (host, workspace, session, text)):
            return
        runtime = str(args.get("runtime") or "codex")
        target = format_dispatch_target(
            SessionRef(host, runtime, session), workspace, args.get("title"),
        )
        await self._edge.send(
            event.chat_id, event.message_id,
            target + "\n\n" + format_task_prompt(text), "preview",
        )

    async def _try_bind(self, event: FeishuInbound) -> None:
        parts = event.text.strip().split()
        if len(parts) != 2 or parts[0] != "绑定" or len(parts[1]) != 6 or not parts[1].isdigit():
            return
        if not ingress_config.consume_challenge(self._bind_challenge_path, parts[1]):
            return
        p2p_chat_id = event.chat_id if event.chat_type == "p2p" else None
        ingress_config.write_binding(
            self._binding_path, app_id=self.app_id, chat_id=event.chat_id,
            sender_id=event.sender_id, p2p_chat_id=p2p_chat_id, chat_type=event.chat_type,
        )
        await self._edge.send(event.chat_id, event.message_id, "绑定成功。")


def _format_operation_reply(result) -> str:
    as_dict = getattr(result, "as_dict", None)
    envelope = as_dict() if callable(as_dict) else result
    if not isinstance(envelope, dict):
        envelope = {
            "ok": bool(getattr(result, "ok", False)),
            "code": getattr(result, "code", None),
            "error": getattr(result, "error", None),
            "data": getattr(result, "data", {}) or {},
        }
    return render_result(envelope)


class IngressManager:
    def __init__(self, home: GatewayHome, *, ask, edge_factory=None) -> None:
        self.home = home
        self._ask = ask
        self._edge_factory = edge_factory
        self._ingress: FeishuIngress | None = None
        self._enabled = False
        self._connected = False
        self._last_error: str | None = None
        self._app_id: str | None = None
        self._signature: tuple[object, ...] | None = None
        self._delivery_store: DeliveryStore | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        await self._apply()

    async def run(self, *, interval: float = 1.0) -> None:
        await self._apply()
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
                break
            except TimeoutError:
                pass
            signature = self._config_signature()
            if signature != self._signature:
                await self._apply()
            try:
                await self._drain_egress()
            except Exception:
                LOGGER.warning("Feishu egress drain failed")

    def _config_signature(self) -> tuple[object, ...]:
        try:
            current = ingress_config.read_ingress(self.home.ingress_config_path)
            app_id, secret = ingress_config.read_credentials(self.home.env_path)
        except ingress_config.IngressError:
            return ("invalid",)
        return (
            current["feishu"]["enabled"], current["feishu"]["profile"],
            app_id, bool(secret),
        )

    async def _apply(self) -> None:
        signature = self._config_signature()
        try:
            current = ingress_config.read_ingress(self.home.ingress_config_path)
        except ingress_config.IngressError as error:
            self._last_error = str(error)
            return
        enabled = bool(current["feishu"]["enabled"])
        self._enabled = enabled
        if not enabled:
            await self._disconnect()
            self._signature = signature
            return
        app_id, secret = ingress_config.read_credentials(self.home.env_path)
        if app_id == self._app_id and self._connected and self._ingress is not None:
            return
        await self._disconnect()
        self._app_id = app_id
        if not app_id or not secret:
            self._last_error = "Feishu credentials are incomplete"
            return
        try:
            edge = self._edge_factory(app_id, secret) if self._edge_factory else None
            if edge is None:
                from feishu_gateway.edge.feishu import FeishuEdge
                edge = await asyncio.to_thread(
                    FeishuEdge.from_settings,
                    {"FEISHU_APP_ID": app_id, "FEISHU_APP_SECRET": secret},
                )
            self._ingress = FeishuIngress(self.home, app_id=app_id, edge=edge, ask=self._ask)
            await self._ingress.connect()
            self._connected = True
            self._last_error = None
            self._signature = signature
        except Exception as error:
            LOGGER.exception("Feishu ingress connect failed")
            self._last_error = type(error).__name__
            self._connected = False

    async def _disconnect(self) -> None:
        if self._ingress is not None:
            try:
                await self._ingress.close()
            except Exception:
                pass
            self._ingress = None
        self._connected = False

    async def stop(self) -> None:
        self._stop.set()
        await self._disconnect()

    def status(self) -> dict[str, Any]:
        try:
            binding = ingress_config.read_binding(self.home.ingress_binding_path, app_id=self._app_id)
        except ingress_config.IngressError:
            binding = None
        return {
            "enabled": self._enabled,
            "connected": self._connected,
            "bound": binding is not None,
            "app_id": (self._app_id[:8] + "…") if self._app_id else None,
            "chat_type": binding.get("chat_type") if binding else None,
            "last_error": self._last_error,
        }

    def bind(self) -> str | None:
        if self._ingress is None or not self._connected:
            return None
        return self._ingress.issue_binding_code()

    def _deliveries(self) -> DeliveryStore:
        if self._delivery_store is None:
            self._delivery_store = DeliveryStore(self.home.daemon_state_path)
        return self._delivery_store

    async def notify_result(self, brief, delivery=None) -> None:
        """Durably enqueue the task's origin receipt, then attempt one send."""
        if not delivery or not delivery.get("chat_id"):
            return
        chat_id = str(delivery["chat_id"])
        text = format_execution_brief(brief)
        try:
            store = await asyncio.to_thread(self._deliveries)
            await asyncio.to_thread(store.enqueue, brief.task_id, chat_id, text, now=time.time())
            row = await asyncio.to_thread(store.outbox, brief.task_id)
        except Exception:
            # Without a durable record, sending would make loss invisible.
            LOGGER.warning("Feishu completion could not be queued; not sending.")
            return
        if row is not None and str(row.get("state") or "") == "sent":
            return
        try:
            claimed = await asyncio.to_thread(store.claim, brief.task_id)
        except Exception:
            LOGGER.warning("Feishu completion could not claim the outbox row.")
            return
        if not claimed:
            return
        if self._ingress is None or not self._connected:
            try:
                await asyncio.to_thread(store.fail, brief.task_id, "ingress_unavailable")
            except Exception:
                LOGGER.warning("Feishu egress unavailable state could not be recorded")
            return
        message_id = str(delivery.get("message_id") or "")
        try:
            if message_id:
                await self._ingress._edge.send(chat_id, message_id, text, "completion")
            else:
                await self._ingress._edge.send_fallback(
                    chat_id, text, dedupe_key=brief.task_id,
                )
        except Exception:
            LOGGER.warning("Feishu completion egress failed; kept pending for retry.")
            try:
                await asyncio.to_thread(store.fail, brief.task_id, "send_failed")
            except Exception:
                LOGGER.warning("Feishu egress failure state could not be recorded")
            return
        try:
            await asyncio.to_thread(store.mark_sent, brief.task_id)
        except Exception:
            LOGGER.warning("Feishu egress success state could not be recorded")

    async def _drain_egress(self, *, now: float | None = None) -> None:
        if self._ingress is None or not self._connected:
            return
        current = time.time() if now is None else now
        try:
            store = await asyncio.to_thread(self._deliveries)
            rows = await asyncio.to_thread(store.pending, current)
        except Exception:
            LOGGER.warning("Feishu egress queue could not be read")
            return
        for row in rows:
            task_id = str(row.get("task_id") or "")
            try:
                claimed = await asyncio.to_thread(store.claim, task_id)
            except Exception:
                LOGGER.warning("Feishu egress row could not be claimed")
                continue
            if not claimed:
                continue
            try:
                await self._ingress._edge.send_fallback(
                    str(row.get("chat_id") or ""), str(row.get("text") or ""),
                    dedupe_key=task_id,
                )
            except Exception as error:
                try:
                    await asyncio.to_thread(store.fail, task_id, type(error).__name__)
                except Exception:
                    LOGGER.warning("Feishu egress retry state could not be recorded")
            else:
                try:
                    await asyncio.to_thread(store.mark_sent, task_id)
                except Exception:
                    LOGGER.warning("Feishu egress success state could not be recorded")
