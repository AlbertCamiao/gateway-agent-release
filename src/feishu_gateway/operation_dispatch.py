"""Operation dispatch boundary for the R1.2 daemon/CLI migration."""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Mapping

from feishu_gateway.operations import Operation
from feishu_gateway.execution.errors import GatewayError

LOGGER = logging.getLogger("feishu-gateway")


@dataclass(frozen=True, slots=True)
class OperationResult:
    ok: bool
    data: Mapping[str, object] = field(default_factory=dict)
    message: str = ""
    code: str = ""
    error: str = ""

    @classmethod
    def success(cls, data: Mapping[str, object] | None = None, message: str = "") -> "OperationResult":
        return cls(True, dict(data or {}), message)

    @classmethod
    def failure(
        cls, code: str, message: str, *, data: Mapping[str, object] | None = None,
    ) -> "OperationResult":
        return cls(False, dict(data or {}), "", code, message)

    def as_dict(self) -> dict[str, object]:
        if self.ok:
            return {"ok": True, "data": dict(self.data), "message": self.message}
        result: dict[str, object] = {
            "ok": False,
            "error": {"code": self.code or "operation_failed", "message": self.error or "operation failed"},
        }
        if self.data:
            result["data"] = dict(self.data)
        return result


OperationHandler = Callable[[Operation], Awaitable[OperationResult | Mapping[str, object] | None]]


class OperationDispatcher:
    """Validates the operation envelope and routes it to one handler."""

    def __init__(self, handlers: Mapping[str, OperationHandler]) -> None:
        if not isinstance(handlers, Mapping):
            raise TypeError("handlers must be a mapping")
        self._handlers = dict(handlers)

    @property
    def operations(self) -> frozenset[str]:
        return frozenset(self._handlers)

    @property
    def handlers(self) -> dict[str, "OperationHandler"]:
        return dict(self._handlers)

    async def dispatch(self, operation: Operation) -> OperationResult:
        if not isinstance(operation, Operation):
            return OperationResult.failure("invalid_operation", "operation envelope is required")
        handler = self._handlers.get(operation.name)
        if handler is None:
            return OperationResult.failure(
                "operation_unavailable", f"operation is not available: {operation.name}",
            )
        if operation.mutating and not operation.confirmed:
            return OperationResult.failure(
                "confirmation_required", "mutating operation requires confirmation",
            )
        try:
            result = handler(operation)
            if inspect.isawaitable(result):
                result = await result
        except GatewayError as error:
            LOGGER.warning("Operation handler failed: %s (%s)", operation.name, error.code)
            return error.as_failure()
        except Exception:
            LOGGER.exception("Operation handler failed unexpectedly: %s", operation.name)
            return OperationResult.failure("internal_error", "operation failed")
        if result is None:
            return OperationResult.success()
        if isinstance(result, OperationResult):
            return result
        if isinstance(result, Mapping):
            return OperationResult.success(result)
        return OperationResult.failure("invalid_handler_result", "operation returned invalid data")

    async def dispatch_payload(self, payload: Mapping[str, object]) -> OperationResult:
        if not isinstance(payload, Mapping):
            return OperationResult.failure("invalid_operation", "operation payload must be an object")
        try:
            operation = Operation(
                request_id=str(payload.get("request_id") or ""),
                name=str(payload.get("name") or ""),
                args=payload.get("args") or {},
                origin=str(payload.get("origin") or "cli"),  # type: ignore[arg-type]
                confirmed=payload.get("confirmed") is True,
                delivery=(
                    dict(payload["delivery"])
                    if isinstance(payload.get("delivery"), Mapping) else None
                ),
            )
        except (TypeError, ValueError) as error:
            return OperationResult.failure("invalid_operation", str(error))
        return await self.dispatch(operation)
