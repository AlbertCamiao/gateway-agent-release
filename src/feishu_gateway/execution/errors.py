"""Typed execution errors shared by local adapters and handlers."""

from __future__ import annotations

from enum import StrEnum
from typing import Mapping


class ErrorCode(StrEnum):
    TRANSPORT_UNAVAILABLE = "transport_unavailable"
    CATALOG_UNAVAILABLE = "catalog_unavailable"
    SESSION_UNAVAILABLE = "session_unavailable"
    OWNER_UNAVAILABLE = "owner_unavailable"
    SUBMISSION_REJECTED = "submission_rejected"
    SUBMISSION_UNCERTAIN = "submission_uncertain"
    COMPLETION_UNKNOWN = "completion_unknown"
    RECEIPT_FAILED = "receipt_failed"
    INVALID_PROTOCOL = "invalid_protocol"
    VERSION_UNSUPPORTED = "version_unsupported"
    INTERNAL_ERROR = "internal_error"


class GatewayError(Exception):
    """A failure that already has a user-facing code and safe message."""

    def __init__(
        self,
        code: ErrorCode | str,
        message: str,
        *,
        data: Mapping[str, object] | None = None,
    ) -> None:
        self.code = code.value if isinstance(code, ErrorCode) else str(code)
        self.message = str(message)
        self.data = dict(data or {})
        super().__init__(self.message)

    def as_failure(self):
        from feishu_gateway.operation_dispatch import OperationResult

        return OperationResult.failure(self.code, self.message, data=self.data)
