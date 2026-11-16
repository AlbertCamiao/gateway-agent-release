"""Direct execution submission to Codex owner IPC or the DSH bridge.

This is the transport-side submit primitive. It does not touch Journal,
ResultStore, delivery, or business state.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import AbstractAsyncContextManager, nullcontext
from typing import Any, Awaitable, Callable

from feishu_gateway.contracts import (
    CompletionSource, SessionRef, SessionSnapshot, SubmissionReceipt,
)
from feishu_gateway.executors.codex_app_server import PreviewError
from feishu_gateway.executors.codex_local import submit_formatted_execution_to_owner
from feishu_gateway.executors.dsh import DSHReadTransport


CodexLookup = Callable[[str | None, str | None], Any | None]
DshLookup = Callable[[str | None, str | None], DSHReadTransport | None]
Admission = Callable[[str, str], AbstractAsyncContextManager]
FindTurn = Callable[[Any, str, str], str | None]
SubmitOwner = Callable[..., Awaitable[str]]
Catalog = Callable[[frozenset[str], frozenset[str], str | None], Awaitable[tuple[tuple[SessionSnapshot, ...], frozenset[str], frozenset[tuple[str, str]]]]]
Observe = Callable[[SessionRef], Awaitable[SessionSnapshot]]

_SUBMISSION_TIMEOUT = 45.0
LOGGER = logging.getLogger("feishu-gateway")


def _rejected(code: str, message: str) -> SubmissionReceipt:
    return SubmissionReceipt("rejected", error_code=code, error_message=message)


def _uncertain(code: str, message: str) -> SubmissionReceipt:
    return SubmissionReceipt("uncertain", error_code=code, error_message=message)


async def submit_execution(
    target: SessionRef,
    workspace: str,
    prompt: str,
    request_id: str,
    *,
    codex_lookup: CodexLookup,
    dsh_lookup: DshLookup,
    admission: Admission | None = None,
    find_turn: FindTurn | None = None,
    submit_owner: SubmitOwner = submit_formatted_execution_to_owner,
    timeout: float = _SUBMISSION_TIMEOUT,
) -> SubmissionReceipt:
    """Submit once and report accepted/rejected/uncertain.

    The adapter owns native transport details; the caller owns Journal,
    delivery, leases, and business task state.
    """
    if not isinstance(workspace, str) or not workspace:
        return _rejected("invalid_args", "workspace is required")
    if not isinstance(request_id, str) or not request_id:
        return _rejected("invalid_args", "request_id is required")

    if target.runtime == "dsh":
        return await _submit_dsh(
            target, workspace, prompt, request_id,
            dsh_lookup=dsh_lookup, admission=admission,
        )
    return await _submit_codex(
        target, workspace, prompt, request_id,
        codex_lookup=codex_lookup, admission=admission,
        find_turn=find_turn, submit_owner=submit_owner, timeout=timeout,
    )


async def _submit_dsh(
    target: SessionRef, workspace: str, prompt: str, request_id: str,
    *, dsh_lookup: DshLookup, admission: Admission | None,
) -> SubmissionReceipt:
    transport = dsh_lookup(target.host, workspace)
    if transport is None or not transport.permits(target.session_id, workspace):
        return _rejected("dsh_scope_mismatch", "DSH session is outside the authorized scope")
    accepted = False
    try:
        async with _admission(admission, target.host, workspace):
            await transport.submit(
                target.session_id, prompt, request_id, cwd=workspace,
            )
            accepted = True
    except PreviewError as error:
        if accepted:
            return SubmissionReceipt("accepted", execution_ref=request_id)
        if error.code == "submission_uncertain":
            return _uncertain(error.code, str(error))
        return _rejected(error.code, str(error))
    except (Exception, asyncio.CancelledError):
        if accepted:
            return SubmissionReceipt("accepted", execution_ref=request_id)
        return _uncertain("submission_uncertain", "DSH submission result is unknown")
    return SubmissionReceipt("accepted", execution_ref=request_id)


async def _submit_codex(
    target: SessionRef, workspace: str, prompt: str, request_id: str,
    *, codex_lookup: CodexLookup, admission: Admission | None,
    find_turn: FindTurn | None, submit_owner: SubmitOwner, timeout: float,
) -> SubmissionReceipt:
    backend = codex_lookup(target.host, workspace)
    if backend is None:
        return _rejected("owner_unavailable", "target host execution end is unavailable")
    if not backend.allows_write(target.session_id) or (
        backend.server_factory is not None and backend.owner_socket is None
    ):
        return _rejected("owner_rejected", "target session does not accept external input")

    turn: str | None = None

    async def run() -> None:
        nonlocal turn
        if backend.ensure_owner is not None:
            await backend.ensure_owner()
        async with _admission(admission, target.host, backend.project_root):
            turn = await submit_owner(
                target.session_id,
                prompt,
                client_message_id=request_id,
                **backend.owner_options(),
            )

    try:
        async with asyncio.timeout(timeout):
            await run()
    except TimeoutError:
        LOGGER.warning("Codex owner submission timed out (turn=%s)", bool(turn))
        if turn is None and backend.transport is not None and find_turn is not None:
            try:
                turn = await asyncio.to_thread(
                    find_turn, backend.transport, target.session_id, request_id,
                )
            except Exception:
                turn = None
        if turn is None:
            return _rejected("submission_timeout", "execution submission timed out")
    except PreviewError as error:
        LOGGER.warning("Codex owner submission rejected code=%s turn=%s", error.code, bool(turn))
        if turn is not None:
            return SubmissionReceipt("accepted", execution_ref=turn)
        if error.code == "submission_uncertain":
            if backend.transport is not None and find_turn is not None:
                turn = await asyncio.to_thread(
                    find_turn, backend.transport, target.session_id, request_id,
                )
            if turn is None:
                return _uncertain(error.code, str(error))
        else:
            return _rejected(error.code, str(error))
    except (Exception, asyncio.CancelledError):
        LOGGER.exception("Codex owner submission failed (turn=%s)", bool(turn))
        if turn is None and backend.transport is not None and find_turn is not None:
            try:
                turn = await asyncio.to_thread(
                    find_turn, backend.transport, target.session_id, request_id,
                )
            except Exception:
                turn = None
        if turn is not None:
            return SubmissionReceipt("accepted", execution_ref=turn)
        return _uncertain("submission_uncertain", "Codex submission result is unknown")

    if turn is None:
        return _uncertain("submission_uncertain", "execution submission did not return a turn")
    return SubmissionReceipt("accepted", execution_ref=turn)


def _admission(
    admission: Admission | None, host: str, workspace: str,
) -> AbstractAsyncContextManager:
    if admission is None:
        return nullcontext()
    return admission(host, workspace)


class RuntimeExecutionPort:
    """ExecutionPort adapter for submit/list/status/completion_source."""

    def __init__(
        self,
        *,
        codex_lookup: CodexLookup,
        dsh_lookup: DshLookup,
        admission: Admission | None = None,
        find_turn: FindTurn | None = None,
        submit_owner: SubmitOwner = submit_formatted_execution_to_owner,
        timeout: float = _SUBMISSION_TIMEOUT,
        catalog: Catalog | None = None,
        observe: Observe | None = None,
        completion_source: CompletionSource | None = None,
    ) -> None:
        self._codex_lookup = codex_lookup
        self._dsh_lookup = dsh_lookup
        self._admission = admission
        self._find_turn = find_turn
        self._submit_owner = submit_owner
        self._timeout = timeout
        self._catalog = catalog
        self._observe = observe
        self._completion_source = completion_source

    async def submit(
        self, target: SessionRef, workspace: str, prompt: str, request_id: str,
    ) -> SubmissionReceipt:
        receipt = await submit_execution(
            target, workspace, prompt, request_id,
            codex_lookup=self._codex_lookup,
            dsh_lookup=self._dsh_lookup,
            admission=self._admission,
            find_turn=self._find_turn,
            submit_owner=self._submit_owner,
            timeout=self._timeout,
        )
        if target.runtime != "dsh":
            backend = self._codex_lookup(target.host, workspace)
            transport = getattr(backend, "transport", None)
            LOGGER.info(
                "Codex submit result state=%s ref=%s source=%s transport=%s",
                receipt.state, bool(receipt.execution_ref),
                self._completion_source is not None, transport is not None,
            )
            if (
                receipt.state == "accepted"
                and receipt.execution_ref
                and self._completion_source is not None
                and transport is not None
            ):
                self._completion_source._watch_codex(
                    target=target,
                    execution_ref=receipt.execution_ref,
                    transport=transport,
                )
        return receipt

    async def list(
        self, *, hosts: frozenset[str], runtimes: frozenset[str],
        workspace: str | None = None,
    ) -> tuple[tuple[SessionSnapshot, ...], frozenset[str], frozenset[tuple[str, str]]]:
        if self._catalog is None:
            raise RuntimeError("execution port catalog is not configured")
        return await self._catalog(hosts, runtimes, workspace)

    async def status(self, target: SessionRef) -> SessionSnapshot:
        if self._observe is None:
            raise RuntimeError("execution port observation is not configured")
        return await self._observe(target)

    def completion_source(self) -> CompletionSource:
        if self._completion_source is None:
            raise RuntimeError("execution port completion source is not configured")
        return self._completion_source
