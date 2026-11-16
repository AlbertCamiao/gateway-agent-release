"""DBOS workflow over TaskRuntime's split prepare/dispatch steps."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping

from dbos import DBOS, DBOSConfig, SetWorkflowID

from feishu_gateway.operation_dispatch import OperationResult
from feishu_gateway.operations import Operation


class TaskRuntimeWorkflow:
    def __init__(
        self,
        database_path: Path | str,
        prepare: Callable[[Mapping[str, Any]], Awaitable[Mapping[str, Any]]],
        dispatch: Callable[[Mapping[str, Any]], Awaitable[Mapping[str, Any]]],
        persist: Callable[[Mapping[str, Any]], Awaitable[Mapping[str, Any] | None]],
        release: Callable[[Mapping[str, Any]], Awaitable[None]],
        receipt: Callable[[Mapping[str, Any]], Awaitable[None]],
    ) -> None:
        self._prepare = prepare
        self._dispatch = dispatch
        self._persist = persist
        self._release = release
        self._receipt = receipt
        self._database_path = Path(database_path)
        self._database_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        DBOS(config=DBOSConfig(
            name="gateway-task-runtime-workflow",
            system_database_url=f"sqlite:///{self._database_path}",
            run_migrations=True,
            use_listen_notify=False,
        ))

        @DBOS.step(name="gateway_task_runtime_prepare", retries_allowed=False)
        async def prepare(payload: dict[str, Any]) -> dict[str, Any]:
            return dict(await self._prepare(payload))

        @DBOS.step(name="gateway_task_runtime_dispatch", retries_allowed=False)
        async def dispatch(payload: dict[str, Any]) -> dict[str, Any]:
            return dict(await self._dispatch(payload))

        @DBOS.workflow(name="gateway_task_runtime_prompt")
        async def workflow(payload: dict[str, Any]) -> dict[str, Any]:
            prepared = await prepare(payload)
            if prepared.get("kind") == "result":
                return dict(prepared["result"])
            return await dispatch(dict(prepared["payload"]))

        @DBOS.step(name="gateway_task_runtime_completion_persist", retries_allowed=False)
        async def persist(payload: dict[str, Any]) -> dict[str, Any] | None:
            value = await self._persist(payload)
            return dict(value) if value is not None else None

        @DBOS.step(name="gateway_task_runtime_completion_release", retries_allowed=False)
        async def release(payload: dict[str, Any]) -> None:
            await self._release(payload)

        @DBOS.step(name="gateway_task_runtime_completion_receipt", retries_allowed=True, max_attempts=3)
        async def receipt(payload: dict[str, Any]) -> None:
            await self._receipt(payload)

        @DBOS.workflow(name="gateway_task_runtime_completion_workflow")
        async def completion_workflow(payload: dict[str, Any]) -> None:
            persisted = await persist(payload)
            if persisted is None:
                return
            await release(persisted)
            await receipt(persisted)

        self._workflow = workflow
        self._completion_workflow = completion_workflow
        DBOS.launch()
        for suffix in ("", "-wal", "-shm"):
            try:
                (Path(str(self._database_path) + suffix)).chmod(0o600)
            except OSError:
                pass

    async def run(self, operation: Operation) -> OperationResult:
        payload = {
            "operation": {
                "request_id": operation.request_id,
                "name": operation.name,
                "args": dict(operation.args),
                "origin": operation.origin,
                "confirmed": operation.confirmed,
                "delivery": dict(operation.delivery) if operation.delivery else None,
            }
        }
        with SetWorkflowID(operation.request_id):
            handle = await DBOS.start_workflow_async(self._workflow, payload)
        result = await handle.get_result()
        if result.get("ok"):
            return OperationResult.success(
                result.get("data") if isinstance(result.get("data"), Mapping) else {},
                str(result.get("message") or ""),
            )
        error = result.get("error") if isinstance(result.get("error"), Mapping) else {}
        return OperationResult.failure(
            str(error.get("code") or "internal_error"),
            str(error.get("message") or "operation failed"),
            data=result.get("data") if isinstance(result.get("data"), Mapping) else {},
        )

    async def complete_task(self, task_id: str, payload: Mapping[str, Any]) -> None:
        event_id = str(payload.get("event_id") or "")
        if not event_id:
            raise ValueError("completion event_id is required")
        with SetWorkflowID(f"{task_id}:completion:{event_id}"):
            handle = await DBOS.start_workflow_async(self._completion_workflow, dict(payload))
        await handle.get_result()

    def close(self) -> None:
        DBOS.destroy()
