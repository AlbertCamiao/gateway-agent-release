"""Management Operation handlers for the R1.2 dispatcher."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from functools import partial
from pathlib import PurePosixPath
from types import SimpleNamespace

from feishu_gateway import config, host_admin
from feishu_gateway.admin.diagnostics import diagnose
from feishu_gateway.admin.formatting import format_host_add, format_host_remove
from feishu_gateway.admin.health import service_status
from feishu_gateway.bridge.apply import BridgeApplyError, apply_bridge_only, format_apply
from feishu_gateway.bridge.local import LocalDshError, ensure_local_dsh, format_connect
from feishu_gateway.bridge.status import BridgeStatusError, status as bridge_status
from feishu_gateway.completion.sync import sync_endpoint
from feishu_gateway.completion.codex_hook import CodexHookInstallerAdapter
from feishu_gateway.execution.arbiter import SessionArbiter
from feishu_gateway.execution.result_store import ResultStore
from feishu_gateway.execution.connect_adapters import (
    BridgeAdapter, CompletionAdapter, HostRegistryAdapter,
)
from feishu_gateway.execution.connect_service import HostConnectService
from feishu_gateway.home import GatewayHome
from feishu_gateway.operation_dispatch import OperationDispatcher, OperationResult
from feishu_gateway.workspaces.changes import WorkspaceChangeError, change_workspace_from_cli
from feishu_gateway.workspaces.registry import read_registry
from feishu_gateway.workspaces.replies import format_workspace_result
from feishu_gateway.discovery import scan_session_rows, scan_sessions


def _filter_runtime(rows, runtime):
    if runtime is None or runtime == "":
        return rows
    return [row for row in rows if row.get("runtime") == str(runtime)]


def _failure(error: Exception, code: str = "upstream_error") -> OperationResult:
    return OperationResult.failure(code, str(error))


def build_management_dispatcher(home: GatewayHome | None = None) -> OperationDispatcher:
    current = home or GatewayHome.discover()
    # The guard must read the same lease store TaskRuntime writes by default.
    arbiter = SessionArbiter(current.daemon_state_path)
    results = ResultStore(current.daemon_state_path)

    async def task_guard():
        unfinished = await asyncio.to_thread(results.unfinished_tasks)
        if unfinished:
            first = unfinished[0]
            return OperationResult.failure(
                "task_active",
                f"task is unresolved: {first.get('task_id', '')} "
                f"({first.get('state', '')}); resolve it explicitly first",
            )
        active = await asyncio.to_thread(arbiter.active_all)
        if active:
            return OperationResult.failure(
                "task_active", f"task is active: {active[0].get('task_id', '')}",
            )
        return None

    async def host_list(_operation):
        result = await asyncio.to_thread(
            host_admin.list_hosts,
            ssh_path=current.ssh_config_path,
            dsh_path=current.dsh_config_path,
            registry_path=current.registry_path,
        )
        return OperationResult.success(result)

    async def host_candidates(_operation):
        result = await asyncio.to_thread(
            host_admin.list_hosts,
            ssh_path=current.ssh_config_path,
            dsh_path=current.dsh_config_path,
            registry_path=current.registry_path,
        )
        configured: set[str] = set()
        for row in result.get("hosts", ()):
            configured.add(str(row.get("host", "")).casefold())
            configured.update(str(alias).casefold() for alias in row.get("aliases", ()))
        rows = await asyncio.to_thread(
            host_admin.candidate_hosts, configured=configured, timeout=5,
        )
        return OperationResult.success({"hosts": list(rows)})

    async def host_add(operation):
        blocked = await task_guard()
        if blocked is not None:
            return blocked
        args = operation.args
        try:
            result = await asyncio.to_thread(
                host_admin.add_host,
                str(args["host"]),
                aliases=tuple(args.get("aliases", ())),
                project_roots=tuple(args.get("project_roots", ())),
                workspaces=tuple(args.get("workspaces", ())),
                remote_codex=args.get("remote_codex"),
                dsh=args.get("dsh"),
                dsh_bridge=args.get("dsh_bridge"),
                dsh_profile=str(args.get("dsh_profile") or "web"),
                restart=None,
            )
        except (host_admin.HostAdminError, OSError) as error:
            return _failure(error, getattr(error, "code", "host_admin_failed"))
        message = format_host_add(result, "not_restarted")
        return OperationResult.success(result, message)

    async def host_remove(operation):
        blocked = await task_guard()
        if blocked is not None:
            return blocked
        args = operation.args
        try:
            result = await asyncio.to_thread(
                host_admin.remove_host,
                str(args["host"]),
                force=bool(args.get("force", False)),
                restart=None,
            )
        except (host_admin.HostAdminError, OSError) as error:
            return _failure(error, getattr(error, "code", "host_admin_failed"))
        message = format_host_remove(result, "not_restarted")
        return OperationResult.success(result, message)

    async def workspace_list(operation):
        registry = await asyncio.to_thread(read_registry, current.registry_path)
        requested = operation.args.get("host")
        hosts = [
            {"host": host, "workspaces": [str(path) for path in paths]}
            for host, paths in sorted(registry.items())
        ]
        if requested is not None:
            requested = str(requested)
            ssh_scopes = await asyncio.to_thread(
                config.read_ssh_scopes, current.ssh_config_path,
            )
            known = {"local", *(scope.transport.host for scope in ssh_scopes)}
            if requested not in known:
                return OperationResult.success({
                    "status": "host_not_configured", "host": requested, "hosts": [],
                })
            hosts = [row for row in hosts if row["host"] == requested]
        status = "available" if any(row["workspaces"] for row in hosts) else "empty"
        return OperationResult.success({"status": status, "host": requested, "hosts": hosts})

    async def workspace_change(operation):
        blocked = await task_guard()
        if blocked is not None:
            return blocked
        action = "scan" if operation.name == "workspace.register" else "remove"
        host = str(operation.args["host"])
        workspace = str(operation.args["workspace"])

        def run():
            return change_workspace_from_cli(
                action, host, workspace,
                load_ssh=lambda: config.read_ssh_scopes(current.ssh_config_path),
                load_dsh=lambda scopes: config.read_dsh_transports(current.dsh_config_path, scopes),
                scan=partial(scan_sessions, report=lambda *args, **kwargs: None),
            )

        try:
            result = await asyncio.to_thread(run)
        except (WorkspaceChangeError, OSError, ValueError) as error:
            return _failure(error, getattr(error, "code", "workspace_change_failed"))
        target = SimpleNamespace(host=host, workspace=PurePosixPath(workspace), action=action)
        message = format_workspace_result(target, result=result)
        data = {
            "status": result.status,
            "host": result.host,
            "workspace": str(result.workspace) if result.workspace is not None else None,
            "changed": result.changed,
            "already_registered": result.already_registered,
            "bridge_sync_pending": result.bridge_sync_pending,
        }
        return OperationResult.success(data, message)

    async def session_list(operation):
        host = operation.args.get("host")
        workspace = operation.args.get("workspace")
        if workspace is not None and host is None:
            return OperationResult.failure("invalid_args", "workspace filter requires host")
        rows, _ = await scan_session_rows(
            str(host) if host is not None else None,
            PurePosixPath(str(workspace)) if workspace is not None else None,
            registry=await asyncio.to_thread(read_registry, current.registry_path),
        )
        rows = _filter_runtime(rows, operation.args.get("runtime"))
        return OperationResult.success({"sessions": rows})

    async def session_refresh(operation):
        host = operation.args.get("host")
        if host is None:
            return OperationResult.failure("invalid_args", "session.refresh requires host")
        workspace = operation.args.get("workspace")
        rows, _ = await scan_session_rows(
            str(host), PurePosixPath(str(workspace)) if workspace is not None else None,
            registry=await asyncio.to_thread(read_registry, current.registry_path),
        )
        rows = _filter_runtime(rows, operation.args.get("runtime"))
        return OperationResult.success({"sessions": rows})

    async def session_status(operation):
        host = operation.args.get("host")
        session = operation.args.get("session")
        if host is None or session is None:
            return OperationResult.failure("invalid_args", "session.status requires host and session")
        rows, _ = await scan_session_rows(
            str(host), registry=await asyncio.to_thread(read_registry, current.registry_path),
        )
        rows = _filter_runtime(rows, operation.args.get("runtime"))
        for row in rows:
            if row["session_id"] == str(session):
                return OperationResult.success(row)
        return OperationResult.failure("target_not_found", "session was not found in the authorized catalog")

    async def dsh_status(operation):
        try:
            result = await asyncio.to_thread(
                bridge_status, str(operation.args["host"]), operation.args.get("profile"),
            )
        except BridgeStatusError as error:
            return _failure(error, "upstream_error")
        return OperationResult.success(result)

    async def dsh_connect(operation):
        blocked = await task_guard()
        if blocked is not None:
            return blocked
        args = operation.args
        try:
            remote = await asyncio.to_thread(
                apply_bridge_only, str(args["host"]), args.get("profile"),
            )
            if bool(args.get("bridge_only", False)):
                return OperationResult.success(remote, format_apply(remote))
            after = remote["after"]
            bridge_dir = str(PurePosixPath(str(after["home"])) / ".dsh" / "gateway-bridge")
            local = await asyncio.to_thread(
                ensure_local_dsh,
                after["host"], after["profile"], bridge_dir,
                workspaces=tuple(args.get("workspaces", ())),
            )
        except (BridgeStatusError, BridgeApplyError, LocalDshError, OSError) as error:
            return _failure(error, "dsh_connect_failed")
        message = format_connect(remote, local, "not_restarted")
        return OperationResult.success({"remote": remote, "local": local}, message)

    async def connect(operation):
        blocked = await task_guard()
        if blocked is not None:
            return blocked
        args = operation.args
        service = HostConnectService(
            hosts=HostRegistryAdapter(), bridge=BridgeAdapter(),
            completion=CompletionAdapter(), hooks=CodexHookInstallerAdapter(),
        )
        try:
            result = await service.connect(
                str(args["host"]),
                profile=args.get("profile"),
                include_dsh=not bool(args.get("no_dsh", False)),
                include_codex=not bool(args.get("no_codex", False)),
            )
        except Exception as error:
            import logging as _logging
            _logging.getLogger("feishu-gateway").exception("connect operation failed")
            return _failure(error, "connect_failed")
        message = f"Connected {result.host}."
        if result.manual_steps:
            message += "\n- " + "\n- ".join(result.manual_steps)
        return OperationResult.success(
            {
                "host": result.host,
                "bridge": asdict(result.bridge) if result.bridge is not None else None,
                "completion": asdict(result.completion) if result.completion is not None else None,
                "hook": asdict(result.hook) if result.hook is not None else None,
                "manual_steps": list(result.manual_steps),
            },
            message,
        )

    async def completion_sync(operation):
        blocked = await task_guard()
        if blocked is not None:
            return blocked
        host = str(operation.args.get("host") or "")
        if not host:
            return OperationResult.failure("invalid_args", "completion.sync requires host")
        try:
            result = await asyncio.to_thread(
                sync_endpoint, host, dsh=bool(operation.args.get("dsh", True)),
            )
        except (OSError, ValueError) as error:
            return OperationResult.failure("upstream_error", str(error))
        return OperationResult.success(result)

    async def doctor(operation):
        host = operation.args.get("host")
        workspace = operation.args.get("workspace")
        try:
            ssh_scopes = await asyncio.to_thread(
                config.read_ssh_scopes, current.ssh_config_path,
            )
            dsh_transports = await asyncio.to_thread(
                config.read_dsh_transports, current.dsh_config_path, ssh_scopes,
            )
            registry = await asyncio.to_thread(read_registry, current.registry_path)
            health = await service_status()
            result = await diagnose(
                ssh_scopes=ssh_scopes, dsh_transports=dsh_transports, registry=registry,
                codex_command=None, gateway_status=health,
                host=str(host) if host is not None else None,
                workspace=PurePosixPath(str(workspace)) if workspace is not None else None,
            )
        except (OSError, ValueError) as error:
            return _failure(error, "doctor_failed")
        return OperationResult.success(result)

    return OperationDispatcher({
        "host.list": host_list,
        "host.candidates": host_candidates,
        "host.add": host_add,
        "host.remove": host_remove,
        "workspace.list": workspace_list,
        "workspace.register": workspace_change,
        "workspace.remove": workspace_change,
        "session.list": session_list,
        "session.refresh": session_refresh,
        "session.status": session_status,
        "dsh.status": dsh_status,
        "dsh.plan": dsh_status,
        "dsh.connect": dsh_connect,
        "connect": connect,
        "completion.sync": completion_sync,
        "doctor": doctor,
    })
