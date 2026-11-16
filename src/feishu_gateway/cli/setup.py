"""Guided non-Feishu setup built from the existing init/connect commands."""

from __future__ import annotations

import asyncio
import getpass
import os
import shutil
import sys
from functools import partial
from pathlib import Path
from types import SimpleNamespace

from feishu_gateway import bootstrap, config
from feishu_gateway.completion.codex_hook import CodexHookInstallerAdapter
from feishu_gateway.discovery import scan_sessions
from feishu_gateway.execution.connect_adapters import (
    BridgeAdapter, CompletionAdapter, HostRegistryAdapter,
)
from feishu_gateway.execution.connect_service import HostConnectService
from feishu_gateway.workspaces.changes import WorkspaceChangeError, change_workspace_from_cli
from feishu_gateway.workspaces.replies import format_workspace_result
from feishu_gateway.workspaces.registry import read_registry
from dotenv import dotenv_values


def _ask(label: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{label}{suffix}: ").strip()
    return value or default


def _settings(home) -> dict[str, str]:
    if not home.env_path.is_file():
        return {}
    return {
        str(key): str(value) for key, value in dotenv_values(home.env_path).items()
        if value is not None
    }


def _already_configured(settings: dict[str, str], codex: str | None) -> bool:
    backend = settings.get("GATEWAY_ROUTER_BACKEND", "auto")
    key = settings.get("DEEPSEEK_API_KEY", "").strip()
    return (
        (backend == "deepseek" and bool(key))
        or (backend == "codex" and bool(settings.get("GATEWAY_CODEX_COMMAND") or codex))
        or (backend == "auto" and bool(key or settings.get("GATEWAY_CODEX_COMMAND") or codex))
    )


def _provider(args, settings: dict[str, str]) -> str:
    if args.provider:
        return args.provider
    backend = settings.get("GATEWAY_ROUTER_BACKEND", "")
    if _already_configured(settings, shutil.which("codex")):
        return ""
    if backend in {"deepseek", "codex"}:
        return backend
    if args.non_interactive:
        raise SystemExit("--provider is required with --non-interactive")
    return _ask("LLM provider (deepseek/openai-compatible/codex)", "deepseek")


def _api_key(args, provider: str, settings: dict[str, str]) -> str:
    if provider == "codex":
        return ""
    key = settings.get("DEEPSEEK_API_KEY", "").strip() or os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if args.api_key_stdin:
        key = sys.stdin.read().strip()
    elif not key and not args.non_interactive:
        key = getpass.getpass("API key: ").strip()
    if not key and args.non_interactive:
        raise SystemExit("API key is required: set DEEPSEEK_API_KEY or use --api-key-stdin")
    return key


def _connect(host: str, profile: str | None) -> tuple[str, ...]:
    service = HostConnectService(
        hosts=HostRegistryAdapter(), bridge=BridgeAdapter(),
        completion=CompletionAdapter(), hooks=CodexHookInstallerAdapter(),
    )
    result = asyncio.run(service.connect(host, profile=profile))
    return tuple(result.manual_steps)


def _workspace(host: str, workspace: str) -> str:
    result = change_workspace_from_cli(
        "scan", host, workspace,
        load_ssh=lambda: config.read_ssh_scopes(config.SSH_CONFIG_PATH),
        load_dsh=lambda scopes: config.read_dsh_transports(config.DSH_CONFIG_PATH, scopes),
        scan=partial(scan_sessions, report=lambda *args, **kwargs: None),
        registry_path=config.REGISTRY_PATH,
    )
    target = SimpleNamespace(host=host, workspace=Path(workspace), action="scan")
    return format_workspace_result(target, result=result)


def run(args) -> int:
    home = config.HOME
    if not home.env_path.is_file() or args.force:
        bootstrap.initialize_local(
            home,
            project=Path(args.project) if args.project else None,
            force=args.force,
        )
    settings = _settings(home)
    codex_detected = shutil.which("codex")
    provider = _provider(args, settings)
    if provider and provider not in {"deepseek", "openai-compatible", "codex"}:
        raise SystemExit("provider must be deepseek, openai-compatible, or codex")

    values = {}
    if provider:
        key = _api_key(args, provider, settings)
        if provider == "codex":
            values["GATEWAY_ROUTER_BACKEND"] = "codex"
            values["GATEWAY_ROUTER_FALLBACK"] = "none"
        else:
            values["GATEWAY_ROUTER_BACKEND"] = "deepseek"
            values["DEEPSEEK_API_KEY"] = key
            values["DEEPSEEK_BASE_URL"] = args.base_url or settings.get(
                "DEEPSEEK_BASE_URL",
            ) or ("https://api.deepseek.com" if args.non_interactive else _ask(
                "Base URL", "https://api.deepseek.com",
            ))
            values["DEEPSEEK_MODEL"] = args.model or settings.get(
                "DEEPSEEK_MODEL",
            ) or ("deepseek-flash" if args.non_interactive else _ask(
                "Model", "deepseek-flash",
            ))
            values["DEEPSEEK_THINKING"] = (
                "off" if provider == "openai-compatible"
                else settings.get("DEEPSEEK_THINKING", "disabled")
            )
            values["GATEWAY_ROUTER_FALLBACK"] = args.fallback or settings.get(
                "GATEWAY_ROUTER_FALLBACK",
            ) or ("codex" if args.non_interactive else _ask(
                "Fallback (codex/none)", "codex",
            ))
    codex = args.codex_command or settings.get("GATEWAY_CODEX_COMMAND") or codex_detected
    if not codex and not args.non_interactive:
        codex = _ask("Codex command", "codex") or ""
    if not codex and provider == "codex":
        raise SystemExit("codex command not found; pass --codex-command PATH")
    if codex:
        values["GATEWAY_CODEX_COMMAND"] = codex
    changes = {
        key: value for key, value in values.items()
        if settings.get(key) != value
    }
    if changes:
        bootstrap.update_env(home, changes)
        print("Gateway environment configured.")
        print(f"  env: {home.env_path}")
    else:
        print("Gateway environment already configured; unchanged.")

    if args.host:
        try:
            steps = _connect(args.host, args.profile)
        except Exception as error:
            print(f"connect skipped: {type(error).__name__}: {error}")
            print(f"Retry: gateway-agent connect {args.host}")
        else:
            print(f"connect: {args.host} complete")
            for step in steps:
                print(f"  manual: {step}")
    else:
        print("connect: skipped (set --host or run gateway-agent connect HOST)")

    if args.workspace:
        host = args.host
        if not host:
            print("workspace: skipped (--workspace requires --host)")
        else:
            try:
                print(_workspace(host, args.workspace))
            except (WorkspaceChangeError, OSError, ValueError) as error:
                print(f"workspace registration failed: {error}")
    else:
        print("workspace: skipped (set --workspace or run gateway-agent console, then workspace register)")

    print("Next: gateway-agent start")
    print("Then: gateway-agent console")
    print("Manual: reload DSH if bridge changed; trust the Codex Stop hook once.")
    print("Feishu: gateway-agent ingress enable ... && gateway-agent ingress bind feishu")
    return 0
