"""Command line entry point for Gateway lifecycle and configuration."""

import argparse
import getpass
import json
import os
import sys
from pathlib import Path

from feishu_gateway import bootstrap, config, ingress_config
from feishu_gateway.cli import usage


def _daemon_ingress_status():
    from feishu_gateway.daemon import DaemonClient, DaemonError, DaemonPaths

    try:
        return DaemonClient(DaemonPaths.from_home()).ingress_status()
    except DaemonError:
        return None


def _daemon_ingress_bind():
    from feishu_gateway.daemon import DaemonClient, DaemonError, DaemonPaths

    try:
        return DaemonClient(DaemonPaths.from_home()).ingress_bind()
    except DaemonError as error:
        if "not running" in str(error).lower():
            return None
        return {"ok": False, "error": {"code": str(error), "message": str(error)}}


def _ingress(args) -> int:
    home = config.HOME
    if args.ingress_action == "enable":
        app_id = args.app_id or os.environ.get("FEISHU_APP_ID")
        secret = None
        if args.app_secret_stdin:
            secret = sys.stdin.read().strip()
        elif os.environ.get("FEISHU_APP_SECRET"):
            secret = os.environ["FEISHU_APP_SECRET"]
        elif sys.stdin.isatty():
            secret = getpass.getpass("Feishu App Secret: ").strip()
        if not app_id:
            print("Feishu App ID is required.", file=sys.stderr)
            return 2
        if not secret:
            print("Feishu App Secret is required (use --app-secret-stdin).", file=sys.stderr)
            return 2
        try:
            ingress_config.write_credentials(home.env_path, app_id, secret)
            ingress_config.write_ingress(
                home.ingress_config_path, enabled=True, profile=args.profile,
            )
        except ingress_config.IngressError as error:
            print(str(error), file=sys.stderr)
            return 1
        print("Feishu ingress enabled. It will start when the daemon reloads.")
        return 0
    if args.ingress_action == "disable":
        try:
            current = ingress_config.read_ingress(home.ingress_config_path)
            ingress_config.write_ingress(
                home.ingress_config_path, enabled=False,
                profile=current["feishu"]["profile"],
            )
        except ingress_config.IngressError as error:
            print(str(error), file=sys.stderr)
            return 1
        print("Feishu ingress disabled. Binding and allowlist were preserved.")
        return 0
    if args.ingress_action == "status":
        try:
            current = ingress_config.read_ingress(home.ingress_config_path)
            app_id, secret = ingress_config.read_credentials(home.env_path)
            binding = ingress_config.read_binding(home.ingress_binding_path, app_id=app_id)
        except ingress_config.IngressError as error:
            print(str(error), file=sys.stderr)
            return 1
        daemon_status = _daemon_ingress_status()
        if daemon_status is not None:
            print(f"enabled: {str(daemon_status.get('enabled', False)).lower()}")
            print(f"connected: {str(daemon_status.get('connected', False)).lower()}")
            print(f"bound: {str(daemon_status.get('bound', False)).lower()}")
            print(f"app_id: {daemon_status.get('app_id') or 'unconfigured'}")
            print(f"credentials: {'configured' if app_id and secret else 'incomplete'}")
            print(f"chat_type: {daemon_status.get('chat_type') or '-'}")
            print(f"last_error: {daemon_status.get('last_error') or '-'}")
            return 0
        feishu = current["feishu"]
        print(f"enabled: {str(feishu['enabled']).lower()}")
        print("connected: false")
        print(f"bound: {str(binding is not None).lower()}")
        print(f"app_id: {app_id[:8] + '…' if app_id else 'unconfigured'}")
        print(f"credentials: {'configured' if app_id and secret else 'incomplete'}")
        print(f"chat_type: {binding.get('chat_type') if binding else '-'}")
        print("last_error: -")
        return 0
    if args.ingress_action == "bind":
        result = _daemon_ingress_bind()
        if result is None:
            print("Gateway daemon is not running. Run: gateway-agent start", file=sys.stderr)
            return 1
        code = result.get("code")
        if not result.get("ok") or not isinstance(code, str):
            error = result.get("error") or {}
            print(f"{error.get('code', 'ingress_unavailable')}: {error.get('message', '')}", file=sys.stderr)
            return 1
        print(f"绑定码：{code}")
        print("10 分钟内给 bot 发送：绑定 " + code)
        return 0
    return 1


def _config(args):
    home = config.HOME
    if args.config_action == "path":
        for name, path in (
            ("home", home.root), ("config", home.config_dir),
            ("data", home.data_dir), ("logs", home.log_dir),
            ("env", home.env_path), ("preview", home.config_path),
        ):
            print(f"{name}: {path}")
        return 0
    state = bootstrap.inspect(home)
    print(json.dumps(state.as_dict(), ensure_ascii=False))
    return 0 if state.ready else 1


def _init(args):
    try:
        if args.pair:
            state = bootstrap.initialize(
                config.HOME, non_interactive=args.non_interactive, pair=True,
                force=args.force, project=Path(args.project) if args.project else None,
            )
            print("Gateway preview configuration created.")
            print(f"  home: {state.home.root}")
            print(f"  env: {state.home.env_path}")
            print(f"  config: {state.home.config_path}")
            print("Next: gateway-agent start")
            return 0
        state = bootstrap.initialize_local(
            config.HOME, project=Path(args.project) if args.project else None,
            force=args.force,
        )
    except (bootstrap.BootstrapError, ingress_config.IngressError) as error:
        print(f"Configuration failed: {error}", file=sys.stderr)
        print(f"Config: {config.HOME.env_path}", file=sys.stderr)
        return 1
    print("Local Gateway configuration created (Feishu is optional).")
    print(f"  home: {state.home.root}")
    print(f"  env: {state.home.env_path}")
    print("  ingress: disabled")
    if state.sessions:
        print("Local bootstrap discovery (read-only):")
        for row in state.sessions:
            print(f"  {row['host']} / {row['runtime']} | {row['title'] or 'Untitled'} | {row['workspace']}")
    else:
        print("Local bootstrap discovery: no local sessions found.")
    print("Next: gateway-agent start")
    print("Then: gateway-agent console")
    return 0


def main():
    argv = sys.argv[1:]
    if not argv or argv[0] in {"-h", "--help"}:
        return usage.main([])
    if argv[0] in {"help", "帮助"}:
        return usage.main(argv[1:])
    parser = argparse.ArgumentParser(
        prog="gateway-agent", description="Manage the local Gateway daemon and its optional Feishu ingress.",
        epilog="Run gateway-agent help COMMAND for arguments and examples.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    def add_command(name, summary, *, aliases=(), description=None, example=None):
        command = commands.add_parser(
            name, help=summary, aliases=aliases, description=description or summary,
            epilog=f"Example: gateway-agent {example}" if example else None,
        )
        command.set_defaults(action=name)
        return command

    add_command("start", "Start the local Gateway daemon.", aliases=("启动",))
    add_command("stop", "Stop the Gateway daemon from outside the console.", aliases=("关闭",))
    add_command("status", "Show Gateway daemon status.", aliases=("状态",))
    add_command("console", "Attach the interactive Gateway console.", aliases=("控制台",))
    ingress_command = add_command(
        "ingress", "Enable, disable, bind, or inspect the optional Feishu ingress.",
        description="The daemon runs without Feishu; this command only configures the optional ingress.",
        example="ingress status",
    )
    ingress_commands = ingress_command.add_subparsers(dest="ingress_action", required=True, metavar="ACTION")
    ingress_enable = ingress_commands.add_parser("enable", help="Configure Feishu credentials and enable ingress.")
    ingress_enable.add_argument("--app-id", metavar="APP_ID", help="Feishu App ID (defaults to FEISHU_APP_ID).")
    ingress_enable.add_argument("--app-secret-stdin", action="store_true", help="Read App Secret from stdin.")
    ingress_enable.add_argument("--profile", default="web", metavar="PROFILE")
    ingress_disable = ingress_commands.add_parser("disable", help="Disable Feishu ingress; keep binding.")
    ingress_disable.add_argument("target", choices=("feishu",))
    ingress_commands.add_parser("status", help="Show ingress status without secrets.")
    ingress_bind = ingress_commands.add_parser("bind", help="Generate a one-time Feishu binding code.")
    ingress_bind.add_argument("target", choices=("feishu",))

    init = add_command(
        "init", "Create private configuration for a new installation.",
        description="Create GatewayHome directories, .env, private JSON templates, and the prompt-preview skill.",
        example="init",
    )
    init.add_argument("--non-interactive", action="store_true", help="Read required values from environment variables.")
    init.add_argument("--pair", action="store_true", help="Write one-time pairing environment instead of a preview config.")
    init.add_argument("--force", action="store_true", help="Overwrite existing JSON templates.")
    init.add_argument("--project", metavar="PATH", help="Project that receives .agents/skills/gateway-prompt-preview/SKILL.md.")
    setup = add_command(
        "setup", "Guided local setup for router, Codex, host, and workspace.",
        description="Initialize local configuration and optionally configure non-Feishu infrastructure.",
        example="setup",
    )
    setup.add_argument("--non-interactive", action="store_true")
    setup.add_argument("--provider", choices=("deepseek", "openai-compatible", "codex"))
    setup.add_argument("--api-key-stdin", action="store_true")
    setup.add_argument("--base-url")
    setup.add_argument("--model")
    setup.add_argument("--fallback", choices=("codex", "none"))
    setup.add_argument("--codex-command")
    setup.add_argument("--project", metavar="PATH")
    setup.add_argument("--host")
    setup.add_argument("--profile")
    setup.add_argument("--workspace", metavar="PATH")
    setup.add_argument("--force", action="store_true")
    config_command = add_command("config", "Show configuration paths and readiness.", example="config show")
    config_commands = config_command.add_subparsers(dest="config_action", required=True, metavar="ACTION")
    config_commands.add_parser("path", help="Print GatewayHome paths without creating them.")
    config_show = config_commands.add_parser("show", help="Print redacted readiness information.")
    config_show.add_argument("--json", action="store_true", help="Print machine-readable JSON (default).")

    args = parser.parse_args()
    try:
        if args.action == "ingress":
            return _ingress(args)
        if args.action == "init":
            return _init(args)
        if args.action == "setup":
            from feishu_gateway.cli.setup import run as setup_run
            return setup_run(args)
        if args.action == "config":
            return _config(args)
        if args.action in {"start", "stop", "status", "console"}:
            from feishu_gateway.daemon import cli as daemon_cli

            return daemon_cli([args.action])
    except config.GatewayConfigError:
        print("Gateway configuration is unavailable or invalid.", file=sys.stderr)
    except OSError as error:
        print(str(error), file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
