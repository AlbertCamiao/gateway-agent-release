"""Read the gateway-agent command list and per-command usage."""

from __future__ import annotations

import difflib
import sys
from dataclasses import dataclass
from unicodedata import normalize


@dataclass(frozen=True, slots=True)
class Command:
    name: str
    summary: str
    usage: tuple[str, ...]
    details: tuple[str, ...]
    examples: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()


COMMANDS: tuple[Command, ...] = (
    Command(
        name="start",
        aliases=("启动",),
        summary="Start the local Gateway daemon.",
        usage=("gateway-agent start",),
        details=(
            "Start the detached Gateway daemon; it does not require Feishu.",
            "Use console to attach; exit only detaches and does not stop the daemon.",
            "Stop with gateway-agent stop.",
        ),
        examples=("gateway-agent start",),
    ),
    Command(
        name="stop",
        aliases=("关闭",),
        summary="Stop the Gateway daemon from outside the console.",
        usage=("gateway-agent stop",),
        details=(
            "Stop the daemon; this is the authoritative stop path.",
            "Does not delete configuration, workspace registrations, or execution records.",
        ),
        examples=("gateway-agent stop",),
    ),
    Command(
        name="status",
        aliases=("状态",),
        summary="Show Gateway daemon status.",
        usage=("gateway-agent status",),
        details=(
            "Shows whether the daemon is running.",
            "Use doctor from Console to check hosts, SSH, Codex, or DSH.",
        ),
        examples=("gateway-agent status",),
    ),
    Command(
        name="console",
        aliases=("控制台",),
        summary="Attach the interactive Gateway console.",
        usage=("gateway-agent console",),
        details=(
            "Attach to the running daemon; exit only detaches.",
            "Console is the only business front-end: natural language / ask, task prompt|status|result|resolve, session, workspace, host, dsh, connect, doctor, completion.",
            "The daemon keeps running after console exit.",
            "Textual: F1 help, Ctrl+B catalog, Ctrl+R refresh, Ctrl+L clear, Ctrl+Q detach; catalog selection only prepares a command.",
        ),
        examples=("gateway-agent console",),
    ),
    Command(
        name="init",
        summary="Create private configuration for a new installation.",
        usage=("gateway-agent init [--non-interactive] [--pair] [--force] [--project PATH]",),
        details=(
            "Creates GatewayHome directories, .env, private JSON templates, and the prompt-preview skill.",
            "Feishu is optional; the default local setup does not require Feishu credentials.",
            "Initialization never installs a background service.",
            "Use --pair to write one-time preview pairing values instead of a local setup.",
            "Set DEEPSEEK_API_KEY and GATEWAY_CODEX_COMMAND in .env when those backends are used.",
        ),
        examples=("gateway-agent init", "gateway-agent init --pair", "gateway-agent init --non-interactive"),
    ),
    Command(
        name="setup",
        summary="Guided local setup for router, Codex, host, and workspace.",
        usage=("gateway-agent setup [--non-interactive] [--provider ...]",),
        details=(
            "Initializes local configuration if needed, then configures the LLM router and Codex command.",
            "Optionally runs connect for a host and registers one workspace.",
            "Feishu remains a separate ingress enable/bind step.",
            "Manual steps: reload DSH if the bridge changed, trust the Codex Stop hook once.",
        ),
        examples=("gateway-agent setup", "gateway-agent setup --non-interactive --provider codex"),
    ),
    Command(
        name="config",
        summary="Show configuration paths and readiness.",
        usage=("gateway-agent config path", "gateway-agent config show"),
        details=(
            "path prints GatewayHome paths without creating them.",
            "show prints redacted configuration readiness and does not reveal secrets.",
        ),
        examples=("gateway-agent config path", "gateway-agent config show"),
    ),
    Command(
        name="ingress",
        summary="Enable, disable, bind, or inspect the optional Feishu ingress.",
        usage=(
            "gateway-agent ingress enable --app-id APP_ID --app-secret-stdin",
            "gateway-agent ingress disable feishu",
            "gateway-agent ingress status",
            "gateway-agent ingress bind feishu",
        ),
        details=(
            "The daemon and CLI run without Feishu; ingress is optional.",
            "App Secret is read from stdin/env and never printed.",
            "Disable keeps the binding; an App ID change invalidates it.",
        ),
        examples=("gateway-agent ingress status",),
    ),
    Command(
        name="help",
        aliases=("帮助",),
        summary="Show the command list or usage for one command.",
        usage=(
            "gateway-agent help",
            "gateway-agent help <COMMAND>",
        ),
        details=(
            "Without COMMAND, lists all commands.",
            "With COMMAND, shows meaning, usage, arguments, and examples.",
        ),
        examples=(
            "gateway-agent help",
            "gateway-agent help scan",
        ),
    ),
)


def resolve(name: str) -> Command | None:
    key = normalize("NFKC", name).strip().casefold()
    if not key:
        return None
    for command in COMMANDS:
        names = (command.name, *command.aliases)
        if any(key == normalize("NFKC", value).casefold() for value in names):
            return command
    return None


def format_list() -> str:
    lines = [
        "gateway-agent - Feishu Agent Gateway local administration",
        "",
        "Usage: gateway-agent <command> [options]",
        "",
        "Available commands:",
    ]
    for command in COMMANDS:
        aliases = f" (aliases: {', '.join(command.aliases)})" if command.aliases else ""
        lines.append(f"  {command.name}{aliases}  {command.summary}")
    lines.extend((
        "",
        "Notes:",
        "  HOST is local, a configured SSH alias (for example my-host), or an address.",
        "  WORKSPACE must be an absolute path on the target host; ~ and relative paths are not supported.",
        "  Daemon start/stop must run in a local terminal; authorized admin and task requests run through Console or Feishu.",
        "  host add/remove modify local trust configuration; the authorized Feishu chat can trigger the same operations.",
        "",
        "Show one command: gateway-agent help <command>",
        "Example: gateway-agent help scan",
    ))
    return "\n".join(lines)


def format_command(command: Command) -> str:
    lines = [f"{command.name} - {command.summary}", ""]
    if command.aliases:
        lines.extend((f"Aliases: {', '.join(command.aliases)}", ""))
    lines.append("Usage:")
    lines.extend(f"  {line}" for line in command.usage)
    lines.extend(("", "Details:"))
    lines.extend(f"  {line}" for line in command.details)
    if command.examples:
        lines.extend(("", "Examples:"))
        lines.extend(f"  {line}" for line in command.examples)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in {"-h", "--help"}:
        print(format_list())
        return 0
    if len(args) > 1:
        print("Only one command can be queried at a time.", file=sys.stderr)
        return 2
    command = resolve(args[0])
    if command is None:
        print(f"Unknown command: {args[0]}", file=sys.stderr)
        matches = difflib.get_close_matches(
            args[0], [item.name for item in COMMANDS], n=3,
        )
        if matches:
            print("Did you mean: " + ", ".join(matches), file=sys.stderr)
        print("Run gateway-agent help to list all commands.", file=sys.stderr)
        return 2
    print(format_command(command))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
