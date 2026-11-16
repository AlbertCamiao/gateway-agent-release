"""Console backend selection: Textual TTY UI or line fallback."""

from __future__ import annotations

import importlib.util
import sys


def textual_available() -> bool:
    return importlib.util.find_spec("textual") is not None


def choose_backend(stdin=None, stdout=None, *, textual: bool | None = None) -> str:
    stdin = sys.stdin if stdin is None else stdin
    stdout = sys.stdout if stdout is None else stdout
    if textual is None:
        textual = textual_available()
    if textual and stdin.isatty() and stdout.isatty():
        return "textual"
    return "line"
