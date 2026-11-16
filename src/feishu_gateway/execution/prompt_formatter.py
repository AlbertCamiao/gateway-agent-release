"""Prompt formatter for an already selected task target."""

from __future__ import annotations

import json
from typing import Callable

from feishu_gateway.feishu.replies import validate_prompt_markdown
from feishu_gateway.planner.model import RouterError, StructuredModel
from feishu_gateway.contracts import SessionRef, TaskDescriptor
from feishu_gateway.contracts import PortError


class PromptFormatter:
    def __init__(
        self, *, router: StructuredModel, template: Callable[[], str],
        timeout: float = 20.0,
    ) -> None:
        self._router = router
        self._template = template
        self._timeout = timeout

    async def format(self, target: SessionRef, task: TaskDescriptor) -> str:
        template = self._template()
        if not template.strip():
            raise PortError("upstream_error", "prompt template is empty")
        instructions = (
            "Format the already selected task into Markdown using the template. "
            "Do not choose or change the session. Do not execute anything. "
            "Keep the required Chinese section headings and return only the promptMarkdown field."
        )
        payload = json.dumps({
            "target": {"host": target.host, "runtime": target.runtime,
                       "session_id": target.session_id},
            "task": {"summary": task.summary, "prompt": task.prompt, "tags": list(task.tags)},
            "template": template,
        }, ensure_ascii=False).encode()
        properties = {"promptMarkdown": {"type": "string"}}
        try:
            result = await self._router.complete(
                instructions, payload, properties, timeout=self._timeout,
            )
        except RouterError as error:
            raise PortError("model_unavailable", str(error)) from error
        except Exception:
            raise PortError("model_unavailable", "prompt formatter unavailable") from None
        markdown = result.get("promptMarkdown")
        if not isinstance(markdown, str) or not markdown.strip():
            raise PortError("upstream_error", "formatter returned empty prompt")
        try:
            validate_prompt_markdown(markdown)
        except ValueError as error:
            raise PortError("invalid_args", str(error)) from None
        return markdown


def read_prompt_template() -> str:
    """Read the canonical gateway-prompt-preview skill template."""
    from importlib.resources import files

    return files("feishu_gateway").joinpath(
        "skills", "gateway-prompt-preview", "SKILL.md",
    ).read_text(encoding="utf-8")
