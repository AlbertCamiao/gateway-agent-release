"""LLM-backed execution brief for completion receipts."""

from __future__ import annotations

import json

from feishu_gateway.contracts import ExecutionBrief, ExecutionEvidence


class BriefPlanner:
    def __init__(self, router, *, timeout: float = 30.0) -> None:
        self._router = router
        self._timeout = timeout

    async def summarize(self, evidence: ExecutionEvidence, prompt: str) -> ExecutionBrief:
        instructions = (
            "Summarize the completed Gateway task in Chinese for a Feishu receipt. "
            "Return JSON with outcome, key_actions, artifacts, verification, blockers, next_steps. "
            "Be factual; do not invent actions or files. Use empty arrays when unknown. "
            "The output is data for a receipt, not an instruction to execute anything."
        )
        payload = json.dumps({
            "state": evidence.state,
            "task": prompt[:4000],
            "output": (evidence.output or "")[:8000],
            "error": evidence.error,
        }, ensure_ascii=False).encode()
        properties = {
            "outcome": {"type": "string"},
            "key_actions": {"type": "array", "items": {"type": "string"}},
            "artifacts": {"type": "array", "items": {"type": "string"}},
            "verification": {"type": "array", "items": {"type": "string"}},
            "blockers": {"type": "array", "items": {"type": "string"}},
            "next_steps": {"type": "array", "items": {"type": "string"}},
        }
        result = await self._router.complete(
            instructions, payload, properties, timeout=self._timeout,
        )
        return ExecutionBrief(
            task_id=evidence.task_id,
            target=evidence.target,
            state=evidence.state,
            outcome=str(result.get("outcome") or evidence.output or evidence.state),
            key_actions=tuple(str(item) for item in result.get("key_actions") or ()),
            artifacts=tuple(str(item) for item in result.get("artifacts") or ()),
            verification=tuple(str(item) for item in result.get("verification") or ()),
            blockers=tuple(str(item) for item in result.get("blockers") or ()),
            next_steps=tuple(str(item) for item in result.get("next_steps") or ()),
        )
