"""Read-only observation view derived from persisted task facts.

The stored task state stays authoritative and is never rewritten here. This
module only answers one question truthfully: can the current daemon still
observe this task? Users may therefore see "observation interrupted, needs
verification" while the stored execution state remains ``running``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


TERMINAL_STATES = frozenset({"completed", "failed", "resolved"})
UNCERTAIN_STATE = "uncertain"
TERMINAL_JOURNAL_STAGES = frozenset(
    {"completed", "failed", "uncertain", "queried", "preview_only"},
)
DEFAULT_SUBMISSION_GRACE_SECONDS = 60.0


@dataclass(frozen=True, slots=True)
class Observation:
    observation: str
    verification_required: bool
    observation_reason: str
    last_observed_at: float | None
    lease_expires_at: float | None

    def as_fields(self) -> dict[str, object]:
        return {
            "observation": self.observation,
            "verification_required": self.verification_required,
            "observation_reason": self.observation_reason,
            "last_observed_at": self.last_observed_at,
            "lease_expires_at": self.lease_expires_at,
        }


def _timestamp(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def describe_observation(
    row: Mapping[str, object],
    *,
    journal_stage: str | None,
    lease_task_id: str | None,
    lease_expires_at: float | None,
    runtime_started_at: float | None,
    now: float,
    grace_seconds: float = DEFAULT_SUBMISSION_GRACE_SECONDS,
    heartbeat_at: float | None = None,
    heartbeat_stale_after: float | None = None,
    degraded_reason: str | None = None,
    observation_window_seconds: float | None = None,
) -> Observation:
    """Derive observation facts without mutating the stored task record."""
    state = str(row.get("state") or "unknown")
    updated = _timestamp(row.get("updated_at"))
    task_id = str(row.get("task_id") or "")
    if state in TERMINAL_STATES:
        return Observation("settled", False, "stored_terminal", updated, lease_expires_at)
    if journal_stage in {"completed", "failed"}:
        return Observation("inconsistent", True, "journal_terminal", updated, lease_expires_at)
    if state == UNCERTAIN_STATE:
        return Observation(
            "uncertain", True, "uncertain_unverified", updated, lease_expires_at,
        )
    if journal_stage in TERMINAL_JOURNAL_STAGES:
        return Observation("inconsistent", True, "journal_terminal", updated, lease_expires_at)
    if degraded_reason is not None:
        return Observation("interrupted", True, degraded_reason, updated, lease_expires_at)
    if runtime_started_at is not None and updated is not None and updated < runtime_started_at:
        return Observation("interrupted", True, "before_runtime_start", updated, lease_expires_at)
    if (
        lease_task_id is not None and task_id == lease_task_id
        and heartbeat_at is not None and heartbeat_stale_after is not None
        and now - heartbeat_at > heartbeat_stale_after
    ):
        return Observation("interrupted", True, "heartbeat_stale", updated, lease_expires_at)
    if (
        observation_window_seconds is not None and observation_window_seconds > 0
        and updated is not None and now - updated > observation_window_seconds
    ):
        return Observation(
            "interrupted", True, "observation_window_elapsed", updated, lease_expires_at,
        )
    if lease_task_id is not None and task_id == lease_task_id:
        return Observation("live", False, "lease_active", updated, lease_expires_at)
    if lease_task_id is not None:
        return Observation("interrupted", True, "lease_held_by_other_task", updated, lease_expires_at)
    if updated is not None and 0 <= now - updated <= grace_seconds:
        return Observation("pending", False, "within_submission_grace", updated, lease_expires_at)
    return Observation("interrupted", True, "lease_expired_or_missing", updated, lease_expires_at)
