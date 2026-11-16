"""Codex Stop hook: forward the final response to the local completion receiver."""

import json
import pathlib
import sys
import time
import urllib.request

HOOKS = pathlib.Path.home() / ".codex" / "hooks"


def main() -> None:
    try:
        port = int((HOOKS / ".feishu-port").read_text().strip())
        token = (HOOKS / ".feishu-token").read_text().strip()
        host = (HOOKS / ".feishu-host").read_text().strip()
        event = json.load(sys.stdin)
        status = str(event.get("status") or event.get("reason") or "").lower()
        state = "failed" if status in {"failed", "error", "aborted", "cancelled"} else "completed"
        payload = {
            "event_id": f"codex-stop:{event.get('session_id')}:{event.get('turn_id')}",
            "source": "codex-stop-hook",
            "host": host,
            "session_id": event.get("session_id"),
            "turn_id": event.get("turn_id"),
            "state": state,
            "seq": time.time_ns(),
            "output": event.get("last_assistant_message") or "",
        }
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/completion",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "X-Gateway-Token": token},
            method="POST",
        )
        try:
            urllib.request.urlopen(request, timeout=2).read()
        except Exception:
            pass
    finally:
        print(json.dumps({"continue": True}))


if __name__ == "__main__":
    main()
