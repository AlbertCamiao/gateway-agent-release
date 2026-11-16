"""Shared Codex preview types and read-only App Server client."""

import asyncio
import json


SKILL_NAME = "gateway-prompt-preview"
OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {"preview": {"type": "string"}},
    "required": ["preview"],
    "additionalProperties": False,
}


class PreviewError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class _AppServer:
    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self.process = process
        self.request_id = 0

    async def send(self, message: dict) -> None:
        self.process.stdin.write((json.dumps(message) + "\n").encode())
        await self.process.stdin.drain()

    async def receive(self) -> dict:
        line = await self.process.stdout.readline()
        if not line:
            raise PreviewError("server_closed", "Codex 预览连接已关闭。")
        message = json.loads(line)
        if not isinstance(message, dict):
            raise PreviewError("protocol_error", "Codex 预览连接返回了无效数据。")
        if "id" in message and "method" in message:
            raise PreviewError("interaction_required", "Codex 请求交互，预览已停止。")
        return message

    async def call(
        self,
        method: str,
        params: dict,
    ) -> dict:
        self.request_id += 1
        await self.send({"id": self.request_id, "method": method, "params": params})
        while True:
            message = await self.receive()
            if message.get("id") == self.request_id:
                error = message.get("error")
                if isinstance(error, dict):
                    code = error.get("code")
                    details = f"{method}，错误码 {code}" if type(code) is int else method
                    raise PreviewError("request_failed", f"Codex 拒绝预览请求（{details}）。")
                if not isinstance(message.get("result"), dict):
                    raise PreviewError("protocol_error", f"Codex 返回了无效数据（{method}）。")
                return message["result"]


async def _close_server(process: asyncio.subprocess.Process) -> None:
    async def discard_output() -> None:
        while await process.stdout.read(65536):
            pass

    output_drain = asyncio.create_task(discard_output())
    process.stdin.close()
    try:
        for stop in (None, process.terminate):
            if process.returncode is not None:
                break
            if stop is not None:
                try:
                    stop()
                except ProcessLookupError:
                    break
            try:
                await asyncio.wait_for(process.wait(), 2)
                break
            except TimeoutError:
                pass
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()
    finally:
        output_drain.cancel()
        await asyncio.gather(output_drain, return_exceptions=True)
