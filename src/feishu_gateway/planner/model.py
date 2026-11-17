"""Call a structured-output model through a replaceable provider.

The Gateway only needs strict JSON from this layer: intent, destination and an
optional Markdown preview.  DeepSeek's Chat Completions API is the default
because it is independent from the Codex account that the target sessions use.
The Codex CLI remains available as a fallback for installations without an API
key, but it is no longer on the only path.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from collections.abc import Mapping
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from typing import Any, Protocol

import httpx


LOGGER = logging.getLogger("feishu-gateway")

CODEX_MODEL = "gpt-6.1-sol"
_CLI_CONFIG = (
    'approval_policy="never"',
    'web_search="disabled"',
    "mcp_servers={}",
    "project_doc_max_bytes=0",
    "skills.bundled.enabled=false",
    "skills.include_instructions=false",
    "analytics.enabled=false",
    "agents.enabled=false",
    'model_reasoning_effort="medium"',
)
_DISABLED_FEATURES = (
    "apps", "auth_elicitation", "browser_use", "code_mode", "code_mode_host", "computer_use",
    "default_mode_request_user_input", "goals", "hooks", "image_generation", "memories",
    "multi_agent", "plugins", "shell_tool", "sleep_tool", "unified_exec", "view_image",
)


class RouterError(RuntimeError):
    """A single backend attempt failed. ``retryable`` allows one quick retry."""

    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class RouterUnavailable(RouterError):
    def __init__(self, message: str = "结构化路由模型暂不可用。") -> None:
        super().__init__("router_unavailable", message)


class RouterBackend(Protocol):
    name: str

    async def complete(
        self, instructions: str, payload: bytes, properties: dict, *, timeout: float,
    ) -> dict[str, Any]: ...

    async def aclose(self) -> None: ...


def _schema(properties: dict) -> dict:
    return {
        "type": "object", "properties": properties, "required": list(properties),
        "additionalProperties": False,
    }


def _validate(result: object, properties: dict) -> dict:
    if not isinstance(result, dict) or set(result) != set(properties):
        raise RouterError("router_protocol", "路由模型返回的字段与约定不一致。")
    return result


def _parse_json_object(content: str) -> dict:
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    try:
        result = json.loads(text)
    except ValueError:
        raise RouterError("router_protocol", "路由模型未返回有效 JSON。") from None
    if not isinstance(result, dict):
        raise RouterError("router_protocol", "路由模型返回的不是 JSON 对象。")
    return result


def _codex_env() -> dict[str, str]:
    # The CLI uses its normal local login.  Gateway secrets are not forwarded.
    return {key: os.environ[key] for key in ("HOME", "PATH", "TMPDIR", "LANG", "LC_ALL")
            if key in os.environ}


async def _run_cli(
    command: list[str], *, cwd: str, env: dict[str, str], payload: bytes | None, timeout: float,
) -> bytes:
    process = await asyncio.create_subprocess_exec(
        *command, cwd=cwd, env=env, start_new_session=True,
        stdin=asyncio.subprocess.PIPE if payload is not None else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    communication = asyncio.create_task(process.communicate(payload))
    try:
        output, _ = await asyncio.wait_for(asyncio.shield(communication), timeout)
    finally:
        if not communication.done():
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            await communication
    if process.returncode != 0:
        raise RouterError("router_codex_failed", "Codex 路由进程执行失败。", retryable=True)
    return output


def _events_result(events: bytes) -> dict:
    final = None
    completed = False
    for line in events.splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
            event_type = event["type"]
        except (ValueError, TypeError, KeyError):
            raise RouterError("router_protocol", "Codex 路由输出格式异常。") from None
        if event_type.startswith("item."):
            item = event["item"]
            if item["type"] not in {"agent_message", "reasoning"}:
                raise RouterError("router_protocol", "Codex 路由返回了非文本内容。")
            if event_type == "item.completed" and item["type"] == "agent_message":
                final = item["text"]
        elif event_type == "turn.completed":
            completed = True
        elif event_type not in {"thread.started", "turn.started"}:
            raise RouterError("router_protocol", "Codex 路由返回了未知事件。")
    if not completed or not isinstance(final, str):
        raise RouterError("router_protocol", "Codex 路由未完成。")
    return _parse_json_object(final)


class DeepSeekBackend:
    name = "deepseek"

    def __init__(
        self, api_key: str, model: str, base_url: str, *,
        max_tokens: int = 4096, client: httpx.AsyncClient | None = None,
        thinking: str = "disabled",
    ) -> None:
        if not api_key.strip():
            raise ValueError("DeepSeek API key is required.")
        self._api_key = api_key.strip()
        self._model = model
        self._max_tokens = max_tokens
        self._thinking = thinking
        self._owns_client = client is None
        self._base_url = base_url.rstrip("/") + "/"
        self._client = client
        self._client_loop = asyncio.get_running_loop() if client is not None else None

    def _client_for_loop(self) -> httpx.AsyncClient:
        loop = asyncio.get_running_loop()
        if self._client is not None and self._client_loop is loop:
            return self._client
        if self._client is not None and self._owns_client:
            try:
                self._client.close()
            except Exception:
                pass
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=None,
            follow_redirects=False,
            limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
        )
        self._client_loop = loop
        return self._client

    async def complete(
        self, instructions: str, payload: bytes, properties: dict, *, timeout: float,
    ) -> dict[str, Any]:
        body = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": self._system_prompt(instructions, properties)},
                {"role": "user", "content": payload.decode("utf-8")},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0,
            "max_tokens": self._max_tokens,
            "stream": False,
        }
        if self._thinking in {"enabled", "disabled"}:
            body["thinking"] = {"type": self._thinking}
        try:
            response = await self._client_for_loop().post(
                "chat/completions",
                json=body,
                headers={"Authorization": f"Bearer {self._api_key}", "Accept": "application/json"},
                timeout=httpx.Timeout(
                    connect=min(3.0, timeout), read=timeout, write=min(10.0, timeout),
                    pool=min(2.0, timeout),
                ),
            )
        except httpx.TimeoutException:
            raise RouterError("router_timeout", "路由模型请求超时。", retryable=True) from None
        except httpx.HTTPError as error:
            raise RouterError(
                "router_network", f"路由模型网络错误（{type(error).__name__}）。", retryable=True,
            ) from None

        if response.status_code == 401 or response.status_code == 403:
            raise RouterError("router_auth", "路由模型鉴权失败。", retryable=False)
        if response.status_code == 429:
            raise RouterError("router_rate_limited", "路由模型限流。", retryable=True)
        if response.status_code >= 500:
            raise RouterError("router_server", "路由模型服务端错误。", retryable=True)
        if response.status_code >= 400:
            detail = _error_detail(response)
            raise RouterError("router_rejected", f"路由模型拒绝了请求{detail}。", retryable=False)

        try:
            data = response.json()
            content = data["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError):
            raise RouterError("router_protocol", "路由模型响应结构异常。") from None
        if not isinstance(content, str) or not content.strip():
            raise RouterError("router_protocol", "路由模型返回了空内容。")
        usage = data.get("usage")
        if isinstance(usage, dict):
            prompt_details = usage.get("prompt_tokens_details") or {}
            completion_details = usage.get("completion_tokens_details") or {}
            LOGGER.info(
                "DeepSeek %s tokens: prompt=%s completion=%s cached=%s reasoning=%s",
                self._model, usage.get("prompt_tokens"), usage.get("completion_tokens"),
                prompt_details.get("cached_tokens"), completion_details.get("reasoning_tokens"),
            )
        return _parse_json_object(content)

    @staticmethod
    def _system_prompt(instructions: str, properties: dict) -> str:
        keys = ", ".join(properties)
        return (
            f"{instructions}\n\n"
            "Return exactly one JSON object and nothing else. "
            f"The object must contain exactly these keys: {keys}. "
            "Do not wrap it in a code block, do not add commentary, and do not omit or rename keys.\n"
            f"JSON schema: {json.dumps(_schema(properties), ensure_ascii=False)}"
        )

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None


def _error_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
        message = payload.get("error", {}).get("message")
    except (ValueError, AttributeError, TypeError):
        return ""
    if isinstance(message, str) and message.strip():
        return f"（{message.strip()[:200]}）"
    return ""


class CodexBackend:
    name = "codex"

    def __init__(self, command: str, model: str = CODEX_MODEL) -> None:
        self._command = command
        self._model = model
        self._profile: dict | None = None

    async def complete(
        self, instructions: str, payload: bytes, properties: dict, *, timeout: float,
    ) -> dict[str, Any]:
        profile = await self._load_profile(min(5.0, timeout))
        schema = _schema(properties)
        with TemporaryDirectory(prefix="feishu-router-") as workspace:
            catalog_path = os.path.join(workspace, "model-catalog.json")
            schema_path = os.path.join(workspace, "result.schema.json")
            with open(catalog_path, "w", encoding="utf-8") as handle:
                json.dump({"models": [profile]}, handle)
            with open(schema_path, "w", encoding="utf-8") as handle:
                json.dump(schema, handle)
            command = [
                self._command, "exec", "--ephemeral", "--ignore-user-config", "--ignore-rules",
                "--skip-git-repo-check", "--sandbox", "read-only", "--model", self._model,
                "--output-schema", schema_path, "--json", "--color", "never",
                "--config", f"model_catalog_json={json.dumps(catalog_path)}",
                "--config", f"developer_instructions={json.dumps(instructions, ensure_ascii=False)}",
            ]
            for config in _CLI_CONFIG:
                command.extend(("--config", config))
            for feature in _DISABLED_FEATURES:
                command.extend(("--disable", feature))
            events = await _run_cli(
                command + ["-"], cwd=workspace, env=_codex_env(), payload=payload, timeout=timeout,
            )
        return _events_result(events)

    async def _load_profile(self, timeout: float) -> dict:
        if self._profile is not None:
            return dict(self._profile)
        with TemporaryDirectory(prefix="feishu-router-models-") as workspace:
            bundled = await _run_cli(
                [self._command, "debug", "models", "--bundled"],
                cwd=workspace, env=_codex_env(), payload=None, timeout=timeout,
            )
        try:
            profile = next(
                (entry for entry in json.loads(bundled)["models"] if entry["slug"] == self._model), None,
            )
        except (ValueError, KeyError, TypeError):
            raise RouterError("router_protocol", "Codex 模型目录格式异常。") from None
        if profile is None:
            raise RouterError("router_model_missing", "Codex 未提供配置的路由模型。", retryable=False)
        profile = dict(profile)
        profile.pop("apply_patch_tool_type", None)
        profile.pop("tool_mode", None)
        profile["experimental_supported_tools"] = []
        self._profile = profile
        return dict(profile)

    async def aclose(self) -> None:
        return None


class _Breaker:
    def __init__(self, threshold: int, cooldown: float) -> None:
        self._threshold = max(1, threshold)
        self._cooldown = max(1.0, cooldown)
        self._failures = 0
        self._opened_at: float | None = None

    def allows(self) -> bool:
        if self._opened_at is None:
            return True
        if time.monotonic() - self._opened_at >= self._cooldown:
            self._opened_at = None
            self._failures = 0
            return True
        return False

    def success(self) -> None:
        self._failures = 0
        self._opened_at = None

    def failure(self) -> None:
        self._failures += 1
        if self._failures >= self._threshold:
            self._opened_at = time.monotonic()


@dataclass(frozen=True, slots=True)
class RouterSettings:
    backend: str = "auto"
    fallback: str | None = None
    deepseek_api_key: str | None = None
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_model: str = "deepseek-flash"
    deepseek_thinking: str = "disabled"
    codex_model: str = CODEX_MODEL
    timeout_seconds: float = 20.0
    fallback_timeout_seconds: float = 12.0
    max_concurrency: int = 4
    breaker_threshold: int = 3
    breaker_cooldown_seconds: float = 60.0
    max_tokens: int = 4096


def _text(values: Mapping[str, object], name: str, default: str = "") -> str:
    value = values.get(name, default)
    return str(value).strip() if value is not None else default


def _positive_float(values: Mapping[str, object], name: str, default: float) -> float:
    try:
        value = float(values.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _positive_int(values: Mapping[str, object], name: str, default: int) -> int:
    try:
        value = int(values.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


class StructuredModel:
    """One retrying, breaker-protected structured-output call."""

    def __init__(
        self, settings: RouterSettings, *, codex_command: str | None = None,
        backends: Mapping[str, RouterBackend] | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.settings = settings
        self._semaphore = asyncio.Semaphore(max(1, settings.max_concurrency))
        self._backends = dict(backends or {})
        if not self._backends:
            if settings.backend == "deepseek" and settings.deepseek_api_key:
                self._backends["deepseek"] = DeepSeekBackend(
                    settings.deepseek_api_key, settings.deepseek_model, settings.deepseek_base_url,
                    max_tokens=settings.max_tokens, client=client,
                    thinking=settings.deepseek_thinking,
                )
            if "codex" in {settings.backend, settings.fallback} and codex_command:
                self._backends["codex"] = CodexBackend(codex_command, settings.codex_model)
        self._primary = settings.backend if settings.backend in self._backends else next(
            iter(self._backends), None,
        )
        self._fallback = (
            settings.fallback if settings.fallback in self._backends
            and settings.fallback != self._primary else None
        )
        self._breakers = {
            name: _Breaker(settings.breaker_threshold, settings.breaker_cooldown_seconds)
            for name in self._backends
        }

    @classmethod
    def from_env(
        cls, settings: Mapping[str, object] | None, *, codex_command: str | None,
        client: httpx.AsyncClient | None = None,
    ) -> "StructuredModel":
        values = {key: value for key, value in (settings or {}).items() if value is not None}
        backend = _text(values, "GATEWAY_ROUTER_BACKEND", "auto").lower()
        if backend not in {"auto", "deepseek", "codex"}:
            LOGGER.warning("忽略未知的 GATEWAY_ROUTER_BACKEND=%s，改用 auto。", backend)
            backend = "auto"
        key = _text(values, "DEEPSEEK_API_KEY")
        if backend == "auto":
            backend = "deepseek" if key else "codex"
        if backend == "deepseek" and not key:
            LOGGER.error("GATEWAY_ROUTER_BACKEND=deepseek 但未配置 DEEPSEEK_API_KEY。")
            backend = "codex" if codex_command else "none"
        if backend == "none":
            LOGGER.error("没有可用的自然语言路由后端：请配置 DEEPSEEK_API_KEY 或 codex_command。")
        fallback = _text(values, "GATEWAY_ROUTER_FALLBACK").lower()
        if not fallback:
            fallback = "codex" if backend == "deepseek" and codex_command else "none"
        if fallback not in {"codex", "deepseek", "none"}:
            fallback = "none"
        if fallback == backend:
            fallback = "none"
        settings_object = RouterSettings(
            backend=backend,
            fallback=None if fallback == "none" else fallback,
            deepseek_api_key=key or None,
            deepseek_base_url=_text(values, "DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
            deepseek_model=_text(values, "DEEPSEEK_MODEL", "deepseek-flash"),
            deepseek_thinking=_text(values, "DEEPSEEK_THINKING", "disabled").lower(),
            timeout_seconds=_positive_float(values, "GATEWAY_ROUTER_TIMEOUT_SECONDS", 20.0),
            fallback_timeout_seconds=_positive_float(
                values, "GATEWAY_ROUTER_FALLBACK_TIMEOUT_SECONDS", 12.0,
            ),
            max_concurrency=_positive_int(values, "GATEWAY_ROUTER_MAX_CONCURRENCY", 4),
            breaker_threshold=_positive_int(values, "GATEWAY_ROUTER_BREAKER_THRESHOLD", 3),
            breaker_cooldown_seconds=_positive_float(
                values, "GATEWAY_ROUTER_BREAKER_COOLDOWN_SECONDS", 60.0,
            ),
            max_tokens=_positive_int(values, "GATEWAY_ROUTER_MAX_TOKENS", 4096),
        )
        return cls(settings_object, codex_command=codex_command, client=client)

    @property
    def available(self) -> bool:
        return self._primary is not None

    def summary(self) -> str:
        names = [name for name in (self._primary, self._fallback) if name is not None]
        return "→".join(names) if names else "未配置"

    async def complete(
        self, instructions: str, payload: bytes, properties: dict, *,
        purpose: str = "route", timeout: float | None = None,
    ) -> dict[str, Any]:
        if self._primary is None:
            raise RouterUnavailable("未配置自然语言路由模型。")
        budget = timeout if timeout is not None and timeout > 0 else self.settings.timeout_seconds
        started = time.monotonic()
        try:
            async with asyncio.timeout(budget):
                await self._semaphore.acquire()
                try:
                    return await self._attempt(
                        instructions, payload, properties, purpose, started, budget,
                    )
                finally:
                    self._semaphore.release()
        except TimeoutError:
            raise RouterUnavailable("路由模型排队或调用超过时间预算。") from None

    async def _attempt(
        self, instructions: str, payload: bytes, properties: dict, purpose: str,
        started: float, budget: float,
    ) -> dict[str, Any]:
        deadline = started + budget
        last: RouterError | None = None
        for name in (self._primary, self._fallback):
            if name is None:
                continue
            backend = self._backends[name]
            breaker = self._breakers[name]
            if not breaker.allows():
                last = RouterError("router_open", "路由后端熔断中。")
                continue
            attempt_timeout = min(
                self.settings.timeout_seconds if name == self._primary
                else self.settings.fallback_timeout_seconds,
                max(0.1, deadline - time.monotonic()),
            )
            if attempt_timeout <= 0.2:
                break
            attempts = 2 if name == self._primary else 1
            for attempt in range(attempts):
                if attempt:
                    await asyncio.sleep(0.4)
                remaining = deadline - time.monotonic()
                if remaining <= 0.5:
                    break
                try:
                    result = await backend.complete(
                        instructions, payload, properties, timeout=min(attempt_timeout, remaining),
                    )
                    result = _validate(result, properties)
                except RouterError as error:
                    breaker.failure()
                    last = error
                    LOGGER.warning(
                        "路由后端 %s 失败（%s，purpose=%s）。", name, error.code, purpose,
                    )
                    if not error.retryable or attempt + 1 >= attempts:
                        break
                    continue
                except (OSError, ValueError, TypeError, KeyError, AttributeError) as error:
                    breaker.failure()
                    last = RouterError("router_protocol", f"路由后端异常（{type(error).__name__}）。")
                    break
                breaker.success()
                LOGGER.info(
                    "路由后端 %s 完成（purpose=%s，%.0fms）。",
                    name, purpose, (time.monotonic() - started) * 1000,
                )
                return result
        raise last or RouterUnavailable()

    async def aclose(self) -> None:
        for backend in self._backends.values():
            await backend.aclose()
