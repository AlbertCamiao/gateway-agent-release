"""Format Feishu replies without changing the Markdown sent to Codex."""

import re


_PROMPT_SECTIONS = ("任务目标", "输入与约束", "执行步骤", "验收标准")
_HEADING = re.compile(r"^ {0,3}#{1,6}[ \t]+(.+?)(?:[ \t]+#+)?[ \t]*$")
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
_AT_TAG = re.compile(r"<\s*at(?=[\s/>])", re.IGNORECASE)
_OPEN_ID = re.compile(r"ou_[A-Za-z0-9_]+")


def validate_prompt_markdown(markdown: str) -> None:
    if _AT_TAG.search(markdown):
        raise ValueError("Prompt 正文含有飞书提及标签。")

    sections = []
    populated = set()
    fence = None
    for line in markdown.splitlines():
        marker = _FENCE.match(line)
        if fence is not None:
            if marker and marker[1].startswith(fence) and not marker[2].strip():
                fence = None
            elif sections and line.strip():
                populated.add(sections[-1])
            continue
        if marker:
            fence = marker[1]
            continue

        heading = _HEADING.fullmatch(line)
        if heading and heading[1] in _PROMPT_SECTIONS:
            sections.append(heading[1])
        elif sections and line.strip() and not heading:
            populated.add(sections[-1])

    if fence is not None or tuple(sections) != _PROMPT_SECTIONS or populated != set(_PROMPT_SECTIONS):
        raise ValueError("Prompt 缺少完整的任务目标、输入与约束、执行步骤或验收标准。")


def markdown_card(text: str, phase: str | None, *, mention_open_id: str | None = None) -> dict:
    # Reply text is data, not an instruction to Feishu. Escape accidental
    # mention tags instead of dropping the entire receipt.
    text = _AT_TAG.sub("&lt;at", text)
    if phase in {"preview", "admin"}:
        markdown = text
    else:
        title = {
            "routing": "会话路由",
            "preview-error": "Prompt 预览",
        }.get(phase, "执行回执" if phase and phase.startswith("status:") else "消息回执")
        markdown = f"### {title}\n\n**状态**　{text}"

    elements = []
    if mention_open_id is not None:
        if not _OPEN_ID.fullmatch(mention_open_id):
            raise ValueError("飞书提及必须使用指定用户的 open_id。")
        elements.append({"tag": "markdown", "content": f"<at id={mention_open_id}></at>"})
    elements.append({"tag": "markdown", "content": markdown})
    return {"schema": "2.0", "config": {"update_multi": True}, "body": {"elements": elements}}
