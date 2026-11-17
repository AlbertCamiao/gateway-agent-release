"""Render read-only Gateway results for the authorized Feishu chat."""

import re
from html import escape


def _value(text):
    return re.sub(r'([\\`*_\[\]~|])', r'\\\1', escape(text, quote=False))


def _restart_sentence(state):
    if state == 'manual':
        return 'Gateway 正在手动运行；请重启 Gateway 后生效。'
    return 'Gateway 未运行；配置将在下次启动时生效。'


def format_host_add(result, restart_state):
    candidate = result['candidate']
    content = [
        '### 端管理',
        f"已添加端 **{_value(result['host'])}** "
        f"（{_value(candidate['user'])}@{_value(candidate['hostname'])}:{candidate['port']}）。",
        ('- Codex：未在远端 PATH 找到（可稍后配置）'
         if result['remote_codex'] == 'codex' and result.get('remote', {}).get('codex') is None
         else f"- Codex：{_value(result['remote_codex'])}"),
    ]
    dsh = result.get('dsh')
    if dsh:
        content.append(f"- DSH：{_value(dsh['bridge_dir'])}（{_value(dsh['profile'])}）")
        workspaces = ', '.join(_value(row['path']) for row in dsh['workspaces'])
        if workspaces:
            content.append(f'- DSH 工作区：{workspaces}')
            content.append('工作区尚未登记；如需飞书调用，请再扫描并登记。')
    else:
        content.append('- DSH：未配置')
    content.append(_restart_sentence(restart_state))
    return '\n\n'.join(content)


def format_host_remove(result, restart_state):
    content = ['### 端管理', f"已移除端 **{_value(result['host'])}**。"]
    removed = result.get('removed_workspaces', 0)
    if removed:
        content.append(f'- 同时移除 {removed} 个工作区登记。')
    content.append(_restart_sentence(restart_state))
    return '\n\n'.join(content)
