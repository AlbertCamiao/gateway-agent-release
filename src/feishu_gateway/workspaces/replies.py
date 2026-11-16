"""Format one exact, read-only Gateway workspace change preview."""

import re
from html import escape


def _label(value: str) -> str:
    return re.sub(r'([\\`*_\[\]~|])', r'\\\1', escape(value, quote=False))


def format_workspace_result(plan, *, result=None, error=None) -> str:
    refusals = {
        'unknown_host': '主机尚未接入 Gateway，或主机名称不明确。',
        'invalid_workspace': '请提供目标主机上实际存在的规范目录完整路径。',
        'not_registered': '登记表没有这条精确路径。',
        'ssh_unavailable': '无法验证目标主机的 SSH 连接。请检查连接后重新发起。',
        'ssh_changed': 'SSH 连接已变化，请重新发起变更。',
        'host_changed': '主机配置已变化，请重新发起变更。',
        'workspace_changed': '工作目录已变化，请重新发起变更。',
        'registration_changed': '该主机的登记已变化，请重新发起变更。',
        'bridge_sync_pending': '登记未生效，DSH 桥接范围尚待同步。',
    }
    if result is None:
        return refusals.get(error, '变更未执行，请重新发起。')
    target = f'{_label(plan.host)} · {_label(str(plan.workspace))}'
    if result.status:
        summary = '扫描失败，工作区登记保持原状。'
    elif plan.action == 'remove':
        summary = '已撤销工作区登记。'
    elif result.already_registered:
        summary = '工作区已登记，扫描完成。'
    else:
        summary = '扫描完成，已登记工作区。'
    if result.bridge_sync_pending:
        summary += 'DSH 桥接范围尚待同步。'
    return f'{summary}\n\n{target}'
