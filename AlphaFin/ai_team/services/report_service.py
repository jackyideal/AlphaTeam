"""
研究报告服务 - 报告生成、存储和检索
"""
import os
import json
import time

from AlphaFin.ai_team.config import REPORTS_DIR
from AlphaFin.ai_team.core.memory import (
    save_report,
    get_reports,
    get_report_by_id,
    delete_report_by_id,
)


def save_report_file(report_id, content):
    """将报告保存为 Markdown 文件"""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    filepath = os.path.join(REPORTS_DIR, 'report_%d.md' % report_id)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    return filepath


def delete_report_file(report_id):
    """删除报告对应的 Markdown 文件。"""
    filepath = os.path.join(REPORTS_DIR, 'report_%d.md' % report_id)
    if os.path.exists(filepath):
        os.remove(filepath)
        return True
    return False


def list_reports(report_type=None, limit=20):
    """获取报告列表"""
    reports = get_reports(report_type=report_type, limit=limit)
    for r in reports:
        r['created_at_str'] = time.strftime('%Y-%m-%d %H:%M', time.localtime(r['created_at']))
    return reports


def get_report_detail(report_id):
    """获取报告详情"""
    report = get_report_by_id(report_id)
    if report:
        report['created_at_str'] = time.strftime('%Y-%m-%d %H:%M', time.localtime(report['created_at']))
        if report.get('participants'):
            try:
                report['participants'] = json.loads(report['participants'])
            except (json.JSONDecodeError, TypeError):
                pass
    return report


def delete_report(report_id):
    """删除报告记录及其落地文件。"""
    report = get_report_by_id(report_id)
    if not report:
        return False
    deleted = delete_report_by_id(report_id)
    if deleted:
        delete_report_file(report_id)
    return deleted
