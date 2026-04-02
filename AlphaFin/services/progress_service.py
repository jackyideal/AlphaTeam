"""
线程安全的进度追踪服务
"""
import threading

_lock = threading.Lock()
_store = {}

progress_store = _store


def update_progress(task_id, step, total, message, done=False,
                    chart_paths=None, chart_titles=None,
                    eta_seconds=None, result=None, meta=None):
    """更新任务进度"""
    with _lock:
        existing = _store.get(task_id, {})
        if eta_seconds is None:
            eta_seconds = existing.get('eta_seconds')
        if result is None:
            result = existing.get('result')
        if meta is None:
            meta = existing.get('meta')
        _store[task_id] = {
            'step': step,
            'total': total,
            'message': message,
            'done': done,
            'chart_paths': chart_paths or existing.get('chart_paths', []),
            'chart_titles': chart_titles or existing.get('chart_titles', []),
            'eta_seconds': eta_seconds,
            'result': result,
            'meta': meta or {},
        }


def get_progress(task_id):
    """获取任务进度"""
    with _lock:
        return _store.get(task_id, {}).copy()


def cleanup(task_id):
    """清理已完成的任务"""
    with _lock:
        _store.pop(task_id, None)
