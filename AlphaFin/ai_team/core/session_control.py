"""
会话级停止控制。

用于让 orchestrator / portfolio / ask 这类长流程在收到“停止当前工作”后，
不仅停止新入口，还能让正在运行中的循环尽快感知并退出。
"""
import threading
import time


_lock = threading.Lock()
_cancelled_sessions = {}
_session_deadlines = {}
_session_progress = {}
_session_overtime = {}


def cancel_session(session_id, reason=''):
    sid = str(session_id or '').strip()
    if not sid:
        return False
    with _lock:
        _cancelled_sessions[sid] = {
            'reason': str(reason or '用户手动停止'),
            'ts': time.time(),
        }
    return True


def clear_session_cancel(session_id):
    sid = str(session_id or '').strip()
    if not sid:
        return False
    with _lock:
        return _cancelled_sessions.pop(sid, None) is not None


def set_session_deadline(session_id, total_seconds, workflow='', title='', source='', soft_ratio=0.20):
    sid = str(session_id or '').strip()
    if not sid:
        return {'active': False, 'session_id': ''}
    total = max(0, int(total_seconds or 0))
    if total <= 0:
        clear_session_deadline(sid)
        return {'active': False, 'session_id': sid}
    ratio = float(soft_ratio or 0.20)
    ratio = min(0.80, max(0.05, ratio))
    now_ts = time.time()
    with _lock:
        _session_deadlines[sid] = {
            'started_at': now_ts,
            'deadline_ts': now_ts + total,
            'total_seconds': total,
            'workflow': str(workflow or ''),
            'title': str(title or ''),
            'source': str(source or ''),
            'soft_ratio': ratio,
        }
    return get_session_timing(sid, now_ts=now_ts)


def clear_session_deadline(session_id):
    sid = str(session_id or '').strip()
    if not sid:
        return False
    with _lock:
        return _session_deadlines.pop(sid, None) is not None


def set_session_progress(
        session_id,
        workflow='',
        title='',
        steps=None,
        current_index=None,
        current_step='',
        detail='',
        state='running',
        actor='',
        prompt_profile=None
):
    sid = str(session_id or '').strip()
    if not sid:
        return {'active': False, 'session_id': ''}
    now_ts = time.time()
    with _lock:
        row = dict(_session_progress.get(sid) or {})
        if 'created_at' not in row:
            row['created_at'] = now_ts
        row['updated_at'] = now_ts
        if workflow:
            row['workflow'] = str(workflow or '')
        if title:
            row['title'] = str(title or '')
        if steps is not None:
            clean_steps = [str(x or '').strip() for x in (steps or []) if str(x or '').strip()]
            row['steps'] = clean_steps
        if current_index is not None:
            try:
                row['current_index'] = max(0, int(current_index))
            except Exception:
                row['current_index'] = 0
        if current_step is not None:
            row['current_step'] = str(current_step or '')
        if detail is not None:
            row['detail'] = str(detail or '')
        if state is not None:
            row['state'] = str(state or 'running')
        if actor is not None:
            row['actor'] = str(actor or '')
        if prompt_profile is not None:
            row['prompt_profile'] = prompt_profile if isinstance(prompt_profile, dict) else {}
        _session_progress[sid] = row
    return get_session_progress(sid)


def get_session_progress(session_id):
    sid = str(session_id or '').strip()
    if not sid:
        return {'active': False, 'session_id': ''}
    with _lock:
        row = dict(_session_progress.get(sid) or {})
    if not row:
        return {'active': False, 'session_id': sid}
    steps = [str(x or '').strip() for x in (row.get('steps') or []) if str(x or '').strip()]
    current_index = 0
    try:
        current_index = max(0, int(row.get('current_index') or 0))
    except Exception:
        current_index = 0
    if steps and current_index > len(steps):
        current_index = len(steps)
    return {
        'active': True,
        'session_id': sid,
        'workflow': str(row.get('workflow') or ''),
        'title': str(row.get('title') or ''),
        'steps': steps,
        'current_index': current_index,
        'current_step': str(row.get('current_step') or ''),
        'detail': str(row.get('detail') or ''),
        'state': str(row.get('state') or 'running'),
        'actor': str(row.get('actor') or ''),
        'created_at': float(row.get('created_at') or 0),
        'updated_at': float(row.get('updated_at') or 0),
        'prompt_profile': row.get('prompt_profile') if isinstance(row.get('prompt_profile'), dict) else {},
    }


def clear_session_progress(session_id):
    sid = str(session_id or '').strip()
    if not sid:
        return False
    with _lock:
        return _session_progress.pop(sid, None) is not None


def is_session_cancelled(session_id):
    sid = str(session_id or '').strip()
    if not sid:
        return False
    with _lock:
        return sid in _cancelled_sessions


def get_session_timing(session_id, now_ts=None):
    sid = str(session_id or '').strip()
    if not sid:
        return {'active': False, 'session_id': ''}
    with _lock:
        row = dict(_session_deadlines.get(sid) or {})
    if not row:
        return {'active': False, 'session_id': sid}
    now_ts = float(now_ts or time.time())
    started_at = float(row.get('started_at') or now_ts)
    deadline_ts = float(row.get('deadline_ts') or now_ts)
    total_seconds = max(0, int(row.get('total_seconds') or 0))
    elapsed_seconds = max(0, int(now_ts - started_at))
    remaining_seconds = max(0, int(deadline_ts - now_ts))
    soft_ratio = float(row.get('soft_ratio') or 0.20)
    soft_seconds = max(10, int(total_seconds * soft_ratio)) if total_seconds > 0 else 0
    soft_deadline_ts = max(started_at, deadline_ts - soft_seconds)
    is_expired = bool(total_seconds > 0 and now_ts >= deadline_ts)
    is_converging = bool(total_seconds > 0 and not is_expired and now_ts >= soft_deadline_ts)
    state = 'expired' if is_expired else ('converging' if is_converging else 'running')
    return {
        'active': True,
        'session_id': sid,
        'workflow': str(row.get('workflow') or ''),
        'title': str(row.get('title') or ''),
        'source': str(row.get('source') or ''),
        'started_at': started_at,
        'deadline_ts': deadline_ts,
        'total_seconds': total_seconds,
        'elapsed_seconds': elapsed_seconds,
        'remaining_seconds': remaining_seconds,
        'soft_ratio': soft_ratio,
        'soft_seconds': soft_seconds,
        'soft_deadline_ts': soft_deadline_ts,
        'is_converging': is_converging,
        'is_expired': is_expired,
        'state': state,
    }


def is_session_converging(session_id):
    return bool(get_session_timing(session_id).get('is_converging'))


def is_session_expired(session_id):
    return bool(get_session_timing(session_id).get('is_expired'))


def request_session_overtime_decision(
        session_id,
        workflow='',
        title='',
        message='',
        default_extend_seconds=300
):
    sid = str(session_id or '').strip()
    if not sid:
        return {'active': False, 'session_id': ''}
    now_ts = time.time()
    extend_seconds = max(60, int(default_extend_seconds or 300))
    with _lock:
        row = dict(_session_overtime.get(sid) or {})
        if row.get('waiting'):
            return get_session_overtime_state(sid)
        decided = str(row.get('decision') or '').strip().lower()
        if decided in ('extend', 'summarize', 'stop') and not row.get('waiting'):
            return get_session_overtime_state(sid)
        row.update({
            'workflow': str(workflow or row.get('workflow') or ''),
            'title': str(title or row.get('title') or ''),
            'message': str(message or row.get('message') or ''),
            'waiting': True,
            'decision': '',
            'requested_at': now_ts,
            'resolved_at': 0.0,
            'default_extend_seconds': extend_seconds,
            'extend_seconds': 0,
        })
        _session_overtime[sid] = row
    return get_session_overtime_state(sid)


def get_session_overtime_state(session_id):
    sid = str(session_id or '').strip()
    if not sid:
        return {'active': False, 'session_id': ''}
    with _lock:
        row = dict(_session_overtime.get(sid) or {})
    if not row:
        return {'active': False, 'session_id': sid}
    return {
        'active': True,
        'session_id': sid,
        'workflow': str(row.get('workflow') or ''),
        'title': str(row.get('title') or ''),
        'message': str(row.get('message') or ''),
        'waiting': bool(row.get('waiting')),
        'decision': str(row.get('decision') or ''),
        'requested_at': float(row.get('requested_at') or 0),
        'resolved_at': float(row.get('resolved_at') or 0),
        'default_extend_seconds': max(60, int(row.get('default_extend_seconds') or 300)),
        'extend_seconds': max(0, int(row.get('extend_seconds') or 0)),
    }


def resolve_session_overtime_decision(session_id, decision, extend_seconds=0):
    sid = str(session_id or '').strip()
    if not sid:
        return {'active': False, 'session_id': ''}
    choice = str(decision or '').strip().lower()
    if choice not in ('extend', 'summarize', 'stop'):
        raise ValueError('不支持的超时决策: %s' % choice)
    now_ts = time.time()
    with _lock:
        row = dict(_session_overtime.get(sid) or {})
        row['waiting'] = False
        row['decision'] = choice
        row['resolved_at'] = now_ts
        row['extend_seconds'] = 0
        if choice == 'extend':
            ext = max(60, int(extend_seconds or row.get('default_extend_seconds') or 300))
            row['extend_seconds'] = ext
            deadline = _session_deadlines.get(sid)
            if deadline:
                deadline['deadline_ts'] = float(deadline.get('deadline_ts') or now_ts) + ext
                deadline['total_seconds'] = max(0, int(deadline.get('total_seconds') or 0)) + ext
                _session_deadlines[sid] = deadline
        _session_overtime[sid] = row
    return get_session_overtime_state(sid)


def clear_session_overtime_state(session_id):
    sid = str(session_id or '').strip()
    if not sid:
        return False
    with _lock:
        return _session_overtime.pop(sid, None) is not None


def get_waiting_overtime_sessions(limit=5):
    """
    返回当前所有“等待用户决策”的超时会话（按 requested_at 倒序）。
    用于当前会话指针丢失时，前端仍可展示“继续等待/立即汇总”入口。
    """
    try:
        max_n = max(1, int(limit or 5))
    except Exception:
        max_n = 5
    with _lock:
        rows = []
        for sid, row in list(_session_overtime.items()):
            if not isinstance(row, dict):
                continue
            if not bool(row.get('waiting')):
                continue
            rows.append((str(sid or ''), dict(row)))
    rows.sort(key=lambda x: float((x[1] or {}).get('requested_at') or 0), reverse=True)
    out = []
    for sid, _ in rows[:max_n]:
        out.append({
            'session_id': sid,
            'session_timing': get_session_timing(sid),
            'session_progress': get_session_progress(sid),
            'session_overtime': get_session_overtime_state(sid),
        })
    return out


def get_session_cancel_reason(session_id, default_reason='用户手动停止'):
    sid = str(session_id or '').strip()
    if not sid:
        return str(default_reason or '用户手动停止')
    with _lock:
        row = _cancelled_sessions.get(sid) or {}
    return str(row.get('reason') or default_reason or '用户手动停止')
