"""
研究周期调度器 - 智能路由 + 并行协调
工作流：Director判断任务类型 → 简单对话直接回答 / 定向分析派相关智能体 / 全面研究走完整流程
"""
import json
import os
import re
import time
import uuid
import random
import threading
import traceback

from AlphaFin.ai_team.config import (
    DEFAULT_CYCLE_INTERVAL,
    IDLE_LEARNING_ENABLED,
    DEFAULT_IDLE_INTERVAL,
    IDLE_LEARNING_TOPICS,
    IDLE_ALLOWED_TOOLS,
    IDLE_BLOCKED_TOOLS,
    OFFICE_CHAT_ENABLED,
    DEFAULT_OFFICE_CHAT_INTERVAL,
    OFFICE_CHAT_TOPICS,
    ORCHESTRATOR_STATE_PATH,
    TOKEN_BUDGET_ENABLED,
    TOKEN_BUDGET_DAILY_LIMIT,
    TOKEN_BUDGET_SESSION_LIMIT,
    TOKEN_BUDGET_WARN_RATIO,
    TOKEN_BUDGET_HARD_RATIO,
    TEAM_WORKFLOW_DEFAULT_TIMEOUT,
    MANUAL_ANALYZE_DEFAULT_TIMEOUT,
    WORKFLOW_DEADLINE_SOFT_RATIO,
)
from AlphaFin.ai_team.core.message_bus import bus
from AlphaFin.ai_team.core.memory import (
    save_report,
    get_token_budget_snapshot,
    trace_start_run,
    trace_finish_run,
    trace_event,
    get_trace_runs,
)
from AlphaFin.ai_team.core.session_control import (
    cancel_session,
    clear_session_cancel,
    is_session_cancelled,
    get_session_cancel_reason,
    set_session_deadline,
    clear_session_deadline,
    get_session_timing,
    is_session_converging,
    is_session_expired,
    set_session_progress,
    get_session_progress,
    clear_session_progress,
    request_session_overtime_decision,
    get_session_overtime_state,
    resolve_session_overtime_decision,
    clear_session_overtime_state,
)
from AlphaFin.ai_team.prompt_catalog import format_ai_team_prompt

# 直连问答工具轮次配置
USER_ASK_MAX_TOOL_ROUNDS = 4
USER_ASK_QUICK_MAX_TOOL_ROUNDS = 1
USER_ASK_DEEP_MAX_TOOL_ROUNDS = 6
USER_ASK_TEAM_MAX_TOOL_ROUNDS = 4
# 路由判定轮次（提升复杂问题分流准确性）
ROUTING_MAX_TOOL_ROUNDS = 3


class Orchestrator:
    """研究周期调度器（智能路由版）"""

    def __init__(self):
        self.cycle_interval = DEFAULT_CYCLE_INTERVAL
        self.idle_enabled = bool(IDLE_LEARNING_ENABLED)
        self.idle_interval = max(300, int(DEFAULT_IDLE_INTERVAL))
        self.idle_topics = list(IDLE_LEARNING_TOPICS)
        self.idle_allowed_tools = list(IDLE_ALLOWED_TOOLS)
        self.idle_blocked_tools = list(IDLE_BLOCKED_TOOLS)
        self.office_chat_enabled = bool(OFFICE_CHAT_ENABLED)
        self.office_chat_interval = max(600, int(DEFAULT_OFFICE_CHAT_INTERVAL))
        self.office_chat_topics = list(OFFICE_CHAT_TOPICS)
        self.specialists_phase1 = ['intel', 'quant', 'analyst', 'restructuring']
        self.specialists_phase2 = ['risk', 'auditor']
        self.specialists_all = list(self.specialists_phase1 + self.specialists_phase2)
        self.user_ask_workflow = self._default_user_ask_workflow()
        self._last_user_ask_meta = {}
        self._trace_runs = {}
        self._budget_alert_ts = 0.0
        self._deadline_notices = set()

        self.running = False
        self.paused = True  # 默认暂停，等用户手动开启或首次触发
        self.manual_only = False  # 仅手动模式（不按周期自动触发）
        self.current_session = None
        self.last_cycle_time = 0
        self.cycle_count = 0
        self.last_idle_time = 0
        self.idle_count = 0
        self.last_office_chat_time = 0
        self.office_chat_count = 0
        self._lock = threading.Lock()
        self._config_path = ORCHESTRATOR_STATE_PATH
        self._config_mtime = 0.0
        self._load_persisted_config()

    @staticmethod
    def _to_int(value, default_value, min_value):
        try:
            v = int(value)
        except Exception:
            v = int(default_value)
        return max(int(min_value), v)

    @staticmethod
    def _to_bool(value, default_value=False):
        if isinstance(value, bool):
            return value
        if value is None:
            return bool(default_value)
        if isinstance(value, str):
            s = value.strip().lower()
            if s in ('1', 'true', 'yes', 'on'):
                return True
            if s in ('0', 'false', 'no', 'off'):
                return False
        return bool(value)

    @staticmethod
    def _copy_json(obj, default=None):
        try:
            return json.loads(json.dumps(obj, ensure_ascii=False))
        except Exception:
            return default if default is not None else {}

    @staticmethod
    def _budget_rank(level):
        order = {'normal': 0, 'warning': 1, 'critical': 2, 'exhausted': 3}
        return order.get(str(level or 'normal'), 0)

    def _get_budget_snapshot(self, session_id=''):
        if not TOKEN_BUDGET_ENABLED:
            return {'level': 'normal'}
        return get_token_budget_snapshot(
            session_id=session_id or '',
            daily_limit=TOKEN_BUDGET_DAILY_LIMIT,
            session_limit=TOKEN_BUDGET_SESSION_LIMIT,
            warn_ratio=TOKEN_BUDGET_WARN_RATIO,
            hard_ratio=TOKEN_BUDGET_HARD_RATIO,
        )

    def _notify_budget(self, session_id, snapshot, force=False):
        if not snapshot:
            return
        level = snapshot.get('level', 'normal')
        if level == 'normal' and not force:
            return
        now = time.time()
        if not force and now - self._budget_alert_ts < 20:
            return
        self._budget_alert_ts = now
        bus.post_activity(
            'orchestrator',
            'status',
            '预算状态: %s（日:%s/%s，会话:%s/%s）' % (
                level,
                snapshot.get('day_used', 0),
                snapshot.get('day_limit', 0) or '∞',
                snapshot.get('session_used', 0),
                snapshot.get('session_limit', 0) or '∞',
            ),
            metadata={
                'mode': 'budget',
                'session_id': session_id or '',
                'budget': snapshot
            }
        )

    @staticmethod
    def _session_stop_requested(session_id):
        return bool(session_id) and is_session_cancelled(session_id)

    @staticmethod
    def _session_timing(session_id):
        return get_session_timing(session_id)

    @staticmethod
    def _session_progress(session_id):
        return get_session_progress(session_id)

    @staticmethod
    def _session_overtime(session_id):
        return get_session_overtime_state(session_id)

    @staticmethod
    def _session_should_converge(session_id):
        return bool(session_id) and (
            is_session_converging(session_id) or is_session_expired(session_id)
        )

    def _start_session_deadline(self, session_id, workflow, title='', time_limit_seconds=None, source='workflow'):
        seconds = max(0, int(time_limit_seconds or 0))
        if seconds <= 0:
            clear_session_deadline(session_id)
            return {'active': False, 'session_id': session_id or ''}
        timing = set_session_deadline(
            session_id,
            seconds,
            workflow=workflow,
            title=title,
            source=source,
            soft_ratio=WORKFLOW_DEADLINE_SOFT_RATIO,
        )
        bus.post_activity(
            'orchestrator', 'status',
            '任务时限已设置：%d分钟（session=%s）' % (max(1, int(seconds / 60)), session_id),
            metadata={
                'session_id': session_id or '',
                'mode': workflow or '',
                'phase': 'session_deadline_started',
                'session_timing': timing,
            }
        )
        return timing

    def _clear_session_deadline(self, session_id):
        if session_id:
            clear_session_deadline(session_id)
        self._deadline_notices.discard(str(session_id or ''))

    def _set_session_progress(
            self,
            session_id,
            workflow,
            steps,
            current_index,
            current_step,
            detail='',
            state='running',
            actor='orchestrator',
            title='',
            prompt_profile=None
    ):
        return set_session_progress(
            session_id=session_id,
            workflow=workflow,
            title=title,
            steps=steps,
            current_index=current_index,
            current_step=current_step,
            detail=detail,
            state=state,
            actor=actor,
            prompt_profile=prompt_profile,
        )

    def _clear_session_progress(self, session_id):
        if session_id:
            clear_session_progress(session_id)

    def _request_session_overtime(self, session_id, workflow, title='', message='', default_extend_seconds=300):
        return request_session_overtime_decision(
            session_id=session_id,
            workflow=workflow,
            title=title,
            message=message,
            default_extend_seconds=default_extend_seconds,
        )

    def _consume_session_overtime_decision(self, session_id):
        state = get_session_overtime_state(session_id)
        if not state.get('active'):
            return state
        if not state.get('waiting'):
            clear_session_overtime_state(session_id)
        return state

    def _handle_session_expired(self, session_id, workflow, title=''):
        timing = self._session_timing(session_id)
        if not timing.get('is_expired'):
            return 'continue'
        overtime_message = '任务已达到设定时限，请选择继续等待，或立即停止任务。'
        overtime = self._request_session_overtime(
            session_id=session_id,
            workflow=workflow,
            title=title,
            message=overtime_message,
            default_extend_seconds=300
        )
        self._set_session_progress(
            session_id=session_id,
            workflow=workflow,
            title=title,
            steps=(self._session_progress(session_id).get('steps') or []),
            current_index=self._session_progress(session_id).get('current_index') or 0,
            current_step=self._session_progress(session_id).get('current_step') or '等待用户决策',
            detail='已达到时限，等待用户选择继续等待或立即停止。',
            state='waiting_user',
            actor='orchestrator',
        )
        if overtime.get('waiting'):
            bus.post_activity(
                'orchestrator', 'status',
                '任务达到时限，等待用户决定：继续等待或立即停止。',
                metadata={
                    'session_id': session_id or '',
                    'mode': workflow or '',
                    'phase': 'session_overtime_waiting',
                    'session_timing': timing,
                    'session_progress': self._session_progress(session_id),
                    'session_overtime': overtime,
                }
            )
        recover_notice_sent = False
        while True:
            if self._session_stop_requested(session_id):
                return 'stop'
            state = get_session_overtime_state(session_id)
            if not state.get('active'):
                state = self._request_session_overtime(
                    session_id=session_id,
                    workflow=workflow,
                    title=title,
                    message=overtime_message,
                    default_extend_seconds=300
                )
                if state.get('waiting') and not recover_notice_sent:
                    bus.post_activity(
                        'orchestrator', 'status',
                        '超时决策状态异常已自动恢复，等待用户决定：继续等待或立即停止。',
                        metadata={
                            'session_id': session_id or '',
                            'mode': workflow or '',
                            'phase': 'session_overtime_recovered',
                            'session_timing': self._session_timing(session_id),
                            'session_progress': self._session_progress(session_id),
                            'session_overtime': state,
                        }
                    )
                    recover_notice_sent = True
                time.sleep(0.5)
                continue
            if state.get('waiting'):
                time.sleep(0.5)
                continue
            decision = str(state.get('decision') or '').strip().lower()
            extend_seconds = int(state.get('extend_seconds') or 0)
            clear_session_overtime_state(session_id)
            if decision == 'extend':
                try:
                    from AlphaFin.ai_team.core.agent_registry import clear_stop_all_agents
                    clear_stop_all_agents()
                except Exception:
                    pass
                bus.post_activity(
                    'orchestrator', 'status',
                    '用户选择继续等待 %d 分钟，任务继续推进。' % max(1, int(extend_seconds / 60)),
                    metadata={
                        'session_id': session_id or '',
                        'mode': workflow or '',
                        'phase': 'session_overtime_extended',
                        'session_timing': self._session_timing(session_id),
                    }
                )
                return 'continue'
            if decision == 'stop':
                try:
                    from AlphaFin.ai_team.core.agent_registry import request_stop_agents_for_session
                    request_stop_agents_for_session(session_id, reason='用户在超时面板选择停止任务')
                except Exception:
                    pass
                cancel_session(session_id, reason='用户在超时面板选择停止任务')
                clear_session_deadline(session_id)
                bus.post_activity(
                    'orchestrator', 'status',
                    '用户选择停止当前任务，系统已下发终止指令。',
                    metadata={
                        'session_id': session_id or '',
                        'mode': workflow or '',
                        'phase': 'session_overtime_stop',
                        'session_timing': self._session_timing(session_id),
                    }
                )
                return 'stop'
            if decision == 'summarize':
                try:
                    from AlphaFin.ai_team.core.agent_registry import request_stop_agents_for_session, clear_stop_agents
                    request_stop_agents_for_session(session_id, reason='用户要求立即汇总', exclude=['director'])
                    clear_stop_agents(['director'])
                except Exception:
                    pass
                # 用户已明确选择“立即汇总”，后续不再重复触发超时门控
                clear_session_deadline(session_id)
                bus.post_activity(
                    'orchestrator', 'status',
                    '用户选择立即汇总，系统将基于当前已完成结果生成结论。',
                    metadata={
                        'session_id': session_id or '',
                        'mode': workflow or '',
                        'phase': 'session_overtime_summarize',
                        'session_timing': self._session_timing(session_id),
                    }
                )
                return 'summarize'

    def _maybe_announce_session_converging(self, session_id, mode):
        sid = str(session_id or '')
        if not sid:
            return {'active': False, 'session_id': ''}
        timing = self._session_timing(sid)
        if not timing.get('active'):
            return timing
        if timing.get('state') == 'running':
            self._deadline_notices.discard(sid)
            return timing
        if sid in self._deadline_notices:
            return timing
        self._deadline_notices.add(sid)
        bus.post_activity(
            'orchestrator', 'status',
            '任务进入时间收敛阶段：剩余 %d 秒，将优先输出结果。' % int(timing.get('remaining_seconds') or 0),
            metadata={
                'session_id': sid,
                'mode': mode or '',
                'phase': 'session_converging',
                'session_timing': timing,
            }
        )
        return timing

    def _post_session_stopped(self, session_id, mode, title='任务已停止'):
        reason = get_session_cancel_reason(session_id, '用户手动停止')
        bus.post_activity(
            'orchestrator', 'status',
            '%s：%s' % (title, reason),
            metadata={
                'session_id': session_id or '',
                'mode': mode or '',
                'phase': 'session_stopped',
                'reason': reason,
            }
        )
        return reason

    def _budget_parallel_cap(self, session_id, requested_count):
        snapshot = self._get_budget_snapshot(session_id)
        level = snapshot.get('level', 'normal')
        cap = max(1, int(requested_count or 1))
        if level == 'warning':
            cap = min(cap, 4)
        elif level == 'critical':
            cap = min(cap, 2)
        elif level == 'exhausted':
            cap = 0
        self._notify_budget(session_id, snapshot)
        return cap, snapshot

    def _budget_meeting_cap(self, session_id, requested_rounds):
        snapshot = self._get_budget_snapshot(session_id)
        level = snapshot.get('level', 'normal')
        cap = max(1, int(requested_rounds or 1))
        if level == 'warning':
            cap = min(cap, 2)
        elif level == 'critical':
            cap = min(cap, 1)
        elif level == 'exhausted':
            cap = 0
        self._notify_budget(session_id, snapshot)
        return cap, snapshot

    def _trace_start_session(self, session_id, workflow, topic=''):
        rid = trace_start_run(
            session_id=session_id or '',
            workflow=workflow or '',
            origin='orchestrator',
            topic=(topic or '')[:220],
            meta={'cycle_count': self.cycle_count}
        )
        self._trace_runs[session_id] = rid
        trace_event(
            run_id=rid,
            session_id=session_id or '',
            workflow=workflow or '',
            span_type='orchestrator_event',
            name='session_start',
            agent_id='orchestrator',
            data={'topic': topic or ''}
        )
        return rid

    def _trace_finish_session(self, session_id, status='ok', meta=None):
        rid = self._trace_runs.get(session_id) or ''
        if not rid:
            # 兜底：尝试按 session 反查最近 run
            runs = get_trace_runs(session_id=session_id, limit=1)
            if runs:
                rid = runs[0].get('run_id', '')
        if rid:
            trace_event(
                run_id=rid,
                session_id=session_id or '',
                workflow=str((meta or {}).get('workflow') or ''),
                span_type='orchestrator_event',
                name='session_finish',
                agent_id='orchestrator',
                data=meta or {},
                status=status,
            )
            trace_finish_run(rid, status=status, meta=meta or {})
        if session_id in self._trace_runs:
            self._trace_runs.pop(session_id, None)

    def _default_user_ask_workflow(self):
        return {
            'mode': 'auto',  # auto | director_only | custom
            'name': '默认透明流程',
            'auto_for_simple': True,
            'custom_steps': [
                {
                    'id': 'step_route',
                    'type': 'assign_by_router',
                    'name': '总监路由分配'
                },
                {
                    'id': 'step_team',
                    'type': 'agent_task',
                    'name': '成员执行',
                    'source': 'assigned',
                    'parallel': True,
                    'agents': []
                },
                {
                    'id': 'step_meeting',
                    'type': 'meeting',
                    'name': '分歧讨论',
                    'mode': 'auto',
                    'participants': [],
                    'max_rounds': 1
                },
                {
                    'id': 'step_final',
                    'type': 'director_synthesis',
                    'name': '总监最终答复'
                }
            ],
            'limits': {
                'max_parallel_agents': 4,
                'max_meeting_rounds': 2,
                'timeout_seconds': TEAM_WORKFLOW_DEFAULT_TIMEOUT,
            }
        }

    def _workflow_presets(self):
        base = self._default_user_ask_workflow()
        p_auto = self._copy_json(base)
        p_auto['mode'] = 'auto'
        p_auto['name'] = '智能路由（推荐）'

        p_director = self._copy_json(base)
        p_director['mode'] = 'director_only'
        p_director['name'] = '仅总监直答'

        p_fast = self._copy_json(base)
        p_fast['mode'] = 'custom'
        p_fast['name'] = '快速协作'
        p_fast['custom_steps'] = [
            {'id': 'route', 'type': 'assign_by_router', 'name': '总监路由'},
            {'id': 'team', 'type': 'agent_task', 'name': '并行执行', 'source': 'assigned', 'parallel': True},
            {'id': 'final', 'type': 'director_synthesis', 'name': '最终答复'},
        ]

        p_deep = self._copy_json(base)
        p_deep['mode'] = 'custom'
        p_deep['name'] = '深度审议'
        p_deep['custom_steps'] = [
            {'id': 'route', 'type': 'assign_by_router', 'name': '总监路由'},
            {
                'id': 'phase1',
                'type': 'agent_task',
                'name': '一级并行研判',
                'source': 'assigned',
                'parallel': True
            },
            {
                'id': 'meeting',
                'type': 'meeting',
                'name': '强制讨论',
                'mode': 'always',
                'max_rounds': 2
            },
            {'id': 'decision', 'type': 'director_synthesis', 'name': '总监裁决'},
        ]

        return {
            'auto': p_auto,
            'director_only': p_director,
            'custom_fast': p_fast,
            'custom_deep': p_deep,
        }

    def _normalize_workflow_step(self, step, idx):
        if not isinstance(step, dict):
            step = {}
        stype = str(step.get('type') or '').strip()
        allowed = ('assign_by_router', 'agent_task', 'meeting', 'director_synthesis', 'director_direct')
        if stype not in allowed:
            stype = 'director_synthesis'

        name = str(step.get('name') or '').strip() or ('步骤%d' % (idx + 1))
        item = {
            'id': str(step.get('id') or ('step_%d' % (idx + 1)))[:40],
            'type': stype,
            'name': name[:64],
        }
        if stype == 'agent_task':
            source = str(step.get('source') or 'assigned').strip()
            if source not in ('assigned', 'fixed'):
                source = 'assigned'
            agents = step.get('agents') if isinstance(step.get('agents'), list) else []
            agents = [a for a in agents if a in self.specialists_all]
            seen = set()
            dedup = []
            for aid in agents:
                if aid not in seen:
                    seen.add(aid)
                    dedup.append(aid)
            item['source'] = source
            item['parallel'] = self._to_bool(step.get('parallel', True), True)
            item['agents'] = dedup
        elif stype == 'meeting':
            mode = str(step.get('mode') or 'auto').strip().lower()
            if mode not in ('auto', 'always', 'never'):
                mode = 'auto'
            participants = step.get('participants') if isinstance(step.get('participants'), list) else []
            participants = [a for a in participants if a in self.specialists_all]
            max_rounds = self._to_int(step.get('max_rounds', 1), 1, 1)
            max_rounds = min(6, max_rounds)
            item['mode'] = mode
            item['participants'] = participants
            item['max_rounds'] = max_rounds
        elif stype == 'director_direct':
            item['stop_after'] = self._to_bool(step.get('stop_after', True), True)
            item['max_tool_rounds'] = min(
                4, self._to_int(step.get('max_tool_rounds', 1), 1, 1)
            )
        return item

    def _normalize_user_ask_workflow(self, cfg):
        base = self._default_user_ask_workflow()
        data = cfg if isinstance(cfg, dict) else {}

        mode = str(data.get('mode') or base['mode']).strip()
        if mode not in ('auto', 'director_only', 'custom'):
            mode = 'auto'

        limits = data.get('limits') if isinstance(data.get('limits'), dict) else {}
        max_parallel_agents = min(
            max(1, self._to_int(limits.get('max_parallel_agents', 4), 4, 1)),
            max(1, len(self.specialists_all))
        )
        max_meeting_rounds = min(max(1, self._to_int(limits.get('max_meeting_rounds', 2), 2, 1)), 6)
        timeout_seconds = self._to_int(
            limits.get('timeout_seconds', TEAM_WORKFLOW_DEFAULT_TIMEOUT),
            TEAM_WORKFLOW_DEFAULT_TIMEOUT,
            60
        )
        timeout_seconds = min(max(60, timeout_seconds), 3600)

        raw_steps = data.get('custom_steps')
        if not isinstance(raw_steps, list):
            raw_steps = base['custom_steps']
        steps = []
        for idx, step in enumerate(raw_steps[:16]):
            steps.append(self._normalize_workflow_step(step, idx))
        if not steps:
            steps = self._copy_json(base['custom_steps'], [])

        normalized = {
            'mode': mode,
            'name': str(data.get('name') or base['name'])[:64],
            'auto_for_simple': self._to_bool(data.get('auto_for_simple', base['auto_for_simple']), True),
            'custom_steps': steps,
            'limits': {
                'max_parallel_agents': max_parallel_agents,
                'max_meeting_rounds': max_meeting_rounds,
                'timeout_seconds': timeout_seconds,
            }
        }
        return normalized

    def _snapshot_runtime_config(self):
        return {
            'cycle_interval': int(self.cycle_interval),
            'paused': bool(self.paused),
            'manual_only': bool(self.manual_only),
            'idle_enabled': bool(self.idle_enabled),
            'idle_interval': int(self.idle_interval),
            'office_chat_enabled': bool(self.office_chat_enabled),
            'office_chat_interval': int(self.office_chat_interval),
            'user_ask_workflow': self._copy_json(self.user_ask_workflow, self._default_user_ask_workflow()),
        }

    def _apply_runtime_config(self, data):
        if not isinstance(data, dict):
            return
        self.cycle_interval = self._to_int(
            data.get('cycle_interval', self.cycle_interval),
            self.cycle_interval,
            300
        )
        self.manual_only = self._to_bool(data.get('manual_only', self.manual_only), self.manual_only)
        self.paused = self._to_bool(data.get('paused', self.paused), self.paused)
        if self.manual_only:
            self.paused = True
        self.idle_enabled = self._to_bool(data.get('idle_enabled', self.idle_enabled), self.idle_enabled)
        self.idle_interval = self._to_int(
            data.get('idle_interval', self.idle_interval),
            self.idle_interval,
            300
        )
        self.office_chat_enabled = self._to_bool(
            data.get('office_chat_enabled', self.office_chat_enabled),
            self.office_chat_enabled
        )
        self.office_chat_interval = self._to_int(
            data.get('office_chat_interval', self.office_chat_interval),
            self.office_chat_interval,
            600
        )
        self.user_ask_workflow = self._normalize_user_ask_workflow(
            data.get('user_ask_workflow', self.user_ask_workflow)
        )

    def _persist_runtime_config(self):
        payload = self._snapshot_runtime_config()
        payload['updated_at'] = int(time.time())
        os.makedirs(os.path.dirname(self._config_path), exist_ok=True)
        tmp_path = self._config_path + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self._config_path)
        try:
            self._config_mtime = os.path.getmtime(self._config_path)
        except OSError:
            self._config_mtime = time.time()

    def _load_persisted_config(self):
        os.makedirs(os.path.dirname(self._config_path), exist_ok=True)
        if not os.path.exists(self._config_path):
            self._persist_runtime_config()
            return
        try:
            with open(self._config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self._apply_runtime_config(data)
            try:
                self._config_mtime = os.path.getmtime(self._config_path)
            except OSError:
                self._config_mtime = time.time()
        except Exception as e:
            print('[Orchestrator] 读取持久化配置失败: %s' % str(e))

    def _sync_runtime_config(self):
        try:
            mtime = os.path.getmtime(self._config_path)
        except OSError:
            return
        if mtime <= self._config_mtime:
            return
        with self._lock:
            try:
                with open(self._config_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self._apply_runtime_config(data)
                self._config_mtime = mtime
            except Exception as e:
                print('[Orchestrator] 同步持久化配置失败: %s' % str(e))

    def get_user_ask_workflow(self):
        self._sync_runtime_config()
        with self._lock:
            return self._copy_json(self.user_ask_workflow, self._default_user_ask_workflow())

    def get_user_ask_workflow_presets(self):
        presets = self._workflow_presets()
        return self._copy_json(presets, {})

    def set_user_ask_workflow(self, cfg):
        normalized = self._normalize_user_ask_workflow(cfg)
        with self._lock:
            self.user_ask_workflow = normalized
            self._persist_runtime_config()
        bus.post_activity(
            'orchestrator', 'status',
            '直连问答工作流已更新：%s' % normalized.get('name', '未命名'),
            metadata={
                'mode': 'user_ask',
                'phase': 'user_ask_workflow_updated',
                'workflow_mode': normalized.get('mode'),
                'workflow_name': normalized.get('name'),
            }
        )
        return self._copy_json(normalized, self._default_user_ask_workflow())

    def run_forever(self):
        """主循环：定期触发研究周期"""
        self.running = True
        print('[Orchestrator] 调度器主循环启动，间隔=%d秒' % self.cycle_interval)

        while self.running:
            try:
                self._sync_runtime_config()
                if not self.paused:
                    try:
                        from AlphaFin.ai_team.core.agent_registry import has_active_workflow
                        if has_active_workflow({'user_ask'}):
                            time.sleep(2)
                            continue
                    except Exception:
                        pass
                    # 如有会话在运行，等待其完成
                    if self.current_session:
                        time.sleep(2)
                        continue

                    elapsed = time.time() - self.last_cycle_time
                    if elapsed >= self.cycle_interval:
                        self.run_cycle()
                        continue

                    if self.idle_enabled:
                        idle_elapsed = time.time() - self.last_idle_time
                        if idle_elapsed >= self.idle_interval:
                            self.run_idle_cycle()
                            continue

                    if self.office_chat_enabled:
                        office_elapsed = time.time() - self.last_office_chat_time
                        if office_elapsed >= self.office_chat_interval:
                            self.run_office_chat_cycle()
                time.sleep(5)
            except Exception as e:
                print('[Orchestrator] 主循环异常: %s' % str(e))
                traceback.print_exc()
                time.sleep(30)

    @staticmethod
    def _is_portfolio_busy():
        """投资调度正在执行时，不启动同事闲聊，避免争抢智能体。"""
        try:
            from AlphaFin.ai_team.core.portfolio_scheduler import portfolio_scheduler
            return bool(portfolio_scheduler.current_session)
        except Exception:
            return False

    @staticmethod
    def _agent_display_name(agent_id):
        return {
            'director': '决策总监',
            'analyst': '分析师',
            'risk': '风控官',
            'intel': '情报员',
            'quant': '量化师',
            'auditor': '审计员',
            'restructuring': '重组专家',
        }.get(str(agent_id or ''), str(agent_id or ''))

    def _format_agent_names(self, agent_ids, sep='、'):
        rows = [self._agent_display_name(aid) for aid in (agent_ids or []) if aid]
        return sep.join(rows)

    def _phase_step_label(self, base, agent_ids):
        labels = self._format_agent_names(agent_ids, sep='/')
        if not labels:
            return str(base or '')
        return '%s（%s）' % (str(base or ''), labels)

    def _build_agent_task_assignments(self, topic, required_agents, raw_agent_tasks=None):
        raw_agent_tasks = raw_agent_tasks if isinstance(raw_agent_tasks, dict) else {}
        topic_text = str(topic or '当前研究主题').strip() or '当前研究主题'
        fallback = {
            'intel': '围绕%s收集新闻、政策、行业动态与催化事件，并提炼最关键的情报变化。' % topic_text,
            'quant': '围绕%s做量化与市场结构判断，给出当前信号强弱、节奏位置和关键观察位。' % topic_text,
            'analyst': '围绕%s做基本面、技术面与资金面分析，梳理支持逻辑、反证路径和关键结论。' % topic_text,
            'restructuring': '围绕%s检查重组、资产注入、股权变化与资本运作催化，区分事实、预期与风险。' % topic_text,
            'risk': '基于前序研究，评估%s的核心风险、失效条件、仓位边界与风控建议。' % topic_text,
            'auditor': '审计本轮关于%s的证据链与逻辑链，指出需要复核的数据口径、漏洞与盲点。' % topic_text,
        }
        out = {}
        for aid in (required_agents or []):
            if aid not in self.specialists_all:
                continue
            task_text = str(raw_agent_tasks.get(aid) or '').strip()
            out[aid] = task_text or fallback.get(aid, '围绕%s完成你的专业分析任务。' % topic_text)
        return out

    def _format_assignment_lines(self, agent_tasks, agent_ids):
        lines = []
        tasks = agent_tasks if isinstance(agent_tasks, dict) else {}
        for aid in (agent_ids or []):
            if aid not in tasks:
                continue
            task_text = str(tasks.get(aid) or '').strip()
            if not task_text:
                continue
            lines.append('%s：%s' % (self._agent_display_name(aid), task_text))
        return lines

    def run_cycle(self, topic=None, time_limit_seconds=None):
        """
        执行一轮完整的研究周期（智能路由版）。

        Director先判断任务类型：
        - chat: 简单对话，Director直接回答
        - analysis: 定向分析，只派相关智能体
        - research: 全面研究，完整流程
        """
        session_id = str(uuid.uuid4())[:8]
        clear_session_cancel(session_id)
        try:
            from AlphaFin.ai_team.core.agent_registry import clear_stop_all_agents
            clear_stop_all_agents()
        except Exception:
            pass
        self.current_session = session_id
        self.cycle_count += 1
        self.last_cycle_time = time.time()
        progress_steps = ['总监拆解与分工', '阶段1研究', '阶段2评估', '必要会议', '总监报告']
        self._start_session_deadline(
            session_id,
            'auto_research',
            title=str(topic or '手动研究')[:80],
            time_limit_seconds=time_limit_seconds,
            source='manual_research' if topic else 'auto_research'
        )
        self._set_session_progress(
            session_id=session_id,
            workflow='auto_research',
            title=str(topic or '手动研究')[:80],
            steps=progress_steps,
            current_index=1,
            current_step='总监拆解与分工',
            detail='决策总监正在拆解问题、明确子任务，并安排各智能体分工。',
            state='running',
            actor='director',
        )
        self._trace_start_session(session_id, workflow='auto_research', topic=topic or '')
        run_status = 'ok'
        run_meta = {'workflow': 'auto_research', 'cycle_count': self.cycle_count}

        budget_snapshot = self._get_budget_snapshot(session_id)
        if budget_snapshot.get('level') == 'exhausted':
            self._notify_budget(session_id, budget_snapshot, force=True)
            bus.post_activity(
                'orchestrator', 'error',
                'Token 预算已耗尽，本轮自动研究取消。',
                metadata={'session_id': session_id, 'topic': topic, 'budget': budget_snapshot}
            )
            self._trace_finish_session(
                session_id,
                status='error',
                meta={'workflow': 'auto_research', 'reason': 'budget_exhausted'}
            )
            self.current_session = None
            return

        bus.post_activity('orchestrator', 'status',
                          '研究周期 #%d 启动 (session=%s)' % (self.cycle_count, session_id),
                          metadata={'session_id': session_id, 'topic': topic})

        from AlphaFin.ai_team.core.agent_registry import get_agent

        try:
            if self._session_stop_requested(session_id):
                run_status = 'error'
                run_meta = {'workflow': 'auto_research', 'reason': 'stopped_before_start'}
                self._post_session_stopped(session_id, 'auto_research')
                return
            director = get_agent('director')

            # ── 阶段0：Director 智能路由 ──
            bus.post_activity('orchestrator', 'status', '决策总监正在拆解任务并安排分工...')

            if director and topic:
                routing = self._route_task(director, topic, session_id)
            else:
                # 无主题时默认走完整研究流程
                routing = {
                    'task_type': 'research',
                    'direct_answer': '',
                    'required_agents': list(self.specialists_all),
                    'task_plan': '全市场扫描研究',
                    'reason': '无特定主题，执行全面扫描'
                }

            task_type = routing.get('task_type', 'research')

            # ── chat: Director 直接回答 ──
            if task_type == 'chat':
                if self._session_stop_requested(session_id):
                    run_status = 'error'
                    run_meta = {'workflow': 'auto_research', 'reason': 'stopped_during_chat'}
                    self._post_session_stopped(session_id, 'auto_research')
                    return
                answer = routing.get('direct_answer', '')
                bus.post_activity('director', 'speaking', answer,
                                  metadata={'phase': 'direct_answer', 'title': '直接回答'})
                bus.save_result(session_id, 'director_answer', answer)
                save_report(
                    report_type='chat',
                    title='对话: %s' % (topic[:30] if topic else ''),
                    content=answer,
                    participants=['director']
                )
                bus.broadcast('director', 'consensus', '已直接回答',
                              metadata={'phase': 'chat', 'title': '对话回复'})
                self._set_session_progress(
                    session_id=session_id,
                    workflow='auto_research',
                    title=str(topic or '手动研究')[:80],
                    steps=progress_steps,
                    current_index=len(progress_steps),
                    current_step='总监报告',
                    detail='本轮任务已由总监直接完成。',
                    state='completed',
                    actor='director',
                )
                return

            # ── analysis / research: 分阶段执行 ──
            required = [
                a for a in routing.get('required_agents', list(self.specialists_all))
                if a in self.specialists_all
            ]
            if not required:
                required = list(self.specialists_all)
            agent_tasks = self._build_agent_task_assignments(
                topic,
                required,
                routing.get('agent_tasks')
            )
            task_plan = routing.get('task_plan', '')
            phase1_agents = [a for a in required if a in self.specialists_phase1]
            phase2_agents = [a for a in required if a in self.specialists_phase2]
            progress_steps = [
                '总监拆解与分工',
                self._phase_step_label('阶段1研究', phase1_agents),
                self._phase_step_label('阶段2评估', phase2_agents),
                '必要会议',
                '总监报告',
            ]
            route_lines = []
            route_lines.extend(self._format_assignment_lines(agent_tasks, phase1_agents))
            route_lines.extend(self._format_assignment_lines(agent_tasks, phase2_agents))
            route_detail = '决策总监已完成任务拆解。'
            if route_lines:
                route_detail += ' 当前分工：' + '；'.join(route_lines[:6])
            self._set_session_progress(
                session_id=session_id,
                workflow='auto_research',
                title=str(topic or '手动研究')[:80],
                steps=progress_steps,
                current_index=1,
                current_step='总监拆解与分工',
                detail=route_detail,
                state='running',
                actor='director',
            )
            bus.post_activity(
                'director', 'status',
                route_detail,
                metadata={
                    'phase': 'task_plan',
                    'title': '任务拆解',
                    'mode': 'auto_research',
                    'session_id': session_id,
                    'required_agents': required,
                    'phase1_agents': phase1_agents,
                    'phase2_agents': phase2_agents,
                    'agent_tasks': agent_tasks,
                }
            )

            # 广播任务计划
            assignment_text = ''
            if agent_tasks:
                assignment_lines = self._format_assignment_lines(agent_tasks, required)
                if assignment_lines:
                    assignment_text = '\n\n【本轮分工】\n- ' + '\n- '.join(assignment_lines)
            if task_plan or assignment_text:
                bus.broadcast('director', 'task', (task_plan or '决策总监任务拆解如下。') + assignment_text,
                              metadata={'phase': 0, 'title': '任务分解',
                                        'task_type': task_type,
                                        'required_agents': required,
                                        'agent_tasks': agent_tasks})

            # 阶段1：第一批智能体并行工作（情报/量化/分析）
            if phase1_agents:
                self._set_session_progress(
                    session_id=session_id,
                    workflow='auto_research',
                    title=str(topic or '手动研究')[:80],
                    steps=progress_steps,
                    current_index=2,
                    current_step=self._phase_step_label('阶段1研究', phase1_agents),
                    detail='第一阶段分工：%s' % '；'.join(self._format_assignment_lines(agent_tasks, phase1_agents)),
                    state='running',
                    actor='orchestrator',
                )
                bus.post_activity('orchestrator', 'status',
                                  '阶段1: %s 并行工作' % self._format_agent_names(phase1_agents, sep='/'))
                self._run_parallel(phase1_agents, session_id, topic, agent_assignments=agent_tasks)
                if self._session_stop_requested(session_id):
                    run_status = 'error'
                    run_meta = {'workflow': 'auto_research', 'reason': 'stopped_after_phase1'}
                    self._post_session_stopped(session_id, 'auto_research')
                    return
                deadline_action = self._handle_session_expired(
                    session_id,
                    'auto_research',
                    title=str(topic or '手动研究')[:80]
                )
                if deadline_action == 'stop':
                    run_status = 'error'
                    run_meta = {'workflow': 'auto_research', 'reason': 'stopped_on_overtime_gate'}
                    self._post_session_stopped(session_id, 'auto_research')
                    return
                if self._session_should_converge(session_id):
                    self._maybe_announce_session_converging(session_id, 'auto_research')
                    phase2_agents = []
                    if deadline_action == 'summarize':
                        phase2_agents = []
                else:
                    phase2_agents = [a for a in required if a in self.specialists_phase2]
            else:
                phase2_agents = [a for a in required if a in self.specialists_phase2]
            # 阶段2：第二批智能体并行评估（风控/审计）
            if phase2_agents:
                self._set_session_progress(
                    session_id=session_id,
                    workflow='auto_research',
                    title=str(topic or '手动研究')[:80],
                    steps=progress_steps,
                    current_index=3,
                    current_step=self._phase_step_label('阶段2评估', phase2_agents),
                    detail='第二阶段分工：%s' % '；'.join(self._format_assignment_lines(agent_tasks, phase2_agents)),
                    state='running',
                    actor='orchestrator',
                )
                bus.post_activity('orchestrator', 'status',
                                  '阶段2: %s 并行评估' % self._format_agent_names(phase2_agents, sep='/'))
                self._run_parallel(phase2_agents, session_id, agent_assignments=agent_tasks)
                if self._session_stop_requested(session_id):
                    run_status = 'error'
                    run_meta = {'workflow': 'auto_research', 'reason': 'stopped_after_phase2'}
                    self._post_session_stopped(session_id, 'auto_research')
                    return
                deadline_action = self._handle_session_expired(
                    session_id,
                    'auto_research',
                    title=str(topic or '手动研究')[:80]
                )
                if deadline_action == 'stop':
                    run_status = 'error'
                    run_meta = {'workflow': 'auto_research', 'reason': 'stopped_on_overtime_gate'}
                    self._post_session_stopped(session_id, 'auto_research')
                    return
            else:
                deadline_action = 'continue'
            if self._session_should_converge(session_id):
                self._maybe_announce_session_converging(session_id, 'auto_research')
            elif deadline_action != 'summarize':
                # 阶段3：会议讨论（由总监或智能体自主决定）
                self._set_session_progress(
                    session_id=session_id,
                    workflow='auto_research',
                    title=str(topic or '手动研究')[:80],
                    steps=progress_steps,
                    current_index=4,
                    current_step='必要会议',
                    detail='系统正在判断是否需要会议，并在必要时组织讨论。',
                    state='running',
                    actor='director',
                )
                self._maybe_run_meeting(
                    session_id=session_id,
                    topic=topic or task_plan or '团队研究议题',
                    mode='auto_research',
                    candidate_participants=required,
                    director=director
                )
                if self._session_stop_requested(session_id):
                    run_status = 'error'
                    run_meta = {'workflow': 'auto_research', 'reason': 'stopped_after_meeting'}
                    self._post_session_stopped(session_id, 'auto_research')
                    return
                deadline_action = self._handle_session_expired(
                    session_id,
                    'auto_research',
                    title=str(topic or '手动研究')[:80]
                )
                if deadline_action == 'stop':
                    run_status = 'error'
                    run_meta = {'workflow': 'auto_research', 'reason': 'stopped_on_overtime_gate'}
                    self._post_session_stopped(session_id, 'auto_research')
                    return

            # 阶段4：Director 综合报告
            self._set_session_progress(
                session_id=session_id,
                workflow='auto_research',
                title=str(topic or '手动研究')[:80],
                steps=progress_steps,
                current_index=5,
                current_step='总监报告',
                detail='决策总监正在基于已完成材料生成最终研究报告。',
                state='running',
                actor='director',
            )
            bus.post_activity('orchestrator', 'status', '阶段4: 决策总监综合报告')
            if director:
                self._phase_synthesis(director, session_id)
            self._set_session_progress(
                session_id=session_id,
                workflow='auto_research',
                title=str(topic or '手动研究')[:80],
                steps=progress_steps,
                current_index=5,
                current_step='总监报告',
                detail='最终研究报告已生成。',
                state='completed',
                actor='director',
            )

        except Exception as e:
            run_status = 'error'
            run_meta = {'workflow': 'auto_research', 'error': str(e)}
            bus.post_activity('orchestrator', 'error',
                              '研究周期 #%d 异常中止: %s' % (self.cycle_count, str(e)))
            traceback.print_exc()
        finally:
            bus.clear_session(session_id)
            self.current_session = None
            self._clear_session_deadline(session_id)
            clear_session_overtime_state(session_id)
            self._clear_session_progress(session_id)
            bus.post_activity('orchestrator', 'status',
                              '研究周期 #%d 结束' % self.cycle_count)
            self._trace_finish_session(
                session_id,
                status=run_status,
                meta=run_meta
            )

    def run_idle_cycle(self, theme=None):
        """
        执行闲时自学习周期（只读学习，不允许交易/审批）。

        目标：
        - 没有用户任务时持续学习和复盘
        - 产出结构化学习纪要并写入长期记忆
        - 严格禁止提交交易信号
        """
        from AlphaFin.ai_team.core.agent_registry import get_agent

        if self.current_session:
            return

        try:
            from AlphaFin.ai_team.core.agent_registry import clear_stop_all_agents
            clear_stop_all_agents()
        except Exception:
            pass
        session_id = 'idle_' + str(uuid.uuid4())[:8]
        clear_session_cancel(session_id)
        self.current_session = session_id
        self.idle_count += 1
        self.last_idle_time = time.time()
        self._trace_start_session(session_id, workflow='idle_learning', topic=theme or '')
        run_status = 'ok'
        run_meta = {'workflow': 'idle_learning', 'idle_count': self.idle_count}

        # 轮询学习主题
        if not theme:
            idx = (self.idle_count - 1) % max(len(self.idle_topics), 1)
            theme = self.idle_topics[idx] if self.idle_topics else '市场结构与风险学习'

        bus.post_activity(
            'orchestrator', 'status',
            '闲时学习 #%d 启动 (session=%s): %s' % (self.idle_count, session_id, theme),
            metadata={'session_id': session_id, 'theme': theme, 'mode': 'idle_learning'}
        )

        budget_snapshot = self._get_budget_snapshot(session_id)
        if budget_snapshot.get('level') == 'exhausted':
            self._notify_budget(session_id, budget_snapshot, force=True)
            run_status = 'error'
            run_meta = {'workflow': 'idle_learning', 'reason': 'budget_exhausted'}
            bus.post_activity(
                'orchestrator', 'error',
                'Token 预算已耗尽，本轮闲时学习取消。',
                metadata={'session_id': session_id, 'mode': 'idle_learning', 'budget': budget_snapshot}
            )
            bus.clear_session(session_id)
            self.current_session = None
            self._trace_finish_session(session_id, status=run_status, meta=run_meta)
            return

        try:
            if self._session_stop_requested(session_id):
                run_status = 'error'
                run_meta = {'workflow': 'idle_learning', 'reason': 'stopped_before_start'}
                self._post_session_stopped(session_id, 'idle_learning')
                return
            phase1_agents = list(self.specialists_all)
            self._run_parallel(
                phase1_agents,
                session_id,
                topic=theme,
                prompt_builder=self._get_idle_prompt,
                allowed_tools=self.idle_allowed_tools,
                blocked_tools=self.idle_blocked_tools,
                result_suffix='_idle',
                mode='idle_learning'
            )
            if self._session_stop_requested(session_id):
                run_status = 'error'
                run_meta = {'workflow': 'idle_learning', 'reason': 'stopped_after_parallel'}
                self._post_session_stopped(session_id, 'idle_learning')
                return

            director = get_agent('director')
            # 闲时学习是否开会由总监与智能体协作自主决定
            self._maybe_run_meeting(
                session_id=session_id,
                topic=theme,
                mode='idle_learning',
                candidate_participants=phase1_agents,
                director=director,
                allowed_tools=self.idle_allowed_tools,
                blocked_tools=self.idle_blocked_tools
            )
            if self._session_stop_requested(session_id):
                run_status = 'error'
                run_meta = {'workflow': 'idle_learning', 'reason': 'stopped_after_meeting'}
                self._post_session_stopped(session_id, 'idle_learning')
                return

            if director:
                self._phase_idle_synthesis(
                    director, session_id, theme,
                    allowed_tools=self.idle_allowed_tools,
                    blocked_tools=self.idle_blocked_tools
                )
        except Exception as e:
            run_status = 'error'
            run_meta = {'workflow': 'idle_learning', 'error': str(e)}
            bus.post_activity(
                'orchestrator', 'error',
                '闲时学习 #%d 异常中止: %s' % (self.idle_count, str(e))
            )
            traceback.print_exc()
        finally:
            bus.clear_session(session_id)
            self.current_session = None
            bus.post_activity(
                'orchestrator', 'status',
                '闲时学习 #%d 结束' % self.idle_count,
                metadata={'mode': 'idle_learning'}
            )
            self._trace_finish_session(session_id, status=run_status, meta=run_meta)

    def run_office_chat_cycle(self, topic=None):
        """
        执行一轮“同事闲聊”：
        - 仅用于团队沟通氛围与经验交换
        - 禁止交易执行、信号审批
        - 投资任务繁忙时自动跳过
        """
        if self.current_session or self._is_portfolio_busy():
            return

        from AlphaFin.ai_team.core.agent_registry import get_agent

        try:
            from AlphaFin.ai_team.core.agent_registry import clear_stop_all_agents
            clear_stop_all_agents()
        except Exception:
            pass
        session_id = 'chat_' + str(uuid.uuid4())[:8]
        clear_session_cancel(session_id)
        self.current_session = session_id
        self.office_chat_count += 1
        self.last_office_chat_time = time.time()
        self._trace_start_session(session_id, workflow='office_chat', topic=topic or '')
        run_status = 'ok'
        run_meta = {'workflow': 'office_chat', 'office_chat_count': self.office_chat_count}

        if not topic:
            if self.office_chat_topics:
                topic = random.choice(self.office_chat_topics)
            else:
                topic = random.choice([
                    '最近市场里最有戏剧性的叙事',
                    '你最想提醒同事注意的一条宏观变量',
                    '哪个行业变化最可能超预期',
                ])

        # 每次随机抽3人闲聊，避免日志过载
        candidate_ids = list(self.specialists_all)
        selected_ids = random.sample(candidate_ids, min(3, len(candidate_ids)))

        bus.post_activity(
            'orchestrator', 'status',
            '同事闲聊 #%d 启动 (session=%s): %s' % (self.office_chat_count, session_id, topic),
            metadata={
                'session_id': session_id,
                'topic': topic,
                'mode': 'office_chat'
            }
        )

        budget_snapshot = self._get_budget_snapshot(session_id)
        if budget_snapshot.get('level') == 'exhausted':
            self._notify_budget(session_id, budget_snapshot, force=True)
            run_status = 'error'
            run_meta = {'workflow': 'office_chat', 'reason': 'budget_exhausted'}
            bus.post_activity(
                'orchestrator', 'error',
                'Token 预算已耗尽，本轮同事闲聊取消。',
                metadata={'session_id': session_id, 'mode': 'office_chat', 'budget': budget_snapshot}
            )
            bus.clear_session(session_id)
            self.current_session = None
            self._trace_finish_session(session_id, status=run_status, meta=run_meta)
            return

        history_lines = []
        try:
            for agent_id in selected_ids:
                if self._session_stop_requested(session_id):
                    run_status = 'error'
                    run_meta = {'workflow': 'office_chat', 'reason': 'stopped_mid_cycle'}
                    self._post_session_stopped(session_id, 'office_chat')
                    return
                agent = get_agent(agent_id)
                if not agent:
                    continue

                agent.process_incoming_messages(session_id)
                prompt = self._get_office_chat_prompt(agent_id, topic, history_lines)
                reply = agent.think(
                    prompt,
                    session_id=session_id,
                    allowed_tools=['get_current_time'],
                    blocked_tools=[
                        'submit_trade_signal', 'review_trade_signal',
                        'create_skill', 'execute_skill', 'send_message_to_agent'
                    ]
                )

                short_reply = (reply or '').strip()

                history_lines.append('%s: %s' % (agent_id, short_reply))
                bus.save_result(session_id, 'office_chat_%s' % agent_id, short_reply)
                bus.broadcast(
                    agent_id, 'speaking', short_reply,
                    metadata={
                        'phase': 'office_chat',
                        'title': '同事闲聊',
                        'mode': 'office_chat'
                    }
                )

            director = get_agent('director')
            if director and history_lines:
                if self._session_stop_requested(session_id):
                    run_status = 'error'
                    run_meta = {'workflow': 'office_chat', 'reason': 'stopped_before_summary'}
                    self._post_session_stopped(session_id, 'office_chat')
                    return
                summary_prompt = format_ai_team_prompt(
                    'workflow.office_chat_summary',
                    topic=topic,
                    history='\n'.join(history_lines)
                )
                summary = director.think(
                    summary_prompt,
                    session_id=session_id,
                    allowed_tools=['get_current_time'],
                    blocked_tools=[
                        'submit_trade_signal', 'review_trade_signal',
                        'create_skill', 'execute_skill', 'send_message_to_agent'
                    ]
                )
                bus.save_result(session_id, 'office_chat_summary', summary)
                bus.broadcast(
                    'director', 'consensus', summary,
                    metadata={
                        'phase': 'office_chat_summary',
                        'title': '闲聊共识',
                        'mode': 'office_chat'
                    }
                )
        except Exception as e:
            run_status = 'error'
            run_meta = {'workflow': 'office_chat', 'error': str(e)}
            bus.post_activity(
                'orchestrator', 'error',
                '同事闲聊 #%d 异常中止: %s' % (self.office_chat_count, str(e)),
                metadata={'mode': 'office_chat'}
            )
            traceback.print_exc()
        finally:
            bus.clear_session(session_id)
            self.current_session = None
            bus.post_activity(
                'orchestrator', 'status',
                '同事闲聊 #%d 结束' % self.office_chat_count,
                metadata={'mode': 'office_chat'}
            )
            self._trace_finish_session(session_id, status=run_status, meta=run_meta)

    def _route_task(self, director, topic, session_id):
        """Director 判断任务类型并返回路由决策"""
        prompt = format_ai_team_prompt('workflow.route_task', topic=topic)

        raw = director.think(
            prompt,
            session_id=session_id,
            # 路由阶段只做任务拆解与分工，不允许总监先跑工具
            max_tool_rounds=0,
            allowed_tools=[],
            response_style='team'
        )
        routing = self._parse_routing(raw)
        required = [
            aid for aid in (routing.get('required_agents') or [])
            if aid in self.specialists_all
        ]
        if routing.get('task_type') != 'chat' and not required:
            required = list(self.specialists_all)
        routing['required_agents'] = required
        routing['agent_tasks'] = self._build_agent_task_assignments(
            topic,
            required,
            routing.get('agent_tasks')
        )
        bus.save_result(session_id, 'director_routing', json.dumps(routing, ensure_ascii=False))
        return routing

    def _parse_routing(self, raw_text):
        """从 Director 回复中提取 JSON 路由决策，容错处理"""
        try:
            # 尝试直接解析
            return json.loads(raw_text.strip())
        except (json.JSONDecodeError, ValueError):
            pass

        # 尝试从文本中提取 JSON 块
        patterns = [
            r'```json\s*(.*?)\s*```',
            r'```\s*(.*?)\s*```',
            r'(\{[^{}]*"task_type"[^{}]*\})',
        ]
        for pattern in patterns:
            match = re.search(pattern, raw_text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1).strip())
                except (json.JSONDecodeError, ValueError):
                    continue

        # 解析失败，默认走完整研究流程
        print('[Orchestrator] 路由JSON解析失败，默认走research流程。原始回复: %s' % raw_text[:200])
        return {
            'task_type': 'research',
            'direct_answer': '',
            'required_agents': list(self.specialists_all),
            'task_plan': raw_text,
            'reason': 'JSON解析失败，降级为完整研究'
        }

    @staticmethod
    def _extract_json_obj(raw_text, required_key=None):
        text = (raw_text or '').strip()
        if not text:
            return {}
        try:
            obj = json.loads(text)
            if isinstance(obj, dict) and (not required_key or required_key in obj):
                return obj
        except Exception:
            pass

        patterns = [
            r'```json\s*(\{.*?\})\s*```',
            r'```\s*(\{.*?\})\s*```',
            r'(\{.*\})',
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.DOTALL)
            if not m:
                continue
            cand = (m.group(1) or '').strip()
            if not cand:
                continue
            try:
                obj = json.loads(cand)
                if isinstance(obj, dict) and (not required_key or required_key in obj):
                    return obj
            except Exception:
                continue
        return {}

    def _normalize_meeting_participants(self, participants, fallback=None):
        fallback = fallback or []
        source = participants if isinstance(participants, list) else fallback
        seen = set()
        clean = []
        for aid in source:
            if aid in self.specialists_all and aid not in seen:
                seen.add(aid)
                clean.append(aid)
        return clean

    def _summarize_session_results(self, session_id, max_items=12, max_len=220):
        results = bus.get_session_results(session_id)
        rows = []
        for rid, rcontent in results.items():
            txt = (rcontent or '').replace('\n', ' ').strip()
            if not txt:
                continue
            rows.append('【%s】%s' % (rid, txt[:max_len]))
            if len(rows) >= max_items:
                break
        return '\n'.join(rows) if rows else '暂无阶段成果。'

    def _extract_meeting_requests(self, session_id, candidate_participants=None):
        candidate_set = set(candidate_participants or [])
        cues = (
            '建议开会', '需要开会', '需开会', '建议会议', '会议讨论',
            '提议开会', '召开会议', '[会议建议]', 'MEETING_REQUIRED'
        )
        results = bus.get_session_results(session_id)
        requests = []
        for rid, rcontent in results.items():
            txt = (rcontent or '').strip()
            if not txt:
                continue
            if not any(c in txt for c in cues):
                continue
            owner = rid.split('_')[0] if '_' in rid else rid
            if owner not in self.specialists_all:
                continue
            if candidate_set and owner not in candidate_set:
                continue
            reason = ''
            for line in txt.splitlines():
                line = line.strip()
                if line and any(c in line for c in cues):
                    reason = line[:180]
                    break
            if not reason:
                reason = txt.replace('\n', ' ')[:180]
            requests.append({
                'agent_id': owner,
                'source': rid,
                'reason': reason
            })
        return requests

    def _parse_meeting_plan(self, raw_text, default_participants):
        obj = self._extract_json_obj(raw_text, required_key='need_meeting')
        if not obj:
            return {
                'need_meeting': False,
                'participants': list(default_participants),
                'reason': '',
                'focus': '',
            }
        return {
            'need_meeting': self._to_bool(obj.get('need_meeting'), False),
            'participants': self._normalize_meeting_participants(
                obj.get('participants'),
                fallback=default_participants
            ),
            'reason': str(obj.get('reason') or '').strip(),
            'focus': str(obj.get('focus') or '').strip(),
        }

    def _plan_meeting(self, director, session_id, topic, mode, candidate_participants,
                      allowed_tools=None, blocked_tools=None):
        candidates = self._normalize_meeting_participants(
            candidate_participants, fallback=list(self.specialists_all)
        )
        agent_requests = self._extract_meeting_requests(session_id, candidates)
        agent_need = bool(agent_requests)

        director_need = False
        director_reason = ''
        director_focus = ''
        director_parts = list(candidates)

        if director:
            context = self._summarize_session_results(session_id, max_items=10, max_len=180)
            req_text = '无'
            if agent_requests:
                req_lines = []
                for item in agent_requests[:8]:
                    req_lines.append('%s: %s' % (item['agent_id'], item['reason']))
                req_text = '\n'.join(req_lines)

            prompt = format_ai_team_prompt(
                'workflow.meeting_plan',
                topic=topic or '团队议题',
                mode=mode,
                candidates=','.join(candidates) if candidates else '-',
                context=context,
                req_text=req_text
            )
            raw = director.think(
                prompt,
                session_id=session_id,
                max_tool_rounds=1,
                allowed_tools=[],
                blocked_tools=blocked_tools
            )
            parsed = self._parse_meeting_plan(raw, default_participants=candidates)
            director_need = bool(parsed.get('need_meeting'))
            director_reason = parsed.get('reason', '')
            director_focus = parsed.get('focus', '')
            director_parts = parsed.get('participants') or list(candidates)

        if mode == 'user_ask' and agent_need:
            strong_cues = ('冲突', '矛盾', '分歧', '相反', '待核实', '证据不足', '无法定论')
            strong_requests = []
            for item in agent_requests:
                reason = str(item.get('reason') or '')
                if any(k in reason for k in strong_cues):
                    strong_requests.append(item)
            # 直连问答默认更快：至少2个成员明确指出冲突才因“成员建议”触发会议
            if len(strong_requests) < 2:
                agent_need = False

        need_meeting = bool(director_need or agent_need)
        if not need_meeting:
            reason = director_reason or '当前结论已可执行，无需额外会议。'
            return {
                'need_meeting': False,
                'participants': [],
                'reason': reason,
                'focus': director_focus,
                'source': 'none',
                'agent_requests': agent_requests,
            }

        participants = list(director_parts if director_need else candidates)
        if agent_need:
            for item in agent_requests:
                aid = item.get('agent_id')
                if aid in self.specialists_all and aid not in participants:
                    participants.append(aid)
        participants = self._normalize_meeting_participants(participants, fallback=candidates)
        if not participants:
            participants = self._normalize_meeting_participants(candidates, fallback=['analyst'])

        source_tags = []
        if director_need:
            source_tags.append('director')
        if agent_need:
            source_tags.append('agents')
        source = '+'.join(source_tags) if source_tags else 'director'

        reason_parts = []
        if director_reason:
            reason_parts.append('总监判断: %s' % director_reason)
        if agent_need:
            proposers = ','.join([x.get('agent_id', '') for x in agent_requests if x.get('agent_id')]) or '成员'
            reason_parts.append('智能体建议开会: %s' % proposers)
        reason = '；'.join(reason_parts) if reason_parts else '任务复杂，进入会议讨论。'

        return {
            'need_meeting': True,
            'participants': participants,
            'reason': reason,
            'focus': director_focus,
            'source': source,
            'agent_requests': agent_requests,
        }

    def _parse_meeting_continue(self, raw_text):
        obj = self._extract_json_obj(raw_text, required_key='continue_meeting')
        if not obj:
            return {
                'continue_meeting': False,
                'reason': '继续决策解析失败，默认结束会议。',
                'next_focus': ''
            }
        return {
            'continue_meeting': self._to_bool(obj.get('continue_meeting'), False),
            'reason': str(obj.get('reason') or '').strip(),
            'next_focus': str(obj.get('next_focus') or '').strip(),
        }

    def _should_continue_meeting(self, director, session_id, topic, transcript, round_no,
                                 mode, allowed_tools=None, blocked_tools=None):
        recent = '\n'.join(transcript[-12:]) if transcript else '（暂无发言）'
        unresolved_keys = ('分歧', '待核实', '证据不足', '不确定', '缺口', '矛盾')
        heur_continue = any(k in recent for k in unresolved_keys)

        if not director:
            return {
                'continue_meeting': False,
                'reason': '无总监主持，默认单轮结束。',
                'next_focus': ''
            }

        prompt = format_ai_team_prompt(
            'workflow.meeting_continue',
            topic=topic or '团队议题',
            mode=mode,
            round_no=round_no,
            recent=recent
        )
        raw = director.think(
            prompt,
            session_id=session_id,
            max_tool_rounds=1,
            allowed_tools=[],
            blocked_tools=blocked_tools
        )
        parsed = self._parse_meeting_continue(raw)
        if parsed.get('reason'):
            return parsed
        if heur_continue:
            parsed['continue_meeting'] = True
            parsed['reason'] = '检测到未决分歧，继续会议。'
        return parsed

    def _maybe_run_meeting(self, session_id, topic, mode, candidate_participants,
                           director=None, allowed_tools=None, blocked_tools=None,
                           max_rounds=None):
        if self._session_stop_requested(session_id):
            self._post_session_stopped(session_id, mode, title='会议已取消')
            return ''
        if self._session_should_converge(session_id):
            self._maybe_announce_session_converging(session_id, mode)
            return ''
        plan = self._plan_meeting(
            director=director,
            session_id=session_id,
            topic=topic,
            mode=mode,
            candidate_participants=candidate_participants,
            allowed_tools=allowed_tools,
            blocked_tools=blocked_tools
        )

        meta = {
            'phase': 'meeting_plan',
            'title': '会议决策',
            'mode': mode,
            'session_id': session_id,
            'meeting_topic': topic or '团队议题',
            'meeting_active': False,
            'participants': plan.get('participants') or candidate_participants or [],
            'meeting_reason': plan.get('reason', ''),
            'meeting_source': plan.get('source', 'none'),
        }

        if not plan.get('need_meeting'):
            bus.post_activity(
                'orchestrator', 'status',
                '本轮不召开会议：%s' % (plan.get('reason', '无')),
                metadata=meta
            )
            return ''

        bus.post_activity(
            'orchestrator', 'status',
            '会议触发（%s）：%s' % (
                plan.get('source', 'director'),
                (plan.get('reason') or '任务需要协同讨论')[:140]
            ),
            metadata=meta
        )
        return self._run_meeting(
            session_id=session_id,
            participants=plan.get('participants') or candidate_participants,
            meeting_topic=(plan.get('focus') or topic or '团队议题'),
            mode=mode,
            rounds=None,
            max_rounds=max_rounds,
            allowed_tools=allowed_tools,
            blocked_tools=blocked_tools
        )

    def _run_parallel(self, agent_ids, session_id, topic=None,
                      prompt_builder=None, allowed_tools=None,
                      blocked_tools=None, result_suffix='', mode='research',
                      agent_assignments=None):
        """多个智能体并行执行，全部完成后返回"""
        from AlphaFin.ai_team.core.agent_registry import get_agent

        if self._session_stop_requested(session_id):
            self._post_session_stopped(session_id, mode, title='并行任务已取消')
            return

        requested_count = len(agent_ids or [])
        cap, budget_snapshot = self._budget_parallel_cap(session_id, requested_count)
        if cap <= 0:
            bus.post_activity(
                'orchestrator', 'error',
                '预算限制：并行执行已暂停。',
                metadata={'session_id': session_id, 'mode': mode, 'budget': budget_snapshot}
            )
            return
        if requested_count > cap:
            agent_ids = list(agent_ids or [])[:cap]
            bus.post_activity(
                'orchestrator', 'status',
                '预算限制：并行智能体已降级为 %d 个（原 %d 个）' % (len(agent_ids), requested_count),
                metadata={'session_id': session_id, 'mode': mode, 'budget': budget_snapshot}
            )

        threads = []
        errors = {}

        def _agent_work(agent_id):
            try:
                if self._session_stop_requested(session_id):
                    return
                agent = get_agent(agent_id)
                if not agent:
                    errors[agent_id] = '智能体不可用'
                    return

                # 先处理收到的消息（包括Director的任务分解）
                agent.process_incoming_messages(session_id)
                if self._session_stop_requested(session_id):
                    return

                # 获取该智能体的工作 prompt
                if prompt_builder:
                    prompt = prompt_builder(agent_id, topic, session_id)
                else:
                    prompt = self._get_agent_prompt(agent_id, topic, session_id)
                assigned_task = ''
                if isinstance(agent_assignments, dict):
                    assigned_task = str(agent_assignments.get(agent_id) or '').strip()
                if assigned_task:
                    prompt = format_ai_team_prompt(
                        'workflow.assigned_task_wrapper',
                        assigned_task=assigned_task
                    ) + prompt
                if mode in ('auto_research', 'portfolio_investment', 'market_watch'):
                    prompt += format_ai_team_prompt('workflow.collaboration_note.research')
                elif mode == 'user_ask':
                    prompt += format_ai_team_prompt('workflow.collaboration_note.user_ask')

                result = agent.think(
                    prompt,
                    session_id=session_id,
                    allowed_tools=allowed_tools,
                    blocked_tools=blocked_tools
                )

                # 保存结果并广播
                result_key = agent_id + result_suffix if result_suffix else agent_id
                bus.save_result(session_id, result_key, result)
                bus.broadcast(agent_id, 'report', result,
                              metadata={
                                  'title': agent.name + ('学习纪要' if result_suffix else '报告'),
                                  'mode': mode
                              })

            except Exception as e:
                errors[agent_id] = str(e)
                bus.post_activity(agent_id, 'error', '执行失败: %s' % str(e))
                traceback.print_exc()

        for aid in agent_ids:
            t = threading.Thread(target=_agent_work, args=(aid,), daemon=True)
            threads.append(t)
            t.start()

        # 等待所有线程完成（10分钟超时）
        for t in threads:
            waited = 0.0
            while t.is_alive() and waited < 600:
                if self._session_stop_requested(session_id):
                    break
                if is_session_expired(session_id):
                    try:
                        from AlphaFin.ai_team.core.agent_registry import request_stop_agents_for_session
                        request_stop_agents_for_session(
                            session_id,
                            reason='当前阶段达到时限，等待用户决策'
                        )
                    except Exception:
                        pass
                    self._request_session_overtime(
                        session_id=session_id,
                        workflow=mode,
                        title=str(topic or '')[:80],
                        message='当前阶段运行时间已到，请选择继续等待，或立即停止任务。',
                        default_extend_seconds=300,
                    )
                    progress_snapshot = self._session_progress(session_id)
                    self._set_session_progress(
                        session_id=session_id,
                        workflow=str(progress_snapshot.get('workflow') or mode or ''),
                        title=str(progress_snapshot.get('title') or str(topic or '')[:80]),
                        steps=(progress_snapshot.get('steps') or []),
                        current_index=progress_snapshot.get('current_index') or 0,
                        current_step=progress_snapshot.get('current_step') or '等待用户决策',
                        detail='当前阶段运行时间已到，等待你选择继续等待或立即停止。',
                        state='waiting_user',
                        actor='orchestrator',
                    )
                    bus.post_activity(
                        'orchestrator', 'status',
                        '当前阶段已达到时限，暂停推进并等待用户决策。',
                        metadata={
                            'session_id': session_id or '',
                            'mode': mode or '',
                            'phase': 'session_overtime_waiting',
                            'session_timing': self._session_timing(session_id),
                            'session_progress': self._session_progress(session_id),
                            'session_overtime': self._session_overtime(session_id),
                        }
                    )
                    break
                t.join(timeout=0.5)
                waited += 0.5
            if is_session_expired(session_id):
                break

        if self._session_stop_requested(session_id):
            self._post_session_stopped(session_id, mode, title='并行任务已停止')
            return

        if errors:
            bus.post_activity('orchestrator', 'error',
                              '部分智能体执行失败: %s' % str(errors))

    def _get_agent_prompt(self, agent_id, topic, session_id):
        """根据智能体ID生成对应的工作prompt"""
        # 获取已有结果作为上下文
        results = bus.get_session_results(session_id)
        context = ''
        if results:
            context = '\n\n=== 已有团队研究成果 ===\n'
            for rid, rcontent in results.items():
                if rid != agent_id:
                    summary = rcontent[:500] if len(rcontent) > 500 else rcontent
                    context += '\n【%s】：\n%s\n' % (rid, summary)

        event_suffix = ''
        if self._is_event_prediction_query_text(topic):
            event_suffix = (
                '\n\n【前瞻事件推演硬约束】\n'
                '1. 对停牌/重组/定增/控制权变更等事项，禁止给出“必然发生/必然不发生”的绝对结论。\n'
                '2. 不得把“公告未披露”直接等价为“不会发生”。\n'
                '3. 必须输出：事实层、推断层、反方层；并给出基准/上行/下行情景及触发信号。\n'
                '4. 对每个情景给出概率或置信等级，并明确验证路径。'
            )

        def _with_event(text):
            base = str(text or '')
            if event_suffix and event_suffix not in base:
                return (base + event_suffix).strip()
            return base

        if agent_id == 'intel':
            if topic:
                return _with_event(format_ai_team_prompt(
                    'workflow.specialist.intel',
                    topic=topic,
                    context=context
                ))
            return _with_event(('请执行今日市场情报收集：\n'
                    '1. 搜索A股市场最新重要新闻和政策动态\n'
                    '2. 获取板块热点分析报告\n'
                    '3. 总结当前宏观环境和市场情绪\n'
                    '请综合分析后给出情报摘要。') + context)

        elif agent_id == 'quant':
            if topic:
                return _with_event(format_ai_team_prompt(
                    'workflow.specialist.quant',
                    topic=topic,
                    context=context
                ))
            return _with_event(('请执行市场扫描：\n'
                    '1. 运行市场估值指标(ind_18_valuation)判断整体估值水平\n'
                    '2. 运行MA200突破指标(ind_16_ma200)判断市场强弱\n'
                    '3. 如果市场环境允许，查询数据库筛选近期放量突破的股票\n'
                    '请给出扫描结果和推荐标的。') + context)

        elif agent_id == 'analyst':
            if topic:
                return _with_event(format_ai_team_prompt(
                    'workflow.specialist.analyst',
                    topic=topic,
                    context=context
                ))
            return _with_event(('请对市场热点板块的龙头股进行深度分析：\n'
                    '1. 获取K线、财务和筹码数据\n'
                    '2. 综合分析基本面和技术面\n'
                    '3. 给出投资评级\n') + context
            )

        elif agent_id == 'restructuring':
            if topic:
                return _with_event(format_ai_team_prompt(
                    'workflow.specialist.restructuring',
                    topic=topic,
                    context=context
                ))
            return _with_event(('请扫描全市场潜在重组机会：\n'
                    '1. 识别有重组预期、资产注入预期、国企整合预期的标的\n'
                    '2. 列出证据链（公告/交易所问询/大股东动作）\n'
                    '3. 评估每条线索的置信度和风险点\n'
                    '请输出重组线索清单。') + context)

        elif agent_id == 'risk':
            return _with_event(format_ai_team_prompt(
                'workflow.specialist.risk',
                topic=topic,
                context=context
            ))

        elif agent_id == 'auditor':
            return _with_event(format_ai_team_prompt(
                'workflow.specialist.auditor',
                topic=topic,
                context=context
            ))

        return _with_event(format_ai_team_prompt('workflow.specialist.default', topic=topic, context=context))

    def _get_idle_prompt(self, agent_id, topic, session_id):
        """闲时自学习专用 prompt（只读学习，不做交易）"""
        results = bus.get_session_results(session_id)
        context = ''
        if results:
            context = '\n\n=== 本轮闲时学习已产出内容 ===\n'
            for rid, rcontent in results.items():
                if rid.startswith(agent_id):
                    continue
                summary = rcontent[:500] if len(rcontent) > 500 else rcontent
                context += '\n【%s】：\n%s\n' % (rid, summary)

        common_rule = (
            '\n\n【硬性约束】\n'
            '1. 当前是“闲时自学习模式”，禁止交易执行与审批，不得提交交易信号。\n'
            '2. 必须输出“学习纪要”，包含：学习主题、关键发现、证据来源、可验证假设、下一步计划。\n'
            '3. 至少调用一次只读工具获取数据，并把1-2条高价值结论写入长期记忆(save_knowledge)。\n'
        )

        if agent_id == 'intel':
            return format_ai_team_prompt(
                'workflow.idle.intel',
                topic=topic,
                common_rule=common_rule,
                context=context
            )
        elif agent_id == 'quant':
            return format_ai_team_prompt(
                'workflow.idle.quant',
                topic=topic,
                common_rule=common_rule,
                context=context
            )
        elif agent_id == 'analyst':
            return format_ai_team_prompt(
                'workflow.idle.analyst',
                topic=topic,
                common_rule=common_rule,
                context=context
            )
        elif agent_id == 'restructuring':
            return format_ai_team_prompt(
                'workflow.idle.restructuring',
                topic=topic,
                common_rule=common_rule,
                context=context
            )
        elif agent_id == 'risk':
            return format_ai_team_prompt(
                'workflow.idle.risk',
                topic=topic,
                common_rule=common_rule,
                context=context
            )
        elif agent_id == 'auditor':
            return format_ai_team_prompt(
                'workflow.idle.auditor',
                topic=topic,
                common_rule=common_rule,
                context=context
            )

        return format_ai_team_prompt(
            'workflow.idle.default',
            topic=topic,
            common_rule=common_rule,
            context=context
        )

    def _get_office_chat_prompt(self, agent_id, topic, history_lines):
        """生成同事闲聊提示词（短句、轻量、可读）。"""
        role_map = {
            'intel': '市场情报员',
            'quant': '量化策略师',
            'analyst': '投资分析师',
            'restructuring': '资产重组专家',
            'risk': '风控官',
            'auditor': '审计员',
            'director': '决策总监',
        }
        role_name = role_map.get(agent_id, agent_id)
        history = '\n'.join(history_lines[-4:]) if history_lines else '（当前你是第一位发言）'
        return (
            '你现在在团队办公区进行“同事闲聊”。\n'
            '你的身份: %s。\n'
            '闲聊主题: %s（可延展到投资、宏观政策、国际政治、行业趋势、职场观察）。\n'
            '前文:\n%s\n\n'
            '请用80-180字，像同事聊天一样自然发言，要求：\n'
            '1. 口吻轻松、可以幽默，避免模板化官话\n'
            '2. 给出1条你真正关心的观点或吐槽，最好有一个具体例子\n'
            '3. 可以讨论政治/政策与市场传导，但要理性，不煽动极端立场\n'
            '4. 不要给出交易执行指令，不要审批信号'
        ) % (role_name, topic, history)

    @staticmethod
    def _meeting_round_progress(round_transcript):
        """评估本轮会议是否有新增有效信息（证据/行动/新增标的）。"""
        rows = [str(x or '') for x in (round_transcript or []) if str(x or '').strip()]
        if not rows:
            return {'score': 0, 'evidence_hits': 0, 'action_hits': 0, 'code_hits': 0}
        text = '\n'.join(rows)
        evidence_keys = ('证据', '数据', '来源', '公告', '新闻', '指标', '查询', '图', '%', '亿')
        action_keys = ('下一步', '建议', '请', '需要', '补充', '验证', '跟踪', '执行')
        evidence_hits = 0
        action_hits = 0
        for line in rows:
            if any(k in line for k in evidence_keys):
                evidence_hits += 1
            if any(k in line for k in action_keys):
                action_hits += 1
        codes = set(re.findall(r'(?<!\d)\d{6}(?:\.(?:SH|SZ))?(?!\d)', text.upper()))
        code_hits = len(codes)
        score = evidence_hits * 2 + action_hits + code_hits * 2
        return {
            'score': int(score),
            'evidence_hits': int(evidence_hits),
            'action_hits': int(action_hits),
            'code_hits': int(code_hits),
        }

    def _run_meeting(self, session_id, participants=None, meeting_topic=None,
                     mode='auto_research', rounds=None, max_rounds=None,
                     allowed_tools=None, blocked_tools=None):
        """会议讨论：是否继续由主持人按共识收敛状态动态决定。"""
        from AlphaFin.ai_team.core.agent_registry import get_agent

        if self._session_stop_requested(session_id):
            self._post_session_stopped(session_id, mode, title='会议已取消')
            return ''

        requested = participants or list(self.specialists_all)
        meeting_order = []
        for aid in requested:
            if aid in self.specialists_all and aid not in meeting_order:
                meeting_order.append(aid)
        if not meeting_order:
            return ''

        director = get_agent('director')
        all_participants = list(meeting_order)
        if director and 'director' not in all_participants:
            all_participants.append('director')

        topic = (meeting_topic or '团队研究议题').strip() or '团队研究议题'
        trace_run_id = self._trace_runs.get(session_id, '')
        if trace_run_id:
            trace_event(
                run_id=trace_run_id,
                session_id=session_id,
                workflow=mode,
                span_type='meeting',
                name='meeting_start',
                agent_id='orchestrator',
                data={'topic': topic, 'participants': all_participants}
            )
        transcript = []
        next_focus = ''
        forced_min_rounds = 0
        safety_max_rounds = 60
        hard_max_rounds = safety_max_rounds
        try:
            if rounds is not None:
                forced_min_rounds = max(0, int(rounds))
        except Exception:
            forced_min_rounds = 0
        try:
            if max_rounds is not None:
                hard_max_rounds = max(1, min(safety_max_rounds, int(max_rounds)))
        except Exception:
            hard_max_rounds = safety_max_rounds

        budget_round_cap, budget_snapshot = self._budget_meeting_cap(session_id, hard_max_rounds)
        if budget_round_cap <= 0:
            bus.post_activity(
                'orchestrator', 'status',
                '预算限制：会议跳过，直接进入总结阶段。',
                metadata={
                    'phase': 'meeting_plan',
                    'mode': mode,
                    'session_id': session_id,
                    'meeting_active': False,
                    'budget': budget_snapshot
                }
            )
            return ''
        if budget_round_cap < hard_max_rounds:
            hard_max_rounds = budget_round_cap
            bus.post_activity(
                'orchestrator', 'status',
                '预算限制：会议最多 %d 轮。' % hard_max_rounds,
                metadata={
                    'phase': 'meeting_plan',
                    'mode': mode,
                    'session_id': session_id,
                    'meeting_active': True,
                    'budget': budget_snapshot
                }
            )

        bus.broadcast(
            'orchestrator', 'status',
            '团队进入会议室，开始讨论（轮次由共识收敛动态决定）',
            metadata={
                'phase': 'meeting_start',
                'title': '会议室讨论启动',
                'mode': mode,
                'session_id': session_id,
                'meeting_topic': topic,
                'meeting_active': True,
                'participants': all_participants,
                'meeting_order': meeting_order,
                'meeting_round_total': hard_max_rounds,
                'rounds': 'dynamic',
                'max_rounds': hard_max_rounds,
            }
        )
        bus.post_activity(
            'orchestrator', 'status',
            '会议发言顺序：%s；最终由 director 总结' % (' -> '.join(meeting_order)),
            metadata={
                'phase': 'meeting_order',
                'title': '会议顺序',
                'mode': mode,
                'session_id': session_id,
                'meeting_topic': topic,
                'meeting_active': True,
                'participants': all_participants,
                'meeting_order': meeting_order,
                'meeting_round_total': hard_max_rounds,
                'max_rounds': hard_max_rounds,
            }
        )

        def _context_digest():
            results = bus.get_session_results(session_id)
            if not results:
                return '暂无前置成果。'
            rows = []
            for rid, rcontent in results.items():
                text = (rcontent or '').replace('\n', ' ').strip()
                if text:
                    rows.append('【%s】%s' % (rid, text[:220]))
            return '\n'.join(rows) if rows else '暂无前置成果。'

        round_no = 0
        stagnation_streak = 0
        repeated_focus_streak = 0
        last_focus_text = ''
        while True:
            if self._session_stop_requested(session_id):
                self._post_session_stopped(session_id, mode, title='会议已停止')
                break
            if self._session_should_converge(session_id):
                self._maybe_announce_session_converging(session_id, mode)
                break
            round_start_len = len(transcript)
            round_no += 1
            if trace_run_id:
                trace_event(
                    run_id=trace_run_id,
                    session_id=session_id,
                    workflow=mode,
                    span_type='meeting_round',
                    name='meeting_round_start',
                    agent_id='orchestrator',
                    data={'round': round_no}
                )
            bus.post_activity(
                'orchestrator', 'status',
                '会议第%d轮开始（主题：%s）' % (round_no, topic[:30]),
                metadata={
                    'phase': 'meeting_round',
                    'title': '会议第%d轮' % round_no,
                    'mode': mode,
                    'session_id': session_id,
                    'meeting_topic': topic,
                    'meeting_active': True,
                    'participants': all_participants,
                    'meeting_round': round_no,
                    'meeting_round_total': hard_max_rounds,
                    'meeting_order': meeting_order,
                }
            )

            for seat_index, agent_id in enumerate(meeting_order):
                if self._session_stop_requested(session_id):
                    self._post_session_stopped(session_id, mode, title='会议已停止')
                    break
                if self._session_should_converge(session_id):
                    self._maybe_announce_session_converging(session_id, mode)
                    break
                agent = get_agent(agent_id)
                if not agent:
                    continue

                bus.post_activity(
                    'orchestrator', 'status',
                    '第%d轮发言：%s（%d/%d）' % (
                        round_no, agent_id, seat_index + 1, len(meeting_order)
                    ),
                    metadata={
                        'phase': 'meeting_turn',
                        'title': '会议发言顺序',
                        'mode': mode,
                        'session_id': session_id,
                        'meeting_topic': topic,
                        'meeting_active': True,
                        'participants': all_participants,
                        'meeting_order': meeting_order,
                        'meeting_round': round_no,
                        'meeting_round_total': hard_max_rounds,
                        'meeting_speaker': agent_id,
                        'meeting_speaker_seq': seat_index + 1,
                        'meeting_speaker_total': len(meeting_order),
                    }
                )

                agent.process_incoming_messages(session_id)
                context_digest = _context_digest()
                transcript_view = '\n'.join(transcript[-8:]) if transcript else '（暂无前序发言）'
                focus_line = ('\n本轮聚焦：%s\n' % next_focus) if next_focus else '\n'
                prompt = format_ai_team_prompt(
                    'workflow.meeting_member_turn',
                    round_no=round_no,
                    topic=topic,
                    focus_line=focus_line,
                    context_digest=context_digest,
                    transcript_view=transcript_view
                )

                try:
                    reply = agent.think(
                        prompt,
                        session_id=session_id,
                        max_tool_rounds=1,
                        allowed_tools=allowed_tools,
                        blocked_tools=blocked_tools
                    )
                    short_reply = (reply or '').strip()
                    if len(short_reply) > 320:
                        short_reply = short_reply[:320] + '...'
                    transcript.append('%s: %s' % (agent_id, short_reply))
                    bus.save_result(session_id, '%s_meeting_r%d' % (agent_id, round_no), short_reply)
                    bus.broadcast(
                        agent_id, 'speaking', short_reply,
                        metadata={
                            'phase': 'meeting',
                            'title': '会议发言',
                            'mode': mode,
                            'session_id': session_id,
                            'meeting_topic': topic,
                            'meeting_active': True,
                            'participants': all_participants,
                            'meeting_order': meeting_order,
                            'meeting_round': round_no,
                            'meeting_round_total': hard_max_rounds,
                            'seat_index': seat_index,
                            'meeting_speaker': agent_id,
                            'meeting_speaker_seq': seat_index + 1,
                            'meeting_speaker_total': len(meeting_order),
                        }
                    )
                except Exception as e:
                    bus.post_activity(agent_id, 'error', '会议发言失败: %s' % str(e))

            if self._session_stop_requested(session_id):
                self._post_session_stopped(session_id, mode, title='会议已停止')
                break

            if round_no >= hard_max_rounds:
                continue_decision = {
                    'continue_meeting': False,
                    'reason': '达到会议轮次上限(%d轮)，结束会议。' % hard_max_rounds,
                    'next_focus': ''
                }
            elif round_no < forced_min_rounds:
                continue_decision = {
                    'continue_meeting': True,
                    'reason': '满足最小轮次要求，继续会议。',
                    'next_focus': ''
                }
            else:
                continue_decision = self._should_continue_meeting(
                    director=director,
                    session_id=session_id,
                    topic=topic,
                    transcript=transcript,
                    round_no=round_no,
                    mode=mode,
                    allowed_tools=allowed_tools,
                    blocked_tools=blocked_tools
                )

            round_slice = transcript[round_start_len:]
            progress = self._meeting_round_progress(round_slice)
            if progress.get('score', 0) <= 2:
                stagnation_streak += 1
            else:
                stagnation_streak = 0

            next_focus_raw = str(continue_decision.get('next_focus') or '').strip()
            if next_focus_raw and next_focus_raw == last_focus_text:
                repeated_focus_streak += 1
            else:
                repeated_focus_streak = 0
            if next_focus_raw:
                last_focus_text = next_focus_raw

            if round_no >= 2 and stagnation_streak >= 2:
                continue_decision = {
                    'continue_meeting': False,
                    'reason': '连续2轮无新增证据或行动项，自动结束会议。',
                    'next_focus': ''
                }
            elif round_no >= 3 and repeated_focus_streak >= 1:
                continue_decision = {
                    'continue_meeting': False,
                    'reason': '讨论焦点重复且未形成新增信息，自动收敛结束会议。',
                    'next_focus': ''
                }

            next_focus = continue_decision.get('next_focus', '')
            bus.post_activity(
                'orchestrator', 'status',
                '会议轮次决策：%s（%s）' % (
                    '继续' if continue_decision.get('continue_meeting') else '结束',
                    continue_decision.get('reason') or '无'
                ),
                metadata={
                    'phase': 'meeting_decision',
                    'title': '会议轮次决策',
                    'mode': mode,
                    'session_id': session_id,
                    'meeting_topic': topic,
                    'meeting_active': bool(continue_decision.get('continue_meeting')),
                    'participants': all_participants,
                    'meeting_order': meeting_order,
                    'meeting_round': round_no,
                    'meeting_round_total': hard_max_rounds,
                    'next_focus': next_focus,
                    'meeting_progress': progress,
                    'meeting_stagnation_streak': stagnation_streak,
                }
            )
            if trace_run_id:
                trace_event(
                    run_id=trace_run_id,
                    session_id=session_id,
                    workflow=mode,
                    span_type='meeting_round',
                    name='meeting_round_decision',
                    agent_id='orchestrator',
                    data={
                        'round': round_no,
                        'continue': bool(continue_decision.get('continue_meeting')),
                        'reason': continue_decision.get('reason', ''),
                        'progress': progress,
                    }
                )

            if not continue_decision.get('continue_meeting'):
                break

        if director:
            try:
                if self._session_stop_requested(session_id):
                    self._post_session_stopped(session_id, mode, title='会议总结已取消')
                    return ''
                director.process_incoming_messages(session_id)
                summary_prompt = format_ai_team_prompt(
                    'workflow.meeting_summary',
                    topic=topic,
                    transcript='\n'.join(transcript[-24:])
                )
                summary = director.think(
                    summary_prompt,
                    session_id=session_id,
                    max_tool_rounds=1,
                    allowed_tools=[],
                    blocked_tools=blocked_tools
                )
                bus.save_result(session_id, 'meeting_summary', summary)
                bus.broadcast(
                    'director', 'consensus', summary,
                    metadata={
                        'phase': 'meeting_summary',
                        'title': '会议共识',
                        'mode': mode,
                        'session_id': session_id,
                        'meeting_topic': topic,
                        'meeting_active': False,
                        'participants': all_participants,
                        'meeting_order': meeting_order,
                        'meeting_round': round_no,
                        'meeting_round_total': hard_max_rounds,
                    }
                )
                summary_preview = (summary or '').replace('\n', ' ').strip()
                if len(summary_preview) > 160:
                    summary_preview = summary_preview[:160] + '...'
                bus.post_activity(
                    'orchestrator', 'status',
                    '会议结果已生成：%s' % (summary_preview or '请查看会议共识详情'),
                    metadata={
                        'phase': 'meeting_result',
                        'title': '会议结果通知',
                        'mode': mode,
                        'session_id': session_id,
                        'meeting_topic': topic,
                        'meeting_active': False,
                        'participants': all_participants,
                        'meeting_order': meeting_order,
                        'meeting_round': round_no,
                        'meeting_round_total': hard_max_rounds,
                        'summary_preview': summary_preview,
                    }
                )
                return summary
            except Exception as e:
                bus.post_activity('director', 'error', '会议总结失败: %s' % str(e))

        bus.broadcast(
            'orchestrator', 'status', '会议结束（无总监参与总结）',
            metadata={
                'phase': 'meeting_end',
                'title': '会议结束',
                'mode': mode,
                'session_id': session_id,
                'meeting_topic': topic,
                'meeting_active': False,
                'participants': all_participants,
                'meeting_order': meeting_order,
                'meeting_round': round_no,
                'meeting_round_total': hard_max_rounds,
            }
        )
        bus.post_activity(
            'orchestrator', 'status', '会议结果：会议已结束（无总监共识）',
            metadata={
                'phase': 'meeting_result',
                'title': '会议结果通知',
                'mode': mode,
                'session_id': session_id,
                'meeting_topic': topic,
                'meeting_active': False,
                'participants': all_participants,
                'meeting_order': meeting_order,
                'meeting_round': round_no,
                'meeting_round_total': hard_max_rounds,
            }
        )
        return ''

    def _phase_idle_synthesis(self, director, session_id, theme,
                              allowed_tools=None, blocked_tools=None):
        """闲时学习由总监汇总为可执行的学习改进结论"""
        director.process_incoming_messages(session_id)

        results = bus.get_session_results(session_id)
        context = '\n=== 闲时学习产出汇总 ===\n'
        for rid, rcontent in results.items():
            context += '\n【%s】：\n%s\n' % (rid, rcontent)

        prompt = format_ai_team_prompt(
            'workflow.idle_synthesis',
            theme=theme,
            context=context
        )

        result = director.think(
            prompt,
            session_id=session_id,
            allowed_tools=allowed_tools,
            blocked_tools=blocked_tools
        )

        save_report(
            report_type='idle_learning',
            title='闲时学习 #%d 报告 - %s' % (self.idle_count, theme),
            content=result,
            participants=['director', 'analyst', 'risk', 'intel', 'quant', 'auditor', 'restructuring']
        )

        bus.broadcast(
            director.agent_id, 'consensus', '闲时学习报告已生成',
            metadata={'phase': 'idle_synthesis', 'title': '闲时学习报告', 'mode': 'idle_learning'}
        )

    def _phase_synthesis(self, agent, session_id):
        """Director综合决策与报告"""
        agent.process_incoming_messages(session_id)

        # 收集所有结果
        results = bus.get_session_results(session_id)
        context = '\n=== 完整团队研究成果 ===\n'
        for rid, rcontent in results.items():
            context += '\n【%s】：\n%s\n' % (rid, rcontent)

        prompt = format_ai_team_prompt(
            'workflow.research_final_report',
            context_title='研究周期',
            context=context
        )

        result = agent.think(
            prompt,
            session_id=session_id,
            max_tool_rounds=1,
            allowed_tools=[],
            response_style='deep'
        )

        # 保存报告到数据库
        save_report(
            report_type='cycle',
            title='研究周期 #%d 报告' % self.cycle_count,
            content=result,
            participants=['director', 'analyst', 'risk', 'intel', 'quant', 'auditor', 'restructuring']
        )

        bus.broadcast(agent.agent_id, 'consensus', '研究报告已生成',
                      metadata={'phase': 4, 'title': '最终报告'})

    @staticmethod
    def _is_status_query_text(text):
        q = str(text or '').strip().lower()
        if not q:
            return False
        keys = (
            '当前大家在进行什么工作', '大家在进行什么工作', '当前在做什么',
            '各智能体在做什么', '谁在工作', '工作进展', '团队状态',
            '大家在干嘛', '大家在干什么', '现在在干嘛', '现在在做什么',
            '各位在忙什么', '都在忙什么', '目前在做啥'
        )
        return any(k in q for k in keys)

    @staticmethod
    def _is_realtime_quote_query_text(text):
        q = str(text or '').strip()
        if not q:
            return False
        realtime_keys = ('今日', '今天', '实时', '最新', '当前', '现在', '盘中')
        quote_keys = ('股价', '价格', '行情', '收盘价', '开盘价', '涨跌', '报价')
        has_realtime = any(k in q for k in realtime_keys)
        has_quote = any(k in q for k in quote_keys)
        has_stock_hint = bool(re.search(r'(?<!\d)\d{6}(?:\.(?:SH|SZ))?(?!\d)', q.upper())) or ('股' in q)
        return bool(has_realtime and has_quote and has_stock_hint)

    @staticmethod
    def _normalize_user_ask_mode(ask_mode):
        mode = str(ask_mode or '').strip().lower()
        if mode in ('quick', 'deep', 'team'):
            return mode
        return 'team'

    @staticmethod
    def _is_simple_user_ask_text(text):
        q = str(text or '').strip()
        if not q:
            return True
        ql = q.lower()

        simple_keys = (
            '你好', '在吗', '谢谢', '辛苦了',
            '早上好', '中午好', '晚上好', '午安',
            '你是谁', '你是干嘛的', '你能做什么', '你可以做什么',
            '怎么用', '怎么使用', '如何使用',
        )
        if any(k in q for k in simple_keys):
            return True
        if re.search(r'\b(?:hi|hello)\b', ql):
            return True
        return False

    @staticmethod
    def _is_market_query_text(text):
        q = str(text or '').strip()
        if not q:
            return False
        if re.search(r'(?<!\d)\d{6}(?:\.(?:SH|SZ))?(?!\d)', q.upper()):
            return True
        keys = (
            '股票', '个股', '指数', '行情', '股价', '涨跌', 'K线', '分时', '板块',
            '大盘', 'A股', '沪深', '上证', '深证', '创业板', '市盈率', '估值',
            '仓位', '买入', '卖出', '交易',
        )
        return any(k in q for k in keys)

    @staticmethod
    def _is_event_prediction_query_text(text):
        q = str(text or '').strip()
        if not q:
            return False
        event_keys = (
            '停牌', '重组', '并购', '借壳', '定增', '资产注入', '控制权变更',
            '重大事项', '股权转让', '混改', '摘帽', '退市', 'st'
        )
        forecast_keys = (
            '是否会', '会不会', '可能', '概率', '预期', '前瞻', '会否',
            '有没有可能', '什么时候', '会在', '会出现'
        )
        has_event = any(k in q for k in event_keys)
        has_forecast = any(k in q for k in forecast_keys) or ('?' in q) or ('？' in q)
        return bool(has_event and has_forecast)

    def _force_event_prediction_route(self, routing, question=''):
        route = dict(routing or {})
        route['task_type'] = 'analysis'
        required = [x for x in (route.get('required_agents') or []) if x in self.specialists_all]
        required.extend(['restructuring', 'risk', 'auditor', 'intel', 'analyst'])
        route['required_agents'] = self._dedup_keep_order(required)
        task_plan = str(route.get('task_plan') or '').strip()
        extra_plan = (
            '本题为前瞻事件推演（停牌/重组/定增等），必须采用概率情景分析；'
            '禁止把“公告未披露”直接视为“不会发生”。'
        )
        if task_plan:
            if extra_plan not in task_plan:
                task_plan = task_plan + '\n' + extra_plan
        else:
            task_plan = extra_plan
        route['task_plan'] = task_plan
        reason = str(route.get('reason') or '').strip()
        extra_reason = '命中前瞻事件问题强制协作路由'
        route['reason'] = (reason + '；' + extra_reason).strip('；') if reason else extra_reason
        return route

    def _build_user_ask_status_snapshot(self):
        from AlphaFin.ai_team.core.agent_registry import get_all_status
        status_map = {
            'idle': '空闲', 'thinking': '思考中', 'using_tool': '使用工具',
            'speaking': '输出中', 'offline': '离线'
        }
        now_str = time.strftime('%Y-%m-%d %H:%M:%S')
        rows = get_all_status()
        lines = ['当前团队实时工作状态（北京时间 %s）:' % now_str, '']
        for s in rows:
            aid = s.get('agent_id', '-')
            name = s.get('name', aid)
            wf = s.get('current_workflow_label', '空闲待命')
            phase = status_map.get(s.get('status', ''), s.get('status', '未知'))
            task = (s.get('current_task') or '').strip() or '无明确任务'
            if len(task) > 120:
                task = task[:120] + '...'
            lines.append('- %s(%s)：%s / %s；任务：%s' % (name, aid, wf, phase, task))
        return '\n'.join(lines)

    @staticmethod
    def _dedup_keep_order(items):
        seen = set()
        out = []
        for item in items or []:
            if item in seen:
                continue
            seen.add(item)
            out.append(item)
        return out

    @staticmethod
    def _user_ask_progress_steps(ask_mode, workflow_mode, workflow_cfg=None):
        mode = str(workflow_mode or '').strip().lower()
        if ask_mode == 'quick':
            return ['总监快答', '返回结果']
        if ask_mode == 'deep':
            return ['总监深度分析', '返回结果']
        if mode == 'director_only':
            return ['总监直答', '返回结果']
        if mode == 'custom':
            cfg = workflow_cfg if isinstance(workflow_cfg, dict) else {}
            custom_steps = cfg.get('custom_steps') if isinstance(cfg.get('custom_steps'), list) else []
            names = [str((x or {}).get('name') or '').strip() for x in custom_steps]
            names = [x for x in names if x]
            if names:
                return names
        return ['总监路由', '成员执行', '必要会议', '总监答复']

    @staticmethod
    def _normalize_web_links(items, limit=12):
        out = []
        seen = set()
        for row in (items or []):
            if not isinstance(row, dict):
                continue
            url = str(row.get('url') or row.get('link') or '').strip()
            title = str(row.get('title') or '').strip()
            source = str(row.get('source') or '').strip()
            published_at = str(row.get('published_at') or '').strip()
            summary = str(row.get('summary') or row.get('snippet') or '').strip()
            if not url:
                continue
            key = (url.lower(), title)
            if key in seen:
                continue
            seen.add(key)
            out.append({
                'title': title,
                'url': url,
                'source': source,
                'published_at': published_at,
                'summary': summary,
            })
            if len(out) >= max(1, int(limit or 12)):
                break
        return out

    @classmethod
    def _merge_web_links(cls, base, extra, limit=12):
        merged = list(base or [])
        merged.extend(list(extra or []))
        return cls._normalize_web_links(merged, limit=limit)

    @classmethod
    def _parse_think_result(cls, result):
        if isinstance(result, dict):
            reply = str(result.get('reply') or result.get('content') or '')
            links = cls._normalize_web_links(
                result.get('web_links') or result.get('search_links') or [],
                limit=12
            )
            return reply, links
        return str(result or ''), []

    def _announce_user_ask_assignment(self, session_id, question, required):
        agent_name = {
            'analyst': '分析师',
            'risk': '风控官',
            'intel': '情报员',
            'quant': '量化师',
            'auditor': '审计员',
            'restructuring': '重组专家',
        }
        required_names = ['%s(%s)' % (agent_name.get(a, a), a) for a in required]
        bus.post_activity(
            'director', 'status',
            '直连问答任务分配：%s' % ('、'.join(required_names) if required_names else '暂无'),
            metadata={
                'phase': 'user_ask_assign',
                'title': '问答任务分配',
                'mode': 'user_ask',
                'session_id': session_id,
                'required_agents': required,
                'question': question
            }
        )

    def _run_user_ask_agent_step(
            self,
            agents,
            session_id,
            question,
            parallel=True,
            allowed_tools=None,
            blocked_tools=None
    ):
        clean = [a for a in (agents or []) if a in self.specialists_all]
        clean = self._dedup_keep_order(clean)
        if not clean:
            return []

        if parallel:
            self._run_parallel(
                clean,
                session_id,
                topic=question,
                mode='user_ask',
                allowed_tools=allowed_tools,
                blocked_tools=blocked_tools
            )
        else:
            for aid in clean:
                self._run_parallel(
                    [aid],
                    session_id,
                    topic=question,
                    mode='user_ask',
                    allowed_tools=allowed_tools,
                    blocked_tools=blocked_tools
                )
        return clean

    def _run_director_user_ask_auto(self, director, session_id, question, enable_web_search=True):
        q = (question or '').strip()
        blocked_tools = [] if enable_web_search else ['web_search']
        progress_steps = self._user_ask_progress_steps('team', 'auto', None)
        if self._session_stop_requested(session_id):
            self._post_session_stopped(session_id, 'user_ask')
            return '当前问答已停止。', {
                'participants': ['director'],
                'required_agents': [],
                'workflow_steps': ['stopped'],
                'search_links': []
            }
        if self._is_status_query_text(q):
            answer = self._build_user_ask_status_snapshot()
            return answer, {
                'participants': ['director'],
                'required_agents': [],
                'workflow_steps': ['status_snapshot'],
                'search_links': []
            }

        # 日常简短问题走总监直答，不做团队分配
        if self._is_simple_user_ask_text(q):
            self._set_session_progress(
                session_id=session_id,
                workflow='user_ask',
                title=q[:80],
                steps=progress_steps,
                current_index=1,
                current_step='总监路由',
                detail='命中简答策略，由总监直接完成回答。',
                state='running',
                actor='director',
            )
            bus.post_activity(
                'director', 'status',
                '直连问答命中“直接答复”策略（无需团队分配）',
                metadata={
                    'phase': 'user_ask_direct',
                    'title': '直接答复',
                    'mode': 'user_ask',
                    'session_id': session_id,
                    'question': question,
                }
            )
            think_ret = director.think(
                question,
                session_id=session_id,
                max_tool_rounds=USER_ASK_TEAM_MAX_TOOL_ROUNDS,
                response_style='auto',
                blocked_tools=blocked_tools,
                return_meta=True,
            )
            answer, search_links = self._parse_think_result(think_ret)
            self._set_session_progress(
                session_id=session_id,
                workflow='user_ask',
                title=q[:80],
                steps=progress_steps,
                current_index=len(progress_steps),
                current_step='总监答复',
                detail='最终答复已生成。',
                state='completed',
                actor='director',
            )
            return answer, {
                'participants': ['director'],
                'required_agents': [],
                'workflow_steps': ['director_direct'],
                'search_links': search_links
            }

        routing = self._route_task(director, question, session_id)
        if self._is_event_prediction_query_text(question):
            routing = self._force_event_prediction_route(routing, question=question)
        task_type = routing.get('task_type', 'analysis')
        if task_type == 'chat':
            answer = routing.get('direct_answer', '').strip()
            search_links = []
            if not answer:
                think_ret = director.think(
                    question,
                    session_id=session_id,
                    max_tool_rounds=USER_ASK_TEAM_MAX_TOOL_ROUNDS,
                    response_style='auto',
                    blocked_tools=blocked_tools,
                    return_meta=True,
                )
                answer, search_links = self._parse_think_result(think_ret)
            return answer, {
                'participants': ['director'],
                'required_agents': [],
                'routing': routing,
                'workflow_steps': ['route_task', 'director_direct'],
                'search_links': search_links
            }

        required = routing.get('required_agents', list(self.specialists_all))
        required = [a for a in required if a in self.specialists_all]
        if not required:
            required = ['analyst', 'risk']
        required = self._dedup_keep_order(required)
        self._announce_user_ask_assignment(session_id, question, required)

        task_plan = routing.get('task_plan', '')
        if task_plan:
            bus.broadcast(
                'director', 'task', task_plan,
                metadata={
                    'phase': 'user_ask_plan',
                    'title': '问答任务分解',
                    'mode': 'user_ask',
                    'session_id': session_id,
                    'required_agents': required,
                    'question': question
                }
            )

        phase1_agents = [a for a in required if a in self.specialists_phase1]
        phase2_agents = [a for a in required if a in self.specialists_phase2]

        if phase1_agents:
            self._set_session_progress(
                session_id=session_id,
                workflow='user_ask',
                title=q[:80],
                steps=progress_steps,
                current_index=2,
                current_step='成员执行',
                detail='团队成员正在执行第一阶段：%s' % '、'.join(phase1_agents),
                state='running',
                actor='orchestrator',
            )
            self._run_user_ask_agent_step(
                phase1_agents,
                session_id,
                question,
                parallel=True,
                blocked_tools=blocked_tools
            )
            if self._session_stop_requested(session_id):
                self._post_session_stopped(session_id, 'user_ask')
                return '当前问答已停止。', {
                    'participants': ['director'] + list(required),
                    'required_agents': list(required),
                    'routing': routing,
                    'workflow_steps': ['route_task', 'phase1', 'stopped'],
                    'search_links': []
                }
            bus.post_activity(
                'orchestrator', 'status',
                '直连问答阶段一已完成：%s' % '、'.join(phase1_agents),
                metadata={
                    'phase': 'user_ask_phase1_done',
                    'title': '问答阶段进度',
                    'mode': 'user_ask',
                    'session_id': session_id,
                    'participants': phase1_agents,
                }
            )
        deadline_action = self._handle_session_expired(session_id, 'user_ask', title=q[:80])
        if deadline_action == 'stop':
            self._post_session_stopped(session_id, 'user_ask')
            return '当前问答已停止。', {
                'participants': ['director'] + list(required),
                'required_agents': list(required),
                'routing': routing,
                'workflow_steps': ['route_task', 'phase1', 'stopped'],
                'search_links': []
            }
        if self._session_should_converge(session_id):
            self._maybe_announce_session_converging(session_id, 'user_ask')
            phase2_agents = []
        if phase2_agents and deadline_action != 'summarize':
            self._set_session_progress(
                session_id=session_id,
                workflow='user_ask',
                title=q[:80],
                steps=progress_steps,
                current_index=2,
                current_step='成员执行',
                detail='团队成员正在执行第二阶段：%s' % '、'.join(phase2_agents),
                state='running',
                actor='orchestrator',
            )
            self._run_user_ask_agent_step(
                phase2_agents,
                session_id,
                question,
                parallel=True,
                blocked_tools=blocked_tools
            )
            if self._session_stop_requested(session_id):
                self._post_session_stopped(session_id, 'user_ask')
                return '当前问答已停止。', {
                    'participants': ['director'] + list(required),
                    'required_agents': list(required),
                    'routing': routing,
                    'workflow_steps': ['route_task', 'phase1', 'phase2', 'stopped'],
                    'search_links': []
                }
            bus.post_activity(
                'orchestrator', 'status',
                '直连问答阶段二已完成：%s' % '、'.join(phase2_agents),
                metadata={
                    'phase': 'user_ask_phase2_done',
                    'title': '问答阶段进度',
                    'mode': 'user_ask',
                    'session_id': session_id,
                    'participants': phase2_agents,
                }
            )
            deadline_action = self._handle_session_expired(session_id, 'user_ask', title=q[:80])
            if deadline_action == 'stop':
                self._post_session_stopped(session_id, 'user_ask')
                return '当前问答已停止。', {
                    'participants': ['director'] + list(required),
                    'required_agents': list(required),
                    'routing': routing,
                    'workflow_steps': ['route_task', 'phase1', 'phase2', 'stopped'],
                    'search_links': []
                }

        if self._session_should_converge(session_id):
            self._maybe_announce_session_converging(session_id, 'user_ask')
        elif deadline_action != 'summarize':
            self._set_session_progress(
                session_id=session_id,
                workflow='user_ask',
                title=q[:80],
                steps=progress_steps,
                current_index=3,
                current_step='必要会议',
                detail='系统正在判断是否需要团队会议，并在必要时组织讨论。',
                state='running',
                actor='director',
            )
            self._maybe_run_meeting(
                session_id=session_id,
                topic=question,
                mode='user_ask',
                candidate_participants=required,
                director=director,
                max_rounds=1,
                blocked_tools=blocked_tools
            )
            if self._session_stop_requested(session_id):
                self._post_session_stopped(session_id, 'user_ask')
                return '当前问答已停止。', {
                    'participants': ['director'] + list(required),
                    'required_agents': list(required),
                    'routing': routing,
                    'workflow_steps': ['route_task', 'phase1', 'phase2', 'meeting', 'stopped'],
                    'search_links': []
                }

        self._set_session_progress(
            session_id=session_id,
            workflow='user_ask',
            title=q[:80],
            steps=progress_steps,
            current_index=4,
            current_step='总监答复',
            detail='决策总监正在汇总团队材料并形成最终答复。',
            state='running',
            actor='director',
        )
        bus.post_activity(
            'director', 'status',
            '直连问答正在汇总最终答复...',
            metadata={
                'phase': 'user_ask_synthesizing',
                'title': '最终答复汇总',
                'mode': 'user_ask',
                'session_id': session_id,
                'participants': ['director'] + list(required),
            }
        )
        answer, search_links = self._phase_user_ask_synthesis(
            director,
            session_id,
            question,
            participants=['director'] + list(required),
            blocked_tools=blocked_tools
        )
        self._set_session_progress(
            session_id=session_id,
            workflow='user_ask',
            title=q[:80],
            steps=progress_steps,
            current_index=4,
            current_step='总监答复',
            detail='最终答复已生成。',
            state='completed',
            actor='director',
        )
        return answer, {
            'participants': ['director'] + list(required),
            'required_agents': list(required),
            'routing': routing,
            'workflow_steps': ['route_task', 'phase1', 'phase2', 'meeting', 'director_synthesis'],
            'search_links': search_links
        }

    def _run_director_user_ask_custom(
            self,
            director,
            session_id,
            question,
            workflow_cfg,
            enable_web_search=True
    ):
        cfg = workflow_cfg if isinstance(workflow_cfg, dict) else self._default_user_ask_workflow()
        blocked_tools = [] if enable_web_search else ['web_search']
        steps = cfg.get('custom_steps') if isinstance(cfg.get('custom_steps'), list) else []
        limits = cfg.get('limits') if isinstance(cfg.get('limits'), dict) else {}
        max_parallel = min(
            max(1, int(limits.get('max_parallel_agents') or 4)),
            max(1, len(self.specialists_all))
        )
        max_meeting_rounds = min(max(1, int(limits.get('max_meeting_rounds') or 2)), 6)

        ctx = {
            'routing': {},
            'required_agents': [],
            'participants': ['director'],
            'executed_steps': [],
        }
        answer = ''
        search_links = []
        progress_steps = self._user_ask_progress_steps('team', 'custom', cfg)

        for idx, step in enumerate(steps):
            if self._session_stop_requested(session_id):
                self._post_session_stopped(session_id, 'user_ask')
                break
            stype = str(step.get('type') or '')
            if self._session_should_converge(session_id) and stype not in ('director_synthesis', 'director_direct'):
                self._maybe_announce_session_converging(session_id, 'user_ask')
                break
            sname = str(step.get('name') or ('步骤%d' % (idx + 1)))
            self._set_session_progress(
                session_id=session_id,
                workflow='user_ask',
                title=str(question or '')[:80],
                steps=progress_steps,
                current_index=min(len(progress_steps), idx + 1),
                current_step=sname,
                detail='当前执行自定义工作流步骤：%s' % sname,
                state='running',
                actor='orchestrator',
            )
            bus.post_activity(
                'orchestrator', 'status',
                '工作流步骤[%d/%d]：%s' % (idx + 1, len(steps), sname),
                metadata={
                    'phase': 'user_ask_custom_step',
                    'title': '自定义流程',
                    'mode': 'user_ask',
                    'session_id': session_id,
                    'step_index': idx + 1,
                    'step_total': len(steps),
                    'step_type': stype,
                    'step_name': sname,
                }
            )
            ctx['executed_steps'].append(stype)

            if stype == 'assign_by_router':
                routing = self._route_task(director, question, session_id)
                if self._is_event_prediction_query_text(question):
                    routing = self._force_event_prediction_route(routing, question=question)
                required = routing.get('required_agents', list(self.specialists_all))
                required = [a for a in required if a in self.specialists_all]
                if not required:
                    required = ['analyst', 'risk']
                required = self._dedup_keep_order(required)
                ctx['routing'] = routing
                ctx['required_agents'] = required
                ctx['participants'] = self._dedup_keep_order(ctx['participants'] + list(required))
                self._announce_user_ask_assignment(session_id, question, required)
                task_plan = routing.get('task_plan', '')
                if task_plan:
                    bus.broadcast(
                        'director', 'task', task_plan,
                        metadata={
                            'phase': 'user_ask_plan',
                            'title': '问答任务分解',
                            'mode': 'user_ask',
                            'session_id': session_id,
                            'required_agents': required,
                            'question': question
                        }
                    )
                continue

            if stype == 'agent_task':
                source = str(step.get('source') or 'assigned')
                if source == 'fixed':
                    agents = [a for a in (step.get('agents') or []) if a in self.specialists_all]
                else:
                    agents = list(ctx.get('required_agents') or [])
                agents = self._dedup_keep_order(agents)[:max_parallel]
                parallel = self._to_bool(step.get('parallel', True), True)
                done_agents = self._run_user_ask_agent_step(
                    agents,
                    session_id,
                    question,
                    parallel=parallel,
                    blocked_tools=blocked_tools
                )
                ctx['participants'] = self._dedup_keep_order(ctx['participants'] + done_agents)
                continue

            if stype == 'meeting':
                mode = str(step.get('mode') or 'auto').lower()
                participants = [a for a in (step.get('participants') or []) if a in self.specialists_all]
                if not participants:
                    participants = list(ctx.get('required_agents') or [])
                participants = self._dedup_keep_order(participants)
                if not participants:
                    participants = ['analyst', 'risk']
                rounds = min(
                    max(1, int(step.get('max_rounds') or 1)),
                    max_meeting_rounds
                )
                if mode == 'never':
                    bus.post_activity(
                        'orchestrator', 'status',
                        '会议步骤已跳过（配置为 never）',
                        metadata={
                            'phase': 'meeting_plan',
                            'mode': 'user_ask',
                            'session_id': session_id,
                            'participants': participants
                        }
                    )
                elif mode == 'always':
                    self._run_meeting(
                        session_id=session_id,
                        participants=participants,
                        meeting_topic=question,
                        mode='user_ask',
                        max_rounds=rounds,
                        blocked_tools=blocked_tools
                    )
                else:
                    self._maybe_run_meeting(
                        session_id=session_id,
                        topic=question,
                        mode='user_ask',
                        candidate_participants=participants,
                        director=director,
                        max_rounds=rounds,
                        blocked_tools=blocked_tools
                    )
                ctx['participants'] = self._dedup_keep_order(ctx['participants'] + participants)
                continue

            if stype == 'director_direct':
                max_tool_rounds = min(4, max(1, int(step.get('max_tool_rounds') or 1)))
                think_ret = director.think(
                    question,
                    session_id=session_id,
                    max_tool_rounds=max_tool_rounds,
                    response_style='auto',
                    blocked_tools=blocked_tools,
                    return_meta=True
                )
                answer, links = self._parse_think_result(think_ret)
                search_links = self._merge_web_links(search_links, links)
                if self._to_bool(step.get('stop_after', True), True):
                    break
                continue

            if stype == 'director_synthesis':
                answer, links = self._phase_user_ask_synthesis(
                    director,
                    session_id,
                    question,
                    participants=list(ctx.get('participants') or ['director']),
                    blocked_tools=blocked_tools
                )
                search_links = self._merge_web_links(search_links, links)
                continue

        if not answer:
            if self._session_stop_requested(session_id):
                return '当前问答已停止。', {
                    'participants': list(ctx.get('participants') or ['director']),
                    'required_agents': list(ctx.get('required_agents') or []),
                    'routing': ctx.get('routing') or {},
                    'workflow_steps': list(ctx.get('executed_steps') or []) + ['stopped'],
                    'search_links': search_links
                }
            if self._session_should_converge(session_id):
                self._maybe_announce_session_converging(session_id, 'user_ask')
                answer, links = self._phase_user_ask_synthesis(
                    director,
                    session_id,
                    question,
                    participants=list(ctx.get('participants') or ['director']),
                    blocked_tools=blocked_tools
                )
                search_links = self._merge_web_links(search_links, links)
                self._set_session_progress(
                    session_id=session_id,
                    workflow='user_ask',
                    title=str(question or '')[:80],
                    steps=progress_steps,
                    current_index=len(progress_steps),
                    current_step='总监答复',
                    detail='最终答复已生成。',
                    state='completed',
                    actor='director',
                )
                return answer, {
                    'participants': list(ctx.get('participants') or ['director']),
                    'required_agents': list(ctx.get('required_agents') or []),
                    'routing': ctx.get('routing') or {},
                    'workflow_steps': list(ctx.get('executed_steps') or []) + ['director_synthesis'],
                    'search_links': search_links
                }
            bus.post_activity(
                'director', 'status',
                '自定义流程未产出最终答复，降级为总监直答',
                metadata={'phase': 'user_ask_direct', 'mode': 'user_ask', 'session_id': session_id}
            )
            think_ret = director.think(
                question,
                session_id=session_id,
                max_tool_rounds=USER_ASK_TEAM_MAX_TOOL_ROUNDS,
                response_style='auto',
                blocked_tools=blocked_tools,
                return_meta=True
            )
            answer, links = self._parse_think_result(think_ret)
            search_links = self._merge_web_links(search_links, links)
            bus.save_result(session_id, 'director_user_ask_final', answer)
            bus.broadcast(
                'director', 'consensus', '直连问答已完成（降级直答）',
                metadata={
                    'phase': 'user_ask_final',
                    'title': '直连问答答复',
                    'mode': 'user_ask',
                    'session_id': session_id,
                    'full_content': answer,
                    'participants': list(ctx.get('participants') or ['director']),
                }
            )
        self._set_session_progress(
            session_id=session_id,
            workflow='user_ask',
            title=str(question or '')[:80],
            steps=progress_steps,
            current_index=len(progress_steps),
            current_step='总监答复',
            detail='最终答复已生成。',
            state='completed',
            actor='director',
        )

        return answer, {
            'participants': list(ctx.get('participants') or ['director']),
            'required_agents': list(ctx.get('required_agents') or []),
            'routing': ctx.get('routing') or {},
            'workflow_steps': ctx.get('executed_steps') or [],
            'search_links': search_links
        }

    def run_director_user_ask(
            self,
            question,
            return_meta=False,
            session_id=None,
            ask_mode='team',
            enable_web_search=True
    ):
        """
        直连问答主入口：
        - quick: 快速回答（仅联网检索 + 时间工具，不走团队）
        - deep: 深度分析（单总监高轮次，不走团队）
        - team: 团队协作（按工作流配置执行）
        - auto: 保持既有“路由-执行-会议-汇总”逻辑
        - director_only: 仅总监作答
        - custom: 执行用户自定义步骤（串行/并行/会议）
        """
        from AlphaFin.ai_team.core.agent_registry import get_agent

        ask_mode = self._normalize_user_ask_mode(ask_mode)
        enable_web_search = self._to_bool(enable_web_search, True)
        session_id = str(session_id or '').strip()
        if not session_id:
            session_id = 'user_ask_' + str(uuid.uuid4())[:8]
        clear_session_cancel(session_id)
        self._trace_start_session(session_id, workflow='user_ask', topic=question or '')
        run_status = 'ok'
        run_meta = {'workflow': 'user_ask', 'ask_mode': ask_mode}
        director = get_agent('director')
        if not director:
            run_status = 'error'
            run_meta = {'workflow': 'user_ask', 'ask_mode': ask_mode, 'reason': 'director_unavailable'}
            failure = {
                'answer': '决策总监当前不可用，请稍后重试。',
                'session_id': session_id,
                'ask_mode': ask_mode,
                'enable_web_search': enable_web_search,
                'workflow_mode': 'offline',
                'workflow_name': '不可用',
                'participants': [],
                'required_agents': [],
                'workflow_steps': [],
                'search_links': [],
            }
            self._last_user_ask_meta = failure
            return failure if return_meta else failure['answer']

        cfg = self.get_user_ask_workflow()
        mode = str(cfg.get('mode') or 'auto')
        workflow_name = str(cfg.get('name') or '默认透明流程')
        workflow_limits = cfg.get('limits') if isinstance(cfg.get('limits'), dict) else {}
        workflow_timeout = 0
        if ask_mode == 'team':
            workflow_timeout = max(
                60,
                int(workflow_limits.get('timeout_seconds') or TEAM_WORKFLOW_DEFAULT_TIMEOUT)
            )
        mode_name_map = {
            'quick': '快速回答（联网）',
            'deep': '深度分析（单总监）',
            'team': workflow_name,
        }
        effective_workflow_mode = ask_mode if ask_mode in ('quick', 'deep') else mode
        effective_workflow_name = mode_name_map.get(ask_mode, workflow_name)
        run_meta = {
            'workflow': 'user_ask',
            'ask_mode': ask_mode,
            'mode': effective_workflow_mode,
            'workflow_name': effective_workflow_name
        }

        meta = {
            'answer': '',
            'session_id': session_id,
            'ask_mode': ask_mode,
            'enable_web_search': enable_web_search,
            'workflow_mode': effective_workflow_mode,
            'workflow_name': effective_workflow_name,
            'participants': ['director'],
            'required_agents': [],
            'workflow_steps': [],
            'search_links': [],
        }
        progress_steps = self._user_ask_progress_steps(ask_mode, mode, cfg)
        if workflow_timeout > 0:
            meta['session_timing'] = self._start_session_deadline(
                session_id,
                'user_ask',
                title=str(question or '')[:80],
                time_limit_seconds=workflow_timeout,
                source='user_ask_workflow'
            )
        else:
            meta['session_timing'] = self._session_timing(session_id)
        self._set_session_progress(
            session_id=session_id,
            workflow='user_ask',
            title=str(question or '')[:80],
            steps=progress_steps,
            current_index=1,
            current_step=progress_steps[0] if progress_steps else '用户问答',
            detail='当前正在准备回答策略与执行路径。',
            state='running',
            actor='director',
        )
        meta['session_progress'] = self._session_progress(session_id)
        meta['session_overtime'] = self._session_overtime(session_id)

        bus.post_activity(
            'orchestrator', 'status',
            '直连问答启动（session=%s）' % session_id,
            metadata={
                'session_id': session_id,
                'mode': 'user_ask',
                'topic': question,
                'ask_mode': ask_mode,
                'enable_web_search': enable_web_search,
                'workflow_mode': effective_workflow_mode,
                'workflow_name': effective_workflow_name,
            }
        )

        budget_snapshot = self._get_budget_snapshot(session_id)
        if budget_snapshot.get('level') == 'exhausted':
            self._notify_budget(session_id, budget_snapshot, force=True)
            run_status = 'error'
            run_meta = {
                'workflow': 'user_ask',
                'ask_mode': ask_mode,
                'mode': effective_workflow_mode,
                'reason': 'budget_exhausted'
            }
            msg = '当前会话 Token 预算已耗尽，直连问答暂不可执行。请稍后重试。'
            meta.update({
                'answer': msg,
                'workflow_steps': ['budget_block'],
            })
            self._last_user_ask_meta = meta
            return meta if return_meta else msg

        try:
            if self._session_stop_requested(session_id):
                msg = '当前问答已停止。'
                meta.update({
                    'answer': msg,
                    'workflow_steps': ['stopped'],
                })
                self._last_user_ask_meta = meta
                return meta if return_meta else msg
            q = (question or '').strip()
            if self._is_status_query_text(q):
                answer = self._build_user_ask_status_snapshot()
                bus.save_result(session_id, 'director_user_ask_final', answer)
                bus.broadcast(
                    'director', 'consensus', '直连问答已完成（团队状态快照）',
                    metadata={
                        'phase': 'user_ask_final',
                        'title': '直连问答答复',
                        'mode': 'user_ask',
                        'session_id': session_id,
                        'meeting_active': False,
                        'participants': ['director'],
                        'full_content': answer,
                    }
                )
                meta.update({
                    'answer': answer,
                    'participants': ['director'],
                    'required_agents': [],
                    'workflow_steps': ['status_snapshot'],
                    'search_links': [],
                })
                self._last_user_ask_meta = meta
                return meta if return_meta else answer

            if ask_mode == 'quick':
                simple_direct = self._is_simple_user_ask_text(q)
                self._set_session_progress(
                    session_id=session_id,
                    workflow='user_ask',
                    title=str(question or '')[:80],
                    steps=progress_steps,
                    current_index=1,
                    current_step='总监快答',
                    detail=('命中简答策略，直接快速答复。' if simple_direct else '决策总监正在进行快速联网回答。'),
                    state='running',
                    actor='director',
                )
                # 快速模式保留低时延，同时允许轻量技术面（K线+技术解读）。
                quick_allowed = ['get_kline_technical', 'get_kline', 'get_intraday_stock_quote', 'get_current_time']
                if enable_web_search:
                    quick_allowed.append('web_search')
                quick_rounds = 1
                think_ret = director.think(
                    question,
                    session_id=session_id,
                    max_tool_rounds=quick_rounds,
                    allowed_tools=quick_allowed,
                    blocked_tools=([] if enable_web_search else ['web_search']),
                    response_style='quick',
                    return_meta=True
                )
                answer, search_links = self._parse_think_result(think_ret)
                bus.save_result(session_id, 'director_user_ask_final', answer)
                bus.broadcast(
                    'director', 'consensus', '直连问答已完成（快速模式）',
                    metadata={
                        'phase': 'user_ask_final',
                        'title': '直连问答答复',
                        'mode': 'user_ask',
                        'session_id': session_id,
                        'full_content': answer,
                        'participants': ['director'],
                        'ask_mode': ask_mode,
                    }
                )
                meta.update({
                    'answer': answer,
                    'workflow_steps': ['director_quick'],
                    'search_links': search_links
                })
                self._set_session_progress(
                    session_id=session_id,
                    workflow='user_ask',
                    title=str(question or '')[:80],
                    steps=progress_steps,
                    current_index=len(progress_steps),
                    current_step='返回结果',
                    detail='快速回答已完成。',
                    state='completed',
                    actor='director',
                )
                self._last_user_ask_meta = meta
                return meta if return_meta else answer

            if ask_mode == 'deep':
                self._set_session_progress(
                    session_id=session_id,
                    workflow='user_ask',
                    title=str(question or '')[:80],
                    steps=progress_steps,
                    current_index=1,
                    current_step='总监深度分析',
                    detail='决策总监正在进行深度分析。',
                    state='running',
                    actor='director',
                )
                think_ret = director.think(
                    question,
                    session_id=session_id,
                    max_tool_rounds=USER_ASK_DEEP_MAX_TOOL_ROUNDS,
                    response_style='deep',
                    blocked_tools=([] if enable_web_search else ['web_search']),
                    return_meta=True
                )
                answer, search_links = self._parse_think_result(think_ret)
                bus.save_result(session_id, 'director_user_ask_final', answer)
                bus.broadcast(
                    'director', 'consensus', '直连问答已完成（深度模式）',
                    metadata={
                        'phase': 'user_ask_final',
                        'title': '直连问答答复',
                        'mode': 'user_ask',
                        'session_id': session_id,
                        'full_content': answer,
                        'participants': ['director'],
                        'ask_mode': ask_mode,
                    }
                )
                meta.update({
                    'answer': answer,
                    'workflow_steps': ['director_deep'],
                    'search_links': search_links
                })
                self._set_session_progress(
                    session_id=session_id,
                    workflow='user_ask',
                    title=str(question or '')[:80],
                    steps=progress_steps,
                    current_index=len(progress_steps),
                    current_step='返回结果',
                    detail='深度分析已完成。',
                    state='completed',
                    actor='director',
                )
                self._last_user_ask_meta = meta
                return meta if return_meta else answer

            # team 模式：执行原有工作流（auto/director_only/custom）
            blocked_tools = [] if enable_web_search else ['web_search']
            if mode == 'director_only':
                self._set_session_progress(
                    session_id=session_id,
                    workflow='user_ask',
                    title=str(question or '')[:80],
                    steps=progress_steps,
                    current_index=1,
                    current_step='总监直答',
                    detail='当前由决策总监直接回答。',
                    state='running',
                    actor='director',
                )
                think_ret = director.think(
                    question,
                    session_id=session_id,
                    max_tool_rounds=USER_ASK_TEAM_MAX_TOOL_ROUNDS,
                    response_style='team',
                    blocked_tools=blocked_tools,
                    return_meta=True
                )
                answer, search_links = self._parse_think_result(think_ret)
                bus.save_result(session_id, 'director_user_ask_final', answer)
                bus.broadcast(
                    'director', 'consensus', '直连问答已完成（仅总监模式）',
                    metadata={
                        'phase': 'user_ask_final',
                        'title': '直连问答答复',
                        'mode': 'user_ask',
                        'session_id': session_id,
                        'full_content': answer,
                        'participants': ['director'],
                    }
                )
                meta.update({
                    'answer': answer,
                    'workflow_steps': ['director_direct'],
                    'search_links': search_links
                })
                self._set_session_progress(
                    session_id=session_id,
                    workflow='user_ask',
                    title=str(question or '')[:80],
                    steps=progress_steps,
                    current_index=len(progress_steps),
                    current_step='返回结果',
                    detail='总监直答已完成。',
                    state='completed',
                    actor='director',
                )
                self._last_user_ask_meta = meta
                return meta if return_meta else answer

            if mode == 'custom':
                if cfg.get('auto_for_simple', True) and self._is_simple_user_ask_text(q):
                    think_ret = director.think(
                        question,
                        session_id=session_id,
                        max_tool_rounds=USER_ASK_TEAM_MAX_TOOL_ROUNDS,
                        response_style='auto',
                        blocked_tools=blocked_tools,
                        return_meta=True
                    )
                    answer, search_links = self._parse_think_result(think_ret)
                    bus.save_result(session_id, 'director_user_ask_final', answer)
                    bus.broadcast(
                        'director', 'consensus', '直连问答已完成（简答直通）',
                        metadata={
                            'phase': 'user_ask_final',
                            'title': '直连问答答复',
                            'mode': 'user_ask',
                            'session_id': session_id,
                            'full_content': answer,
                            'participants': ['director'],
                        }
                    )
                    meta.update({
                        'answer': answer,
                        'workflow_steps': ['director_direct'],
                        'search_links': search_links
                    })
                else:
                    answer, extra = self._run_director_user_ask_custom(
                        director=director,
                        session_id=session_id,
                        question=question,
                        workflow_cfg=cfg,
                        enable_web_search=enable_web_search
                    )
                    meta.update(extra or {})
                    meta['answer'] = answer
                self._last_user_ask_meta = meta
                return meta if return_meta else meta.get('answer', '')

            answer, extra = self._run_director_user_ask_auto(
                director,
                session_id,
                question,
                enable_web_search=enable_web_search
            )
            meta.update(extra or {})
            meta['answer'] = answer
            self._last_user_ask_meta = meta
            return meta if return_meta else answer
        except Exception as e:
            run_status = 'error'
            run_meta = {
                'workflow': 'user_ask',
                'ask_mode': ask_mode,
                'mode': effective_workflow_mode,
                'error': str(e)
            }
            bus.post_activity(
                'orchestrator', 'error',
                '直连问答异常: %s' % str(e),
                metadata={'session_id': session_id, 'mode': 'user_ask'}
            )
            traceback.print_exc()
            meta.update({
                'answer': '处理该问题时发生异常：%s' % str(e),
                'workflow_steps': list(meta.get('workflow_steps') or []) + ['error']
            })
            self._last_user_ask_meta = meta
            return meta if return_meta else meta['answer']
        finally:
            bus.clear_session(session_id)
            self._clear_session_deadline(session_id)
            clear_session_overtime_state(session_id)
            self._clear_session_progress(session_id)
            self._trace_finish_session(session_id, status=run_status, meta=run_meta)

    def _phase_user_ask_synthesis(
            self,
            director,
            session_id,
            question,
            participants=None,
            blocked_tools=None
    ):
        """直连问答的总监最终回复阶段（确保有连续闭环）。"""
        if self._session_stop_requested(session_id):
            self._post_session_stopped(session_id, 'user_ask')
            return '当前问答已停止。', []
        director.process_incoming_messages(session_id)
        results = bus.get_session_results(session_id)
        context = '\n=== 团队答题过程材料 ===\n'
        for rid, rcontent in results.items():
            context += '\n【%s】：\n%s\n' % (rid, rcontent)

        prompt = format_ai_team_prompt(
            'workflow.user_ask_final_synthesis',
            question=question,
            context=context
        )

        think_ret = director.think(
            prompt,
            session_id=session_id,
            max_tool_rounds=1,
            allowed_tools=[],
            response_style='deep',
            blocked_tools=blocked_tools,
            return_meta=True
        )
        answer, search_links = self._parse_think_result(think_ret)
        bus.save_result(session_id, 'director_user_ask_final', answer)
        preview = (answer or '')[:220]
        if answer and len(answer) > 220:
            preview += '...'
        bus.broadcast(
            'director', 'consensus', '直连问答已生成最终答复：\n' + (preview or '(空)'),
            metadata={
                'phase': 'user_ask_final',
                'title': '直连问答答复',
                'mode': 'user_ask',
                'session_id': session_id,
                'meeting_active': False,
                'participants': participants or ['director'],
                'full_content': answer,
            }
        )
        return answer, search_links

    def set_interval(self, seconds):
        """更新研究周期间隔"""
        with self._lock:
            self.cycle_interval = max(300, seconds)  # 最少5分钟
            if seconds > 0:
                self.manual_only = False
            self._persist_runtime_config()
            bus.post_activity('orchestrator', 'status',
                              '研究周期间隔已更新为 %d 秒' % self.cycle_interval)

    def set_manual_only(self, enabled):
        """设置仅手动模式"""
        enabled = bool(enabled)
        with self._lock:
            self.manual_only = enabled
            if enabled:
                self.paused = True
                bus.post_activity('orchestrator', 'status', '研究周期已切换为仅手动模式')
            else:
                bus.post_activity('orchestrator', 'status', '研究周期已退出仅手动模式')
            self._persist_runtime_config()

    def pause(self):
        """暂停自动周期"""
        with self._lock:
            self.paused = True
            self._persist_runtime_config()
        bus.post_activity('orchestrator', 'status', '自动研究已暂停')

    def resume(self):
        """恢复自动周期"""
        with self._lock:
            if self.manual_only:
                self.manual_only = False
            self.paused = False
            self._persist_runtime_config()
        bus.post_activity('orchestrator', 'status', '自动研究已恢复')

    def set_idle(self, enabled=None, interval=None):
        """更新闲时学习配置"""
        with self._lock:
            if enabled is not None:
                self.idle_enabled = bool(enabled)
                bus.post_activity(
                    'orchestrator', 'status',
                    '闲时学习已%s' % ('开启' if self.idle_enabled else '关闭')
                )
            if interval is not None:
                self.idle_interval = max(300, int(interval))
                bus.post_activity(
                    'orchestrator', 'status',
                    '闲时学习间隔已更新为 %d 秒' % self.idle_interval
                )
            self._persist_runtime_config()

    def set_office_chat(self, enabled=None, interval=None):
        """更新同事闲聊配置"""
        with self._lock:
            if enabled is not None:
                self.office_chat_enabled = bool(enabled)
                bus.post_activity(
                    'orchestrator', 'status',
                    '同事闲聊已%s' % ('开启' if self.office_chat_enabled else '关闭'),
                    metadata={'mode': 'office_chat'}
                )
            if interval is not None:
                self.office_chat_interval = max(600, int(interval))
                bus.post_activity(
                    'orchestrator', 'status',
                    '同事闲聊间隔已更新为 %d 秒' % self.office_chat_interval,
                    metadata={'mode': 'office_chat'}
                )
            self._persist_runtime_config()

    def stop(self):
        """停止后台自动循环（不清理历史数据）。"""
        with self._lock:
            self.running = False
            self.paused = True
        bus.post_activity('orchestrator', 'status', '智能分析模块后台循环已停止')

    def trigger_now(self, topic=None, time_limit_seconds=None):
        """立即触发一次研究周期（在新线程中）"""
        if self.current_session or self._is_portfolio_busy():
            return ''
        try:
            from AlphaFin.ai_team.core.agent_registry import clear_stop_all_agents
            clear_stop_all_agents()
        except Exception:
            pass
        t = threading.Thread(target=self.run_cycle, args=(topic, time_limit_seconds), daemon=True)
        t.start()
        return self.current_session or 'starting'

    def trigger_idle_now(self, theme=None):
        """立即触发一次闲时学习周期（在新线程中）"""
        try:
            from AlphaFin.ai_team.core.agent_registry import clear_stop_all_agents
            clear_stop_all_agents()
        except Exception:
            pass
        t = threading.Thread(target=self.run_idle_cycle, args=(theme,), daemon=True)
        t.start()
        return self.current_session or 'starting'

    def trigger_office_chat_now(self, topic=None):
        """立即触发一次同事闲聊（在新线程中）"""
        if self.current_session or self._is_portfolio_busy():
            return ''
        try:
            from AlphaFin.ai_team.core.agent_registry import clear_stop_all_agents
            clear_stop_all_agents()
        except Exception:
            pass
        t = threading.Thread(target=self.run_office_chat_cycle, args=(topic,), daemon=True)
        t.start()
        return self.current_session or 'starting'

    def get_state(self):
        """获取调度器状态"""
        self._sync_runtime_config()
        budget_snapshot = self._get_budget_snapshot(self.current_session or '')
        with self._lock:
            return {
                'running': self.running,
                'paused': self.paused,
                'manual_only': self.manual_only,
                'cycle_interval': self.cycle_interval,
                'cycle_count': self.cycle_count,
                'current_session': self.current_session,
                'last_cycle_time': self.last_cycle_time,
                'idle_enabled': self.idle_enabled,
                'idle_interval': self.idle_interval,
                'idle_count': self.idle_count,
                'last_idle_time': self.last_idle_time,
                'office_chat_enabled': self.office_chat_enabled,
                'office_chat_interval': self.office_chat_interval,
                'office_chat_count': self.office_chat_count,
                'last_office_chat_time': self.last_office_chat_time,
                'user_ask_workflow_mode': (self.user_ask_workflow or {}).get('mode', 'auto'),
                'user_ask_workflow_name': (self.user_ask_workflow or {}).get('name', '默认透明流程'),
                'session_timing': self._session_timing(self.current_session),
                'session_progress': self._session_progress(self.current_session),
                'session_overtime': self._session_overtime(self.current_session),
                'budget': budget_snapshot,
            }


# 全局单例
orchestrator = Orchestrator()
