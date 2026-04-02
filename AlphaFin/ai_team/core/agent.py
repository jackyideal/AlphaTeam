"""
Agent 基类 - 每个智能体的核心引擎
复用 ai_chat_service.py 的 Qwen 调用模式，扩展为支持 function calling
"""
import json
import datetime
import time
import traceback
import re
import threading
from urllib.parse import urlparse
import requests
from requests.adapters import HTTPAdapter

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

from AlphaFin.ai_team.config import (
    QWEN_BASE_URL, QWEN_MODEL, QWEN_FALLBACK_MODEL,
    QWEN_MAX_PARALLEL_REQUESTS, QWEN_REQUEST_RETRIES, QWEN_RETRY_BACKOFF,
    MAX_CONVERSATION_CONTEXT, TOOL_PROTOCOL_DEFAULT,
    TOKEN_BUDGET_ENABLED, TOKEN_BUDGET_DAILY_LIMIT, TOKEN_BUDGET_SESSION_LIMIT,
    TOKEN_BUDGET_WARN_RATIO, TOKEN_BUDGET_HARD_RATIO,
)
from AlphaFin.ai_team.core.memory import (
    AgentMemory,
    get_token_budget_snapshot,
    record_token_usage,
    trace_get_active_run_id,
    trace_start_run,
    trace_finish_run,
    trace_start_span,
    trace_finish_span,
)
from AlphaFin.ai_team.core.tool_registry import TOOLS_SCHEMA, execute_tool, RUN_INDICATOR_ENABLED
from AlphaFin.ai_team.core.message_bus import bus
from AlphaFin.ai_team.core.session_control import (
    is_session_cancelled,
    get_session_cancel_reason,
    get_session_timing,
)
from AlphaFin.ai_team.prompt_catalog import (
    format_ai_team_prompt,
    get_team_core_context,
    get_agent_role_memory,
    get_agent_memory_seed_pack,
)
from AlphaFin.services.model_config_service import normalize_model_name


class Agent:
    """
    AI 智能体基类。
    每个智能体拥有独立的 API key、对话上下文、记忆和工具集。
    """
    _qwen_semaphore = threading.BoundedSemaphore(max(1, int(QWEN_MAX_PARALLEL_REQUESTS)))

    def __init__(
            self,
            agent_id,
            name,
            api_key,
            system_prompt,
            tool_names=None,
            model_name='',
            fallback_model=''
    ):
        """
        Args:
            agent_id: 智能体唯一ID
            name: 显示名称
            api_key: 独立的 Qwen API key
            system_prompt: 角色定义的系统提示词
            tool_names: 可用工具名称列表（None=全部工具）
        """
        self.agent_id = agent_id
        self.name = name
        self.api_key = api_key
        self.system_prompt = system_prompt
        self.memory = AgentMemory(agent_id)
        self.role_memory_text = get_agent_role_memory(agent_id, name)
        self._request_timeout = 180
        self.model_name = normalize_model_name(model_name or QWEN_MODEL)
        self.fallback_model = str(fallback_model or QWEN_FALLBACK_MODEL).strip()

        # 为每个智能体复用 HTTP 连接，减少并发场景下重复 DNS/建连开销
        self.http = requests.Session()
        pool_size = max(4, int(QWEN_MAX_PARALLEL_REQUESTS) * 2)
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
        self.http.mount('https://', adapter)
        self.http.mount('http://', adapter)

        # 筛选可用工具
        if tool_names:
            self.tools = [t for t in TOOLS_SCHEMA if t['function']['name'] in tool_names]
        else:
            self.tools = list(TOOLS_SCHEMA)
        if not RUN_INDICATOR_ENABLED:
            self.tools = [
                t for t in self.tools
                if t.get('function', {}).get('name') != 'run_indicator'
            ]

        # 对话上下文（仅本智能体）
        self.conversation = []

        # 状态追踪
        self.status = 'idle'           # idle/thinking/using_tool/speaking
        self.current_task = None       # 当前正在处理的任务描述
        self.last_active = 0           # 最后活跃时间戳
        self.current_session_id = ''   # 当前会话ID（用于识别工作流）
        self.current_workflow = 'idle'  # 当前工作流标识
        self.current_workflow_label = '空闲待命'
        self.current_response_style = 'auto'
        self._active_trace_run_id = ''
        self._active_trace_span_id = ''
        self._active_trace_owned_run = False
        self._budget_alert_ts = 0.0
        self._last_call_budget_level = 'normal'
        self._last_call_budget_exhausted = False
        self._stop_requested = False
        self._stop_reason = ''
        self._work_lock = threading.Lock()
        self.current_plan_steps = []
        self.current_plan_details = []
        self.current_step = ''
        self.next_step = ''
        self.current_step_reason = ''
        self.current_step_index = 0
        self.current_step_total = 0
        self.current_tool = ''
        self.current_prompt_profile = {}
        self.current_memory_snapshot = {}
        self.last_runtime_state = {}
        self._seed_core_memory()

        # 注册到消息总线
        bus.register_agent(agent_id)

    @staticmethod
    def _workflow_from_session(session_id):
        """根据 session_id 推断活动工作流分类"""
        if not session_id:
            return ''
        if session_id.startswith('idle_'):
            return 'idle_learning'
        if session_id.startswith('inv_'):
            return 'portfolio_investment'
        if session_id.startswith('watch_'):
            return 'market_watch'
        if session_id.startswith('chat_'):
            return 'office_chat'
        if session_id.startswith('msg_'):
            return 'agent_message'
        if session_id.startswith('user_ask'):
            return 'user_ask'
        # 其余带 session 的任务默认归类为研究流程
        return 'auto_research'

    def _activity_meta(self, session_id=None, extra=None):
        """构造统一活动元信息，便于前端做日志分类/筛选"""
        meta = {}
        if session_id:
            meta['session_id'] = session_id
            workflow = self._workflow_from_session(session_id)
            if workflow:
                meta['workflow'] = workflow
                meta['mode'] = workflow
        if extra:
            meta.update(extra)
        return meta

    @staticmethod
    def _workflow_label(workflow):
        mapping = {
            'idle': '空闲待命',
            'auto_research': '自动研究',
            'idle_learning': '闲时学习',
            'market_watch': '盘中盯盘',
            'portfolio_investment': '投资执行',
            'office_chat': '同事交流',
            'agent_message': '团队协作',
            'user_ask': '用户问答',
            'ad_hoc': '临时任务',
        }
        return mapping.get(workflow or '', '临时任务')

    @staticmethod
    def _normalize_response_style(response_style):
        style = str(response_style or '').strip().lower()
        if style in ('auto', 'quick', 'deep', 'team'):
            return style
        return 'auto'

    def _build_prompt_profile(self, session_id, response_style, timing, enforce_rigorous, high_risk_task):
        style = self._normalize_response_style(response_style)
        workflow = self._workflow_from_session(session_id) or self.current_workflow or 'ad_hoc'
        modifiers = ['角色系统提示词', '团队核心宪章', '角色职责记忆', '记忆协议', '北京时间与交易时段']
        if timing.get('active'):
            modifiers.append('任务时限约束')
        if style == 'quick':
            modifiers.append('快速回答约束')
        elif style == 'deep':
            modifiers.append('深度分析约束')
        elif style == 'team':
            modifiers.append('团队协作约束')
        if enforce_rigorous:
            modifiers.append('可信输出增强')
        if high_risk_task:
            modifiers.append('高风险任务审慎模式')
        return {
            'agent_id': self.agent_id,
            'agent_name': self.name,
            'workflow': workflow,
            'response_style': style,
            'system_prompt_preview': str(self.system_prompt or '')[:220],
            'modifiers': modifiers,
            'timing_state': str(timing.get('state') or ''),
            'high_risk_task': bool(high_risk_task),
            'enforce_rigorous': bool(enforce_rigorous),
            'model': self.model_name,
        }

    def _seed_core_memory(self):
        """为智能体注入团队长期使命、记忆协议与角色职责的 HOT 种子记忆。"""
        try:
            pack = get_agent_memory_seed_pack(self.agent_id, self.name)
        except Exception:
            pack = []
        for item in pack:
            try:
                self.memory.ensure_seed_knowledge(
                    category=str(item.get('category') or 'seed_memory'),
                    subject=str(item.get('subject') or 'seed_subject'),
                    content=str(item.get('content') or ''),
                    confidence=0.98,
                    valid_days=0,
                    tier=str(item.get('tier') or 'hot'),
                    tags=item.get('tags') or [],
                    source_type='team_seed',
                )
            except Exception:
                continue

    def _set_runtime_context(self, session_id, task_message, response_style='auto'):
        workflow = self._workflow_from_session(session_id)
        if not workflow:
            workflow = 'ad_hoc' if (task_message or '').strip() else 'idle'
        self.current_session_id = session_id or ''
        self.current_workflow = workflow
        self.current_workflow_label = self._workflow_label(workflow)
        self.current_response_style = self._normalize_response_style(response_style)

    def _mark_idle(self):
        self._remember_runtime_state()
        self.status = 'idle'
        self.current_task = None
        self.current_session_id = ''
        self.current_workflow = 'idle'
        self.current_workflow_label = self._workflow_label('idle')
        self.current_response_style = 'auto'
        self._active_trace_run_id = ''
        self._active_trace_span_id = ''
        self._active_trace_owned_run = False
        self.current_plan_steps = []
        self.current_plan_details = []
        self.current_step = ''
        self.next_step = ''
        self.current_step_reason = ''
        self.current_step_index = 0
        self.current_step_total = 0
        self.current_tool = ''
        self.current_prompt_profile = {}
        self.current_memory_snapshot = {}

    def _runtime_state_snapshot(self):
        return {
            'current_task': self.current_task,
            'current_session_id': self.current_session_id,
            'current_workflow': self.current_workflow,
            'current_workflow_label': self.current_workflow_label,
            'current_response_style': self.current_response_style,
            'current_plan_steps': list(self.current_plan_steps or []),
            'current_plan_details': self._serialize_plan_details(),
            'current_step': self.current_step,
            'next_step': self.next_step,
            'current_step_reason': self.current_step_reason,
            'current_step_index': self.current_step_index,
            'current_step_total': self.current_step_total,
            'current_tool': self.current_tool,
            'prompt_profile': dict(self.current_prompt_profile or {}),
            'memory_snapshot': dict(self.current_memory_snapshot or {}),
            'updated_at': time.time(),
        }

    def _remember_runtime_state(self):
        snap = self._runtime_state_snapshot()
        if not (
            snap.get('current_session_id') or
            snap.get('current_task') or
            snap.get('current_step') or
            snap.get('current_plan_details') or
            snap.get('prompt_profile') or
            snap.get('memory_snapshot')
        ):
            return
        self.last_runtime_state = snap

    def request_stop(self, reason=''):
        """请求停止当前任务（尽力中断）。"""
        self._stop_requested = True
        self._stop_reason = str(reason or '用户手动停止')
        self.current_step = '停止中'
        self.next_step = '结束当前任务'
        self.current_step_reason = self._stop_reason
        self.last_active = time.time()
        bus.post_activity(
            self.agent_id, 'status',
            '收到停止指令，终止当前任务。',
            metadata=self._activity_meta(self.current_session_id, {'reason': self._stop_reason})
        )

    def clear_stop_request(self):
        """清除停止标记，允许后续新任务执行。"""
        self._stop_requested = False
        self._stop_reason = ''

    def _abort_by_stop(self, session_id=None):
        msg = '已按用户指令停止当前任务。'
        self._trace_finish_think(status='error', error_text='stopped_by_user')
        self._mark_idle()
        bus.post_activity(
            self.agent_id, 'status', msg,
            metadata=self._activity_meta(session_id, {'reason': self._stop_reason or 'user_stop'})
        )
        return msg

    def _abort_by_cancelled_session(self, session_id=None):
        msg = '当前会话已停止，结束本次执行。'
        self._trace_finish_think(status='error', error_text='session_cancelled')
        self._mark_idle()
        bus.post_activity(
            self.agent_id, 'status', msg,
            metadata=self._activity_meta(
                session_id,
                {'reason': get_session_cancel_reason(session_id, '用户手动停止')}
            )
        )
        return msg

    @staticmethod
    def _tool_purpose(tool_name):
        mapping = {
            'web_search': '核验外部新闻与网站信息',
            'get_current_time': '确认当前时间与交易时段',
            'get_kline': '查看价格走势与结构',
            'get_intraday_stock_quote': '核验实时行情',
            'get_intraday_index': '核验指数分时强弱',
            'get_intraday_news': '跟踪盘中新闻变化',
            'get_intraday_sector_heat': '判断行业与风格热度',
            'get_intraday_hotrank': '跟踪热点标的变化',
            'get_financials': '核验财务与基本面',
            'get_chip_distribution': '核验筹码结构',
            'query_database': '提取数据库中的关键数据',
            'run_indicator': '调用指标做交叉验证',
            'submit_trade_signal': '提交交易建议',
            'review_trade_signal': '执行风控或审批决策',
            'get_portfolio_status': '查看组合状态与持仓约束',
            'get_trade_signals': '核验信号队列状态',
            'send_message_to_agent': '向团队成员补充信息',
        }
        return mapping.get(tool_name, '补充当前结论所需的关键信息')

    @staticmethod
    def _task_requires_numeric_details(task_message):
        text = str(task_message or '').strip()
        if not text:
            return False
        keys = (
            '估值', '净值', '仓位', '止损', '止盈', '收益', '回撤',
            '财务', '利润', '收入', '现金流', '目标价', '测算',
            '回测', '胜率', '融资融券', '市盈率', '市净率', '股息',
            '涨跌幅', '价格', '股价'
        )
        return any(k in text for k in keys)

    @staticmethod
    def _plan_titles(plan_details):
        titles = []
        for row in plan_details or []:
            if isinstance(row, dict):
                title = str(row.get('title') or '').strip()
            else:
                title = str(row or '').strip()
            if title:
                titles.append(title)
        return titles

    def _serialize_plan_details(self):
        out = []
        for idx, row in enumerate(self.current_plan_details or [], start=1):
            if not isinstance(row, dict):
                row = {'title': str(row or '')}
            out.append({
                'index': int(row.get('index') or idx),
                'title': str(row.get('title') or '').strip(),
                'goal': str(row.get('goal') or '').strip(),
                'done_when': str(row.get('done_when') or '').strip(),
                'preferred_tools': list(row.get('preferred_tools') or []),
                'status': str(row.get('status') or 'pending'),
                'summary': str(row.get('summary') or '').strip(),
            })
        return out

    @staticmethod
    def _extract_json_object(text):
        raw = str(text or '').strip()
        if not raw:
            return None
        candidates = [raw]
        fence = re.findall(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', raw, flags=re.IGNORECASE)
        candidates.extend(fence)
        first = raw.find('{')
        last = raw.rfind('}')
        if first >= 0 and last > first:
            candidates.append(raw[first:last + 1])
        for cand in candidates:
            try:
                return json.loads(cand)
            except Exception:
                continue
        return None

    def _fallback_task_plan(self, response_style='auto', max_tool_rounds=0):
        style = self._normalize_response_style(response_style)
        if style == 'quick' or int(max_tool_rounds or 0) <= 1:
            rows = [
                ('确认问题范围', '先明确用户问的标的、时间口径与最终要回答的问题。', '已经确认本轮回答的核心问题与边界。'),
                ('快速核验关键信息', '仅核验支撑回答所需的最关键事实或行情。', '已经拿到足以回答问题的关键依据。'),
                ('形成直接答复', '基于已核验信息给出清晰直接的答复。', '已经输出最终答复。'),
            ]
        else:
            rows = [
                ('确认研究范围', '明确任务目标、时间范围、关键对象与需要判断的核心矛盾。', '研究范围、关注对象与回答目标已明确。'),
                ('收集核心证据', '围绕当前任务收集最关键的行情、新闻、财务或工具证据。', '已经拿到支撑判断的核心证据。'),
                ('提炼因果与分歧', '梳理驱动逻辑、反方观点与失效条件，不再重复堆砌数据。', '已经形成清晰的逻辑链和主要分歧。'),
                ('形成结论与建议', '把证据收敛成可执行结论、观察点与后续建议。', '已经输出最终结论与建议。'),
            ]
        out = []
        for idx, row in enumerate(rows, start=1):
            out.append({
                'index': idx,
                'title': row[0],
                'goal': row[1],
                'done_when': row[2],
                'preferred_tools': [],
                'status': 'pending',
                'summary': '',
            })
        return out

    def _normalize_task_plan_details(self, raw_steps, response_style='auto', max_tool_rounds=0):
        fallback = self._fallback_task_plan(response_style=response_style, max_tool_rounds=max_tool_rounds)
        if not isinstance(raw_steps, list):
            return fallback
        normalized = []
        for row in raw_steps:
            if not isinstance(row, dict):
                continue
            title = str(row.get('title') or '').strip()
            goal = str(row.get('goal') or '').strip()
            done_when = str(row.get('done_when') or '').strip()
            preferred_tools = [str(x).strip() for x in (row.get('preferred_tools') or []) if str(x).strip()]
            if not title:
                continue
            normalized.append({
                'index': len(normalized) + 1,
                'title': title[:28],
                'goal': goal[:120],
                'done_when': done_when[:120],
                'preferred_tools': preferred_tools[:6],
                'status': 'pending',
                'summary': '',
            })
        if len(normalized) < 2:
            return fallback
        final_title = str(normalized[-1].get('title') or '')
        if not re.search(r'(答复|结论|建议|汇总|输出)', final_title):
            normalized.append({
                'index': len(normalized) + 1,
                'title': '形成结论与建议',
                'goal': '把前面子任务的结果收敛成清晰答复。',
                'done_when': '已经输出最终答复。',
                'preferred_tools': [],
                'status': 'pending',
                'summary': '',
            })
        if len(normalized) > 6:
            normalized = normalized[:5] + [{
                'index': 6,
                'title': '形成结论与建议',
                'goal': '把前面子任务的结果收敛成清晰答复。',
                'done_when': '已经输出最终答复。',
                'preferred_tools': [],
                'status': 'pending',
                'summary': '',
            }]
        for idx, row in enumerate(normalized, start=1):
            row['index'] = idx
        return normalized

    def _build_task_plan(self, task_message, response_style='auto', max_tool_rounds=0, allowed_tools=None, blocked_tools=None):
        fallback = self._fallback_task_plan(response_style=response_style, max_tool_rounds=max_tool_rounds)
        text = str(task_message or '').strip()
        if not text:
            return fallback
        tool_names = [str(t.get('function', {}).get('name') or '').strip() for t in self.tools]
        tool_names = [x for x in tool_names if x]
        if allowed_tools is not None:
            allow = set(allowed_tools or [])
            tool_names = [x for x in tool_names if x in allow]
        blocked = set(blocked_tools or [])
        tool_names = [x for x in tool_names if x not in blocked]
        style = self._normalize_response_style(response_style)
        if style == 'quick' or int(max_tool_rounds or 0) <= 1:
            step_count_hint = '2到3'
        elif style == 'deep':
            step_count_hint = '3到5'
        else:
            step_count_hint = '3到6'
        prompt = format_ai_team_prompt(
            'agent.task_planner',
            agent_name=self.name,
            step_count_hint=step_count_hint,
            tool_names=', '.join(tool_names[:24]) or '无',
            task_message=text[:1200]
        )
        response = self._call_qwen(
            [
                {'role': 'system', 'content': '你是严谨的任务拆解规划器。你只能输出 JSON。'},
                {'role': 'user', 'content': prompt},
            ],
            use_tools=False
        )
        if not response:
            return fallback
        try:
            content = ((response.get('choices') or [{}])[0].get('message') or {}).get('content') or ''
        except Exception:
            content = ''
        data = self._extract_json_object(content)
        steps = data.get('steps') if isinstance(data, dict) else None
        return self._normalize_task_plan_details(
            steps,
            response_style=response_style,
            max_tool_rounds=max_tool_rounds
        )

    def _set_task_step(self, session_id, step_name, step_index, step_total, reason='', next_step='', tool_name=''):
        self.current_step = str(step_name or '')
        self.current_step_index = int(step_index or 0)
        self.current_step_total = int(step_total or 0)
        self.current_step_reason = str(reason or '')
        self.next_step = str(next_step or '')
        self.current_tool = str(tool_name or '')

    def _update_plan_step_status(self, step_index, status=None, summary=None):
        idx = int(step_index or 0) - 1
        if idx < 0 or idx >= len(self.current_plan_details or []):
            return
        row = dict(self.current_plan_details[idx] or {})
        if status:
            row['status'] = str(status)
        if summary is not None:
            row['summary'] = str(summary or '').strip()[:280]
        self.current_plan_details[idx] = row
        self.current_plan_steps = self._plan_titles(self.current_plan_details)

    def _announce_task_plan(self, session_id, task_message, response_style='auto', max_tool_rounds=0, allowed_tools=None, blocked_tools=None):
        self.current_plan_details = self._build_task_plan(
            task_message=task_message,
            response_style=response_style,
            max_tool_rounds=max_tool_rounds,
            allowed_tools=allowed_tools,
            blocked_tools=blocked_tools,
        )
        self.current_plan_steps = self._plan_titles(self.current_plan_details)
        total = len(self.current_plan_steps)
        if not total:
            return
        first = self.current_plan_steps[0]
        second = self.current_plan_steps[1] if total > 1 else ''
        reason = str((self.current_plan_details[0] or {}).get('goal') or '先执行第一个子任务。')
        self._update_plan_step_status(1, status='running')
        self._set_task_step(session_id, first, 1, total, reason=reason, next_step=second)
        bus.post_activity(
            self.agent_id, 'status',
            '任务拆解完成：%s' % ' | '.join(
                ['%d.%s' % (idx + 1, title) for idx, title in enumerate(self.current_plan_steps)]
            ),
            metadata=self._activity_meta(
                session_id,
                {
                    'phase': 'task_plan',
                    'task_plan': list(self.current_plan_steps),
                    'task_plan_details': self._serialize_plan_details(),
                    'current_step': self.current_step,
                    'next_step': self.next_step,
                    'step_index': self.current_step_index,
                    'step_total': self.current_step_total,
                    'task_preview': str(task_message or '')[:160],
                }
            )
        )

    def _announce_tool_intent(self, session_id, tool_name, arguments, round_idx, max_tool_rounds):
        total = len(self.current_plan_steps or [])
        step_index = max(1, int(self.current_step_index or 1))
        if total <= 0:
            total = 1
        if step_index > total:
            step_index = total
        current_step = self.current_plan_steps[step_index - 1] if self.current_plan_steps else '执行子任务'
        next_step = self.current_plan_steps[step_index] if step_index < total else ''
        goal = ''
        if 0 <= step_index - 1 < len(self.current_plan_details or []):
            goal = str((self.current_plan_details[step_index - 1] or {}).get('goal') or '').strip()
        reason = self._tool_purpose(tool_name)
        if goal:
            reason = '%s；本步骤目标：%s' % (reason, goal)
        self._set_task_step(
            session_id,
            current_step,
            step_index,
            total,
            reason=reason,
            next_step=next_step,
            tool_name=tool_name
        )
        bus.post_activity(
            self.agent_id,
            'status',
            '步骤%d/%d：准备调用 %s，用于%s。完成后将判断是否进入%s。' % (
                self.current_step_index,
                self.current_step_total or total,
                tool_name,
                reason,
                next_step or '输出结论'
            ),
            metadata=self._activity_meta(
                session_id,
                {
                    'phase': 'task_step',
                    'tool': tool_name,
                    'args': arguments,
                    'round_index': int(round_idx or 0),
                    'max_tool_rounds': int(max_tool_rounds or 0),
                    'current_step': self.current_step,
                    'next_step': self.next_step,
                    'step_reason': self.current_step_reason,
                    'step_index': self.current_step_index,
                    'step_total': self.current_step_total,
                    'task_plan_details': self._serialize_plan_details(),
                }
            )
        )

    @staticmethod
    def _tool_result_text(result):
        if isinstance(result, dict):
            return str(result.get('text') or '')
        return str(result or '')

    @staticmethod
    def _extract_tool_links(result, limit=12):
        if not isinstance(result, dict):
            return []
        raw_links = result.get('links') or result.get('search_links') or []
        out = []
        seen = set()
        for row in raw_links:
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
    def _merge_web_links(cls, base_links, new_links, limit=12):
        merged = list(base_links or [])
        merged.extend(list(new_links or []))
        return cls._extract_tool_links({'links': merged}, limit=limit)

    @staticmethod
    def _tool_result_status(result):
        if not isinstance(result, dict):
            return {'ok': True, 'status': 'ok'}
        return {
            'ok': bool(result.get('ok', True)),
            'status': str(result.get('status') or ('ok' if result.get('ok', True) else 'error')),
        }

    def _get_budget_snapshot(self, session_id):
        if not TOKEN_BUDGET_ENABLED:
            return {'level': 'normal'}
        return get_token_budget_snapshot(
            session_id=session_id or '',
            daily_limit=TOKEN_BUDGET_DAILY_LIMIT,
            session_limit=TOKEN_BUDGET_SESSION_LIMIT,
            warn_ratio=TOKEN_BUDGET_WARN_RATIO,
            hard_ratio=TOKEN_BUDGET_HARD_RATIO,
        )

    def _notify_budget_level(self, session_id, snapshot):
        if not snapshot or snapshot.get('level') == 'normal':
            return
        now = time.time()
        # 限频：同一智能体每 20 秒最多提示一次预算状态
        if now - self._budget_alert_ts < 20:
            return
        self._budget_alert_ts = now
        day_used = snapshot.get('day_used', 0)
        day_limit = snapshot.get('day_limit', 0)
        sess_used = snapshot.get('session_used', 0)
        sess_limit = snapshot.get('session_limit', 0)
        msg = 'Token预算状态: %s（日:%s/%s，会话:%s/%s）' % (
            snapshot.get('level'),
            day_used,
            day_limit or '∞',
            sess_used,
            sess_limit or '∞',
        )
        bus.post_activity(
            self.agent_id,
            'status',
            msg,
            metadata=self._activity_meta(session_id, {'budget': snapshot})
        )

    def _apply_budget_policy(self, session_id, max_tool_rounds):
        snapshot = self._get_budget_snapshot(session_id)
        self._last_call_budget_level = snapshot.get('level', 'normal')
        self._last_call_budget_exhausted = (snapshot.get('level') == 'exhausted')
        adjusted = max(1, int(max_tool_rounds or 1))
        level = snapshot.get('level')
        if level == 'warning':
            adjusted = min(adjusted, 4)
        elif level == 'critical':
            adjusted = min(adjusted, 2)
        elif level == 'exhausted':
            adjusted = 0
        self._notify_budget_level(session_id, snapshot)
        return adjusted, snapshot

    @staticmethod
    def _allocate_step_tool_rounds(remaining_rounds, remaining_exec_steps, response_style='auto'):
        remain = max(0, int(remaining_rounds or 0))
        steps = max(1, int(remaining_exec_steps or 1))
        if remain <= 0:
            return 0
        style = str(response_style or '').strip().lower()
        if style == 'quick':
            cap = 1
        elif style == 'deep':
            cap = 3
        else:
            cap = 2
        share = max(1, int((remain + steps - 1) / steps))
        return max(1, min(remain, cap, share))

    @staticmethod
    def _format_completed_step_context(step_results):
        if not step_results:
            return '暂无已完成子任务。'
        rows = []
        for idx, row in enumerate(step_results, start=1):
            title = str(row.get('title') or '子任务').strip()
            summary = str(row.get('summary') or '').strip()
            rows.append('%d. %s：%s' % (idx, title, summary or '已完成'))
        return '\n'.join(rows[:8])

    def _build_step_execution_prompt(self, task_message, step_detail, step_index, step_total,
                                     step_results=None, tool_round_limit=0, final_step=False):
        title = str((step_detail or {}).get('title') or '当前子任务').strip()
        goal = str((step_detail or {}).get('goal') or '').strip()
        done_when = str((step_detail or {}).get('done_when') or '').strip()
        preferred_tools = list((step_detail or {}).get('preferred_tools') or [])
        completed_context = self._format_completed_step_context(step_results)
        if final_step:
            return (
                '现在执行最后一步，请基于前面已经完成的子任务结果，输出最终答复。\n'
                '总任务: %s\n'
                '当前步骤: %d/%d %s\n'
                '当前步骤目标: %s\n'
                '完成标准: %s\n\n'
                '前序子任务结果:\n%s\n\n'
                '要求:\n'
                '1. 直接给出完整最终答复，不要再调用工具。\n'
                '2. 先讲逻辑，再讲必要数据；不要机械堆数字。\n'
                '3. 如果证据有边界，要写出风险、分歧与后续观察点。\n'
            ) % (
                task_message or '',
                step_index,
                step_total,
                title,
                goal or '形成最终结论',
                done_when or '给出最终答复',
                completed_context
            )
        return (
            '现在开始按计划执行当前子任务，请严格只围绕这一步行动。\n'
            '总任务: %s\n'
            '当前步骤: %d/%d %s\n'
            '本步骤目标: %s\n'
            '完成标准: %s\n'
            '推荐工具: %s\n'
            '本步骤工具轮次上限: %d\n\n'
            '已完成子任务:\n%s\n\n'
            '严格要求:\n'
            '1. 只解决当前步骤，不要提前输出最终答复。\n'
            '2. 如果需要外部信息，优先调用真正服务于这一步的工具。\n'
            '3. 当你认为这一步已经完成时，直接输出“本步骤结论”，说明已完成什么、还有什么边界。\n'
            '4. 不要跳步，不要泛泛而谈。\n'
        ) % (
            task_message or '',
            step_index,
            step_total,
            title,
            goal or '完成当前子任务',
            done_when or '完成当前步骤',
            ', '.join(preferred_tools) if preferred_tools else '无特定要求',
            max(0, int(tool_round_limit or 0)),
            completed_context
        )

    def _trace_start_think(self, session_id, task_message, max_tool_rounds):
        workflow = self._workflow_from_session(session_id) or 'ad_hoc'
        run_id = trace_get_active_run_id(session_id) if session_id else ''
        owned = False
        if not run_id:
            run_id = trace_start_run(
                session_id=session_id or '',
                workflow=workflow,
                origin='agent_think',
                topic=(task_message or '')[:180],
                meta={'agent_id': self.agent_id}
            )
            owned = True
        span_id = trace_start_span(
            run_id=run_id,
            session_id=session_id or '',
            workflow=workflow,
            span_type='agent_think',
            name='agent.think',
            agent_id=self.agent_id,
            input_preview=(task_message or '')[:500],
            data={'max_tool_rounds': int(max_tool_rounds or 1)}
        )
        self._active_trace_run_id = run_id
        self._active_trace_span_id = span_id
        self._active_trace_owned_run = bool(owned)
        return run_id, span_id

    def _trace_finish_think(self, status='ok', reply='', error_text=''):
        try:
            if self._active_trace_span_id:
                trace_finish_span(
                    self._active_trace_span_id,
                    status=status,
                    output_preview=(reply or '')[:1200],
                    error_text=(error_text or '')[:500]
                )
            if self._active_trace_owned_run and self._active_trace_run_id:
                trace_finish_run(
                    self._active_trace_run_id,
                    status='ok' if status == 'ok' else 'error',
                    meta={'agent_id': self.agent_id}
                )
        except Exception:
            pass

    def think(self, task_message, session_id=None, max_tool_rounds=8,
              allowed_tools=None, blocked_tools=None, response_style='auto',
              return_meta=False):
        """
        执行一轮完整的思考：发送任务到 Qwen，处理工具调用循环，返回最终回复。

        Args:
            task_message: 任务/问题文本
            session_id: 研究周期会话ID
            max_tool_rounds: 最大工具调用轮次（防止无限循环）
            allowed_tools: 允许调用的工具白名单（None=不限制）
            blocked_tools: 禁止调用的工具列表
            response_style: 回答风格（auto/quick/deep/team）
            return_meta: 是否返回结构化结果（reply + web_links）

        Returns:
            str/dict: 默认返回回复文本；return_meta=True 时返回结构化对象
        """
        search_links = []
        session_id = session_id or ''

        def _pack(reply_text, task_plan=None, task_plan_details=None, current_step=None, next_step=None):
            text = str(reply_text or '')
            if return_meta:
                return {
                    'reply': text,
                    'web_links': list(search_links),
                    'task_plan': list(task_plan if task_plan is not None else self.current_plan_steps),
                    'task_plan_details': list(
                        task_plan_details if task_plan_details is not None else self._serialize_plan_details()
                    ),
                    'current_step': current_step if current_step is not None else self.current_step,
                    'next_step': next_step if next_step is not None else self.next_step,
                    'prompt_profile': dict(self.current_prompt_profile or {}),
                }
            return text

        acquired = self._work_lock.acquire(blocking=False)
        if not acquired:
            busy_session = self.current_session_id or '-'
            msg = '智能体当前正忙（session=%s，步骤=%s），请稍后再试。' % (
                busy_session,
                self.current_step or self.current_workflow_label or '处理中'
            )
            bus.post_activity(
                self.agent_id, 'status', msg,
                metadata=self._activity_meta(
                    self.current_session_id,
                    {'phase': 'busy', 'busy_session_id': busy_session}
                )
            )
            return _pack(msg)

        original_request_timeout = self._request_timeout
        try:
            self.status = 'thinking'
            self.current_task = task_message[:100]
            self.last_active = time.time()
            response_style = self._normalize_response_style(response_style)
            if response_style == 'quick':
                # 快速问答优先体验：缩短单次模型等待，避免长时间卡住。
                self._request_timeout = min(int(self._request_timeout or 180), 45)
            self._set_runtime_context(session_id, task_message, response_style=response_style)

            if self._stop_requested:
                return _pack(self._abort_by_stop(session_id))
            if is_session_cancelled(session_id):
                return _pack(self._abort_by_cancelled_session(session_id))

            max_tool_rounds, budget_snapshot = self._apply_budget_policy(session_id, max_tool_rounds)
            timing_snapshot = get_session_timing(session_id)
            if timing_snapshot.get('active'):
                if timing_snapshot.get('is_expired'):
                    max_tool_rounds = min(max_tool_rounds, 1)
                elif timing_snapshot.get('is_converging'):
                    max_tool_rounds = min(max_tool_rounds, 2 if response_style == 'deep' else 1)
            self._trace_start_think(session_id, task_message, max_tool_rounds)
            self._announce_task_plan(
                session_id=session_id,
                task_message=task_message,
                response_style=response_style,
                max_tool_rounds=max_tool_rounds,
                allowed_tools=allowed_tools,
                blocked_tools=blocked_tools,
            )

            if max_tool_rounds <= 0:
                msg = '当前会话 Token 预算已耗尽，本轮暂停执行。请稍后再试。'
                self._trace_finish_think(status='error', error_text='budget_exhausted')
                self._mark_idle()
                return _pack(msg)

            # 广播状态到活动日志
            bus.post_activity(
                self.agent_id, 'thinking', '正在思考: %s' % task_message[:80],
                metadata=self._activity_meta(
                    session_id,
                    {
                        'budget': budget_snapshot,
                        'session_timing': timing_snapshot,
                        'current_step': self.current_step,
                        'next_step': self.next_step,
                        'task_plan': list(self.current_plan_steps),
                        'task_plan_details': self._serialize_plan_details(),
                    }
                )
            )

            # 构建消息列表
            messages = self._build_messages(
                task_message,
                session_id=session_id,
                response_style=response_style
            )

            # 保存用户消息到记忆
            self.memory.save_conversation('user', task_message, session_id=session_id)

            allowed_set = set(allowed_tools or []) if allowed_tools is not None else None
            blocked_set = set(blocked_tools or [])
            used_tools = []
            tool_evidence = []
            save_knowledge_count = 0
            plan_details = self._serialize_plan_details() or self._fallback_task_plan(
                response_style=response_style,
                max_tool_rounds=max_tool_rounds
            )
            if not self.current_plan_details:
                self.current_plan_details = plan_details
                self.current_plan_steps = self._plan_titles(plan_details)
            total_steps = len(self.current_plan_steps or [])
            remaining_tool_rounds = max(0, int(max_tool_rounds or 0))
            tool_round_idx = 0
            step_results = []

            for step_idx, step_detail in enumerate(self.current_plan_details or [], start=1):
                if self._stop_requested:
                    return _pack(self._abort_by_stop(session_id))
                if is_session_cancelled(session_id):
                    return _pack(self._abort_by_cancelled_session(session_id))

                step_title = str(step_detail.get('title') or ('子任务%d' % step_idx)).strip()
                step_goal = str(step_detail.get('goal') or '').strip()
                next_step = self.current_plan_steps[step_idx] if step_idx < total_steps else ''
                is_final_step = (step_idx == total_steps)

                self.status = 'thinking'
                self._update_plan_step_status(step_idx, status='running')
                self._set_task_step(
                    session_id,
                    step_title,
                    step_idx,
                    total_steps,
                    reason=step_goal or ('执行子任务 %d' % step_idx),
                    next_step=next_step
                )
                bus.post_activity(
                    self.agent_id,
                    'status',
                    '开始执行子任务 %d/%d：%s' % (step_idx, total_steps, step_title),
                    metadata=self._activity_meta(
                        session_id,
                        {
                            'phase': 'task_step',
                            'current_step': self.current_step,
                            'next_step': self.next_step,
                            'step_reason': self.current_step_reason,
                            'step_index': self.current_step_index,
                            'step_total': self.current_step_total,
                            'task_plan': list(self.current_plan_steps),
                            'task_plan_details': self._serialize_plan_details(),
                        }
                    )
                )

                if is_final_step:
                    messages.append({
                        'role': 'user',
                        'content': self._build_step_execution_prompt(
                            task_message,
                            step_detail,
                            step_idx,
                            total_steps,
                            step_results=step_results,
                            tool_round_limit=0,
                            final_step=True
                        )
                    })
                    response = self._call_qwen(messages, use_tools=False)
                    if response:
                        reply = ((response.get('choices') or [{}])[0].get('message') or {}).get('content', '').strip()
                    else:
                        reply = '分析过程中遇到问题，未能生成完整结论。'
                    self._update_plan_step_status(
                        step_idx,
                        status='completed',
                        summary=self._truncate_text(reply, 220)
                    )
                    self._set_task_step(
                        session_id,
                        step_title,
                        step_idx,
                        total_steps,
                        reason='所有子任务已收敛，输出最终答复。',
                        next_step=''
                    )
                    reply, reasoning_trace = self._ensure_explainable_reply(
                        task_message, reply, session_id, tool_evidence, response_style=response_style
                    )
                    final_task_plan = list(self.current_plan_steps)
                    final_task_plan_details = self._serialize_plan_details()
                    final_current_step = self.current_step
                    final_next_step = self.next_step
                    self._trace_finish_think(status='ok', reply=reply)
                    self._finalize_reply(
                        reply, session_id,
                        extra_meta={'reasoning_trace': reasoning_trace}
                    )
                    self._post_reasoning_trace_activity(session_id, reasoning_trace)
                    self._enforce_memory_learning(
                        task_message, reply, session_id,
                        used_tools=used_tools,
                        save_knowledge_count=save_knowledge_count
                    )
                    return _pack(
                        reply,
                        task_plan=final_task_plan,
                        task_plan_details=final_task_plan_details,
                        current_step=final_current_step,
                        next_step=final_next_step,
                    )

                remaining_exec_steps = max(1, total_steps - step_idx)
                step_tool_round_limit = self._allocate_step_tool_rounds(
                    remaining_tool_rounds,
                    remaining_exec_steps,
                    response_style=response_style
                )
                step_summary = ''
                messages.append({
                    'role': 'user',
                    'content': self._build_step_execution_prompt(
                        task_message,
                        step_detail,
                        step_idx,
                        total_steps,
                        step_results=step_results,
                        tool_round_limit=step_tool_round_limit,
                        final_step=False
                    )
                })

                while True:
                    if self._stop_requested:
                        return _pack(self._abort_by_stop(session_id))
                    if is_session_cancelled(session_id):
                        return _pack(self._abort_by_cancelled_session(session_id))

                    use_tools_for_step = step_tool_round_limit > 0 and remaining_tool_rounds > 0
                    response = self._call_qwen(messages, use_tools=use_tools_for_step)
                    if not response:
                        err = '调用 AI 服务失败，请稍后重试。'
                        if self._last_call_budget_exhausted:
                            err = 'Token 预算达到上限，本轮停止。请稍后再试。'
                        elif self._stop_requested:
                            err = self._abort_by_stop(session_id)
                            return _pack(err)
                        self._trace_finish_think(status='error', error_text=err)
                        self._mark_idle()
                        return _pack(err)

                    message = (response.get('choices') or [{}])[0].get('message') or {}
                    tool_calls = message.get('tool_calls') if use_tools_for_step else None
                    if not tool_calls:
                        step_summary = str(message.get('content') or '').strip()
                        if step_summary:
                            messages.append({'role': 'assistant', 'content': step_summary})
                        break

                    tool_round_idx += 1
                    step_tool_round_limit = max(0, step_tool_round_limit - 1)
                    remaining_tool_rounds = max(0, remaining_tool_rounds - 1)
                    messages.append(message)

                    for tc in tool_calls:
                        if self._stop_requested:
                            return _pack(self._abort_by_stop(session_id))
                        if is_session_cancelled(session_id):
                            return _pack(self._abort_by_cancelled_session(session_id))
                        func = tc.get('function', {})
                        tool_name = func.get('name', '')
                        try:
                            arguments = json.loads(func.get('arguments', '{}'))
                        except (json.JSONDecodeError, TypeError):
                            arguments = {}

                        tool_span_id = ''
                        if self._active_trace_run_id:
                            tool_span_id = trace_start_span(
                                run_id=self._active_trace_run_id,
                                parent_span_id=self._active_trace_span_id,
                                session_id=session_id or '',
                                workflow=self.current_workflow or '',
                                span_type='tool_call',
                                name=tool_name or 'unknown_tool',
                                agent_id=self.agent_id,
                                input_preview=json.dumps(arguments, ensure_ascii=False)[:500],
                                data={'round': tool_round_idx}
                            )

                        self.status = 'using_tool'
                        self._announce_tool_intent(
                            session_id=session_id,
                            tool_name=tool_name,
                            arguments=arguments,
                            round_idx=tool_round_idx,
                            max_tool_rounds=max_tool_rounds
                        )
                        bus.post_activity(
                            self.agent_id, 'tool_call',
                            '调用工具: %s(%s)' % (tool_name, json.dumps(arguments, ensure_ascii=False)[:100]),
                            metadata=self._activity_meta(
                                session_id,
                                {
                                    'tool': tool_name,
                                    'args': arguments,
                                    'current_step': self.current_step,
                                    'next_step': self.next_step,
                                    'step_reason': self.current_step_reason,
                                    'step_index': self.current_step_index,
                                    'step_total': self.current_step_total,
                                    'task_plan_details': self._serialize_plan_details(),
                                }
                            )
                        )

                        denied = False
                        if allowed_set is not None and tool_name not in allowed_set:
                            denied = True
                        if tool_name in blocked_set:
                            denied = True

                        if denied:
                            result = {
                                'schema_version': 'tool_result.v2',
                                'tool': tool_name,
                                'args': arguments,
                                'ok': False,
                                'status': 'blocked',
                                'text': '工具受限：%s 在当前模式不可用，请改用只读学习工具。' % tool_name,
                                'error': '',
                            }
                            bus.post_activity(
                                self.agent_id, 'tool_blocked',
                                '工具被拦截: %s' % tool_name,
                                metadata=self._activity_meta(session_id, {'tool': tool_name, 'args': arguments})
                            )
                        else:
                            result = execute_tool(
                                tool_name, arguments,
                                agent_id=self.agent_id,
                                message_bus=bus,
                                return_protocol=TOOL_PROTOCOL_DEFAULT
                            )
                            if tool_name == 'save_knowledge':
                                save_knowledge_count += 1

                        used_tools.append(tool_name + ('#blocked' if denied else ''))
                        if tool_name == 'web_search':
                            links = self._extract_tool_links(result, limit=12)
                            if links:
                                search_links = self._merge_web_links(search_links, links, limit=12)
                        tool_state = self._tool_result_status(result)
                        tool_text = self._tool_result_text(result)
                        tool_for_model = {
                            'tool': tool_name,
                            'ok': tool_state.get('ok', True),
                            'status': tool_state.get('status', 'ok'),
                            'text': tool_text[:3600],
                        }
                        messages.append({
                            'role': 'tool',
                            'tool_call_id': tc.get('id', ''),
                            'content': json.dumps(tool_for_model, ensure_ascii=False)[:4000],
                        })
                        tool_evidence.append(
                            self._build_tool_evidence(
                                round_idx=tool_round_idx,
                                tool_name=tool_name,
                                arguments=arguments,
                                result=tool_text,
                                blocked=denied,
                            )
                        )
                        self.memory.save_conversation(
                            'tool', tool_text[:1000],
                            tool_name=tool_name, session_id=session_id
                        )
                        if tool_span_id:
                            trace_finish_span(
                                tool_span_id,
                                status='ok' if tool_state.get('ok', True) else str(tool_state.get('status') or 'error'),
                                output_preview=tool_text[:1200],
                                data={
                                    'tool': tool_name,
                                    'tool_status': tool_state.get('status'),
                                    'blocked': bool(denied)
                                }
                            )

                    self.status = 'thinking'
                    if step_tool_round_limit <= 0 or remaining_tool_rounds <= 0:
                        messages.append({
                            'role': 'user',
                            'content': (
                                '当前子任务的工具轮次已到上限。请基于已有证据，只输出本步骤结论，'
                                '说明：1) 已确认了什么；2) 还有什么边界；不要再调用工具。'
                            )
                        })

                step_summary = step_summary or '本步骤已完成，但未生成详细摘要。'
                step_results.append({
                    'index': step_idx,
                    'title': step_title,
                    'summary': self._truncate_text(step_summary, 220),
                })
                self._update_plan_step_status(
                    step_idx,
                    status='completed',
                    summary=self._truncate_text(step_summary, 220)
                )
                if step_idx < total_steps:
                    self._set_task_step(
                        session_id,
                        step_title,
                        step_idx,
                        total_steps,
                        reason='当前子任务已完成，准备进入下一步。',
                        next_step=next_step
                    )
                bus.post_activity(
                    self.agent_id,
                    'status',
                    '子任务 %d/%d 已完成：%s' % (step_idx, total_steps, step_title),
                    metadata=self._activity_meta(
                        session_id,
                        {
                            'phase': 'task_step',
                            'current_step': self.current_step,
                            'next_step': self.next_step,
                            'step_reason': self.current_step_reason,
                            'step_index': self.current_step_index,
                            'step_total': self.current_step_total,
                            'task_plan': list(self.current_plan_steps),
                            'task_plan_details': self._serialize_plan_details(),
                            'step_summary': self._truncate_text(step_summary, 240),
                        }
                    )
                )

            reply = '分析过程中未能形成最终结论。'
            self._trace_finish_think(status='error', error_text='missing_final_step')
            self._mark_idle()
            return _pack(reply)
        except Exception as e:
            err = '分析过程中出现异常: %s' % str(e)
            bus.post_activity(
                self.agent_id, 'error', err,
                metadata=self._activity_meta(session_id)
            )
            self._trace_finish_think(status='error', error_text=str(e))
            self._mark_idle()
            return _pack(err)
        finally:
            self._request_timeout = original_request_timeout
            try:
                self._work_lock.release()
            except Exception:
                pass

    def process_incoming_messages(self, session_id=None):
        """处理消息总线中收到的所有消息，返回汇总结果"""
        messages = bus.receive_all(self.agent_id)
        if not messages:
            return None

        # 将收到的消息整合为一条上下文
        context_parts = []
        for msg in messages:
            from_name = msg.get('from', 'unknown')
            content = msg.get('content', '')
            msg_type = msg.get('type', '')
            context_parts.append('[来自%s的%s] %s' % (from_name, msg_type, content))

        combined = '\n\n'.join(context_parts)
        prompt = '你收到了以下来自团队成员的消息，请阅读并根据你的职责做出回应:\n\n%s' % combined

        return self.think(prompt, session_id=session_id)

    def _build_memory_snapshot(
            self,
            session_id,
            task_message,
            layers,
            recalled_rows,
            local_context_rows,
            injection_order=None,
            injection_strategy=None
    ):
        def _pack_knowledge_item(item):
            return {
                'tier': str(item.get('tier') or '').lower(),
                'category': str(item.get('category') or ''),
                'subject': str(item.get('subject') or ''),
                'confidence': round(float(item.get('confidence') or 0), 3),
                'updated_at': item.get('updated_at') or 0,
                'source_type': str(item.get('source_type') or ''),
                'preview': self._truncate_text(item.get('content') or '', 140),
            }

        def _pack_conv_item(item):
            return {
                'role': str(item.get('role') or ''),
                'session_id': str(item.get('session_id') or ''),
                'created_at': item.get('created_at') or 0,
                'preview': self._truncate_text(item.get('content') or '', 140),
            }

        hot = [_pack_knowledge_item(x) for x in list((layers or {}).get('hot') or [])[:6]]
        warm = [_pack_knowledge_item(x) for x in list((layers or {}).get('warm') or [])[:6]]
        cold = [_pack_knowledge_item(x) for x in list((layers or {}).get('cold') or [])[:4]]
        recalled = [_pack_conv_item(x) for x in list(recalled_rows or [])[:8]]
        local_context = [_pack_conv_item(x) for x in list(local_context_rows or [])[-8:]]

        default_injection_order = [
            '系统提示词',
            '团队核心宪章',
            '角色职责记忆',
            '时间与交易时段约束',
            '记忆协议与风格约束',
            'HOT/WARM/COLD 长期记忆摘要',
            '相关历史对话召回',
            '进程内短对话上下文',
            '当前用户任务',
        ]
        default_injection_strategy = {
            'knowledge_injected_in_system': True,
            'recalled_conversations_injected_as_history': True,
            'local_context_injected_as_history': True,
        }
        order = [str(x) for x in list(injection_order or default_injection_order)]
        strategy = dict(default_injection_strategy)
        if isinstance(injection_strategy, dict):
            strategy.update(injection_strategy)

        self.current_memory_snapshot = {
            'session_id': str(session_id or ''),
            'workflow': self._workflow_from_session(session_id) or self.current_workflow or 'ad_hoc',
            'query_preview': self._truncate_text(task_message or '', 160),
            'injection_order': order,
            'injection_strategy': strategy,
            'knowledge_layers': {
                'hot': hot,
                'warm': warm,
                'cold': cold,
            },
            'recalled_conversations': recalled,
            'local_short_context': local_context,
            'counts': {
                'hot': len(hot),
                'warm': len(warm),
                'cold': len(cold),
                'recalled_conversations': len(recalled),
                'local_short_context': len(local_context),
            }
        }

    def _build_messages(self, task_message, session_id=None, response_style='auto'):
        """构建发送给 Qwen 的消息列表"""
        messages = []
        style = self._normalize_response_style(response_style)
        workflow = self._workflow_from_session(session_id) or self.current_workflow or 'ad_hoc'
        high_risk_task = self._is_high_risk_task(task_message)
        now = datetime.datetime.now(ZoneInfo('Asia/Shanghai')) if ZoneInfo else datetime.datetime.now()
        minute = now.hour * 60 + now.minute
        is_weekday = now.weekday() < 5
        if not is_weekday:
            market_phase = '周末休市'
        elif minute < 9 * 60 + 15:
            market_phase = '盘前'
        elif minute < 9 * 60 + 30:
            market_phase = '集合竞价'
        elif minute < 11 * 60 + 30:
            market_phase = '上午连续竞价'
        elif minute < 13 * 60:
            market_phase = '午间休市'
        elif minute < 15 * 60:
            market_phase = '下午连续竞价'
        else:
            market_phase = '收盘后'
        timing = get_session_timing(session_id)
        enforce_rigorous = (
            style in ('deep', 'team') or
            workflow in ('auto_research', 'portfolio_investment', 'market_watch') or
            high_risk_task
        )
        self.current_prompt_profile = self._build_prompt_profile(
            session_id=session_id,
            response_style=style,
            timing=timing,
            enforce_rigorous=enforce_rigorous,
            high_risk_task=high_risk_task
        )

        # 快速模式：仅注入 1+4+8+9（系统提示词 + 时间约束 + 进程短上下文 + 当前任务）
        if style == 'quick':
            system_content = self.system_prompt
            system_content += (
                '\n\n当前系统时间（北京时间）: %s'
                '\n当前交易日期: %s'
                '\n当前市场时段: %s'
                '\n在涉及交易时，必须以当前时间和A股T+1规则约束决策，避免同日来回交易。'
                '\n\n[回答风格]'
                '\n- 当前为快速模式：优先直接回答用户问题，结论清晰、语言简洁。'
                '\n- 如涉及行情，可先联网检索后给出2-4条关键依据。'
                '\n- 不要机械套用固定模板。'
            ) % (
                now.strftime('%Y-%m-%d %H:%M:%S'),
                now.strftime('%Y%m%d'),
                market_phase,
            )
            messages.append({'role': 'system', 'content': system_content})

            context_entries = []
            local_context_entries = []
            seen = set()
            for entry in self.conversation[-MAX_CONVERSATION_CONTEXT:]:
                role = entry.get('role')
                content = (entry.get('content') or '').strip()
                if role not in ('user', 'assistant') or not content:
                    continue
                key = (role, content)
                if key in seen:
                    continue
                seen.add(key)
                packed = {'role': role, 'content': content}
                context_entries.append(packed)
                local_context_entries.append(packed)

            if len(context_entries) > MAX_CONVERSATION_CONTEXT:
                context_entries = context_entries[-MAX_CONVERSATION_CONTEXT:]
            self._build_memory_snapshot(
                session_id=session_id,
                task_message=task_message,
                layers={},
                recalled_rows=[],
                local_context_rows=local_context_entries,
                injection_order=[
                    '系统提示词',
                    '时间与交易时段约束',
                    '进程内短对话上下文',
                    '当前用户任务',
                ],
                injection_strategy={
                    'knowledge_injected_in_system': False,
                    'recalled_conversations_injected_as_history': False,
                    'local_context_injected_as_history': True,
                },
            )
            messages.extend(context_entries)
            messages.append({'role': 'user', 'content': task_message})
            return messages

        # 系统提示 + 分层记忆摘要（HOT/WARM/COLD）
        knowledge_layers = self.memory.get_layered_knowledge(
            query=task_message,
            max_hot=10,
            max_warm=10,
            max_cold=5,
            include_cold=False,
        )
        knowledge_summary = self.memory.render_layered_knowledge_summary(knowledge_layers, max_items=10)
        system_content = self.system_prompt
        team_core_context = get_team_core_context(include_memory_os=True)
        if team_core_context:
            system_content += '\n\n' + team_core_context
        if self.role_memory_text:
            system_content += '\n\n[你的角色职责记忆]\n' + self.role_memory_text

        system_content += (
            '\n\n当前系统时间（北京时间）: %s'
            '\n当前交易日期: %s'
            '\n当前市场时段: %s'
            '\n在涉及交易时，必须以当前时间和A股T+1规则约束决策，避免同日来回交易。'
            '\n\n[记忆协议]'
            '\n1. HOT 规则记忆为长期有效规则，优先执行且不可忽视。'
            '\n2. WARM 记忆仅在当前任务相关时使用，不相关内容不得强行套用。'
            '\n3. COLD 记忆为历史归档，仅作辅助参考，必须重新验证。'
            '\n4. 每次重要任务应产出反思结论；若某模式连续验证成功3次，会自动升级为HOT规则。'
            '\n\n[推理风格要求]'
            '\n1. 采用“主力博弈视角+批判性思维+辩证思维”，优先识别利益驱动与信息差。'
            '\n2. 禁止线性复述，必须同时给出“支持证据、反证路径、失效条件”。'
            '\n3. 对资本市场问题要刨根究底，明确谁受益、谁承担风险、何时可能反转。'
            '\n4. 允许观点锋利，但必须合规、基于事实，不得编造数据或鼓动违法操纵。'
        ) % (
            now.strftime('%Y-%m-%d %H:%M:%S'),
            now.strftime('%Y%m%d'),
            market_phase,
        )
        if timing.get('active'):
            system_content += (
                '\n\n[任务时限约束]'
                '\n- 当前任务总时限: %s 秒'
                '\n- 当前已耗时: %s 秒'
                '\n- 当前剩余时间: %s 秒'
                '\n- 当前时间状态: %s'
                '\n- 若剩余时间不足，必须优先收敛，不要继续扩张任务范围。'
            ) % (
                int(timing.get('total_seconds') or 0),
                int(timing.get('elapsed_seconds') or 0),
                int(timing.get('remaining_seconds') or 0),
                str(timing.get('state') or 'running'),
            )
        if style == 'deep':
            system_content += (
                '\n\n[深度模式输出要求]'
                '\n- 这是“深度”模式：必须给出完整分析过程，不能只给短结论。'
                '\n- 优先顺序：核心判断 -> 关键因果 -> 反方观点/失效条件 -> 行动与观察。'
                '\n- 数字只用于支撑关键结论，不要为了展示而堆砌推导。'
                '\n- 仅当问题本身涉及估值、收益、财务、仓位、涨跌幅等数值敏感主题时，再展开必要测算。'
                '\n- 不要机械套模板，语言要像研究员向投资总监汇报。'
            )
        elif enforce_rigorous:
            system_content += (
                '\n\n[可信输出增强]'
                '\n1. 先讲逻辑链，再讲关键数据；数字服务于判断，不要反过来。'
                '\n2. 关键判断需说明证据来自哪里（工具结果/数据库/新闻/模型估计）。'
                '\n3. 若证据不足，必须显式标注“待核实”，禁止伪造确定性。'
                '\n4. 输出要有分析过程，但避免机械模板化。'
            )
        else:
            system_content += (
                '\n\n[回答风格]'
                '\n- 直接回答用户问题，避免模板化表达。'
                '\n- 仅在研究/投资/高风险问题时再展开因果链与证据链。'
            )
        if self._is_event_driven_prediction_task(task_message):
            system_content += (
                '\n\n[事件驱动前瞻推演要求]'
                '\n1. 对“停牌/重组/定增/控制权变更”等事项，禁止给出“必然发生/必然不发生”的绝对结论。'
                '\n2. 不得把“公告未披露”直接等价为“不会发生”；需明确“公开口径边界”。'
                '\n3. 必须给出基准/上行/下行情景与触发信号，并标注各情景的概率或置信等级。'
                '\n4. 明确区分：已证实事实、可验证线索、推断判断。'
            )
        if knowledge_summary:
            system_content += '\n\n' + knowledge_summary

        messages.append({'role': 'system', 'content': system_content})

        # 召回相关的持久化对话 + 近期进程内上下文
        context_entries = []
        local_context_entries = []
        seen = set()

        recalled = self.memory.get_relevant_conversations(
            task_message,
            limit=max(6, int(MAX_CONVERSATION_CONTEXT / 2)),
            session_id=session_id
        )
        for entry in recalled:
            role = entry.get('role')
            content = (entry.get('content') or '').strip()
            if role not in ('user', 'assistant') or not content:
                continue
            key = (role, content)
            if key in seen:
                continue
            seen.add(key)
            context_entries.append({'role': role, 'content': content})

        for entry in self.conversation[-MAX_CONVERSATION_CONTEXT:]:
            role = entry.get('role')
            content = (entry.get('content') or '').strip()
            if role not in ('user', 'assistant') or not content:
                continue
            key = (role, content)
            if key in seen:
                continue
            seen.add(key)
            packed = {'role': role, 'content': content}
            context_entries.append(packed)
            local_context_entries.append(packed)

        if len(context_entries) > MAX_CONVERSATION_CONTEXT:
            context_entries = context_entries[-MAX_CONVERSATION_CONTEXT:]
        self._build_memory_snapshot(
            session_id=session_id,
            task_message=task_message,
            layers=knowledge_layers,
            recalled_rows=recalled,
            local_context_rows=local_context_entries,
        )
        messages.extend(context_entries)

        # 当前任务
        messages.append({'role': 'user', 'content': task_message})

        return messages

    @staticmethod
    def _extract_focus_subject(task_message, reply, workflow):
        text = '%s\n%s' % (task_message or '', reply or '')
        codes = re.findall(r'\b\d{6}(?:\.(?:SH|SZ))?\b', text.upper())
        if codes:
            code = codes[0]
            if '.' not in code and len(code) == 6:
                code = code + ('.SH' if code.startswith(('6', '9')) else '.SZ')
            return code
        task = (task_message or '').strip().replace('\n', ' ')
        if task:
            return task[:48]
        return workflow or 'general'

    @staticmethod
    def _infer_outcome(reply):
        text = (reply or '')
        success_keys = ('成功', '命中', '有效', '改善', '通过', '执行完成', '收益提升')
        failure_keys = ('失败', '失效', '偏差', '误判', '回撤扩大', '不通过', '被拒绝')
        has_success = any(k in text for k in success_keys)
        has_failure = any(k in text for k in failure_keys)
        if has_success and not has_failure:
            return 'success'
        if has_failure and not has_success:
            return 'failure'
        return 'observation'

    @staticmethod
    def _is_internal_control_task(task_message):
        text = (task_message or '')
        markers = (
            '请严格返回以下JSON格式',
            '不要再调用工具',
            '你收到了以下来自团队成员的消息',
            '团队会议讨论环节',
            '以下交易信号等待你的风控审核',
            '以下交易信号已通过风控审核',
        )
        return any(m in text for m in markers)

    def _enforce_memory_learning(self, task_message, reply, session_id, used_tools=None, save_knowledge_count=0):
        """
        任务后置反思与记忆沉淀：
        - 记录反思日志
        - 未显式写记忆时自动落一条 WARM 记忆
        - 连续成功模式自动晋升 HOT 规则
        """
        try:
            used_tools = used_tools or []
            workflow = self._workflow_from_session(session_id) or 'ad_hoc'
            subject = self._extract_focus_subject(task_message, reply, workflow)
            outcome = self._infer_outcome(reply)
            reply_short = (reply or '').strip()
            rule_text = reply_short.replace('\n', ' ')[:180] if reply_short else ''
            pattern_key = ('%s:%s' % (workflow, subject)).lower()[:120]

            reflection = (
                '任务类型=%s；主题=%s；工具=%s；结果判定=%s；核心结论=%s'
            ) % (
                workflow,
                subject,
                ','.join(used_tools[:8]) if used_tools else 'none',
                outcome,
                reply_short[:300]
            )
            self.memory.save_reflection(
                session_id=session_id,
                workflow=workflow,
                task=task_message,
                reply=reply,
                reflection=reflection
            )

            major_workflows = ('idle_learning', 'portfolio_investment', 'market_watch', 'auto_research')
            if (
                workflow in major_workflows
                and save_knowledge_count <= 0
                and reply_short
                and not self._is_internal_control_task(task_message)
            ):
                category_map = {
                    'idle_learning': 'strategy',
                    'portfolio_investment': 'market_view',
                    'market_watch': 'market_view',
                    'auto_research': 'stock_analysis',
                }
                category = category_map.get(workflow, 'market_view')
                memory_text = (
                    '[自动反思沉淀]\n'
                    '任务: %s\n'
                    '结论: %s\n'
                    '后续动作: 下次遇到类似场景，先复核该结论再决定交易或研究动作。'
                ) % (
                    (task_message or '').replace('\n', ' ')[:180],
                    reply_short[:320]
                )
                saved = self.memory.save_knowledge(
                    category=category,
                    subject=subject,
                    content=memory_text,
                    confidence=0.75 if outcome == 'observation' else 0.82,
                    tier='warm',
                    project='ai_team',
                    domain='a_share',
                    tags=[workflow, 'auto_reflection'],
                    pattern_key=pattern_key,
                    source_type='auto_reflection',
                    source_session=session_id,
                    outcome=outcome,
                    rule_text=rule_text,
                )
                meta = self._activity_meta(session_id, {'subject': subject, 'workflow': workflow})
                bus.post_activity(
                    self.agent_id,
                    'status',
                    '已自动沉淀反思记忆: [%s/%s]' % (category, subject),
                    metadata=meta
                )
                pattern = (saved or {}).get('pattern') or {}
                if pattern.get('promoted_now'):
                    bus.post_activity(
                        self.agent_id,
                        'status',
                        '模式已升级为HOT规则: %s（成功%d次）' % (
                            pattern.get('hot_subject') or pattern.get('pattern_key'),
                            pattern.get('success_count', 0),
                        ),
                        metadata=self._activity_meta(session_id, {'pattern_key': pattern.get('pattern_key')})
                    )
        except Exception as e:
            bus.post_activity(
                self.agent_id,
                'error',
                '反思沉淀失败: %s' % str(e),
                metadata=self._activity_meta(session_id)
            )

    @staticmethod
    def _truncate_text(text, limit=200):
        t = (text or '').replace('\n', ' ').strip()
        if len(t) <= limit:
            return t
        return t[:limit] + '...'

    @staticmethod
    def _source_reliability(tool_name):
        high = {
            'query_database', 'get_kline', 'get_financials', 'get_chip_distribution',
            'get_portfolio_status', 'get_trade_signals', 'run_indicator',
            'review_trade_signal', 'submit_trade_signal', 'get_current_time',
        }
        medium = {
            'get_stock_news', 'get_sector_report', 'get_intraday_news',
            'get_intraday_index', 'get_intraday_sector_heat', 'get_intraday_hotrank',
            'web_search',
        }
        if tool_name in high:
            return 'high'
        if tool_name in medium:
            return 'medium'
        if tool_name == 'send_message_to_agent':
            return 'internal'
        return 'low'

    def _build_tool_evidence(self, round_idx, tool_name, arguments, result, blocked=False):
        return {
            'round': int(round_idx),
            'tool': tool_name,
            'blocked': bool(blocked),
            'reliability': self._source_reliability(tool_name),
            'args': self._truncate_text(json.dumps(arguments or {}, ensure_ascii=False), 160),
            'result': self._truncate_text(str(result), 240),
            'timestamp': int(time.time()),
        }

    @staticmethod
    def _is_casual_prompt(task_message):
        text = (task_message or '').strip().lower()
        if not text:
            return True
        casual_keys = ('你好', 'hi', 'hello', '在吗', '谢谢', '辛苦了', '早上好', '晚上好')
        return len(text) <= 12 and any(k in text for k in casual_keys)

    @staticmethod
    def _is_high_risk_task(task_message):
        text = str(task_message or '').strip()
        if not text:
            return False
        t = text.lower()
        has_stock_code = bool(re.search(r'(?<!\d)\d{6}(?:\.(?:SH|SZ))?(?!\d)', text.upper()))
        high_risk_keys = (
            '买入', '卖出', '仓位', '止损', '止盈', '调仓', '交易',
            '投资', '策略', '风险', '回撤', '净值', '收益',
            '估值', '财报', '宏观', '行业', '板块', '资金面',
            '研究', '复盘', '盯盘', '行情', '预测', '目标价',
            '融资融券', '杠杆', '停牌', '重组', '定增', '资产注入',
            '控制权', '并购', '借壳', '退市', 'st'
        )
        if has_stock_code:
            return True
        return any(k in text or k in t for k in high_risk_keys)

    @staticmethod
    def _is_event_driven_prediction_task(task_message):
        text = str(task_message or '').strip()
        if not text:
            return False
        q = text.lower()
        event_keys = (
            '停牌', '重组', '并购', '借壳', '定增', '资产注入', '控制权变更',
            '重大事项', '重大资产重组', '股权转让', '混改', '摘帽', '退市', 'st'
        )
        forecast_keys = (
            '是否会', '会不会', '可能', '概率', '预期', '前瞻', '会否', '有没有可能',
            '什么时候', '会在', '会出现'
        )
        has_event = any(k in text for k in event_keys)
        has_forecast = any(k in text for k in forecast_keys) or ('?' in text) or ('？' in text)
        return bool(has_event and has_forecast)

    @staticmethod
    def _contains_significant_numbers(text):
        t = text or ''
        nums = re.findall(r'[-+]?\d+(?:\.\d+)?%?', t)
        return len(nums) >= 2

    @staticmethod
    def _extract_causal_steps(reply):
        lines = [x.strip() for x in (reply or '').split('\n') if x.strip()]
        causal_keys = ('因为', '导致', '所以', '因此', '因而', '传导', '触发', '抑制', '推动', '反转', '->', '=>')
        steps = []
        for line in lines:
            if any(k in line for k in causal_keys):
                steps.append(line[:180])
            if len(steps) >= 6:
                break
        return steps

    @staticmethod
    def _extract_derivation_lines(reply):
        lines = [x.strip() for x in (reply or '').split('\n') if x.strip()]
        deriv = []
        for line in lines:
            has_num = bool(re.search(r'[-+]?\d+(?:\.\d+)?%?', line))
            has_formula = any(s in line for s in ('=', '×', '÷', '/', '+', '-', '公式', '代入'))
            if has_num and has_formula:
                deriv.append(line[:180])
            if len(deriv) >= 6:
                break
        return deriv

    @staticmethod
    def _reply_section_flags(reply):
        t = reply or ''
        return {
            'causal': ('因果链' in t or '因果' in t),
            'derivation': ('数字推导' in t or '推导' in t),
            'source': ('证据与来源' in t or '来源' in t),
            'uncertainty': ('不确定性' in t or '反事实' in t or '风险' in t),
        }

    def _workflow_requires_explainability(self, session_id, task_message, response_style='auto'):
        style = self._normalize_response_style(response_style)
        workflow = self._workflow_from_session(session_id) or 'ad_hoc'
        if style == 'quick':
            return False
        if style in ('deep', 'team'):
            if self._is_casual_prompt(task_message) and not self._is_high_risk_task(task_message):
                return False
            return True
        if workflow == 'office_chat':
            return False
        if self._is_casual_prompt(task_message):
            return False
        if workflow in ('auto_research', 'portfolio_investment', 'market_watch'):
            return True
        if workflow in ('user_ask', 'ad_hoc', 'agent_message'):
            return self._is_high_risk_task(task_message)
        return False

    def _needs_explainability_revision(self, task_message, reply, response_style='auto'):
        if not (reply or '').strip():
            return True
        text = (reply or '').strip()
        style = self._normalize_response_style(response_style)
        if style == 'quick':
            return False
        high_risk = self._is_high_risk_task(task_message)
        numeric_required = self._task_requires_numeric_details(task_message)
        flags = self._reply_section_flags(reply)
        need_numeric = (
            self._contains_significant_numbers(reply) or
            self._contains_significant_numbers(task_message)
        )
        if style == 'deep':
            min_len = 850 if high_risk else 520
        elif style == 'team':
            min_len = 700 if high_risk else 420
        else:
            min_len = 180
        if len(text) < min_len:
            return True
        causal_ok = bool(self._extract_causal_steps(reply)) or flags['causal']
        source_ok = flags['source'] or bool(re.search(r'(来源|证据|数据|公告|财报|新闻|数据库|工具)', text))
        risk_ok = flags['uncertainty'] or bool(re.search(r'(风险|不确定|边界|反证|失效)', text))
        if high_risk or numeric_required:
            required_ok = causal_ok and source_ok and risk_ok
        else:
            required_ok = causal_ok and risk_ok
        if not required_ok:
            return True
        if numeric_required and need_numeric and not (
            flags['derivation'] or self._extract_derivation_lines(reply)
        ):
            return True
        return False

    @staticmethod
    def _format_evidence_for_prompt(tool_evidence):
        if not tool_evidence:
            return '（无外部工具证据，仅基于当前上下文推理）'
        rows = []
        for idx, ev in enumerate(tool_evidence[:10]):
            tag = 'E%d' % (idx + 1)
            rows.append(
                '[%s][%s] %s(args=%s) -> %s' % (
                    tag, ev.get('reliability', 'low'),
                    ev.get('tool', ''), ev.get('args', ''), ev.get('result', '')
                )
            )
        return '\n'.join(rows)

    def _revise_reply_with_explainability(
            self,
            task_message,
            draft_reply,
            tool_evidence,
            response_style='auto'
    ):
        evidence_text = self._format_evidence_for_prompt(tool_evidence)
        style = self._normalize_response_style(response_style)
        numeric_needed = self._task_requires_numeric_details(task_message)
        if style == 'deep':
            structure_hint = (
                '建议优先组织为：核心判断 -> 关键因果 -> 反方观点/失效条件 -> 行动与观察 -> 证据与来源（放在文末）。\n'
                '如确有必要，可补充“关键数据”小节，但不要把全文写成审计表。'
            )
            min_len_hint = '整体保持完整展开，通常 700 字以上即可，不必机械追求超长。'
        elif style == 'team':
            structure_hint = (
                '请优先按以下顺序组织：核心判断 -> 关键因果 -> 分歧与风险 -> 行动建议 -> 证据与来源（文末）。\n'
                '若涉及数值敏感问题，再补充关键数据或测算。'
            )
            min_len_hint = '整体保持完整展开，通常 700 字以上即可。'
        else:
            structure_hint = (
                '结构可灵活，但需覆盖：核心判断、关键因果、风险边界、行动与观察。'
            )
            min_len_hint = '整体尽量不短于450字。'
        numeric_requirement = (
            '3) 若问题涉及估值、收益、仓位、财务测算、价格涨跌等数值敏感主题，补充必要的数字计算/估算路径；'
            '若不是此类问题，不要为了形式强行堆数字\n'
        ) if numeric_needed else (
            '3) 如有少量关键数字可帮助判断，可简要说明；不要为了展示而堆砌数字推导\n'
        )
        prompt = format_ai_team_prompt(
            'agent.explainability_rewriter',
            task_message=task_message or '',
            evidence_text=evidence_text,
            draft_reply=draft_reply or '',
            numeric_requirement=numeric_requirement,
            structure_hint=structure_hint,
            min_len_hint=min_len_hint
        )
        messages = [
            {
                'role': 'system',
                'content': '你是投资研究质量审稿官，专注提升可信、可控、可解释、可回溯性。'
            },
            {'role': 'user', 'content': prompt}
        ]
        response = self._call_qwen(messages, use_tools=False)
        if not response:
            return draft_reply
        revised = response['choices'][0]['message'].get('content', '').strip()
        return revised or draft_reply

    def _build_reasoning_trace(self, task_message, reply, tool_evidence, enforced, revised):
        flags = self._reply_section_flags(reply)
        causal_steps = self._extract_causal_steps(reply)
        derivation_lines = self._extract_derivation_lines(reply)
        evidence = list(tool_evidence[:10])
        conf = {'high': 0, 'medium': 0, 'internal': 0, 'low': 0}
        for ev in evidence:
            lv = ev.get('reliability', 'low')
            conf[lv] = conf.get(lv, 0) + 1
        return {
            'version': 'v1',
            'enforced': bool(enforced),
            'revised': bool(revised),
            'workflow': self.current_workflow,
            'task': self._truncate_text(task_message, 160),
            'numeric_claim_count': len(re.findall(r'[-+]?\d+(?:\.\d+)?%?', reply or '')),
            'section_flags': flags,
            'causal_steps': causal_steps,
            'derivation_lines': derivation_lines,
            'evidence_reliability': conf,
            'evidence': evidence,
            'generated_at': int(time.time()),
        }

    def _ensure_explainable_reply(self, task_message, reply, session_id, tool_evidence, response_style='auto'):
        enforce = self._workflow_requires_explainability(
            session_id, task_message, response_style=response_style
        )
        revised = False
        final_reply = reply or ''
        if enforce:
            # 最多两轮修订，避免“短结论”在深度模式下直接通过
            for _ in range(2):
                if not self._needs_explainability_revision(
                    task_message, final_reply, response_style=response_style
                ):
                    break
                updated = self._revise_reply_with_explainability(
                    task_message,
                    final_reply,
                    tool_evidence,
                    response_style=response_style
                )
                if (updated or '').strip():
                    revised = True
                    final_reply = updated
                else:
                    break
        trace = self._build_reasoning_trace(
            task_message=task_message,
            reply=final_reply,
            tool_evidence=tool_evidence,
            enforced=enforce,
            revised=revised
        )
        return final_reply, trace

    def _post_reasoning_trace_activity(self, session_id, trace):
        if not trace:
            return
        preview = '推理链路已记录: 证据%d条, 因果步骤%d步, 推导条目%d条' % (
            len(trace.get('evidence') or []),
            len(trace.get('causal_steps') or []),
            len(trace.get('derivation_lines') or []),
        )
        bus.post_activity(
            self.agent_id,
            'reasoning',
            preview,
            metadata=self._activity_meta(
                session_id,
                {
                    'title': '推理链路',
                    'mode': self.current_workflow or '',
                    'reasoning_trace': trace
                }
            )
        )

    @staticmethod
    def _short_error(err, limit=220):
        txt = (str(err) or '').replace('\n', ' ').strip()
        if len(txt) <= limit:
            return txt
        return txt[:limit] + '...'

    @staticmethod
    def _classify_conn_error(err_text):
        t = (err_text or '').lower()
        if (
            'nodename nor servname provided' in t or
            'name or service not known' in t or
            'temporary failure in name resolution' in t
        ):
            return 'DNS解析失败'
        if 'connection refused' in t:
            return '目标服务拒绝连接'
        if 'network is unreachable' in t:
            return '网络不可达'
        if 'timed out' in t or 'timeout' in t:
            return '网络超时'
        return '网络连接失败'

    @staticmethod
    def _estimate_text_tokens(text):
        s = str(text or '')
        if not s:
            return 0
        # 粗估：中英文混合按平均 3.5 字符 ≈ 1 token
        return max(1, int(len(s) / 3.5))

    def _estimate_message_tokens(self, messages):
        total = 0
        for m in messages or []:
            content = m.get('content', '') if isinstance(m, dict) else str(m)
            if isinstance(content, list):
                txt = []
                for item in content:
                    if isinstance(item, dict):
                        txt.append(str(item.get('text') or ''))
                    else:
                        txt.append(str(item))
                content = '\n'.join(txt)
            total += self._estimate_text_tokens(content) + 4
        return max(1, total)

    @staticmethod
    def _extract_usage(data):
        usage = (data or {}).get('usage') or {}
        p = usage.get('prompt_tokens')
        c = usage.get('completion_tokens')
        t = usage.get('total_tokens')
        try:
            p = int(p) if p is not None else None
        except Exception:
            p = None
        try:
            c = int(c) if c is not None else None
        except Exception:
            c = None
        try:
            t = int(t) if t is not None else None
        except Exception:
            t = None
        return p, c, t

    def _call_qwen(self, messages, use_tools=True):
        """
        调用 Qwen API（复用 ai_chat_service.py 的模式）
        """
        url = QWEN_BASE_URL.rstrip('/') + '/chat/completions'
        headers = {
            'Authorization': 'Bearer ' + self.api_key,
            'Content-Type': 'application/json',
        }
        payload = {
            'model': self.model_name,
            'messages': messages,
            'temperature': 0.7,
        }
        if self._stop_requested or is_session_cancelled(self.current_session_id):
            bus.post_activity(
                self.agent_id, 'status',
                '停止指令生效，取消模型调用',
                metadata=self._activity_meta(
                    self.current_session_id,
                    {'reason': get_session_cancel_reason(self.current_session_id, self._stop_reason or 'user_stop')}
                )
            )
            return None
        if use_tools and self.tools:
            payload['tools'] = self.tools

        retries = max(0, int(QWEN_REQUEST_RETRIES))
        backoff = max(0.2, float(QWEN_RETRY_BACKOFF))

        budget_snapshot = self._get_budget_snapshot(self.current_session_id)
        self._last_call_budget_level = budget_snapshot.get('level', 'normal')
        self._last_call_budget_exhausted = (self._last_call_budget_level == 'exhausted')
        if self._last_call_budget_exhausted:
            bus.post_activity(
                self.agent_id, 'error',
                'Token预算已耗尽，暂停模型调用',
                metadata=self._activity_meta(self.current_session_id, {'budget': budget_snapshot})
            )
            return None

        if self._last_call_budget_level == 'critical':
            # 预算紧张时降温并压缩输出发散
            payload['temperature'] = 0.3

        with self._qwen_semaphore:
            for attempt in range(retries + 1):
                span_id = ''
                call_started = time.time()
                if self._active_trace_run_id:
                    span_id = trace_start_span(
                        run_id=self._active_trace_run_id,
                        parent_span_id=self._active_trace_span_id,
                        session_id=self.current_session_id or '',
                        workflow=self.current_workflow or '',
                        span_type='llm_call',
                        name='qwen.chat.completions',
                        agent_id=self.agent_id,
                        input_preview='model=%s, tools=%s, messages=%d' % (
                            payload.get('model', ''),
                            'on' if use_tools and self.tools else 'off',
                            len(messages or [])
                        ),
                        data={'attempt': attempt + 1}
                    )
                try:
                    resp = self.http.post(
                        url, json=payload, headers=headers, timeout=self._request_timeout
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    prompt_tokens, completion_tokens, total_tokens = self._extract_usage(data)
                    estimated = False
                    if total_tokens is None:
                        estimated = True
                        prompt_tokens = self._estimate_message_tokens(messages)
                        answer_text = ''
                        try:
                            answer_text = ((data.get('choices') or [{}])[0].get('message') or {}).get('content') or ''
                        except Exception:
                            answer_text = ''
                        completion_tokens = self._estimate_text_tokens(answer_text)
                        total_tokens = max(1, prompt_tokens + completion_tokens)

                    record_token_usage(
                        agent_id=self.agent_id,
                        session_id=self.current_session_id or '',
                        workflow=self.current_workflow or '',
                        model=payload.get('model', self.model_name),
                        prompt_tokens=prompt_tokens or 0,
                        completion_tokens=completion_tokens or 0,
                        total_tokens=total_tokens or 0,
                        estimated=estimated,
                        request_id=resp.headers.get('x-request-id', ''),
                        meta={'http_status': int(resp.status_code)}
                    )

                    if span_id:
                        trace_finish_span(
                            span_id,
                            status='ok',
                            output_preview='tokens=%s/%s/%s' % (
                                prompt_tokens or 0,
                                completion_tokens or 0,
                                total_tokens or 0
                            ),
                            data={
                                'latency_ms': int((time.time() - call_started) * 1000),
                                'model': payload.get('model', self.model_name),
                                'prompt_tokens': prompt_tokens or 0,
                                'completion_tokens': completion_tokens or 0,
                                'total_tokens': total_tokens or 0,
                                'estimated': bool(estimated),
                            }
                        )
                    return data
                except requests.exceptions.HTTPError as e:
                    status_code = 0
                    body_text = ''
                    if e.response is not None:
                        status_code = int(getattr(e.response, 'status_code', 0) or 0)
                        body_text = (getattr(e.response, 'text', '') or '').replace('\n', ' ')[:260]

                    # 一些账号对 qwen3.5-plus 不可用，降级到 qwen-plus 兜底。
                    if (
                        status_code in (400, 404)
                        and self.fallback_model
                        and payload.get('model') != self.fallback_model
                    ):
                        try:
                            payload_fb = dict(payload)
                            payload_fb['model'] = self.fallback_model
                            resp_fb = self.http.post(
                                url, json=payload_fb, headers=headers, timeout=self._request_timeout
                            )
                            resp_fb.raise_for_status()
                            data_fb = resp_fb.json()
                            p_fb, c_fb, t_fb = self._extract_usage(data_fb)
                            estimated_fb = False
                            if t_fb is None:
                                estimated_fb = True
                                p_fb = self._estimate_message_tokens(messages)
                                answer_text_fb = ''
                                try:
                                    answer_text_fb = ((data_fb.get('choices') or [{}])[0].get('message') or {}).get('content') or ''
                                except Exception:
                                    answer_text_fb = ''
                                c_fb = self._estimate_text_tokens(answer_text_fb)
                                t_fb = max(1, p_fb + c_fb)
                            record_token_usage(
                                agent_id=self.agent_id,
                                session_id=self.current_session_id or '',
                                workflow=self.current_workflow or '',
                                model=payload_fb.get('model', self.fallback_model),
                                prompt_tokens=p_fb or 0,
                                completion_tokens=c_fb or 0,
                                total_tokens=t_fb or 0,
                                estimated=estimated_fb,
                                request_id=resp_fb.headers.get('x-request-id', ''),
                                meta={'http_status': int(resp_fb.status_code), 'fallback': True}
                            )
                            bus.post_activity(
                                self.agent_id, 'status',
                                '主模型不可用，已降级到 %s' % self.fallback_model
                            )
                            if span_id:
                                trace_finish_span(
                                    span_id,
                                    status='ok',
                                    output_preview='fallback=%s,tokens=%s/%s/%s' % (
                                        self.fallback_model,
                                        p_fb or 0,
                                        c_fb or 0,
                                        t_fb or 0
                                    ),
                                    data={
                                        'latency_ms': int((time.time() - call_started) * 1000),
                                        'model': payload_fb.get('model', self.fallback_model),
                                        'fallback': True,
                                        'prompt_tokens': p_fb or 0,
                                        'completion_tokens': c_fb or 0,
                                        'total_tokens': t_fb or 0,
                                        'estimated': bool(estimated_fb),
                                    }
                                )
                            return data_fb
                        except Exception as fb_err:
                            if span_id:
                                trace_finish_span(
                                    span_id,
                                    status='error',
                                    error_text='fallback_failed:%s' % self._short_error(fb_err),
                                    data={'latency_ms': int((time.time() - call_started) * 1000)}
                                )
                            bus.post_activity(
                                self.agent_id, 'error',
                                'API 调用失败（模型降级后仍失败）: %s' % self._short_error(fb_err)
                            )
                            return None

                    if status_code in (401, 403):
                        bus.post_activity(
                            self.agent_id, 'error',
                            'API 鉴权失败(HTTP %d, key=%s****)' % (
                                status_code,
                                (self.api_key or '')[:8]
                            )
                        )
                    else:
                        bus.post_activity(
                            self.agent_id, 'error',
                            'API 调用失败(HTTP %d): %s' % (
                                status_code or -1,
                                body_text or self._short_error(e)
                            )
                        )
                    if span_id:
                        trace_finish_span(
                            span_id,
                            status='error',
                            error_text='http_%s:%s' % (status_code or -1, body_text or self._short_error(e)),
                            data={'latency_ms': int((time.time() - call_started) * 1000)}
                        )
                    return None
                except requests.exceptions.Timeout:
                    if attempt < retries:
                        if span_id:
                            trace_finish_span(
                                span_id,
                                status='timeout',
                                error_text='timeout_retry',
                                data={'latency_ms': int((time.time() - call_started) * 1000)}
                            )
                        time.sleep(backoff * (2 ** attempt))
                        continue
                    bus.post_activity(
                        self.agent_id, 'error',
                        'API 请求超时（已重试%d次）' % retries
                    )
                    if span_id:
                        trace_finish_span(
                            span_id,
                            status='timeout',
                            error_text='timeout_exhausted',
                            data={'latency_ms': int((time.time() - call_started) * 1000)}
                        )
                    return None
                except requests.exceptions.ConnectionError as e:
                    if attempt < retries:
                        if span_id:
                            trace_finish_span(
                                span_id,
                                status='conn_retry',
                                error_text=self._short_error(e),
                                data={'latency_ms': int((time.time() - call_started) * 1000)}
                            )
                        time.sleep(backoff * (2 ** attempt))
                        continue
                    err_txt = self._short_error(e, limit=260)
                    host = ''
                    try:
                        host = urlparse(url).hostname or ''
                    except Exception:
                        host = ''
                    bus.post_activity(
                        self.agent_id, 'error',
                        'API 调用失败(%s, host=%s): %s' % (
                            self._classify_conn_error(err_txt),
                            host or '-',
                            err_txt
                        )
                    )
                    if span_id:
                        trace_finish_span(
                            span_id,
                            status='conn_error',
                            error_text=err_txt,
                            data={'latency_ms': int((time.time() - call_started) * 1000)}
                        )
                    return None
                except Exception as e:
                    if attempt < retries:
                        if span_id:
                            trace_finish_span(
                                span_id,
                                status='error_retry',
                                error_text=self._short_error(e),
                                data={'latency_ms': int((time.time() - call_started) * 1000)}
                            )
                        time.sleep(backoff * (2 ** attempt))
                        continue
                    bus.post_activity(
                        self.agent_id, 'error',
                        'API 调用失败: %s' % self._short_error(e)
                    )
                    if span_id:
                        trace_finish_span(
                            span_id,
                            status='error',
                            error_text=self._short_error(e),
                            data={'latency_ms': int((time.time() - call_started) * 1000)}
                        )
                    return None

    def _finalize_reply(self, reply, session_id=None, extra_meta=None):
        """完成回复后的收尾工作"""
        self.status = 'speaking'
        self.last_active = time.time()

        # 保存到对话上下文
        self.conversation.append({'role': 'user', 'content': self.current_task or ''})
        self.conversation.append({'role': 'assistant', 'content': reply})

        # 裁剪对话上下文
        if len(self.conversation) > MAX_CONVERSATION_CONTEXT * 2:
            self.conversation = self.conversation[-MAX_CONVERSATION_CONTEXT:]

        # 保存到持久化记忆
        self.memory.save_conversation('assistant', reply, session_id=session_id)

        # 广播到活动日志
        preview = reply[:200] + ('...' if len(reply) > 200 else '')
        meta = {'full_content': reply}
        if extra_meta:
            meta.update(extra_meta)
        bus.post_activity(
            self.agent_id, 'speaking',
            preview,
            metadata=self._activity_meta(session_id, meta)
        )

        self._mark_idle()

    def get_status(self):
        """获取智能体状态（供前端显示，不含敏感信息）"""
        current = self._runtime_state_snapshot()
        has_current_runtime = bool(
            current.get('current_session_id') or
            current.get('current_task') or
            current.get('current_step') or
            current.get('current_plan_details') or
            current.get('prompt_profile') or
            current.get('memory_snapshot')
        )
        display = current if (self.status != 'idle' or has_current_runtime) else dict(self.last_runtime_state or {})
        return {
            'agent_id': self.agent_id,
            'name': self.name,
            'status': self.status,
            'current_task': display.get('current_task'),
            'last_active': self.last_active,
            'current_session_id': display.get('current_session_id', ''),
            'current_workflow': display.get('current_workflow', self.current_workflow),
            'current_workflow_label': display.get('current_workflow_label', self.current_workflow_label),
            'current_response_style': display.get('current_response_style', self.current_response_style),
            'session_timing': get_session_timing(display.get('current_session_id', '')),
            'current_plan_steps': list(display.get('current_plan_steps') or []),
            'current_plan_details': list(display.get('current_plan_details') or []),
            'current_step': display.get('current_step', ''),
            'next_step': display.get('next_step', ''),
            'current_step_reason': display.get('current_step_reason', ''),
            'current_step_index': display.get('current_step_index', 0),
            'current_step_total': display.get('current_step_total', 0),
            'current_tool': display.get('current_tool', ''),
            'prompt_profile': dict(display.get('prompt_profile') or {}),
            'memory_snapshot': dict(display.get('memory_snapshot') or {}),
            'last_runtime_at': float(display.get('updated_at') or 0),
            'model': self.model_name,
            'fallback_model': self.fallback_model,
        }

    def set_model(self, model_name, fallback_model=''):
        self.model_name = normalize_model_name(model_name or self.model_name)
        if str(fallback_model or '').strip():
            self.fallback_model = str(fallback_model).strip()

    def get_model(self):
        return {
            'model': self.model_name,
            'fallback_model': self.fallback_model,
        }
