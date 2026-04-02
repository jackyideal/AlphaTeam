"""
AlphaFin 智能分析团队 - Flask Blueprint 路由
"""
import json
import time
import re
import socket
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
import inspect
import os
from urllib.parse import quote
import requests

from flask import Blueprint, render_template, jsonify, request, Response

from AlphaFin.indicators.indicator_registry import get_grouped

bp = Blueprint('ai_team', __name__,
               template_folder='templates',
               static_folder='static',
               static_url_path='/team_static')

_ASK_TASKS = {}
_ASK_TASK_LOCK = threading.Lock()
_ASK_TASK_TTL_SECONDS = 6 * 3600
_ASK_TASK_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix='team_ask')
_ASK_TASK_BOOTSTRAPPED = False

ASK_MODE_QUICK = 'quick'
ASK_MODE_DEEP = 'deep'
ASK_MODE_TEAM = 'team'
ASK_MODE_DEFAULT = ASK_MODE_TEAM


def _to_bool(v, default=True):
    if isinstance(v, bool):
        return v
    if v is None:
        return bool(default)
    s = str(v).strip().lower()
    if s in ('1', 'true', 'yes', 'on'):
        return True
    if s in ('0', 'false', 'no', 'off'):
        return False
    return bool(default)


def _normalize_ask_mode(v):
    mode = str(v or '').strip().lower()
    if mode in (ASK_MODE_QUICK, ASK_MODE_DEEP, ASK_MODE_TEAM):
        return mode
    return ASK_MODE_DEFAULT


def _normalize_timeout_minutes(value, default_minutes, min_minutes=1, max_minutes=60):
    try:
        mins = int(value)
    except Exception:
        mins = int(default_minutes)
    mins = max(int(min_minutes), min(int(max_minutes), mins))
    return mins


def _default_ask_task_plan(ask_mode):
    mode = _normalize_ask_mode(ask_mode)
    if mode == ASK_MODE_QUICK:
        return ['任务拆解', '快速核验', '直接答复']
    if mode == ASK_MODE_DEEP:
        return ['任务拆解', '证据收集', '逻辑整合', '输出结论']
    return ['任务拆解', '成员执行', '必要讨论', '总监汇总']


def _bootstrap_ask_task_store():
    global _ASK_TASK_BOOTSTRAPPED
    if _ASK_TASK_BOOTSTRAPPED:
        return
    with _ASK_TASK_LOCK:
        if _ASK_TASK_BOOTSTRAPPED:
            return
        _ASK_TASK_BOOTSTRAPPED = True
    try:
        from AlphaFin.ai_team.core.memory import mark_stale_running_ask_tasks
        mark_stale_running_ask_tasks(stale_after_seconds=180)
    except Exception:
        pass


def _cleanup_ask_tasks(now_ts=None):
    _bootstrap_ask_task_store()
    now_ts = float(now_ts or time.time())
    expired = []
    with _ASK_TASK_LOCK:
        for ask_id, item in _ASK_TASKS.items():
            updated_at = float(item.get('updated_at') or item.get('created_at') or 0)
            if now_ts - updated_at > _ASK_TASK_TTL_SECONDS:
                expired.append(ask_id)
        for ask_id in expired:
            _ASK_TASKS.pop(ask_id, None)
    try:
        from AlphaFin.ai_team.core.memory import cleanup_ask_tasks
        cleanup_ask_tasks(_ASK_TASK_TTL_SECONDS)
    except Exception:
        pass


def _set_ask_task(ask_id, **fields):
    _bootstrap_ask_task_store()
    ask_id = str(ask_id or '').strip()
    if not ask_id:
        return {}
    now_ts = time.time()
    with _ASK_TASK_LOCK:
        item = _ASK_TASKS.get(ask_id, {})
        if not item:
            try:
                from AlphaFin.ai_team.core.memory import get_ask_task as load_ask_task
                db_item = load_ask_task(ask_id)
                if isinstance(db_item, dict):
                    item = dict(db_item)
            except Exception:
                item = {}
        item.update(fields)
        item['ask_id'] = ask_id
        if not item.get('created_at'):
            item['created_at'] = now_ts
        item['updated_at'] = float(item.get('updated_at') or now_ts)
        if 'updated_at' not in fields:
            item['updated_at'] = now_ts
        _ASK_TASKS[ask_id] = item
        snapshot = dict(item)
    try:
        from AlphaFin.ai_team.core.memory import upsert_ask_task
        upsert_ask_task(ask_id, snapshot)
    except Exception:
        pass
    return snapshot


def _get_ask_task(ask_id):
    _bootstrap_ask_task_store()
    ask_id = str(ask_id or '').strip()
    if not ask_id:
        return None
    with _ASK_TASK_LOCK:
        item = _ASK_TASKS.get(ask_id)
        if item:
            return dict(item)
    try:
        from AlphaFin.ai_team.core.memory import get_ask_task as load_ask_task
        db_item = load_ask_task(ask_id)
    except Exception:
        db_item = None
    if isinstance(db_item, dict):
        with _ASK_TASK_LOCK:
            _ASK_TASKS[ask_id] = dict(db_item)
        return dict(db_item)
    return None


def _submit_ask_task(ask_id, agent_id, question, ask_mode, enable_web_search, context_text, chat_session_id):
    try:
        _ASK_TASK_EXECUTOR.submit(
            _run_ask_task,
            ask_id, agent_id, question, ask_mode, enable_web_search, context_text, chat_session_id
        )
        return True, ''
    except Exception as e:
        return False, str(e)


def _stop_all_ask_tasks(reason='用户手动停止'):
    """将当前未完成的直连问答任务统一标记为 stopped。"""
    from AlphaFin.ai_team.core.session_control import cancel_session
    now_ts = time.time()
    target_ids = []
    with _ASK_TASK_LOCK:
        for ask_id, item in (_ASK_TASKS or {}).items():
            if bool(item.get('done')):
                continue
            st = str(item.get('status') or '')
            if st in ('queued', 'running'):
                target_ids.append(str(ask_id))
    for ask_id in target_ids:
        item = _get_ask_task(ask_id) or {}
        ask_session = str(item.get('session_id') or '').strip()
        if ask_session:
            cancel_session(ask_session, reason=reason)
        _set_ask_task(
            ask_id,
            status='stopped',
            done=True,
            error=str(reason or '用户手动停止'),
            updated_at=now_ts
        )
    return len(target_ids)


def _normalize_user_ask_session(session_id):
    s = str(session_id or '').strip()
    if not s:
        return ''
    if not s.startswith('user_ask_'):
        return ''
    if not re.fullmatch(r'[A-Za-z0-9_\-]{8,80}', s):
        return ''
    return s[:80]


def _normalize_search_links(rows, limit=12):
    out = []
    seen = set()
    max_n = max(1, int(limit or 12))
    for row in (rows or []):
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
        if len(out) >= max_n:
            break
    return out


def _merge_search_links(base, extra, limit=12):
    merged = []
    merged.extend(list(base or []))
    merged.extend(list(extra or []))
    return _normalize_search_links(merged, limit=limit)


def _format_search_links_for_prompt(rows, limit=6):
    items = _normalize_search_links(rows or [], limit=limit)
    if not items:
        return ''
    lines = []
    for idx, row in enumerate(items, 1):
        lines.append(
            '%d) %s | %s | %s | %s' % (
                idx,
                row.get('published_at') or '-',
                row.get('source') or '-',
                (row.get('title') or '-')[:120],
                row.get('url') or '-',
            )
        )
        if row.get('summary'):
            lines.append('   摘要: %s' % str(row.get('summary') or '')[:180])
    return '\n'.join(lines)


def _format_search_links_for_reply(rows, limit=8):
    items = _normalize_search_links(rows or [], limit=limit)
    if not items:
        return ''
    lines = ['### 联网搜索来源链接']
    for idx, row in enumerate(items, 1):
        title = str(row.get('title') or row.get('url') or '-').strip()
        url = str(row.get('url') or '').strip()
        source = str(row.get('source') or '-').strip() or '-'
        published_at = str(row.get('published_at') or '-').strip() or '-'
        if url:
            lines.append('%d. [%s](%s)（%s | %s）' % (idx, title, url, source, published_at))
        else:
            lines.append('%d. %s（%s | %s）' % (idx, title, source, published_at))
    return '\n'.join(lines)


def _append_search_links_to_reply(reply_text, search_links, enabled=False, used=False):
    text = str(reply_text or '').rstrip()
    links_md = _format_search_links_for_reply(search_links or [], limit=8)
    if not links_md:
        return text
    if (not enabled) and (not used):
        return text
    if '### 联网搜索来源链接' in text or '联网搜索来源链接' in text:
        return text
    if text:
        return (text + '\n\n' + links_md).strip()
    return links_md


def _friendly_web_search_error(err_text):
    s = str(err_text or '').strip()
    if not s:
        return ''
    low = s.lower()
    if 'kimi联网搜索未配置api密钥' in s or 'missing_kimi_api_key' in low:
        return 'Kimi联网搜索未配置API密钥。请先设置 MOONSHOT_API_KEY 或 ALPHAFIN_KIMI_API_KEY。'
    if 'timeout' in low or 'timed out' in low:
        return '联网检索超时，本次未拿到稳定外部来源（可稍后重试）。'
    if 'max retries exceeded' in low or 'failed to establish a new connection' in low:
        return '联网检索连接失败（网络波动或服务暂不可达）。'
    if len(s) > 180:
        return s[:180] + '...'
    return s


def _is_realtime_quote_query(text):
    q = str(text or '').strip()
    if not q:
        return False
    realtime_keys = ('今日', '今天', '实时', '最新', '当前', '现在', '盘中')
    quote_keys = ('股价', '价格', '行情', '收盘价', '开盘价', '涨跌', '报价')
    has_realtime = any(k in q for k in realtime_keys)
    has_quote = any(k in q for k in quote_keys)
    has_stock_hint = bool(re.search(r'(?<!\d)\d{6}(?:\.(?:SH|SZ))?(?!\d)', q.upper())) or ('股' in q)
    return bool(has_realtime and has_quote and has_stock_hint)


def _is_simple_social_query(text):
    q = str(text or '').strip()
    if not q:
        return True
    ql = q.lower()
    zh_simple_keys = (
        '你好', '在吗', '谢谢', '辛苦了',
        '早上好', '中午好', '晚上好', '午安',
        '你是谁', '你是干嘛的', '你能做什么', '你可以做什么',
        '怎么用', '怎么使用', '如何使用',
    )
    if any(k in q for k in zh_simple_keys):
        return True
    if re.search(r'\b(?:hi|hello)\b', ql):
        return True
    return False


def _is_market_query(text):
    q = str(text or '').strip()
    if not q:
        return False
    if re.search(r'(?<!\d)\d{6}(?:\.(?:SH|SZ))?(?!\d)', q.upper()):
        return True
    keys = (
        '股票', '个股', '指数', '行情', '股价', '涨跌', 'K线', '分时', '板块',
        '大盘', 'A股', '沪深', '上证', '深证', '创业板', '仓位', '买入', '卖出', '交易'
    )
    return any(k in q for k in keys)


def _collect_quote_snapshot_rows(question, limit=2):
    try:
        from AlphaFin.services.ai_chat_service import _resolve_stock_entities, _get_local_latest_close
        from AlphaFin.ai_team.services.tushare_watch_service import fetch_intraday_stock_quote
    except Exception:
        return []

    rows = []
    entities = _resolve_stock_entities(str(question or ''), limit=max(1, int(limit or 2))) or []
    for ent in entities:
        code = str(ent.get('ts_code') or '').strip().upper()
        if not code:
            continue
        name = str(ent.get('name') or '').strip()
        row = {'ts_code': code, 'name': name}
        try:
            quote = fetch_intraday_stock_quote(code, freq='1MIN')
        except Exception as e:
            quote = None
            row['quote_error'] = str(e)
        if quote and quote.get('price') not in (None, ''):
            row['price'] = quote.get('price')
            row['prev_close'] = quote.get('prev_close')
            row['time'] = quote.get('time')
            row['source'] = quote.get('source') or 'intraday_quote'
        else:
            local = _get_local_latest_close(code)
            if local and local.get('close') is not None:
                row['price'] = local.get('close')
                row['time'] = local.get('trade_date')
                row['source'] = local.get('source') or 'local_latest_close'
                row['fallback'] = True
        if row.get('price') not in (None, ''):
            rows.append(row)
        if len(rows) >= max(1, int(limit or 2)):
            break
    return rows


def _format_quote_snapshot_for_prompt(rows):
    items = list(rows or [])
    if not items:
        return ''
    lines = ['【行情快照（系统自动抓取）】']
    for idx, row in enumerate(items, 1):
        price = row.get('price')
        try:
            price_text = '%.3f' % float(price)
        except Exception:
            price_text = str(price or '-')
        pct_text = ''
        try:
            prev_close = row.get('prev_close')
            if prev_close not in (None, 0, 0.0):
                pct = (float(price) - float(prev_close)) / float(prev_close) * 100.0
                pct_text = ' | 涨跌幅=%+.2f%%' % pct
        except Exception:
            pct_text = ''
        lines.append(
            '%d) %s(%s) 价格=%s | 时间=%s | 来源=%s%s%s' % (
                idx,
                row.get('name') or '-',
                row.get('ts_code') or '-',
                price_text,
                row.get('time') or '-',
                row.get('source') or '-',
                ' | 回退口径' if row.get('fallback') else '',
                pct_text,
            )
        )
    lines.append('回答硬性要求：若已给出价格快照，结论中必须优先引用该数值并标注时间与来源。')
    return '\n'.join(lines)


def _run_ask_task(
        ask_id,
        agent_id,
        question,
        ask_mode=ASK_MODE_DEFAULT,
        enable_web_search=True,
        context_text='',
        chat_session_id=''
):
    from AlphaFin.ai_team.core.agent_registry import get_agent
    from AlphaFin.ai_team.core.orchestrator import orchestrator

    pre = _get_ask_task(ask_id)
    if pre and str(pre.get('status') or '') == 'stopped':
        return

    ask_mode = _normalize_ask_mode(ask_mode)
    enable_web_search = _to_bool(enable_web_search, True)
    _set_ask_task(
        ask_id,
        status='running',
        done=False,
        error='',
        ask_mode=ask_mode,
        enable_web_search=enable_web_search,
        web_search_used=False,
        web_search_error='',
        web_search_raw='',
    )
    try:
        cur = _get_ask_task(ask_id)
        if cur and str(cur.get('status') or '') == 'stopped':
            return

        agent = get_agent(agent_id)
        if not agent:
            _set_ask_task(
                ask_id,
                status='error',
                done=True,
                error='智能体不存在: %s' % agent_id
            )
            return
        agent_model = ''
        try:
            agent_model = str((agent.get_model() or {}).get('model') or '')
        except Exception:
            agent_model = ''

        final_question = str(question or '')
        extra_context = str(context_text or '').strip()
        if extra_context:
            final_question = (
                final_question +
                '\n\n【用户上传上下文材料】\n' +
                extra_context +
                '\n\n请将这些材料纳入分析与结论。'
            )
        quote_rows = []
        if _is_realtime_quote_query(question):
            quote_rows = _collect_quote_snapshot_rows(question, limit=2)
            quote_text = _format_quote_snapshot_for_prompt(quote_rows)
            if quote_text:
                final_question = final_question + '\n\n' + quote_text
        if _is_market_query(question):
            final_question = (
                final_question +
                '\n\n【技术面执行要求】'
                '\n请优先调用 get_kline_technical（必要时补充 get_kline），'
                '并在结论中明确给出日/周/月三个周期的技术判断（MACD/KDJ与趋势方向）。'
                '\n若是快速模式，请保持简洁：先给结论，再列出日/周/月三条技术依据。'
            )

        session_id = ''
        workflow_mode = ''
        workflow_name = ''
        participants = []
        workflow_steps = []
        task_plan = []
        task_plan_details = []
        prompt_profile = {}
        required_agents = []
        search_links = []
        forced_search_links = []
        web_search_used = False
        web_search_error = ''
        web_raw = ''
        simple_social_query = _is_simple_social_query(question)
        # 用户勾选联网后，统一强制执行 Kimi 联网搜索（不再按问题类型跳过）。
        effective_web_search = bool(enable_web_search)

        prefetch_web = bool(effective_web_search)
        if prefetch_web:
            try:
                from AlphaFin.services.ai_chat_service import (
                    _collect_web_search_snapshot,
                    _format_web_search_packet_for_prompt,
                )
                web_query = str(question or final_question or '').strip()
                web_ret = _collect_web_search_snapshot(
                    web_query,
                    model_name=agent_model,
                    max_items=10,
                )
                web_search_used = True
                if isinstance(web_ret, dict):
                    forced_search_links = _normalize_search_links(web_ret.get('items') or [], limit=16)
                    web_search_error = _friendly_web_search_error(web_ret.get('error'))
                    web_raw = str(web_ret.get('raw') or '').strip()
                    if forced_search_links:
                        web_search_error = ''
                    packet_text = _format_web_search_packet_for_prompt(
                        web_raw,
                        forced_search_links,
                        limit=6,
                        raw_limit=3600
                    )
                    if packet_text:
                        final_question = (
                            final_question +
                            '\n\n【联网检索结果包（系统自动执行）】\n' +
                            packet_text +
                            '\n\n回答硬性要求：'
                            '\n1) 优先依据以上结果包，不要凭记忆臆测。'
                            '\n2) 联网结果是外部证据，不得整段原文照搬；需结合当前问题给出归纳判断。'
                            '\n3) 若来源不足以支持结论，明确写“联网数据不足，待补充验证”。'
                            '\n4) 在文末给出“证据与来源”小节，并附链接。'
                        )
                    elif web_search_error:
                        final_question = (
                            final_question +
                            '\n\n【联网检索状态】本次自动检索失败：' + web_search_error +
                            '\n请避免给出确定性结论；若无法确认，请明确写“联网检索失败，结论待核实”。'
                        )
            except Exception as web_e:
                web_search_used = True
                web_search_error = _friendly_web_search_error(str(web_e))
        _set_ask_task(
            ask_id,
            web_search_used=bool(web_search_used),
            web_search_error=str(web_search_error or ''),
            web_search_raw=str(web_raw or ''),
        )

        ask_session = _normalize_user_ask_session(chat_session_id)
        if ask_mode == ASK_MODE_QUICK:
            # 快速模式统一走“单智能体快速推理”：
            # - 同一 chat_session_id 可持续注入短上下文；
            # - 联网结果仅作为外部证据包，不做“原文直拷贝”。
            ask_session = ask_session or ('user_ask_' + str(uuid.uuid4())[:8])
            quick_allowed = ['get_kline_technical', 'get_kline', 'get_intraday_stock_quote', 'get_current_time']
            think_ret = agent.think(
                final_question,
                session_id=ask_session,
                response_style='quick',
                max_tool_rounds=1,
                allowed_tools=quick_allowed,
                blocked_tools=['web_search'],
                return_meta=True,
            )
            if isinstance(think_ret, dict):
                reply = str(think_ret.get('reply') or '')
                search_links = list(think_ret.get('web_links') or [])
                task_plan = list(think_ret.get('task_plan') or [])
                task_plan_details = list(think_ret.get('task_plan_details') or [])
                prompt_profile = dict(think_ret.get('prompt_profile') or {})
            else:
                reply = str(think_ret or '')
            session_id = ask_session
            workflow_mode = 'single_agent'
            workflow_name = '单智能体快速直答'
            participants = [agent_id]
            workflow_steps = ['single_agent_quick']
            required_agents = [agent_id]
        elif agent_id == 'director':
            # 统一策略：联网开关只控制“系统预取检索”，不再让智能体内部二次触发 web_search。
            # 这样可以避免“工具检索结果未正确回灌到最终回答”的链路不一致问题。
            director_enable_web_search = False
            reply_meta = orchestrator.run_director_user_ask(
                final_question,
                return_meta=True,
                session_id=ask_session or None,
                ask_mode=ask_mode,
                enable_web_search=director_enable_web_search
            )
            if isinstance(reply_meta, dict):
                reply = reply_meta.get('answer', '')
                session_id = str(reply_meta.get('session_id') or '')
                workflow_mode = str(reply_meta.get('workflow_mode') or '')
                workflow_name = str(reply_meta.get('workflow_name') or '')
                ask_mode = _normalize_ask_mode(reply_meta.get('ask_mode') or ask_mode)
                search_links = list(reply_meta.get('search_links') or [])
                participants = list(reply_meta.get('participants') or [])
                workflow_steps = list(reply_meta.get('workflow_steps') or [])
                task_plan = list(reply_meta.get('task_plan') or [])
                task_plan_details = list(reply_meta.get('task_plan_details') or [])
                prompt_profile = dict(reply_meta.get('prompt_profile') or {})
                required_agents = list(reply_meta.get('required_agents') or [])
            else:
                reply = reply_meta
        else:
            ask_session = ask_session or ('user_ask_' + str(uuid.uuid4())[:8])
            response_style = ask_mode if ask_mode in (ASK_MODE_QUICK, ASK_MODE_DEEP) else 'auto'
            if ask_mode == ASK_MODE_QUICK:
                max_rounds = 1
            elif ask_mode == ASK_MODE_DEEP:
                max_rounds = 12
            else:
                max_rounds = 8
            quick_allowed = None
            # 统一策略：联网内容由系统预取（Kimi）后注入上下文，智能体不再直接调用内置 web_search。
            use_internal_web_tool = False
            if ask_mode == ASK_MODE_QUICK:
                # 快速模式保留轻量技术面能力（K线 + 技术解读），不启用内部 web_search。
                quick_allowed = ['get_kline_technical', 'get_kline', 'get_intraday_stock_quote', 'get_current_time']
                max_rounds = 1
            think_ret = agent.think(
                final_question,
                session_id=ask_session,
                response_style=response_style,
                max_tool_rounds=max_rounds,
                allowed_tools=(quick_allowed if ask_mode == ASK_MODE_QUICK else None),
                blocked_tools=([] if use_internal_web_tool else ['web_search']),
                return_meta=True,
            )
            if isinstance(think_ret, dict):
                reply = str(think_ret.get('reply') or '')
                search_links = list(think_ret.get('web_links') or [])
                task_plan = list(think_ret.get('task_plan') or [])
                task_plan_details = list(think_ret.get('task_plan_details') or [])
                prompt_profile = dict(think_ret.get('prompt_profile') or {})
            else:
                reply = str(think_ret or '')
            session_id = ask_session
            workflow_mode = 'single_agent'
            workflow_name = '单智能体直答'
            participants = [agent_id]
            workflow_steps = ['single_agent_reply']
            required_agents = [agent_id]

        search_links = _merge_search_links(forced_search_links, search_links, limit=16)
        if not effective_web_search:
            web_search_used = False
            web_search_error = ''
        elif not web_search_used:
            web_search_used = bool(search_links)
        reply = _append_search_links_to_reply(
            reply,
            search_links,
            enabled=effective_web_search,
            used=web_search_used
        )

        cur = _get_ask_task(ask_id)
        if cur and str(cur.get('status') or '') == 'stopped':
            return

        _set_ask_task(
            ask_id,
            status='done',
            done=True,
            reply=reply or '',
            session_id=session_id,
            ask_mode=ask_mode,
            enable_web_search=enable_web_search,
            search_links=search_links,
            workflow_mode=workflow_mode,
            workflow_name=workflow_name,
            participants=participants,
            workflow_steps=workflow_steps,
            task_plan=task_plan,
            task_plan_details=task_plan_details,
            prompt_profile=prompt_profile,
            required_agents=required_agents,
            web_search_used=web_search_used,
            web_search_error=web_search_error,
            web_search_raw=str(web_raw or ''),
        )
    except Exception as e:
        cur = _get_ask_task(ask_id)
        if cur and str(cur.get('status') or '') == 'stopped':
            return
        _set_ask_task(
            ask_id,
            status='error',
            done=True,
            error='处理失败: %s' % str(e)
        )


def _get_team_prompt_items():
    """返回智能团队全部可管理提示词（含当前值）。"""
    from AlphaFin.ai_team.config import AGENT_META
    from AlphaFin.ai_team.prompt_catalog import get_catalog_prompt_items
    from AlphaFin.ai_team.core.agent_registry import get_agent_prompt_defaults
    from AlphaFin.services.prompt_config_service import get_prompt

    defaults = get_agent_prompt_defaults()
    items = []
    for agent_id, meta in AGENT_META.items():
        default_prompt = defaults.get(agent_id, '')
        current_prompt = get_prompt('ai_team', agent_id, default_prompt)
        items.append({
            'key': agent_id,
            'name': meta.get('name', agent_id),
            'description': meta.get('description', ''),
            'category': '智能体系统提示词',
            'default_prompt': default_prompt,
            'prompt': current_prompt,
            'is_overridden': current_prompt != default_prompt,
            'kind': 'agent_system_prompt',
        })
    items.extend(get_catalog_prompt_items())
    return items


_TOOL_HANDLER_OVERRIDES = {
    'send_message_to_agent': '_exec_send_message',
    'create_skill': '_exec_create_skill',
    'execute_skill': '_exec_execute_skill',
    'list_skills': '_exec_list_skills',
}

_HIGH_RISK_TOOLS = {
    'submit_trade_signal', 'review_trade_signal', 'flag_risk_warning',
    'create_skill', 'execute_skill', 'query_database', 'send_message_to_agent',
}
_MEDIUM_RISK_TOOLS = {
    'web_search', 'run_indicator', 'save_knowledge', 'get_trade_signals',
}


def _clip_text(text, limit=50000):
    raw = str(text or '')
    if len(raw) <= limit:
        return raw, False
    return raw[:limit] + '\n\n...（内容过长，已截断）', True


def _tool_risk_level(tool_name):
    name = str(tool_name or '')
    if name in _HIGH_RISK_TOOLS:
        return 'high'
    if name in _MEDIUM_RISK_TOOLS:
        return 'medium'
    return 'low'


def _skill_risk_level(skill_entry):
    category = str((skill_entry or {}).get('category') or '')
    approved = bool((skill_entry or {}).get('approved'))
    if not approved:
        return 'high'
    if category in ('trading_strategy', 'risk_rule', 'portfolio'):
        return 'high'
    if category in ('visualization', 'statistics'):
        return 'medium'
    return 'low'


def _normalize_skill_file(base_dir, filename):
    base = os.path.abspath(base_dir or '')
    candidate = os.path.abspath(os.path.join(base, str(filename or '')))
    if not candidate.startswith(base + os.sep):
        return ''
    return candidate


def _build_agent_tool_matrix():
    """
    返回每个智能体可用工具列表。
    优先读运行中智能体；缺失时回退到 creator('audit_dummy_key') 构建。
    """
    from AlphaFin.ai_team.config import AGENT_META
    matrix = {aid: [] for aid in AGENT_META.keys()}

    # 1) 运行中智能体
    try:
        from AlphaFin.ai_team.core.agent_registry import get_agents
        running_agents = get_agents()
    except Exception:
        running_agents = {}
    for aid, agent in (running_agents or {}).items():
        tool_names = []
        for item in getattr(agent, 'tools', []) or []:
            fn = (item or {}).get('function') or {}
            name = fn.get('name')
            if name:
                tool_names.append(str(name))
        matrix[aid] = sorted(set(tool_names))

    # 2) 回退：若某智能体当前未加载，构建临时实例读取工具权限
    missing = [aid for aid, tools in matrix.items() if not tools]
    if not missing:
        return matrix

    try:
        from AlphaFin.ai_team.agents.decision_director import create_agent as create_director
        from AlphaFin.ai_team.agents.investment_analyst import create_agent as create_analyst
        from AlphaFin.ai_team.agents.risk_officer import create_agent as create_risk
        from AlphaFin.ai_team.agents.market_intelligence import create_agent as create_intel
        from AlphaFin.ai_team.agents.quant_strategist import create_agent as create_quant
        from AlphaFin.ai_team.agents.audit_reviewer import create_agent as create_auditor
        from AlphaFin.ai_team.agents.restructuring_specialist import create_agent as create_restructuring
        creators = {
            'director': create_director,
            'analyst': create_analyst,
            'risk': create_risk,
            'intel': create_intel,
            'quant': create_quant,
            'auditor': create_auditor,
            'restructuring': create_restructuring,
        }
        for aid in missing:
            creator = creators.get(aid)
            if not creator:
                continue
            try:
                temp_agent = creator('audit_dummy_key')
                temp_tools = []
                for item in getattr(temp_agent, 'tools', []) or []:
                    fn = (item or {}).get('function') or {}
                    name = fn.get('name')
                    if name:
                        temp_tools.append(str(name))
                matrix[aid] = sorted(set(temp_tools))
            except Exception:
                continue
    except Exception:
        pass

    return matrix


def _get_tool_handler(tool_name):
    from AlphaFin.ai_team.core import tool_registry
    name = str(tool_name or '').strip()
    if not name:
        return None, ''
    handler_name = _TOOL_HANDLER_OVERRIDES.get(name, '_exec_' + name)
    handler = getattr(tool_registry, handler_name, None)
    return handler, handler_name


def _read_tool_source(tool_name):
    handler, handler_name = _get_tool_handler(tool_name)
    if not handler:
        return {
            'ok': False,
            'error': '未找到工具处理函数: %s' % tool_name,
            'handler_name': handler_name,
        }
    try:
        lines, start_line = inspect.getsourcelines(handler)
        code = ''.join(lines)
        path = inspect.getsourcefile(handler) or inspect.getfile(handler) or ''
        clipped, truncated = _clip_text(code, limit=60000)
        return {
            'ok': True,
            'handler_name': handler_name,
            'path': path,
            'line_start': int(start_line or 1),
            'line_end': int((start_line or 1) + len(lines) - 1),
            'code': clipped,
            'truncated': truncated,
        }
    except Exception as e:
        return {
            'ok': False,
            'handler_name': handler_name,
            'error': '读取工具源码失败: %s' % str(e),
        }


def _load_skill_manifest():
    from AlphaFin.ai_team.config import SKILLS_DIR
    manifest_path = os.path.join(SKILLS_DIR, 'skills_manifest.json')
    if not os.path.exists(manifest_path):
        return {'skills': [], 'version': '1.0', 'manifest_path': manifest_path}
    with open(manifest_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    data['manifest_path'] = manifest_path
    if 'skills' not in data or not isinstance(data.get('skills'), list):
        data['skills'] = []
    return data


def _build_tools_catalog():
    from AlphaFin.ai_team.core.tool_registry import TOOLS_SCHEMA
    from AlphaFin.ai_team.config import AGENT_META

    matrix = _build_agent_tool_matrix()
    tool_to_agents = {}
    for aid, tools in matrix.items():
        for t in tools:
            tool_to_agents.setdefault(t, []).append(aid)

    tools = []
    for item in TOOLS_SCHEMA:
        fn = (item or {}).get('function') or {}
        name = str(fn.get('name') or '').strip()
        if not name:
            continue
        params = fn.get('parameters') or {}
        props = params.get('properties') or {}
        required = params.get('required') or []
        _, handler_name = _get_tool_handler(name)
        agents = sorted(set(tool_to_agents.get(name, [])))
        tools.append({
            'name': name,
            'description': fn.get('description', ''),
            'risk_level': _tool_risk_level(name),
            'handler_name': handler_name,
            'parameter_keys': sorted(list(props.keys())),
            'required': required,
            'agents': agents,
            'agent_names': [AGENT_META.get(a, {}).get('name', a) for a in agents],
        })
    return tools, matrix


def _build_skills_catalog():
    from AlphaFin.ai_team.config import SKILLS_DIR
    manifest = _load_skill_manifest()
    skills = []
    for s in manifest.get('skills', []):
        abs_path = _normalize_skill_file(SKILLS_DIR, s.get('file', ''))
        skills.append({
            'id': s.get('id', ''),
            'name': s.get('name', ''),
            'description': s.get('description', ''),
            'category': s.get('category', ''),
            'creator': s.get('creator', ''),
            'approved': bool(s.get('approved')),
            'created_at': s.get('created_at'),
            'file': s.get('file', ''),
            'path': abs_path,
            'risk_level': _skill_risk_level(s),
        })
    return skills, manifest


def _static_tool_audit(tool_item, source_payload):
    source = (source_payload or {}).get('code') or ''
    issues = []
    positives = []
    security_score = 92
    reliability_score = 90

    if not source:
        issues.append('未读取到工具源码，无法完成静态审查。')
        security_score -= 35
        reliability_score -= 35
    else:
        low = source.lower()
        if 'except Exception' in source:
            issues.append('存在 broad exception 捕获（except Exception），可能掩盖真实错误。')
            reliability_score -= 8
        if 'requests.' in low or 'ai_chat(' in low:
            issues.append('包含外部网络依赖，稳定性受外部服务与网络波动影响。')
            reliability_score -= 6
            security_score -= 5
        if 'eval(' in low or 'exec(' in low:
            issues.append('检测到动态执行语句，存在高风险。')
            security_score -= 40
        if tool_item.get('name') == 'query_database':
            if '仅允许 SELECT' in source and 'for forbidden' in source:
                positives.append('已实现 SQL 只读约束与危险关键字拦截。')
            else:
                issues.append('数据库查询工具未识别到完整只读防护逻辑。')
                security_score -= 20
        if tool_item.get('name') in _HIGH_RISK_TOOLS:
            issues.append('该工具属于高影响能力（交易/审批/执行/写入），应保留人工触发与审计。')
            security_score -= 10

    if not issues:
        positives.append('未发现明显高危静态风险。')

    security_score = max(0, min(100, security_score))
    reliability_score = max(0, min(100, reliability_score))
    level = 'green'
    if min(security_score, reliability_score) < 70:
        level = 'yellow'
    if min(security_score, reliability_score) < 50:
        level = 'red'
    return {
        'level': level,
        'security_score': security_score,
        'reliability_score': reliability_score,
        'issues': issues[:8],
        'positives': positives[:6],
    }


def _static_skill_audit(skill_item, source_text):
    from AlphaFin.ai_team.core.skill_sandbox import sandbox, SecurityError

    issues = []
    positives = []
    security_score = 90
    reliability_score = 88

    if not source_text:
        issues.append('技能源码为空或不可读取。')
        security_score -= 40
        reliability_score -= 40
    else:
        try:
            sandbox.validate_code(source_text)
            positives.append('通过沙箱AST白名单校验。')
        except SecurityError as e:
            issues.append('未通过沙箱校验: %s' % str(e))
            security_score -= 45
        except Exception as e:
            issues.append('沙箱校验异常: %s' % str(e))
            security_score -= 25

        low = source_text.lower()
        if 'while true' in low:
            issues.append('检测到无限循环模式（while True），存在超时和资源风险。')
            reliability_score -= 20
        if 'result' not in source_text:
            issues.append('未检测到 result 输出变量，执行结果可能不可读。')
            reliability_score -= 15
        else:
            positives.append('检测到 result 输出变量，具备标准结果返回形态。')

    if not bool(skill_item.get('approved')):
        issues.append('该技能当前为未批准状态，不应在生产流程直接启用。')
        security_score -= 15
    else:
        positives.append('技能处于已批准状态。')

    security_score = max(0, min(100, security_score))
    reliability_score = max(0, min(100, reliability_score))
    level = 'green'
    if min(security_score, reliability_score) < 70:
        level = 'yellow'
    if min(security_score, reliability_score) < 50:
        level = 'red'
    return {
        'level': level,
        'security_score': security_score,
        'reliability_score': reliability_score,
        'issues': issues[:8],
        'positives': positives[:6],
    }


def _risk_officer_audit_llm(title, payload):
    """
    仅在用户手动触发审查时调用风控官模型。
    """
    from AlphaFin.ai_team.core.agent_registry import get_agent
    risk_agent = get_agent('risk')
    from AlphaFin.ai_team.prompt_catalog import format_ai_team_prompt
    if not risk_agent:
        return {
            'available': False,
            'error': '风控官当前不可用（可能未初始化或API异常）',
            'review': '',
        }

    payload_text, _ = _clip_text(payload, limit=12000)
    prompt = format_ai_team_prompt(
        'tool_audit.risk_review',
        title=title,
        payload_text=payload_text
    )
    try:
        reply = risk_agent.think(
            prompt,
            session_id='risk_audit_%s' % uuid.uuid4().hex[:8],
            max_tool_rounds=1,
            allowed_tools=[]
        )
        return {'available': True, 'error': '', 'review': reply or ''}
    except Exception as e:
        return {'available': False, 'error': str(e), 'review': ''}


# ──────────────── 页面路由 ────────────────

@bp.route('/team')
def team_page():
    """智能分析团队主页面"""
    groups = get_grouped()
    return render_template('ai_team.html', groups=groups)


# ──────────────── API 路由 ────────────────

@bp.route('/api/team/status')
def team_status():
    """获取所有智能体状态"""
    from AlphaFin.ai_team.core.agent_registry import get_all_status, get_runtime_model_settings
    from AlphaFin.ai_team.core.orchestrator import orchestrator
    from AlphaFin.ai_team.core.portfolio_scheduler import portfolio_scheduler
    from AlphaFin.ai_team.core.session_control import get_waiting_overtime_sessions
    return jsonify({
        'agents': get_all_status(),
        'models': get_runtime_model_settings(),
        'orchestrator': orchestrator.get_state(),
        'portfolio_scheduler': portfolio_scheduler.get_state(),
        'waiting_overtime': get_waiting_overtime_sessions(limit=6),
    })


@bp.route('/api/team/memory_center')
def team_memory_center():
    """获取团队记忆系统与自进化观测数据。"""
    from AlphaFin.ai_team.config import AGENT_META
    from AlphaFin.ai_team.core.agent_registry import get_agents, get_all_status
    from AlphaFin.ai_team.core.memory import AgentMemory
    from AlphaFin.ai_team.prompt_catalog import get_ai_team_prompt

    def _clip(text, limit=180):
        raw = str(text or '').replace('\n', ' ').strip()
        return raw if len(raw) <= limit else (raw[:limit] + '...')

    statuses = get_all_status()
    status_map = {str(row.get('agent_id') or ''): row for row in statuses}
    agents = get_agents()

    memory_agents = []
    evolution_agents = []
    for agent_id, meta in AGENT_META.items():
        agent = agents.get(agent_id)
        memory = agent.memory if agent else AgentMemory(agent_id)
        status = status_map.get(agent_id, {})
        stats = memory.get_memory_stats()
        recent_knowledge = []
        for item in memory.get_recent_knowledge(limit=5):
            recent_knowledge.append({
                'category': str(item.get('category') or ''),
                'subject': str(item.get('subject') or ''),
                'tier': str(item.get('tier') or ''),
                'confidence': float(item.get('confidence') or 0),
                'updated_at': item.get('updated_at') or 0,
                'source_type': str(item.get('source_type') or ''),
                'preview': _clip(item.get('content') or '', 180),
            })
        recent_reflections = []
        for item in memory.get_recent_reflections(limit=4):
            recent_reflections.append({
                'workflow': str(item.get('workflow') or ''),
                'session_id': str(item.get('session_id') or ''),
                'created_at': item.get('created_at') or 0,
                'task_preview': _clip(item.get('task') or '', 120),
                'reflection_preview': _clip(item.get('reflection') or '', 220),
            })
        patterns = []
        promoted_count = 0
        for item in memory.get_pattern_snapshot(limit=6):
            promoted = bool(item.get('promoted'))
            if promoted:
                promoted_count += 1
            patterns.append({
                'pattern_key': str(item.get('pattern_key') or ''),
                'success_count': int(item.get('success_count') or 0),
                'failure_count': int(item.get('failure_count') or 0),
                'observation_count': int(item.get('observation_count') or 0),
                'promoted': promoted,
                'hot_subject': str(item.get('hot_subject') or ''),
                'updated_at': item.get('updated_at') or 0,
            })

        memory_agents.append({
            'agent_id': agent_id,
            'name': meta.get('name', agent_id),
            'workflow': status.get('current_workflow_label', status.get('current_workflow', '')),
            'session_id': status.get('current_session_id', ''),
            'status': status.get('status', 'offline'),
            'stats': stats,
            'memory_snapshot': status.get('memory_snapshot', {}) or {},
            'recent_knowledge': recent_knowledge,
            'recent_reflections': recent_reflections,
        })
        evolution_agents.append({
            'agent_id': agent_id,
            'name': meta.get('name', agent_id),
            'pattern_count': int(stats.get('pattern_count') or 0),
            'promoted_count': int(stats.get('promoted_count') or promoted_count),
            'reflection_count': int(stats.get('reflection_count') or 0),
            'patterns': patterns,
            'recent_reflections': recent_reflections[:3],
        })

    architecture = {
        'memory_operating_system': get_ai_team_prompt('team.memory_operating_system'),
        'team_core_charter': get_ai_team_prompt('team.core_charter'),
        'injection_order': [
            {'order': 1, 'label': '系统提示词', 'position': 'system', 'detail': '每个智能体自己的角色系统提示词。'},
            {'order': 2, 'label': '团队核心宪章', 'position': 'system', 'detail': '团队统一目标、价值观与分析原则。'},
            {'order': 3, 'label': '角色职责记忆', 'position': 'system', 'detail': '该智能体的长期职责与方法偏好。'},
            {'order': 4, 'label': '时钟与交易约束', 'position': 'system', 'detail': '北京时间、交易时段、A股T+1等约束。'},
            {'order': 5, 'label': '记忆协议与风格约束', 'position': 'system', 'detail': 'HOT/WARM/COLD 使用原则，以及 quick/deep/team 风格。'},
            {'order': 6, 'label': '长期记忆摘要', 'position': 'system', 'detail': 'HOT/WARM/COLD 检索结果以摘要形式拼接到 system prompt。'},
            {'order': 7, 'label': '相关历史对话召回', 'position': 'history', 'detail': '与当前问题相关的 user/assistant 历史对话作为历史消息注入。'},
            {'order': 8, 'label': '进程内短上下文', 'position': 'history', 'detail': '当前进程里保留的近期对话窗口继续注入。'},
            {'order': 9, 'label': '当前任务', 'position': 'user', 'detail': '最后才注入当前用户问题或当前工作流任务。'},
        ],
        'design_answer': '当前不是随意直接注入，而是按固定顺序注入：长期规则先进入 system prompt，历史对话再进入 history，当前任务最后进入 user。'
    }

    return jsonify({
        'memory_agents': memory_agents,
        'evolution_agents': evolution_agents,
        'architecture': architecture,
        'generated_at': time.time(),
    })


@bp.route('/api/team/user_ask/workflow', methods=['GET', 'POST'])
def team_user_ask_workflow():
    """获取/更新“直连问答”自定义工作流配置。"""
    from AlphaFin.ai_team.core.orchestrator import orchestrator
    if request.method == 'GET':
        return jsonify({
            'workflow': orchestrator.get_user_ask_workflow(),
            'presets': orchestrator.get_user_ask_workflow_presets(),
        })

    data = request.json or {}
    cfg = data.get('workflow', data)
    saved = orchestrator.set_user_ask_workflow(cfg)
    return jsonify({
        'success': True,
        'workflow': saved,
        'presets': orchestrator.get_user_ask_workflow_presets(),
    })


@bp.route('/api/team/prompts')
def team_prompts():
    """获取智能团队系统提示词（可视化+编辑用）。"""
    return jsonify({
        'module': 'ai_team',
        'prompts': _get_team_prompt_items(),
    })


@bp.route('/api/team/models')
def team_models():
    """获取智能团队模型配置（默认模型 + 每个智能体模型）。"""
    from AlphaFin.ai_team.core.agent_registry import get_runtime_model_settings
    return jsonify({
        'success': True,
        'models': get_runtime_model_settings(),
    })


@bp.route('/api/team/models/default', methods=['PUT'])
def team_model_default_update():
    """更新团队默认模型。"""
    from AlphaFin.ai_team.core.agent_registry import (
        update_team_default_model,
        get_runtime_model_settings,
    )
    data = request.json or {}
    model_name = str(data.get('model') or '').strip()
    if not model_name:
        return jsonify({'error': '模型不能为空'}), 400
    ret = update_team_default_model(model_name)
    return jsonify({
        'success': True,
        'model': ret.get('model', ''),
        'runtime_applied_agents': ret.get('runtime_applied_agents', []),
        'models': get_runtime_model_settings(),
    })


@bp.route('/api/team/models/<agent_id>', methods=['PUT'])
def team_model_agent_update(agent_id):
    """更新单个智能体模型（持久化 + 运行中生效）。"""
    from AlphaFin.ai_team.config import AGENT_META
    from AlphaFin.ai_team.core.agent_registry import (
        update_agent_model,
        get_runtime_model_settings,
    )
    if agent_id not in AGENT_META:
        return jsonify({'error': '智能体不存在: %s' % agent_id}), 404
    data = request.json or {}
    model_name = str(data.get('model') or '').strip()
    if not model_name:
        return jsonify({'error': '模型不能为空'}), 400
    ret = update_agent_model(agent_id, model_name)
    return jsonify({
        'success': True,
        'agent_id': agent_id,
        'model': ret.get('model', ''),
        'runtime_applied': bool(ret.get('runtime_applied')),
        'models': get_runtime_model_settings(),
    })


@bp.route('/api/team/prompts/<agent_id>', methods=['PUT'])
def team_prompt_update(agent_id):
    """更新指定智能体系统提示词。"""
    from AlphaFin.ai_team.config import AGENT_META
    from AlphaFin.ai_team.prompt_catalog import has_catalog_prompt_key
    from AlphaFin.ai_team.core.agent_registry import update_agent_system_prompt
    from AlphaFin.services.prompt_config_service import set_prompt

    if agent_id not in AGENT_META and not has_catalog_prompt_key(agent_id):
        return jsonify({'error': '提示词配置不存在: %s' % agent_id}), 404

    data = request.json or {}
    prompt_text = str(data.get('prompt', '')).strip()
    if not prompt_text:
        return jsonify({'error': '提示词不能为空'}), 400

    try:
        saved_prompt = set_prompt('ai_team', agent_id, prompt_text)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    if agent_id in AGENT_META:
        runtime_applied = update_agent_system_prompt(agent_id, saved_prompt)
    else:
        runtime_applied = True
    return jsonify({
        'success': True,
        'agent_id': agent_id,
        'runtime_applied': runtime_applied,
        'prompts': _get_team_prompt_items(),
    })


@bp.route('/api/team/prompts/<agent_id>/reset', methods=['POST'])
def team_prompt_reset(agent_id):
    """重置指定智能体系统提示词到默认值。"""
    from AlphaFin.ai_team.config import AGENT_META
    from AlphaFin.ai_team.prompt_catalog import (
        has_catalog_prompt_key,
        get_catalog_prompt_default,
    )
    from AlphaFin.ai_team.core.agent_registry import (
        get_agent_prompt_defaults, update_agent_system_prompt
    )
    from AlphaFin.services.prompt_config_service import reset_prompt

    if agent_id not in AGENT_META and not has_catalog_prompt_key(agent_id):
        return jsonify({'error': '提示词配置不存在: %s' % agent_id}), 404

    reset_prompt('ai_team', agent_id)
    if agent_id in AGENT_META:
        default_prompt = get_agent_prompt_defaults().get(agent_id, '')
        runtime_applied = update_agent_system_prompt(agent_id, default_prompt)
    else:
        default_prompt = get_catalog_prompt_default(agent_id)
        runtime_applied = True

    return jsonify({
        'success': True,
        'agent_id': agent_id,
        'runtime_applied': runtime_applied,
        'prompts': _get_team_prompt_items(),
    })


@bp.route('/api/team/module/status')
def team_module_status():
    """获取智能分析模块后台运行状态（首页按钮使用）"""
    from AlphaFin.ai_team import get_team_module_state
    return jsonify(get_team_module_state())


@bp.route('/api/team/module/start', methods=['POST'])
def team_module_start():
    """手动启动智能分析模块后台调度"""
    from AlphaFin.ai_team import start_team_module
    return jsonify(start_team_module())


@bp.route('/api/team/module/stop', methods=['POST'])
def team_module_stop():
    """手动停止智能分析模块后台调度"""
    from AlphaFin.ai_team import stop_team_module
    return jsonify(stop_team_module())


@bp.route('/api/team/stop_all_work', methods=['POST'])
def team_stop_all_work():
    """
    一键停止当前全部工作：
    - 停止 orchestrator/portfolio 后台循环
    - 通知所有智能体中止当前任务
    - 终止直连问答在途任务
    """
    data = request.json or {}
    reason = str(data.get('reason') or '用户点击“停止当前工作”').strip()
    try:
        from AlphaFin.ai_team import stop_team_module
        from AlphaFin.ai_team.core.agent_registry import request_stop_all_agents
        from AlphaFin.ai_team.core.orchestrator import orchestrator
        from AlphaFin.ai_team.core.portfolio_scheduler import portfolio_scheduler
        from AlphaFin.ai_team.core.message_bus import bus
        from AlphaFin.ai_team.core.session_control import (
            cancel_session,
            clear_session_deadline,
            clear_session_progress,
            clear_session_overtime_state,
        )

        session_before = {
            'orchestrator': str(orchestrator.current_session or ''),
            'portfolio': str(portfolio_scheduler.current_session or ''),
        }

        # 先广播停止指令给智能体，再停止后台循环
        stopped_agents = request_stop_all_agents(reason=reason)
        if session_before.get('orchestrator'):
            cancel_session(session_before['orchestrator'], reason=reason)
        if session_before.get('portfolio'):
            cancel_session(session_before['portfolio'], reason=reason)
        module_state = stop_team_module()
        stopped_asks = _stop_all_ask_tasks(reason=reason)

        if session_before.get('orchestrator'):
            bus.clear_session(session_before['orchestrator'])
            clear_session_deadline(session_before['orchestrator'])
            clear_session_progress(session_before['orchestrator'])
            clear_session_overtime_state(session_before['orchestrator'])
        if session_before.get('portfolio'):
            bus.clear_session(session_before['portfolio'])
            clear_session_deadline(session_before['portfolio'])
            clear_session_progress(session_before['portfolio'])
            clear_session_overtime_state(session_before['portfolio'])

        bus.post_activity(
            'system', 'status',
            '已执行停止指令：全部智能体停止当前工作。',
            metadata={
                'mode': 'system_stop',
                'stopped_agents': stopped_agents,
                'stopped_ask_tasks': int(stopped_asks or 0),
                'reason': reason,
                'sessions': session_before,
            }
        )

        return jsonify({
            'success': True,
            'message': '已停止当前全部工作',
            'stopped_agents': stopped_agents,
            'stopped_ask_tasks': int(stopped_asks or 0),
            'sessions': session_before,
            'module_state': module_state,
        })
    except Exception as e:
        return jsonify({'error': '停止当前工作失败: %s' % str(e)}), 500


@bp.route('/api/team/activity')
def team_activity():
    """SSE 实时活动流"""
    from AlphaFin.ai_team.core.message_bus import bus

    def event_stream():
        last_seq = 0
        timeout_count = 0
        while timeout_count < 3600:  # 最多30分钟
            messages = bus.get_activity_since(last_seq)
            if messages:
                for msg in messages:
                    data = json.dumps(msg, ensure_ascii=False)
                    yield 'data: %s\n\n' % data
                    last_seq = msg['seq']
                timeout_count = 0
            else:
                # 每10秒发送心跳保持连接
                if timeout_count % 20 == 0:
                    yield 'data: {"type":"heartbeat"}\n\n'
                timeout_count += 1
            time.sleep(0.5)

    return Response(event_stream(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@bp.route('/api/team/trace/runs')
def team_trace_runs():
    """查询端到端 Trace Run 列表。"""
    from AlphaFin.ai_team.core.memory import get_trace_runs
    session_id = str(request.args.get('session_id', '') or '').strip()
    limit = request.args.get('limit', type=int) or 20
    limit = max(1, min(limit, 100))
    runs = get_trace_runs(session_id=session_id or None, limit=limit)
    return jsonify({'runs': runs, 'count': len(runs)})


@bp.route('/api/team/trace/<run_id>')
def team_trace_detail(run_id):
    """查询单条 Trace 详情（run + spans）。"""
    from AlphaFin.ai_team.core.memory import get_trace_run, get_trace_spans
    if not re.fullmatch(r'[A-Za-z0-9_\-]{6,64}', str(run_id or '')):
        return jsonify({'error': 'run_id 无效'}), 400
    run = get_trace_run(run_id)
    if not run:
        return jsonify({'error': 'trace run 不存在'}), 404
    limit = request.args.get('limit', type=int) or 1200
    limit = max(1, min(limit, 5000))
    spans = get_trace_spans(run_id, limit=limit)
    return jsonify({
        'run': run,
        'spans': spans,
        'span_count': len(spans),
    })


@bp.route('/api/team/token_budget')
def team_token_budget():
    """查询当前 Token 预算使用情况。"""
    from AlphaFin.ai_team.config import (
        TOKEN_BUDGET_DAILY_LIMIT, TOKEN_BUDGET_SESSION_LIMIT,
        TOKEN_BUDGET_WARN_RATIO, TOKEN_BUDGET_HARD_RATIO,
    )
    from AlphaFin.ai_team.core.memory import get_token_budget_snapshot
    session_id = str(request.args.get('session_id', '') or '').strip()
    snapshot = get_token_budget_snapshot(
        session_id=session_id,
        daily_limit=TOKEN_BUDGET_DAILY_LIMIT,
        session_limit=TOKEN_BUDGET_SESSION_LIMIT,
        warn_ratio=TOKEN_BUDGET_WARN_RATIO,
        hard_ratio=TOKEN_BUDGET_HARD_RATIO,
    )
    return jsonify({'budget': snapshot})


@bp.route('/api/team/reports')
def team_reports():
    """获取研究报告列表"""
    from AlphaFin.ai_team.services.report_service import list_reports
    report_type = request.args.get('type')
    limit = int(request.args.get('limit', 20))
    return jsonify(list_reports(report_type=report_type, limit=limit))


@bp.route('/api/team/reports/<int:report_id>')
def team_report_detail(report_id):
    """获取单篇报告详情"""
    from AlphaFin.ai_team.services.report_service import get_report_detail
    report = get_report_detail(report_id)
    if not report:
        return jsonify({'error': '报告不存在'}), 404
    return jsonify(report)


@bp.route('/api/team/reports/<int:report_id>/download')
def team_report_download(report_id):
    """下载单篇研究报告（Markdown）"""
    from AlphaFin.ai_team.services.report_service import get_report_detail
    report = get_report_detail(report_id)
    if not report:
        return jsonify({'error': '报告不存在'}), 404

    title = report.get('title') or ('report_%d' % report_id)
    safe_title = re.sub(r'[\\/:*?"<>|]+', '_', str(title)).strip() or ('report_%d' % report_id)
    filename = safe_title[:80] + '.md'
    encoded = quote(filename)
    content = report.get('content') or ''

    return Response(
        content,
        mimetype='text/markdown; charset=utf-8',
        headers={
            'Content-Disposition': "attachment; filename*=UTF-8''%s" % encoded,
            'Cache-Control': 'no-cache',
        }
    )


@bp.route('/api/team/reports/<int:report_id>', methods=['DELETE'])
def team_report_delete(report_id):
    """删除单篇研究报告"""
    from AlphaFin.ai_team.services.report_service import delete_report
    deleted = delete_report(report_id)
    if not deleted:
        return jsonify({'error': '报告不存在'}), 404
    return jsonify({'ok': True, 'deleted_id': int(report_id)})


@bp.route('/api/team/analyze', methods=['POST'])
def team_analyze():
    """用户手动触发分析"""
    from AlphaFin.ai_team.core.orchestrator import orchestrator
    from AlphaFin.ai_team.core.agent_registry import has_active_workflow
    from AlphaFin.ai_team.core.portfolio_scheduler import portfolio_scheduler
    from AlphaFin.ai_team.config import MANUAL_ANALYZE_DEFAULT_TIMEOUT
    from AlphaFin.services.context_upload_service import (
        build_context_file_refs,
        summarize_context_with_qwen_long,
    )
    if has_active_workflow({'user_ask'}):
        return jsonify({'error': '当前有直连问答正在进行，请先等待问答完成或手动停止'}), 409
    if orchestrator.current_session:
        return jsonify({'error': '当前已有研究任务在执行，请先等待完成或手动停止'}), 409
    if portfolio_scheduler.current_session:
        return jsonify({'error': '当前投资/盯盘任务正在执行，请先等待完成或手动停止'}), 409
    data = request.json or {}
    topic = data.get('topic', '')
    if not topic:
        return jsonify({'error': '请指定分析主题（如股票代码或行业）'}), 400
    time_limit_minutes = _normalize_timeout_minutes(
        data.get('time_limit_minutes'),
        max(1, int(MANUAL_ANALYZE_DEFAULT_TIMEOUT / 60))
    )
    context_ids = data.get('context_ids', [])
    ctx_refs = build_context_file_refs(context_ids)
    summary_payload = summarize_context_with_qwen_long(context_ids, user_question=topic)
    context_text = summary_payload.get('summary', '')
    final_topic = str(topic or '')
    if context_text:
        final_topic = (
            final_topic +
            '\n\n【用户上传上下文材料】\n' +
            context_text +
            '\n\n请优先结合上传材料开展研究。'
        )
    elif ctx_refs.get('fileid_system_content'):
        # 多智能体链路当前不直接支持 fileid://，这里回退为可见提示
        final_topic = (
            final_topic +
            '\n\n【用户上传文件引用】\n' +
            ctx_refs.get('fileid_system_content', '') +
            '\n（注：官方文件已上传，但当前团队链路将以摘要文本为主）'
        )
    session = orchestrator.trigger_now(
        topic=final_topic,
        time_limit_seconds=time_limit_minutes * 60
    )
    if not session:
        return jsonify({'error': '当前已有研究或投资任务在执行，请先等待完成或手动停止'}), 409
    return jsonify({
        'message': '分析已启动',
        'session': session,
        'topic': topic,
        'time_limit_minutes': time_limit_minutes,
        'context_file_count': int(ctx_refs.get('file_count') or 0),
        'context_used_ids': ctx_refs.get('used_ids') or [],
    })


@bp.route('/api/team/ask/<agent_id>', methods=['POST'])
def team_ask_agent(agent_id):
    """向特定智能体提问（总监走“分配-讨论-再答复”闭环）"""
    try:
        from AlphaFin.ai_team.core.agent_registry import get_agent, clear_stop_agents
        from AlphaFin.ai_team.core.session_control import clear_session_cancel

        clear_stop_agents([agent_id])
        agent = get_agent(agent_id)
        if not agent:
            return jsonify({'error': '智能体不可用: %s（请先检查模块是否启动、API key是否可用）' % agent_id}), 404

        data = request.json or {}
        question = data.get('question', '')
        if not question:
            return jsonify({'error': '请输入问题'}), 400
        ask_mode = _normalize_ask_mode(data.get('ask_mode', ASK_MODE_DEFAULT))
        enable_web_search = _to_bool(data.get('enable_web_search', True), True)
        chat_session_id = _normalize_user_ask_session(data.get('chat_session_id', ''))
        if chat_session_id:
            clear_session_cancel(chat_session_id)
        from AlphaFin.services.context_upload_service import (
            build_context_file_refs,
            summarize_context_with_qwen_long,
        )
        context_ids = data.get('context_ids', [])
        ctx_refs = build_context_file_refs(context_ids)
        summary_payload = summarize_context_with_qwen_long(context_ids, user_question=question)
        context_text = summary_payload.get('summary', '')
        if (not context_text) and ctx_refs.get('fileid_system_content'):
            context_text = (
                '用户上传文件引用: %s\n'
                '提示：当前链路会优先使用官方解析摘要。'
            ) % ctx_refs.get('fileid_system_content', '')

        ask_id = uuid.uuid4().hex[:12]
        now_ts = time.time()
        _cleanup_ask_tasks(now_ts)
        _set_ask_task(
            ask_id,
            agent_id=agent_id,
            question=question,
            ask_mode=ask_mode,
            enable_web_search=enable_web_search,
            web_search_used=False,
            web_search_error='',
            web_search_raw='',
            session_id=chat_session_id,
            status='queued',
            done=False,
            reply='',
            error='',
            task_plan=_default_ask_task_plan(ask_mode),
            task_plan_details=[],
            context_file_count=int(ctx_refs.get('file_count') or 0),
            created_at=now_ts,
            updated_at=now_ts
        )

        ok, submit_err = _submit_ask_task(
            ask_id,
            agent_id,
            question,
            ask_mode,
            enable_web_search,
            context_text,
            chat_session_id
        )
        if not ok:
            _set_ask_task(
                ask_id,
                status='error',
                done=True,
                error='任务提交失败: %s' % (submit_err or 'unknown')
            )
            return jsonify({'error': '问答任务提交失败，请稍后重试'}), 500

        return jsonify({
            'ask_id': ask_id,
            'agent_id': agent_id,
            'status': 'running',
            'ask_mode': ask_mode,
            'enable_web_search': enable_web_search,
            'chat_session_id': chat_session_id,
            'context_file_count': int(ctx_refs.get('file_count') or 0),
            'context_used_ids': ctx_refs.get('used_ids') or [],
            'message': '问题已提交，正在处理。可在活动日志“直连问答”查看分配与执行进度。'
        })
    except Exception as e:
        return jsonify({'error': '提交问答任务失败: %s' % str(e)}), 500


@bp.route('/api/team/ask_result/<ask_id>')
def team_ask_result(ask_id):
    """查询直连问答任务结果（异步轮询）。"""
    try:
        from AlphaFin.ai_team.core.session_control import (
            get_session_timing,
            get_session_progress,
            get_session_overtime_state,
        )
        if not re.fullmatch(r'[a-f0-9]{8,32}', str(ask_id or '')):
            return jsonify({'error': 'ask_id 无效'}), 400

        _cleanup_ask_tasks()
        item = _get_ask_task(ask_id)
        if not item:
            return jsonify({'error': '问答任务不存在或已过期'}), 404

        created_at = float(item.get('created_at') or time.time())
        elapsed_sec = max(0, int(time.time() - created_at))
        payload = {
            'ask_id': ask_id,
            'agent_id': item.get('agent_id', ''),
            'status': item.get('status', 'running'),
            'done': bool(item.get('done')),
            'created_at': created_at,
            'updated_at': float(item.get('updated_at') or created_at),
            'elapsed_sec': elapsed_sec,
            'session_id': item.get('session_id', ''),
            'ask_mode': item.get('ask_mode', ASK_MODE_DEFAULT),
            'enable_web_search': bool(item.get('enable_web_search', True)),
            'web_search_used': bool(item.get('web_search_used', False)),
            'web_search_error': str(item.get('web_search_error') or ''),
            'web_search_raw': str(item.get('web_search_raw') or ''),
            'workflow_mode': item.get('workflow_mode', ''),
            'workflow_name': item.get('workflow_name', ''),
            'participants': item.get('participants', []) or [],
            'workflow_steps': item.get('workflow_steps', []) or [],
            'task_plan': item.get('task_plan', []) or [],
            'task_plan_details': item.get('task_plan_details', []) or [],
            'prompt_profile': item.get('prompt_profile', {}) or {},
            'required_agents': item.get('required_agents', []) or [],
            'search_links_count': len(item.get('search_links', []) or []),
        }
        payload['session_timing'] = get_session_timing(payload.get('session_id', ''))
        payload['session_progress'] = get_session_progress(payload.get('session_id', ''))
        payload['session_overtime'] = get_session_overtime_state(payload.get('session_id', ''))
        if not payload['done']:
            try:
                from AlphaFin.ai_team.core.agent_registry import get_all_status
                session_id = str(payload.get('session_id') or '')
                required = set(payload.get('required_agents') or [])
                participants = set(payload.get('participants') or [])
                live_agents = []
                for row in get_all_status():
                    aid = str(row.get('agent_id') or '')
                    same_session = session_id and str(row.get('current_session_id') or '') == session_id
                    in_scope = aid in required or aid in participants or aid == str(item.get('agent_id') or '')
                    if same_session or in_scope:
                        live_agents.append(row)
                live_agents.sort(key=lambda row: (0 if str(row.get('agent_id') or '') == 'director' else 1, str(row.get('agent_id') or '')))
                payload['live_agents'] = live_agents
                payload['director_live'] = next(
                    (row for row in live_agents if str(row.get('agent_id') or '') == 'director'),
                    None
                )
            except Exception:
                payload['live_agents'] = []
                payload['director_live'] = None
        try:
            from AlphaFin.ai_team.core.memory import get_trace_runs
            runs = get_trace_runs(session_id=payload.get('session_id') or '', limit=1)
            payload['trace_run_id'] = runs[0].get('run_id', '') if runs else ''
        except Exception:
            payload['trace_run_id'] = ''
        if payload['done']:
            payload['reply'] = item.get('reply', '')
            payload['error'] = item.get('error', '')
            payload['search_links'] = item.get('search_links', []) or []
        return jsonify(payload)
    except Exception as e:
        return jsonify({'error': '查询问答任务失败: %s' % str(e)}), 500


@bp.route('/api/team/ask_feedback/<ask_id>', methods=['POST'])
def team_ask_feedback(ask_id):
    """提交直连问答质量反馈（评分+建议），并写入团队记忆。"""
    if not re.fullmatch(r'[a-f0-9]{8,32}', str(ask_id or '')):
        return jsonify({'error': 'ask_id 无效'}), 400

    item = _get_ask_task(ask_id)
    if not item:
        return jsonify({'error': '问答任务不存在或已过期'}), 404
    if not item.get('done'):
        return jsonify({'error': '问答尚未完成，暂不可评分'}), 409

    data = request.json or {}
    try:
        score = int(data.get('score', 0))
    except Exception:
        score = -1
    if score < 0 or score > 10:
        return jsonify({'error': '评分需在 0-10 之间'}), 400

    suggestion = str(data.get('suggestion', '') or '').strip()
    if len(suggestion) > 4000:
        suggestion = suggestion[:4000]

    from AlphaFin.ai_team.core.memory import (
        save_user_feedback,
        AgentMemory,
        OUTCOME_SUCCESS,
        OUTCOME_FAILURE,
        OUTCOME_OBS,
    )

    participants = item.get('participants') or [item.get('agent_id') or 'director']
    participants = [p for p in participants if p]
    if not participants:
        participants = ['director']

    record = save_user_feedback(
        ask_id=ask_id,
        session_id=item.get('session_id') or '',
        question=item.get('question') or '',
        answer=item.get('reply') or '',
        workflow_mode=item.get('workflow_mode') or '',
        workflow_name=item.get('workflow_name') or '',
        participants=participants,
        score=score,
        suggestion=suggestion
    )

    if score >= 8:
        outcome = OUTCOME_SUCCESS
    elif score <= 4:
        outcome = OUTCOME_FAILURE
    else:
        outcome = OUTCOME_OBS

    feedback_text = (
        '用户评分: %d/10\n'
        '建议: %s\n'
        '问题: %s\n'
        '工作流: %s'
    ) % (
        score,
        suggestion or '(无)',
        (item.get('question') or '')[:300],
        item.get('workflow_name') or (item.get('workflow_mode') or '-')
    )

    updated_agents = []
    for aid in participants:
        try:
            mem = AgentMemory(aid)
            mem.save_knowledge(
                category='user_feedback',
                subject='直连问答反馈',
                content=feedback_text,
                confidence=max(0.2, min(0.99, score / 10.0)),
                tier='warm',
                source_type='user_feedback',
                source_session=item.get('session_id') or '',
                pattern_key='user_feedback_quality',
                outcome=outcome,
                rule_text='根据用户反馈持续优化回答质量、简洁度与可解释性'
            )
            mem.save_reflection(
                session_id=item.get('session_id') or '',
                workflow='user_ask_feedback',
                task=item.get('question') or '',
                reply=item.get('reply') or '',
                reflection=feedback_text[:1800]
            )
            updated_agents.append(aid)
        except Exception:
            continue

    return jsonify({
        'success': True,
        'record': record,
        'updated_agents': updated_agents,
    })


@bp.route('/api/team/config', methods=['POST'])
def team_config():
    """更新调度配置"""
    from AlphaFin.ai_team.core.orchestrator import orchestrator
    data = request.json or {}

    if 'manual_only' in data:
        orchestrator.set_manual_only(data.get('manual_only'))
    if 'interval' in data:
        orchestrator.set_interval(int(data['interval']))
    if 'paused' in data:
        if data['paused']:
            orchestrator.pause()
        else:
            orchestrator.resume()
    if 'idle_enabled' in data or 'idle_interval' in data:
        orchestrator.set_idle(
            enabled=data.get('idle_enabled') if 'idle_enabled' in data else None,
            interval=data.get('idle_interval') if 'idle_interval' in data else None,
        )
    if 'office_chat_enabled' in data or 'office_chat_interval' in data:
        orchestrator.set_office_chat(
            enabled=data.get('office_chat_enabled') if 'office_chat_enabled' in data else None,
            interval=data.get('office_chat_interval') if 'office_chat_interval' in data else None,
        )

    return jsonify(orchestrator.get_state())


@bp.route('/api/team/qwen_diagnose')
def team_qwen_diagnose():
    """
    诊断团队模块 Qwen 连通性：
    - DNS 解析是否正常
    - 主模块 key（对照组）是否可调用
    - 各智能体 key 是否可调用
    """
    from AlphaFin.ai_team.config import (
        AGENT_API_KEYS, QWEN_BASE_URL, QWEN_MODEL, QWEN_FALLBACK_MODEL
    )
    from AlphaFin.config import QWEN_API_KEY as MAIN_QWEN_API_KEY

    run_call = str(request.args.get('run_call', '1')).lower() not in ('0', 'false', 'no')
    only_agent = (request.args.get('agent_id') or '').strip()
    timeout = request.args.get('timeout', type=int) or 12
    timeout = max(5, min(timeout, 30))

    url = QWEN_BASE_URL.rstrip('/') + '/chat/completions'
    host = QWEN_BASE_URL.replace('https://', '').replace('http://', '').split('/')[0]

    result = {
        'base_url': QWEN_BASE_URL,
        'endpoint': url,
        'model': QWEN_MODEL,
        'fallback_model': QWEN_FALLBACK_MODEL,
        'dns': {'host': host, 'ok': False, 'ip': '', 'error': ''},
        'run_call': run_call,
        'timeout': timeout,
        'checks': []
    }

    try:
        ip = socket.getaddrinfo(host, 443)[0][4][0]
        result['dns'].update({'ok': True, 'ip': ip, 'error': ''})
    except Exception as e:
        result['dns'].update({'ok': False, 'ip': '', 'error': str(e)})

    targets = [('main_control', MAIN_QWEN_API_KEY)]
    for aid, key in AGENT_API_KEYS.items():
        if only_agent and aid != only_agent:
            continue
        targets.append((aid, key))

    if not run_call:
        for aid, key in targets:
            result['checks'].append({
                'agent_id': aid,
                'key_prefix': (key or '')[:8],
                'ok': None,
                'status_code': None,
                'error': 'run_call=0, 仅返回静态检查'
            })
        return jsonify(result)

    if not result['dns']['ok']:
        for aid, key in targets:
            result['checks'].append({
                'agent_id': aid,
                'key_prefix': (key or '')[:8],
                'ok': False,
                'status_code': None,
                'error': 'DNS 解析失败: %s' % result['dns']['error']
            })
        return jsonify(result), 503

    for aid, key in targets:
        row = {
            'agent_id': aid,
            'key_prefix': (key or '')[:8],
            'ok': False,
            'status_code': None,
            'model_used': QWEN_MODEL,
            'error': '',
        }
        if not key:
            row['error'] = 'API key 为空'
            result['checks'].append(row)
            continue

        headers = {
            'Authorization': 'Bearer ' + key,
            'Content-Type': 'application/json',
        }
        payload = {
            'model': QWEN_MODEL,
            'messages': [{'role': 'user', 'content': '请仅回复: OK'}],
            'temperature': 0,
        }
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
            row['status_code'] = resp.status_code
            if 200 <= resp.status_code < 300:
                row['ok'] = True
                result['checks'].append(row)
                continue

            err_text = (resp.text or '').replace('\n', ' ')[:260]
            row['error'] = err_text or ('HTTP %d' % resp.status_code)

            # 模型不可用时尝试降级（避免误判为 key 故障）
            if (
                resp.status_code in (400, 404)
                and QWEN_FALLBACK_MODEL
                and QWEN_FALLBACK_MODEL != QWEN_MODEL
            ):
                payload_fb = dict(payload)
                payload_fb['model'] = QWEN_FALLBACK_MODEL
                resp_fb = requests.post(url, json=payload_fb, headers=headers, timeout=timeout)
                row['status_code_fallback'] = resp_fb.status_code
                row['model_used'] = QWEN_FALLBACK_MODEL if 200 <= resp_fb.status_code < 300 else QWEN_MODEL
                if 200 <= resp_fb.status_code < 300:
                    row['ok'] = True
                    row['error'] = '主模型不可用，fallback 成功'
                else:
                    row['error_fallback'] = (resp_fb.text or '').replace('\n', ' ')[:260]
        except Exception as e:
            row['error'] = str(e)

        result['checks'].append(row)

    return jsonify(result)


@bp.route('/api/team/idle/run', methods=['POST'])
def team_idle_run():
    """手动触发一轮闲时学习（只读，不交易）"""
    from AlphaFin.ai_team.core.orchestrator import orchestrator
    from AlphaFin.ai_team.core.agent_registry import has_active_workflow
    if has_active_workflow({'user_ask'}):
        return jsonify({'error': '当前有直连问答正在进行，请先等待问答完成或手动停止'}), 409
    if orchestrator.current_session:
        return jsonify({'error': '当前已有任务在执行，请稍后重试'}), 409
    data = request.json or {}
    theme = data.get('theme')
    session = orchestrator.trigger_idle_now(theme=theme)
    return jsonify({'message': '闲时学习已启动', 'session': session, 'theme': theme})


@bp.route('/api/team/office_chat/run', methods=['POST'])
def team_office_chat_run():
    """手动触发一轮同事闲聊（非交易，仅团队沟通）"""
    from AlphaFin.ai_team.core.orchestrator import orchestrator
    from AlphaFin.ai_team.core.agent_registry import has_active_workflow
    if has_active_workflow({'user_ask'}):
        return jsonify({'error': '当前有直连问答正在进行，请先等待问答完成或手动停止'}), 409
    if orchestrator.current_session:
        return jsonify({'error': '当前已有任务在执行，请稍后重试'}), 409
    data = request.json or {}
    topic = data.get('topic')
    session = orchestrator.trigger_office_chat_now(topic=topic)
    if not session:
        return jsonify({'error': '当前投资任务正在执行，暂不启动同事闲聊'}), 409
    return jsonify({'message': '同事闲聊已启动', 'session': session, 'topic': topic})


@bp.route('/api/team/skills')
def team_skills():
    """获取智能体创建的技能列表"""
    import os
    from AlphaFin.ai_team.config import SKILLS_DIR
    manifest_path = os.path.join(SKILLS_DIR, 'skills_manifest.json')
    if os.path.exists(manifest_path):
        with open(manifest_path, 'r', encoding='utf-8') as f:
            return jsonify(json.load(f))
    return jsonify({'skills': [], 'version': '1.0'})


@bp.route('/api/team/skills/<skill_id>/approve', methods=['POST'])
def team_approve_skill(skill_id):
    """审核技能"""
    import os
    from AlphaFin.ai_team.config import SKILLS_DIR
    manifest_path = os.path.join(SKILLS_DIR, 'skills_manifest.json')
    if not os.path.exists(manifest_path):
        return jsonify({'error': '技能清单不存在'}), 404

    with open(manifest_path, 'r', encoding='utf-8') as f:
        manifest = json.load(f)

    for skill in manifest.get('skills', []):
        if skill.get('id') == skill_id:
            skill['approved'] = True
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, ensure_ascii=False, indent=2)
            return jsonify({'message': '技能已批准', 'skill_id': skill_id})

    return jsonify({'error': '技能不存在'}), 404


@bp.route('/api/team/tools_audit/catalog')
def team_tools_audit_catalog():
    """工具/Skill 透明化目录：用于前端一键展示。"""
    from AlphaFin.ai_team.config import AGENT_META

    tools, matrix = _build_tools_catalog()
    skills, manifest = _build_skills_catalog()

    summary = {
        'tool_count': len(tools),
        'skill_count': len(skills),
        'skill_approved_count': len([s for s in skills if s.get('approved')]),
        'skill_pending_count': len([s for s in skills if not s.get('approved')]),
        'high_risk_tools': len([t for t in tools if t.get('risk_level') == 'high']),
        'high_risk_skills': len([s for s in skills if s.get('risk_level') == 'high']),
        'generated_at': int(time.time()),
        'manifest_path': manifest.get('manifest_path', ''),
    }

    return jsonify({
        'summary': summary,
        'tools': tools,
        'skills': skills,
        'agent_tool_matrix': matrix,
        'agent_names': {aid: AGENT_META.get(aid, {}).get('name', aid) for aid in AGENT_META.keys()},
    })


@bp.route('/api/team/tools_audit/source')
def team_tools_audit_source():
    """查看指定工具/Skill 的底层源码。"""
    target_type = str(request.args.get('target_type', 'tool')).strip().lower()
    target_id = str(request.args.get('target_id', '')).strip()
    if not target_id:
        return jsonify({'error': '缺少 target_id'}), 400

    if target_type == 'tool':
        tools, _ = _build_tools_catalog()
        tool_item = None
        for t in tools:
            if t.get('name') == target_id:
                tool_item = t
                break
        if not tool_item:
            return jsonify({'error': '工具不存在: %s' % target_id}), 404
        src = _read_tool_source(target_id)
        if not src.get('ok'):
            return jsonify({'error': src.get('error', '读取工具源码失败')}), 500
        # web_search 在工程内是“工具入口 + 联网核心函数”两段调用链，
        # 这里一起展示，避免用户只看到入口包装函数。
        if target_id == 'web_search':
            try:
                from AlphaFin.services.ai_chat_service import _collect_web_search_snapshot
                chain_lines, chain_start = inspect.getsourcelines(_collect_web_search_snapshot)
                chain_code = ''.join(chain_lines)
                chain_path = inspect.getsourcefile(_collect_web_search_snapshot) or inspect.getfile(_collect_web_search_snapshot) or ''
                merged_code = (
                    '# ===== [工具入口] %s =====\n%s\n\n'
                    '# ===== [联网核心实现] _collect_web_search_snapshot =====\n%s'
                ) % (str(src.get('handler_name') or '_exec_web_search'), str(src.get('code') or ''), chain_code)
                clipped, truncated = _clip_text(merged_code, limit=90000)
                src['code'] = clipped
                src['truncated'] = bool(src.get('truncated')) or bool(truncated)
                src['path'] = (
                    str(src.get('path') or '-') +
                    '\n' +
                    str(chain_path or '-')
                )
                src['line_end'] = int(chain_start or src.get('line_end') or 1) + len(chain_lines) - 1
            except Exception:
                pass
        return jsonify({
            'target_type': 'tool',
            'target_id': target_id,
            'meta': tool_item,
            'path': src.get('path', ''),
            'handler_name': src.get('handler_name', ''),
            'line_start': src.get('line_start', 1),
            'line_end': src.get('line_end', 1),
            'truncated': bool(src.get('truncated')),
            'code': src.get('code', ''),
        })

    if target_type == 'skill':
        skills, _ = _build_skills_catalog()
        skill_item = None
        for s in skills:
            if str(s.get('id')) == target_id:
                skill_item = s
                break
        if not skill_item:
            return jsonify({'error': 'Skill不存在: %s' % target_id}), 404
        abs_path = skill_item.get('path', '')
        if not abs_path or not os.path.exists(abs_path):
            return jsonify({'error': 'Skill源码文件不存在'}), 404
        with open(abs_path, 'r', encoding='utf-8') as f:
            code = f.read()
        clipped, truncated = _clip_text(code, limit=60000)
        line_count = len(code.splitlines())
        return jsonify({
            'target_type': 'skill',
            'target_id': target_id,
            'meta': skill_item,
            'path': abs_path,
            'handler_name': '',
            'line_start': 1,
            'line_end': line_count,
            'truncated': truncated,
            'code': clipped,
        })

    return jsonify({'error': '不支持的 target_type，仅支持 tool/skill'}), 400


@bp.route('/api/team/tools_audit/review', methods=['POST'])
def team_tools_audit_review():
    """
    风控官按需审查：
    - 单个工具: {target_type:'tool', target_id:'get_kline'}
    - 单个Skill: {target_type:'skill', target_id:'skill_xxx'}
    - 全量: {target_type:'all'}
    """
    data = request.json or {}
    target_type = str(data.get('target_type', 'tool')).strip().lower()
    target_id = str(data.get('target_id', '')).strip()

    if target_type == 'all':
        tools, _ = _build_tools_catalog()
        skills, _ = _build_skills_catalog()

        audited_items = []
        summary_lines = []
        for t in tools:
            src = _read_tool_source(t.get('name', ''))
            static_report = _static_tool_audit(t, src)
            audited_items.append({
                'target_type': 'tool',
                'target_id': t.get('name', ''),
                'name': t.get('name', ''),
                'risk_level': t.get('risk_level', 'low'),
                'static_audit': static_report,
            })
            summary_lines.append(
                '[TOOL] %s | 风险=%s | 安全=%s | 可靠=%s | 关键问题=%s' % (
                    t.get('name', ''),
                    t.get('risk_level', 'low'),
                    static_report.get('security_score', '-'),
                    static_report.get('reliability_score', '-'),
                    ('；'.join(static_report.get('issues', [])[:2]) or '无')
                )
            )

        for s in skills:
            source_text = ''
            abs_path = s.get('path', '')
            if abs_path and os.path.exists(abs_path):
                with open(abs_path, 'r', encoding='utf-8') as f:
                    source_text = f.read()
            static_report = _static_skill_audit(s, source_text)
            audited_items.append({
                'target_type': 'skill',
                'target_id': s.get('id', ''),
                'name': s.get('name', ''),
                'risk_level': s.get('risk_level', 'low'),
                'static_audit': static_report,
            })
            summary_lines.append(
                '[SKILL] %s(%s) | 风险=%s | 安全=%s | 可靠=%s | 关键问题=%s' % (
                    s.get('name', ''),
                    s.get('id', ''),
                    s.get('risk_level', 'low'),
                    static_report.get('security_score', '-'),
                    static_report.get('reliability_score', '-'),
                    ('；'.join(static_report.get('issues', [])[:2]) or '无')
                )
            )

        llm_payload = '\n'.join(summary_lines)
        llm_result = _risk_officer_audit_llm('全部工具与Skill审查', llm_payload)

        min_security = 100
        min_reliability = 100
        high_risk_count = 0
        for item in audited_items:
            sr = item.get('static_audit') or {}
            min_security = min(min_security, int(sr.get('security_score', 100)))
            min_reliability = min(min_reliability, int(sr.get('reliability_score', 100)))
            if item.get('risk_level') == 'high':
                high_risk_count += 1

        return jsonify({
            'success': True,
            'target_type': 'all',
            'target_id': '',
            'summary': {
                'total_items': len(audited_items),
                'high_risk_items': high_risk_count,
                'min_security_score': min_security if audited_items else 100,
                'min_reliability_score': min_reliability if audited_items else 100,
            },
            'items': audited_items,
            'llm_audit': llm_result,
        })

    if target_type == 'tool':
        if not target_id:
            return jsonify({'error': '缺少 target_id'}), 400
        tools, _ = _build_tools_catalog()
        tool_item = None
        for t in tools:
            if t.get('name') == target_id:
                tool_item = t
                break
        if not tool_item:
            return jsonify({'error': '工具不存在: %s' % target_id}), 404
        src = _read_tool_source(target_id)
        if not src.get('ok'):
            return jsonify({'error': src.get('error', '读取工具源码失败')}), 500
        static_report = _static_tool_audit(tool_item, src)
        src_preview, _ = _clip_text(src.get('code', ''), limit=9000)
        llm_payload = (
            '对象类型: 工具\n'
            '名称: %s\n'
            '处理函数: %s\n'
            '风险等级: %s\n'
            '描述: %s\n'
            '参数: %s\n'
            '静态审查: %s\n\n'
            '源码预览:\n%s'
        ) % (
            tool_item.get('name', ''),
            src.get('handler_name', ''),
            tool_item.get('risk_level', 'low'),
            tool_item.get('description', ''),
            ', '.join(tool_item.get('parameter_keys', [])),
            json.dumps(static_report, ensure_ascii=False),
            src_preview,
        )
        llm_result = _risk_officer_audit_llm('工具审查: %s' % target_id, llm_payload)
        return jsonify({
            'success': True,
            'target_type': 'tool',
            'target_id': target_id,
            'meta': tool_item,
            'static_audit': static_report,
            'llm_audit': llm_result,
        })

    if target_type == 'skill':
        if not target_id:
            return jsonify({'error': '缺少 target_id'}), 400
        skills, _ = _build_skills_catalog()
        skill_item = None
        for s in skills:
            if str(s.get('id')) == target_id:
                skill_item = s
                break
        if not skill_item:
            return jsonify({'error': 'Skill不存在: %s' % target_id}), 404

        source_text = ''
        abs_path = skill_item.get('path', '')
        if abs_path and os.path.exists(abs_path):
            with open(abs_path, 'r', encoding='utf-8') as f:
                source_text = f.read()
        static_report = _static_skill_audit(skill_item, source_text)
        src_preview, _ = _clip_text(source_text, limit=9000)
        llm_payload = (
            '对象类型: Skill\n'
            'ID: %s\n'
            '名称: %s\n'
            '分类: %s\n'
            '创建者: %s\n'
            '是否批准: %s\n'
            '风险等级: %s\n'
            '描述: %s\n'
            '静态审查: %s\n\n'
            '源码预览:\n%s'
        ) % (
            skill_item.get('id', ''),
            skill_item.get('name', ''),
            skill_item.get('category', ''),
            skill_item.get('creator', ''),
            '是' if skill_item.get('approved') else '否',
            skill_item.get('risk_level', 'low'),
            skill_item.get('description', ''),
            json.dumps(static_report, ensure_ascii=False),
            src_preview or '(空)',
        )
        llm_result = _risk_officer_audit_llm('Skill审查: %s' % target_id, llm_payload)
        return jsonify({
            'success': True,
            'target_type': 'skill',
            'target_id': target_id,
            'meta': skill_item,
            'static_audit': static_report,
            'llm_audit': llm_result,
        })

    return jsonify({'error': '不支持的 target_type，仅支持 tool/skill/all'}), 400


# ──────────────── 投资组合 API ────────────────

@bp.route('/api/team/portfolio/init', methods=['POST'])
def portfolio_init():
    """初始化投资组合"""
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    data = request.json or {}
    mode = data.get('mode', 'free')
    target_code = data.get('target_code', '')
    initial_capital = data.get('initial_capital')
    result = pm.init_portfolio(mode=mode, target_code=target_code,
                               initial_capital=initial_capital)
    return jsonify(result)


@bp.route('/api/team/portfolio/config', methods=['GET', 'POST'])
def portfolio_config():
    """获取或更新投资配置"""
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    if request.method == 'GET':
        config = pm.get_config()
        if not config:
            return jsonify({'initialized': False})
        return jsonify(config)
    else:
        data = request.json or {}
        from AlphaFin.ai_team.core.portfolio_scheduler import portfolio_scheduler
        result = pm.update_config(**data)
        if 'auto_run' in data:
            # DB 已由 update_config 持久化，这里只同步运行态
            portfolio_scheduler.set_auto(bool(data['auto_run']), persist=False)
        if 'watch_enabled' in data or 'watch_interval' in data:
            portfolio_scheduler.set_watch(
                enabled=data.get('watch_enabled') if 'watch_enabled' in data else None,
                interval=data.get('watch_interval') if 'watch_interval' in data else None,
            )
        return jsonify(result or {'success': True})


@bp.route('/api/team/portfolio/status')
def portfolio_status():
    """获取投资组合状态"""
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    from AlphaFin.ai_team.core.portfolio_scheduler import portfolio_scheduler
    status = pm.get_portfolio_status()
    # 兜底同步：避免进程重启后调度器内存态与数据库配置不一致
    if status.get('initialized') and 'auto_run' in status:
        desired = bool(status.get('auto_run'))
        if portfolio_scheduler.auto_enabled != desired:
            portfolio_scheduler.set_auto(desired, persist=False)
    status['scheduler'] = portfolio_scheduler.get_state()
    status['market_data'] = pm.get_market_data_status()
    return jsonify(status)


@bp.route('/api/team/portfolio/nav')
def portfolio_nav():
    """获取净值历史"""
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    days = request.args.get('days', type=int)
    nav_history = pm.get_nav_history(days=days)
    return jsonify(nav_history)


@bp.route('/api/team/portfolio/trades')
def portfolio_trades():
    """获取交易记录"""
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    limit = request.args.get('limit', 50, type=int)
    trades = pm.get_trade_history(limit=limit)
    return jsonify(trades)


@bp.route('/api/team/portfolio/stats')
def portfolio_stats():
    """获取绩效统计"""
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    stats = pm.get_performance_stats()
    return jsonify(stats)


@bp.route('/api/team/portfolio/signals')
def portfolio_signals():
    """获取交易信号"""
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    status_filter = request.args.get('status')
    if status_filter:
        signals = pm.get_pending_signals(status=status_filter)
    else:
        signals = pm.get_all_signals(limit=50)
    return jsonify(signals)


@bp.route('/api/team/portfolio/signals/<int:signal_id>/review', methods=['POST'])
def portfolio_signal_review(signal_id):
    """人工审核交易信号"""
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    data = request.json or {}
    approved = data.get('approved', False)
    review_text = data.get('review_text', '人工审核')
    review_type = data.get('type', 'risk')
    if review_type == 'director':
        result = pm.review_signal_director(signal_id, approved, review_text, 'director')
    else:
        result = pm.review_signal_risk(signal_id, approved, review_text, 'risk')
    return jsonify(result)


@bp.route('/api/team/portfolio/compensation')
def portfolio_compensation():
    """获取智能体薪资汇总"""
    from AlphaFin.ai_team.core.portfolio_manager import portfolio_manager as pm
    from AlphaFin.ai_team.config import AGENT_META
    summary = pm.get_compensation_summary()
    agents = []
    for agent_id, meta in AGENT_META.items():
        vals = summary.get(agent_id, {}) if isinstance(summary, dict) else {}
        meta = AGENT_META.get(agent_id, {})
        agents.append({
            'agent_id': agent_id,
            'name': meta.get('name', agent_id),
            'total_salary': vals.get('salary', 0),
            'total_bonus': vals.get('bonus', 0),
            'total_penalty': vals.get('penalty', 0),
            'net': vals.get('net', 0),
        })
    return jsonify({'agents': agents, 'summary': summary})


@bp.route('/api/team/portfolio/run', methods=['POST'])
def portfolio_run():
    """手动触发一轮投资决策"""
    from AlphaFin.ai_team.core.portfolio_scheduler import portfolio_scheduler
    from AlphaFin.ai_team.core.orchestrator import orchestrator
    from AlphaFin.ai_team.core.agent_registry import has_active_workflow
    from AlphaFin.ai_team.config import PORTFOLIO_MANUAL_DEFAULT_TIMEOUT
    if has_active_workflow({'user_ask'}):
        return jsonify({'error': '当前有直连问答正在进行，请先等待问答完成或手动停止'}), 409
    if orchestrator.current_session:
        return jsonify({'error': '当前研究任务正在运行中，请先等待完成或手动停止'}), 409
    if portfolio_scheduler.current_session:
        return jsonify({'error': '投资周期正在运行中'}), 409
    data = request.json or {}
    trade_date = data.get('trade_date')
    time_limit_minutes = _normalize_timeout_minutes(
        data.get('time_limit_minutes'),
        max(1, int(PORTFOLIO_MANUAL_DEFAULT_TIMEOUT / 60))
    )

    import threading
    t = threading.Thread(
        target=portfolio_scheduler.run_investment_cycle,
        args=(trade_date, time_limit_minutes * 60), daemon=True
    )
    t.start()
    return jsonify({
        'message': '投资周期已启动',
        'trade_date': trade_date,
        'time_limit_minutes': time_limit_minutes,
    })


@bp.route('/api/team/portfolio/watch/run', methods=['POST'])
def portfolio_watch_run():
    """手动触发一轮盘中盯盘"""
    from AlphaFin.ai_team.core.portfolio_scheduler import portfolio_scheduler
    from AlphaFin.ai_team.core.orchestrator import orchestrator
    from AlphaFin.ai_team.core.agent_registry import has_active_workflow
    from AlphaFin.ai_team.config import PORTFOLIO_WATCH_MANUAL_DEFAULT_TIMEOUT
    if has_active_workflow({'user_ask'}):
        return jsonify({'error': '当前有直连问答正在进行，请先等待问答完成或手动停止'}), 409
    if orchestrator.current_session:
        return jsonify({'error': '当前研究任务正在运行中，请先等待完成或手动停止'}), 409
    if portfolio_scheduler.current_session:
        return jsonify({'error': '当前已有投资任务在运行中'}), 409
    data = request.json or {}
    trade_date = data.get('trade_date')
    time_limit_minutes = _normalize_timeout_minutes(
        data.get('time_limit_minutes'),
        max(1, int(PORTFOLIO_WATCH_MANUAL_DEFAULT_TIMEOUT / 60))
    )

    import threading
    t = threading.Thread(
        target=portfolio_scheduler.run_market_watch_cycle,
        args=(trade_date, time_limit_minutes * 60), daemon=True
    )
    t.start()
    return jsonify({
        'message': '盘中盯盘已启动',
        'trade_date': trade_date,
        'time_limit_minutes': time_limit_minutes,
    })


@bp.route('/api/team/session/<session_id>/overtime', methods=['POST'])
def team_session_overtime_decision(session_id):
    """处理任务超时后的用户决策：继续等待或停止任务（兼容 summarize）。"""
    from AlphaFin.ai_team.core.session_control import (
        resolve_session_overtime_decision,
        get_session_timing,
        get_session_progress,
        get_session_overtime_state,
    )
    sid = str(session_id or '').strip()
    if not sid:
        return jsonify({'error': 'session_id 不能为空'}), 400
    data = request.json or {}
    decision = str(data.get('decision') or '').strip().lower()
    extend_minutes = _normalize_timeout_minutes(data.get('extend_minutes', 5), 5, min_minutes=1, max_minutes=30)
    try:
        overtime = resolve_session_overtime_decision(
            sid,
            decision=decision,
            extend_seconds=extend_minutes * 60
        )
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    return jsonify({
        'success': True,
        'session_id': sid,
        'decision': overtime.get('decision', ''),
        'session_timing': get_session_timing(sid),
        'session_progress': get_session_progress(sid),
        'session_overtime': get_session_overtime_state(sid),
    })


@bp.route('/api/team/session/<session_id>/overtime/request', methods=['POST'])
def team_session_overtime_request(session_id):
    """主动唤起超时决策（前端兜底触发）。"""
    from AlphaFin.ai_team.core.session_control import (
        request_session_overtime_decision,
        get_session_timing,
        get_session_progress,
        get_session_overtime_state,
        set_session_progress,
    )
    sid = str(session_id or '').strip()
    if not sid:
        return jsonify({'error': 'session_id 不能为空'}), 400

    data = request.json or {}
    current_timing = get_session_timing(sid)
    current_progress = get_session_progress(sid)
    workflow = str(
        data.get('workflow') or
        current_progress.get('workflow') or
        current_timing.get('workflow') or
        ''
    ).strip()
    title = str(
        data.get('title') or
        current_progress.get('title') or
        current_timing.get('title') or
        ''
    ).strip()
    message = str(
        data.get('message') or
        '任务已达到设定时限，请选择继续等待，或立即停止任务。'
    ).strip()
    extend_minutes = _normalize_timeout_minutes(
        data.get('extend_minutes', 5),
        5,
        min_minutes=1,
        max_minutes=30
    )

    overtime = request_session_overtime_decision(
        session_id=sid,
        workflow=workflow,
        title=title,
        message=message,
        default_extend_seconds=extend_minutes * 60
    )

    if current_progress.get('active') and overtime.get('waiting'):
        set_session_progress(
            session_id=sid,
            workflow=workflow or current_progress.get('workflow') or '',
            title=title or current_progress.get('title') or '',
            steps=(current_progress.get('steps') or []),
            current_index=current_progress.get('current_index') or 0,
            current_step=current_progress.get('current_step') or '等待用户决策',
            detail='已达到时限，等待用户选择继续等待或立即停止。',
            state='waiting_user',
            actor=current_progress.get('actor') or 'system',
        )

    return jsonify({
        'success': True,
        'session_id': sid,
        'session_timing': get_session_timing(sid),
        'session_progress': get_session_progress(sid),
        'session_overtime': get_session_overtime_state(sid),
    })
