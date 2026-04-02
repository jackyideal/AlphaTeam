"""
独立 AI 智能分析服务 - 支持联网搜索的 Qwen 对话
默认使用通义千问 qwen3.5-plus（纯文本，支持联网搜索），失败自动降级

联网检索统一走 Kimi `$web_search` 工具调用链，再将检索结果注入分析上下文。
"""
import datetime
import json
import os
import re
import uuid
import sqlite3
import csv
import time
from urllib.parse import urlparse

import requests
try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None

from AlphaFin.config import BASE_DIR, DB_ROOT, QWEN_API_KEY, QWEN_BASE_URL, TUSHARE_TOKEN
from AlphaFin import config as app_config
from AlphaFin.services.prompt_config_service import get_prompt
from AlphaFin.services.model_config_service import (
    normalize_model_name,
    get_module_model,
)

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

# 文本问答模型（可通过环境变量覆盖）
AI_CHAT_MODEL = os.getenv('ALPHAFIN_AI_CHAT_MODEL', 'qwen3.5-plus')
AI_CHAT_FALLBACK_MODEL = os.getenv('ALPHAFIN_AI_CHAT_FALLBACK_MODEL', 'qwen-plus')
AI_CHAT_FILE_MODEL = os.getenv('ALPHAFIN_AI_CHAT_FILE_MODEL', 'qwen-long')
AI_CHAT_TIMEOUT_SECONDS = int(os.getenv('ALPHAFIN_AI_CHAT_TIMEOUT_SECONDS', '120'))
AI_WEB_SEARCH_TIMEOUT_SECONDS = int(os.getenv('ALPHAFIN_WEB_SEARCH_TIMEOUT_SECONDS', '45'))
AI_WEB_SEARCH_RETRIES = max(1, int(os.getenv('ALPHAFIN_WEB_SEARCH_RETRIES', '2')))
AI_WEB_SEARCH_RETRY_BACKOFF = max(0.0, float(os.getenv('ALPHAFIN_WEB_SEARCH_RETRY_BACKOFF', '0.8')))
AI_WEB_SEARCH_MAX_ITEMS = min(8, max(3, int(os.getenv('ALPHAFIN_WEB_SEARCH_MAX_ITEMS', '5'))))
AI_WEB_SEARCH_FALLBACK_MODEL = os.getenv('ALPHAFIN_WEB_SEARCH_FALLBACK_MODEL', 'qwen-plus')
AI_WEB_SEARCH_MODEL = os.getenv('ALPHAFIN_WEB_SEARCH_MODEL', 'qwen-plus')
AI_WEB_SEARCH_RESPONSES_MODEL = os.getenv('ALPHAFIN_WEB_SEARCH_RESPONSES_MODEL', 'qwen3-max-2026-01-23')


def _env_bool(name, default=True):
    raw = str(os.getenv(name, '1' if default else '0') or '').strip().lower()
    if raw in ('1', 'true', 'yes', 'y', 'on'):
        return True
    if raw in ('0', 'false', 'no', 'n', 'off'):
        return False
    return bool(default)


AI_WEB_SEARCH_USE_RESPONSES = _env_bool('ALPHAFIN_WEB_SEARCH_USE_RESPONSES', True)
AI_WEB_SEARCH_USE_EXTRACTOR = _env_bool('ALPHAFIN_WEB_SEARCH_USE_EXTRACTOR', True)
_KIMI_CONFIG_BASE_URL = str(getattr(app_config, 'MOONSHOT_BASE_URL', '') or '').strip()
_KIMI_CONFIG_API_KEY = str(getattr(app_config, 'MOONSHOT_API_KEY', '') or '').strip()
_KIMI_CONFIG_MODEL = str(getattr(app_config, 'MOONSHOT_MODEL', '') or '').strip()

KIMI_WEB_SEARCH_BASE_URL = os.getenv(
    'ALPHAFIN_KIMI_BASE_URL',
    os.getenv('MOONSHOT_BASE_URL', _KIMI_CONFIG_BASE_URL or 'https://api.moonshot.cn/v1')
)
KIMI_WEB_SEARCH_API_KEY = (
    os.getenv('ALPHAFIN_KIMI_API_KEY')
    or os.getenv('MOONSHOT_API_KEY')
    or _KIMI_CONFIG_API_KEY
    or ''
)
KIMI_WEB_SEARCH_MODEL = os.getenv('ALPHAFIN_KIMI_WEB_SEARCH_MODEL', _KIMI_CONFIG_MODEL or 'kimi-k2.5')
KIMI_WEB_SEARCH_MAX_TURNS = max(2, int(os.getenv('ALPHAFIN_KIMI_WEB_SEARCH_MAX_TURNS', '8')))

SYSTEM_PROMPT = """你是 AlphaFin AI 分析助手，一位专业的中国A股市场分析师。你拥有联网搜索能力，可以获取最新的市场数据和新闻。

你的能力包括：
1. 个股基本面分析（财务数据、行业地位、竞争优势）
2. 技术面分析（K线形态、均线系统、量价关系）
3. 宏观经济分析（货币政策、行业政策、经济数据）
4. 市场热点追踪（板块轮动、资金流向、主题投资）
5. 投资策略建议（仓位管理、风险控制、买卖时机）

请用中文回答，语言简洁专业。如果用户问到需要最新数据的问题，请主动搜索获取最新信息。
回答时请注明信息来源和时效性。"""

GROUNDED_SYSTEM_PROMPT = """你是 AlphaFin AI 智能分析助手（A股）。

你必须遵守以下规则：
1. 优先使用“内部实时数据快照”作答，禁止编造价格、时间和事件。
2. 若内部数据与联网搜索冲突，先陈述冲突并优先采用内部数据（同时注明两者来源和时间）。
3. 默认输出应简洁，只展示：核心结论、关键依据、风险提示。
4. “验证细节/推理过程/完整证据链”不要默认展开，保留到可回溯详情中。
5. 对“最新/实时/今天/盘中”问题，必须写明当前北京时间和数据来源字段。
6. 数据缺失时明确说“未获取到”，并说明如何补充验证。

回答风格：
- 结论先行，结构化、可解释、可验证、可回溯。
- 不给确定性投资承诺，强调风险控制。"""

AI_CHAT_PROMPT_MODULE = 'ai_chat'


def get_ai_chat_prompt_definitions():
    """返回 AI 智能分析可管理的系统提示词定义。"""
    return [
        {
            'key': 'direct',
            'name': 'AI智能分析（联网模式）',
            'description': '勾选“联网搜索”时使用',
            'default_prompt': SYSTEM_PROMPT,
        },
        {
            'key': 'grounded',
            'name': 'AI智能分析（证据链模式）',
            'description': '关闭“联网搜索”时使用',
            'default_prompt': GROUNDED_SYSTEM_PROMPT,
        },
    ]


def get_ai_chat_prompt_configs():
    """返回 AI 智能分析提示词（含当前生效值）。"""
    result = []
    for item in get_ai_chat_prompt_definitions():
        current_prompt = get_prompt(
            AI_CHAT_PROMPT_MODULE,
            item['key'],
            item['default_prompt']
        )
        row = dict(item)
        row['prompt'] = current_prompt
        row['is_overridden'] = current_prompt != item['default_prompt']
        result.append(row)
    return result

_TRACE_ROOT = os.path.join(BASE_DIR, 'AlphaFin', 'data', 'ai_chat_traces')
_PRO_CLIENT = None
_OPENAI_CLIENT = None
_KIMI_OPENAI_CLIENT = None
_STOCK_REF_CACHE = {
    'rows': [],
    'loaded_at': 0.0,
}
_MANUAL_STOCK_ALIASES = {
    '青松建化': '600425.SH',
}


def _build_model_candidates(preferred=None):
    models = []
    if preferred:
        for m in preferred:
            name = str(m or '').strip()
            if name and name not in models:
                models.append(name)
    for m in (AI_CHAT_MODEL, AI_CHAT_FALLBACK_MODEL):
        name = str(m or '').strip()
        if name and name not in models:
            models.append(name)
    if not models:
        models = ['qwen3.5-plus']
    return models


def _extract_content_text(message):
    if isinstance(message, str):
        return message.strip()
    if isinstance(message, list):
        chunks = []
        for item in message:
            if isinstance(item, dict):
                txt = item.get('text')
                if txt:
                    chunks.append(str(txt))
            elif isinstance(item, str):
                chunks.append(item)
        return '\n'.join(chunks).strip()
    return str(message or '').strip()


def _get_openai_client():
    global _OPENAI_CLIENT
    if _OPENAI_CLIENT is not None:
        return _OPENAI_CLIENT
    if OpenAI is None:
        return None
    try:
        _OPENAI_CLIENT = OpenAI(
            api_key=QWEN_API_KEY,
            base_url=QWEN_BASE_URL.rstrip('/')
        )
    except Exception:
        _OPENAI_CLIENT = None
    return _OPENAI_CLIENT


def _get_kimi_openai_client():
    global _KIMI_OPENAI_CLIENT
    if _KIMI_OPENAI_CLIENT is not None:
        return _KIMI_OPENAI_CLIENT
    if OpenAI is None:
        return None
    if not KIMI_WEB_SEARCH_API_KEY:
        return None
    try:
        _KIMI_OPENAI_CLIENT = OpenAI(
            api_key=KIMI_WEB_SEARCH_API_KEY,
            base_url=KIMI_WEB_SEARCH_BASE_URL.rstrip('/')
        )
    except Exception:
        _KIMI_OPENAI_CLIENT = None
    return _KIMI_OPENAI_CLIENT


def _call_qwen_chat(
        messages,
        temperature=0.2,
        enable_search=False,
        timeout=None,
        model_candidates=None,
        model_name=''
):
    """
    调用 Qwen 聊天接口，主模型失败时自动降级到 fallback 模型。
    返回: {'content': str, 'model': str, 'raw': dict}
    """
    if not QWEN_API_KEY:
        raise RuntimeError('请在 config.py 中设置 QWEN_API_KEY 后使用此功能。')

    url = QWEN_BASE_URL.rstrip('/') + '/chat/completions'
    headers = {
        'Authorization': 'Bearer ' + QWEN_API_KEY,
        'Content-Type': 'application/json',
    }
    t = timeout if timeout is not None else AI_CHAT_TIMEOUT_SECONDS
    preferred = list(model_candidates or [])
    if model_name:
        preferred.insert(0, model_name)
    models = _build_model_candidates(preferred=preferred)
    last_error = None

    for i, model_name in enumerate(models):
        payload = {
            'model': model_name,
            'messages': messages,
            'temperature': temperature,
        }
        if enable_search:
            payload['enable_search'] = True
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=t)
            resp.raise_for_status()
            data = resp.json()
            choices = data.get('choices') or []
            if not choices:
                raise ValueError('Qwen 返回空 choices')
            msg = choices[0].get('message') or {}
            content = _extract_content_text(msg.get('content'))
            return {
                'content': content,
                'model': model_name,
                'raw': data,
            }
        except requests.exceptions.Timeout as e:
            # 搜索场景下不同模型耗时差异明显，超时时继续尝试候选模型。
            last_error = e
            if i < len(models) - 1:
                continue
            raise
        except Exception as e:
            last_error = e
            if i < len(models) - 1:
                continue
            raise

    if last_error is not None:
        raise last_error
    raise RuntimeError('模型调用失败')


def _now_cn():
    if ZoneInfo:
        return datetime.datetime.now(ZoneInfo('Asia/Shanghai'))
    return datetime.datetime.now()


def _normalize_ts_code(raw_code):
    if not raw_code:
        return ''
    code = str(raw_code).strip().upper()
    if re.fullmatch(r'\d{6}', code):
        return code + ('.SH' if code.startswith(('5', '6', '9')) else '.SZ')
    if re.fullmatch(r'\d{6}\.(SH|SZ)', code):
        return code
    return ''


def _extract_stock_codes(text, limit=2):
    if not text:
        return []
    # 中文场景下 \b 会误判，这里改成数字前后非数字边界
    raw_hits = re.findall(r'(?<!\d)\d{6}(?:\.(?:SH|SZ))?(?!\d)', str(text).upper())
    codes = []
    for raw in raw_hits:
        code = _normalize_ts_code(raw)
        if code and code not in codes:
            codes.append(code)
        if len(codes) >= max(1, int(limit)):
            break
    return codes


def _extract_stock_name_candidates(text, limit=6):
    q = str(text or '')
    # 抽取中文连续片段作为候选（2-8字）
    chunks = re.findall(r'[\u4e00-\u9fff]{2,8}', q)
    stopwords = {
        '上证', '上证指数', '深证', '深证成指', '创业板', '沪深', '沪深300',
        '行情', '走势', '分析', '今天', '实时', '最新', '情况', '股票', '个股', '市场',
        '金融', '请问', '请帮', '一下', '看看', '当前', '现在', '消息', '快讯',
        '星期', '星期几', '周几', '日期', '时间', '几点', '是什么',
    }
    names = []
    for c in chunks:
        c = c.strip()
        if not c or c in stopwords:
            continue
        # 去掉尾部语义后缀，尽量抽出真实股票名
        changed = True
        while changed:
            changed = False
            for suf in ('金融', '股份', '集团', '走势', '行情', '分析', '情况'):
                if c.endswith(suf) and len(c) > len(suf) + 1:
                    c = c[:-len(suf)]
                    changed = True
        if c.endswith(('走势', '行情', '分析', '情况')):
            c = c[:-2]
        if len(c) < 2:
            continue
        if c not in names:
            names.append(c)
        if len(names) >= max(1, int(limit)):
            break
    return names


def _load_stock_reference(ttl_seconds=21600):
    """
    加载股票名称映射缓存（ts_code/symbol/name）。
    优先从 Tushare 获取，失败则返回旧缓存。
    """
    now_ts = _now_cn().timestamp()
    if _STOCK_REF_CACHE['rows'] and now_ts - _STOCK_REF_CACHE['loaded_at'] < ttl_seconds:
        return _STOCK_REF_CACHE['rows']

    # 1) 优先本地 nameschange.csv（避免网络抖动导致解析慢/失败）
    try:
        csv_path = os.path.join(DB_ROOT, 'data', 'nameschange.csv')
        if os.path.isfile(csv_path):
            by_code = {}
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for r in reader:
                    code = str(r.get('ts_code') or '').strip().upper()
                    name = str(r.get('name') or '').strip()
                    if not code or not name:
                        continue
                    end_date = str(r.get('end_date') or '').strip().lower()
                    active = (not end_date) or end_date in ('nan', 'none', 'null')
                    old = by_code.get(code)
                    if old is None:
                        by_code[code] = {'name': name, 'active': active}
                    elif active and not old.get('active'):
                        by_code[code] = {'name': name, 'active': True}

            local_rows = []
            for ts_code, meta in by_code.items():
                symbol = ts_code.split('.')[0]
                local_rows.append({'ts_code': ts_code, 'symbol': symbol, 'name': meta.get('name', '')})
            if local_rows:
                _STOCK_REF_CACHE['rows'] = local_rows
                _STOCK_REF_CACHE['loaded_at'] = now_ts
                return _STOCK_REF_CACHE['rows']
    except Exception:
        pass

    # 2) 本地缺失时再尝试 Tushare stock_basic
    pro = _get_pro()
    if pro is None:
        return _STOCK_REF_CACHE['rows']

    rows = []
    try:
        df = pro.stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code,symbol,name'
        )
        if df is not None and not df.empty:
            for _, r in df.iterrows():
                ts_code = str(r.get('ts_code') or '').strip().upper()
                symbol = str(r.get('symbol') or '').strip()
                name = str(r.get('name') or '').strip()
                if ts_code and name:
                    rows.append({'ts_code': ts_code, 'symbol': symbol, 'name': name})
    except Exception:
        rows = _STOCK_REF_CACHE['rows']

    if rows:
        _STOCK_REF_CACHE['rows'] = rows
        _STOCK_REF_CACHE['loaded_at'] = now_ts
    return _STOCK_REF_CACHE['rows']


def _get_local_latest_close(ts_code):
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            'SELECT close, trade_date FROM daily_kline WHERE ts_code=? ORDER BY trade_date DESC LIMIT 1',
            (ts_code,)
        ).fetchone()
        conn.close()
        if row and row[0] is not None:
            return {'close': float(row[0]), 'trade_date': str(row[1] or ''), 'source': 'local_latest_close'}
    except Exception:
        return None
    return None


def _resolve_stock_entities(text, limit=2):
    """
    从用户文本解析股票实体（支持代码和中文名）。
    返回: [{'ts_code': '600425.SH', 'name': '青松建化'}]
    """
    entities = []
    codes = _extract_stock_codes(text, limit=max(2, limit))
    ref_rows = _load_stock_reference()

    # 1) 先处理代码
    for code in codes:
        name = ''
        if ref_rows:
            for r in ref_rows:
                if r.get('ts_code') == code:
                    name = r.get('name', '')
                    break
        entities.append({'ts_code': code, 'name': name})
        if len(entities) >= max(1, int(limit)):
            return entities

    # 2) 再处理中文名
    candidates = _extract_stock_name_candidates(text, limit=8)
    # 2.1 手工别名兜底（用于高频关注标的）
    for c in candidates:
        alias_code = _MANUAL_STOCK_ALIASES.get(c)
        if alias_code and not any(e.get('ts_code') == alias_code for e in entities):
            entities.append({'ts_code': alias_code, 'name': c})
            if len(entities) >= max(1, int(limit)):
                return entities

    # 2.2 全市场映射（Tushare stock_basic）
    if ref_rows:
        for c in candidates:
            # 精确匹配
            matched = [r for r in ref_rows if r.get('name') == c]
            # 模糊包含匹配（如“青松建化金融”）
            if not matched:
                matched = [r for r in ref_rows if c in r.get('name', '') or r.get('name', '') in c]
            for r in matched:
                ts_code = r.get('ts_code', '')
                if not ts_code:
                    continue
                if any(e.get('ts_code') == ts_code for e in entities):
                    continue
                entities.append({'ts_code': ts_code, 'name': r.get('name', '')})
                if len(entities) >= max(1, int(limit)):
                    return entities

    return entities


def _get_local_daily_snapshot(ts_code):
    """
    从本地 daily_kline.db 获取最近2个交易日快照。
    """
    import sqlite3
    from AlphaFin.config import DB_ROOT

    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return None

    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    trade_date, open, high, low, close, pct_chg, vol, amount,
                    ROW_NUMBER() OVER (
                        PARTITION BY trade_date
                        ORDER BY rowid DESC
                    ) AS rn
                FROM daily_kline
                WHERE ts_code=?
            )
            SELECT trade_date, open, high, low, close, pct_chg, vol, amount
            FROM ranked
            WHERE rn=1
            ORDER BY trade_date DESC
            LIMIT 2
            """,
            (ts_code,)
        ).fetchall()
        conn.close()
    except Exception:
        return None

    if not rows:
        return None

    def _row_to_dict(row):
        return {
            'trade_date': row[0],
            'open': row[1],
            'high': row[2],
            'low': row[3],
            'close': row[4],
            'pct_chg': row[5],
            'vol': row[6],
            'amount': row[7],
        }

    latest = _row_to_dict(rows[0])
    prev = _row_to_dict(rows[1]) if len(rows) > 1 else None
    return {
        'latest': latest,
        'prev': prev,
        'source': 'daily_kline_local',
    }


def _call_ai_team_tool(tool_name, args):
    """调用智能团队现有 skill（tool_registry），返回文本结果。"""
    try:
        from AlphaFin.ai_team.core.tool_registry import execute_tool
        return execute_tool(tool_name, args or {}, agent_id='director', message_bus=None)
    except Exception as e:
        return '工具调用失败[%s]: %s' % (tool_name, str(e))


def _compact_tool_text(text, max_len=700):
    s = str(text or '').strip()
    if not s:
        return s
    # 过滤长 traceback，保留可读错误摘要
    if 'Traceback (most recent call last):' in s:
        s = s.split('Traceback (most recent call last):', 1)[0].strip()
    lines = s.splitlines()
    if len(lines) > 14:
        s = '\n'.join(lines[:14]) + '\n...(已截断)'
    if len(s) > max_len:
        s = s[:max_len] + '...(已截断)'
    return s


def _collect_skill_snapshot(user_message, idx_codes, stock_entities, need_news):
    """
    复用智能团队分析模块 skill，产出同源实时证据。
    """
    lines = []
    evidence = []

    time_text = _compact_tool_text(_call_ai_team_tool('get_current_time', {}), max_len=320)
    lines.append('- skill:get_current_time => %s' % time_text)
    evidence.append({'source': 'skill:get_current_time', 'data': time_text})

    if idx_codes:
        idx_text = _compact_tool_text(_call_ai_team_tool(
            'get_intraday_index',
            {'ts_codes': ','.join(idx_codes), 'freq': '1MIN'}
        ), max_len=900)
        lines.append('- skill:get_intraday_index =>\n%s' % idx_text)
        evidence.append({'source': 'skill:get_intraday_index', 'data': idx_text})

    if need_news:
        news_text = _compact_tool_text(_call_ai_team_tool(
            'get_intraday_news',
            {'hours': 3, 'src': 'cls', 'limit': 5}
        ), max_len=900)
        lines.append('- skill:get_intraday_news =>\n%s' % news_text)
        evidence.append({'source': 'skill:get_intraday_news', 'data': news_text})

    for ent in stock_entities[:2]:
        code = ent.get('ts_code', '')
        if not code:
            continue
        kline_text = _compact_tool_text(_call_ai_team_tool(
            'get_kline',
            {'ts_code': code, 'freq': 'D', 'start_date': (_now_cn() - datetime.timedelta(days=20)).strftime('%Y%m%d')}
        ), max_len=900)
        lines.append('- skill:get_kline(%s) =>\n%s' % (code, kline_text))
        evidence.append({'source': 'skill:get_kline', 'ts_code': code, 'data': kline_text})

    return lines, evidence


def _extract_index_codes(text):
    q = str(text or '')
    mapping = [
        (['上证', '沪指', '上证指数'], '000001.SH'),
        (['深证成指', '深成指'], '399001.SZ'),
        (['创业板', '创业板指'], '399006.SZ'),
        (['沪深300', '300指数'], '000300.SH'),
    ]
    codes = []
    for words, code in mapping:
        if any(w in q for w in words) and code not in codes:
            codes.append(code)
    if not codes and '指数' in q:
        codes = ['000001.SH', '000300.SH', '399001.SZ']
    return codes


def _is_market_related_query(text):
    q = str(text or '')
    if not q:
        return False
    if _extract_stock_codes(q, limit=1):
        return True
    # 仅当中文名可映射到实际股票代码时，才判定为市场问题
    if _resolve_stock_entities(q, limit=1):
        return True
    if _extract_index_codes(q):
        return True
    market_keys = [
        '股票', '个股', '行情', '走势', '涨跌', '大盘', '指数', '板块',
        'A股', '沪深', '上证', '深证', '创业板', '分时', 'K线', '成交',
        '换手', '资金', '估值', '市值', '买入', '卖出', '仓位', '交易',
    ]
    return any(k in q for k in market_keys)


def _need_intraday_news(text):
    q = str(text or '')
    if not q:
        return False
    news_keys = ['突发', '快讯', '消息面', '新闻', '公告', '政策', '热点']
    if any(k in q for k in news_keys):
        return True
    realtime_keys = ['最新', '实时', '盘中', '今天', '当前', '现在']
    if _is_market_related_query(q) and any(k in q for k in realtime_keys):
        return True
    return False


def _need_web_search(text):
    q = str(text or '')
    if not q:
        return False
    trivial_time_keys = ['星期', '星期几', '周几', '几点', '日期', '几号']
    if any(k in q for k in trivial_time_keys) and not _is_market_related_query(q):
        return False
    keys = [
        '最新', '实时', '今天', '当前', '盘中', '新闻', '快讯', '消息', '公告',
        '政策', '研报', '海外', '宏观', '突发', '利好', '利空', '走势',
        '重组', '并购', '资产注入', '预期', '催化',
    ]
    if any(k in q for k in keys):
        return True
    return _is_market_related_query(q)


def _extract_json_payload(text):
    s = str(text or '').strip()
    if not s:
        return None
    m = re.search(r'```(?:json)?\s*([\s\S]*?)```', s, re.IGNORECASE)
    candidate = m.group(1).strip() if m else s

    # 优先数组
    l = candidate.find('[')
    r = candidate.rfind(']')
    if l >= 0 and r > l:
        arr_text = candidate[l:r + 1]
        try:
            return json.loads(arr_text)
        except Exception:
            pass

    # 再尝试对象
    l = candidate.find('{')
    r = candidate.rfind('}')
    if l >= 0 and r > l:
        obj_text = candidate[l:r + 1]
        try:
            return json.loads(obj_text)
        except Exception:
            pass
    return None


def _as_search_items(parsed):
    if parsed is None:
        return []
    if isinstance(parsed, dict):
        for key in ('results', 'items', 'data', 'hits', 'list'):
            val = parsed.get(key)
            if isinstance(val, list):
                parsed = val
                break
    if not isinstance(parsed, list):
        return []

    items = []
    for row in parsed:
        if not isinstance(row, dict):
            continue
        title = str(
            row.get('title')
            or row.get('headline')
            or row.get('name')
            or ''
        ).strip()
        url = str(
            row.get('url')
            or row.get('link')
            or row.get('href')
            or ''
        ).strip()
        source = str(
            row.get('source')
            or row.get('site')
            or row.get('media')
            or ''
        ).strip()
        published_at = str(
            row.get('published_at')
            or row.get('datetime')
            or row.get('time')
            or row.get('date')
            or ''
        ).strip()
        summary = str(
            row.get('summary')
            or row.get('snippet')
            or row.get('content')
            or row.get('desc')
            or ''
        ).strip()
        if not title and not summary:
            continue
        items.append({
            'title': title,
            'url': url,
            'source': source,
            'published_at': published_at,
            'summary': summary,
        })
        if len(items) >= 8:
            break
    return items


def _extract_urls_from_text(raw_text, limit=8):
    text = str(raw_text or '')
    if not text:
        return []

    def _clean_url_tail(v):
        s = str(v or '').strip()
        return s.rstrip('.,;:!?)\]}>，。；：、！）】》')

    def _coerce_url(v):
        s = _clean_url_tail(v)
        if not s:
            return ''
        if s.startswith('//'):
            s = 'https:' + s
        if s.startswith('www.'):
            s = 'https://' + s
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*://', s):
            # 支持无 scheme 的域名链接
            if re.match(r'^(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?:/.*)?$', s):
                s = 'https://' + s
            else:
                return ''
        try:
            p = urlparse(s)
        except Exception:
            return ''
        if not p.netloc or '.' not in p.netloc:
            return ''
        return s

    items = []
    seen = set()
    max_n = max(1, int(limit or 8))
    # 1) 标准 URL
    # 2) Markdown 链接中的 URL 或域名
    # 3) 无 scheme 的域名/路径
    patterns = [
        r'https?://[^\s<>"\'\]\)]+',
        r'\[[^\]]+\]\(([^)\s]+)\)',
        r'(?<![@\w])(?:www\.)?(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?:/[^\s<>"\'\]\)]*)?',
    ]
    candidates = []
    for pat in patterns:
        for m in re.finditer(pat, text):
            if pat.startswith(r'\['):
                raw = str(m.group(1) or '').strip()
                start, end = m.start(1), m.end(1)
            else:
                raw = str(m.group(0) or '').strip()
                start, end = m.start(0), m.end(0)
            candidates.append((raw, start, end))
    candidates.sort(key=lambda x: x[1])

    for raw, start_pos, end_pos in candidates:
        url = _coerce_url(raw)
        if not url:
            continue
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        line_start = text.rfind('\n', 0, start_pos)
        line_end = text.find('\n', end_pos)
        if line_start < 0:
            line_start = 0
        else:
            line_start += 1
        if line_end < 0:
            line_end = len(text)
        line = text[line_start:line_end].strip()
        title = line.replace(raw, '').strip()
        if not title:
            title = re.sub(r'https?://[^\s<>"\'\]\)]+', '', line).strip()
        if not title:
            title = '来源链接'
        items.append({
            'title': title[:120],
            'url': url,
            'source': '',
            'published_at': '',
            'summary': '',
        })
        if len(items) >= max_n:
            break
    return items


def _normalize_web_items(rows, limit=8):
    def _coerce_item_url(v):
        s = str(v or '').strip()
        if not s:
            return ''
        s = s.rstrip('.,;:!?)\]}>，。；：、！）】》')
        if s.startswith('//'):
            s = 'https:' + s
        if s.startswith('www.'):
            s = 'https://' + s
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*://', s):
            if re.match(r'^(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?:/.*)?$', s):
                s = 'https://' + s
            else:
                return ''
        try:
            p = urlparse(s)
        except Exception:
            return ''
        if not p.netloc or '.' not in p.netloc:
            return ''
        return s

    out = []
    seen = set()
    max_n = max(1, int(limit or 8))
    for row in (rows or []):
        if not isinstance(row, dict):
            continue
        url = _coerce_item_url(row.get('url') or row.get('link') or '')
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


def _format_web_items_for_prompt(rows, limit=6):
    items = _normalize_web_items(rows, limit=limit)
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
            lines.append('   摘要: %s' % str(row.get('summary') or '')[:200])
    return '\n'.join(lines)


def _format_web_items_for_reply(rows, limit=6):
    items = _normalize_web_items(rows, limit=limit)
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


def _append_web_sources_to_reply(reply_text, rows, limit=6):
    text = str(reply_text or '').rstrip()
    block = _format_web_items_for_reply(rows, limit=limit)
    if not block:
        return text
    if '### 联网搜索来源链接' in text or '联网搜索来源链接' in text:
        return text
    if text:
        return (text + '\n\n' + block).strip()
    return block


def _format_web_search_packet_for_prompt(raw_text, rows, limit=6, raw_limit=3200):
    """
    将 Kimi 联网搜索结果整理为统一注入包，供其他模型直接消费。
    """
    raw = str(raw_text or '').strip()
    links_block = _format_web_items_for_prompt(rows, limit=limit)
    parts = []
    if raw:
        parts.append('【Kimi联网搜索最终回答】\n' + raw[:max(400, int(raw_limit or 3200))])
    if links_block:
        parts.append('【Kimi联网搜索结构化来源】\n' + links_block)
    return '\n\n'.join([p for p in parts if p]).strip()


def _extract_snapshot_web_items(snapshot, limit=6):
    if not isinstance(snapshot, dict):
        return []
    out = []
    for ev in (snapshot.get('evidence') or []):
        if not isinstance(ev, dict):
            continue
        if str(ev.get('source') or '') != 'web_search_snapshot':
            continue
        items = _normalize_web_items(ev.get('items') or [], limit=limit)
        if not items:
            raw_text = str(ev.get('raw') or '').strip()
            if raw_text:
                items = _normalize_web_items(_extract_urls_from_text(raw_text, limit=limit), limit=limit)
        if items:
            out.extend(items)
    return _normalize_web_items(out, limit=limit)


def _extract_snapshot_web_context(snapshot, limit=6, raw_limit=3200):
    if not isinstance(snapshot, dict):
        return {'items': [], 'raw': '', 'packet': '', 'model': ''}
    items = []
    raw_chunks = []
    model_name = ''
    for ev in (snapshot.get('evidence') or []):
        if not isinstance(ev, dict):
            continue
        if str(ev.get('source') or '') != 'web_search_snapshot':
            continue
        if not model_name:
            model_name = str(ev.get('model') or '').strip()
        rows = _normalize_web_items(ev.get('items') or [], limit=limit)
        if rows:
            items.extend(rows)
        raw_text = str(ev.get('raw') or '').strip()
        if raw_text:
            raw_chunks.append(raw_text)
    items = _normalize_web_items(items, limit=limit)
    raw_text = '\n\n'.join([x for x in raw_chunks if x]).strip()
    if (not items) and raw_text:
        items = _normalize_web_items(_extract_urls_from_text(raw_text, limit=limit), limit=limit)
    packet = _format_web_search_packet_for_prompt(raw_text, items, limit=limit, raw_limit=raw_limit)
    return {
        'items': items,
        'raw': raw_text[:max(400, int(raw_limit or 3200))],
        'packet': packet,
        'model': model_name,
    }


def _dashscope_native_generation_url():
    base = str(QWEN_BASE_URL or '').rstrip('/')
    if '/compatible-mode/v1' in base:
        host = base.split('/compatible-mode/v1', 1)[0]
    else:
        host = 'https://dashscope.aliyuncs.com'
    return host.rstrip('/') + '/api/v1/services/aigc/text-generation/generation'


def _as_dashscope_search_items(rows, limit=8):
    items = []
    seen = set()
    max_n = max(1, int(limit or 8))
    for row in (rows or []):
        if not isinstance(row, dict):
            continue
        title = str(row.get('title') or row.get('name') or '').strip()
        url = str(row.get('url') or row.get('link') or '').strip()
        source = str(row.get('site_name') or row.get('source') or '').strip()
        published_at = str(row.get('time') or row.get('published_at') or '').strip()
        summary = str(row.get('snippet') or row.get('summary') or row.get('content') or '').strip()
        if not title and not url:
            continue
        key = (url.lower(), title)
        if key in seen:
            continue
        seen.add(key)
        items.append({
            'title': title or url,
            'url': url,
            'source': source,
            'published_at': published_at,
            'summary': summary,
        })
        if len(items) >= max_n:
            break
    return items


def _source_from_url(url):
    try:
        host = str(urlparse(str(url or '')).netloc or '').lower()
    except Exception:
        host = ''
    if host.startswith('www.'):
        host = host[4:]
    return host


def _collect_web_search_snapshot_responses(query, timeout_s=45, max_items=6):
    """
    优先调用 DashScope Responses API 的 web_search/web_extractor，
    返回结构化来源列表，便于前端稳定展示“联网来源链接”。
    """
    if not QWEN_API_KEY:
        return {'items': [], 'model': '', 'raw': '', 'error': 'missing_qwen_api_key'}

    url = QWEN_BASE_URL.rstrip('/') + '/responses'
    headers = {
        'Authorization': 'Bearer ' + QWEN_API_KEY,
        'Content-Type': 'application/json',
    }
    model_name = AI_WEB_SEARCH_RESPONSES_MODEL or AI_WEB_SEARCH_MODEL or AI_CHAT_MODEL
    tools = [{'type': 'web_search'}]
    if AI_WEB_SEARCH_USE_EXTRACTOR:
        tools.append({'type': 'web_extractor'})
    payload = {
        'model': model_name,
        'input': str(query or ''),
        'tools': tools,
        'extra_body': {'enable_thinking': False},
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_s)
        resp.raise_for_status()
        data = resp.json() or {}
        output = data.get('output') or []

        by_url = {}
        raw_chunks = []

        def _upsert(url_text, title='', source='', published_at='', summary=''):
            u = str(url_text or '').strip()
            if not u:
                return
            row = by_url.get(u) or {
                'title': '',
                'url': u,
                'source': '',
                'published_at': '',
                'summary': '',
            }
            if title and (not row.get('title')):
                row['title'] = str(title).strip()
            if source and (not row.get('source')):
                row['source'] = str(source).strip()
            if published_at and (not row.get('published_at')):
                row['published_at'] = str(published_at).strip()
            if summary:
                old = str(row.get('summary') or '').strip()
                if len(old) < 40:
                    row['summary'] = str(summary).strip()
            if not row.get('source'):
                row['source'] = _source_from_url(u)
            if not row.get('title'):
                row['title'] = row.get('source') or u
            by_url[u] = row

        for item in output:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get('type') or '').strip()
            if item_type == 'web_search_call':
                action = item.get('action') or {}
                query_text = str(action.get('query') or query or '').strip()
                for src in (action.get('sources') or []):
                    if not isinstance(src, dict):
                        continue
                    _upsert(
                        url_text=src.get('url'),
                        title=src.get('title') or query_text,
                        source=src.get('source') or '',
                        published_at=src.get('published_at') or '',
                        summary=''
                    )
            elif item_type == 'web_extractor_call':
                extracted = str(item.get('output') or '').strip()
                if extracted:
                    raw_chunks.append(extracted)
                urls = item.get('urls') or []
                if isinstance(urls, list):
                    for u in urls:
                        _upsert(url_text=u, summary=extracted[:260] if extracted else '')
            elif item_type == 'message':
                for part in (item.get('content') or []):
                    if not isinstance(part, dict):
                        continue
                    if str(part.get('type') or '') == 'output_text':
                        txt = str(part.get('text') or '').strip()
                        if txt:
                            raw_chunks.append(txt)
                            for row in _extract_urls_from_text(txt, limit=max_items * 3):
                                _upsert(
                                    url_text=row.get('url'),
                                    title=row.get('title') or '',
                                    source=row.get('source') or '',
                                    published_at=row.get('published_at') or '',
                                    summary=''
                                )

        items = list(by_url.values())
        if not items:
            raw_text = '\n'.join(raw_chunks).strip()
            if raw_text:
                for row in _extract_urls_from_text(raw_text, limit=max_items * 3):
                    _upsert(
                        url_text=row.get('url'),
                        title=row.get('title') or '',
                        source=row.get('source') or '',
                        published_at=row.get('published_at') or '',
                        summary=''
                    )
                items = list(by_url.values())

        return {
            'items': items[:max(1, int(max_items))],
            'model': model_name,
            'raw': '\n'.join(raw_chunks).strip()[:4000],
            'error': '' if items else 'empty_search_items',
        }
    except Exception as e:
        err = str(e)
        if ('timed out' in err.lower()) or ('timeout' in err.lower()):
            err = 'web_search_timeout'
        return {'items': [], 'model': model_name, 'raw': '', 'error': err}


def _parse_date_to_yyyymmdd(raw_text):
    s = str(raw_text or '').strip()
    if not s:
        return ''
    # 兼容: 2026-03-25 / 2026/3/25 / 2026年3月25日 / 20260325
    m = re.search(r'(20\d{2})\D?(\d{1,2})\D?(\d{1,2})', s)
    if not m:
        m2 = re.search(r'\b(20\d{2})(\d{2})(\d{2})\b', s)
        if not m2:
            return ''
        y, mm, dd = m2.group(1), m2.group(2), m2.group(3)
    else:
        y, mm, dd = m.group(1), m.group(2), m.group(3)
    try:
        dt = datetime.date(int(y), int(mm), int(dd))
    except Exception:
        return ''
    return dt.strftime('%Y%m%d')


def _infer_item_date_yyyymmdd(item_row):
    row = item_row if isinstance(item_row, dict) else {}
    for field in ('published_at', 'title', 'url'):
        day_key = _parse_date_to_yyyymmdd(row.get(field))
        if day_key:
            return day_key
    return ''


def _days_old_yyyymmdd(yyyymmdd):
    s = str(yyyymmdd or '').strip()
    if not s:
        return None
    try:
        dt = datetime.datetime.strptime(s, '%Y%m%d').date()
        today = _now_cn().date()
        return (today - dt).days
    except Exception:
        return None


def _contains_any_token(text, tokens):
    s = str(text or '')
    for t in (tokens or []):
        if t and t in s:
            return True
    return False


def _build_web_search_hints(query):
    q = str(query or '').strip()
    entities = _resolve_stock_entities(q, limit=2)
    names = []
    codes = []
    for ent in entities:
        code = str(ent.get('ts_code') or '').strip().upper()
        name = str(ent.get('name') or '').strip()
        if code and code not in codes:
            codes.append(code)
        if name and name not in names:
            names.append(name)
    for c in _extract_stock_codes(q, limit=2):
        if c and c not in codes:
            codes.append(c)
    tokens = []
    for n in names:
        if n and n not in tokens:
            tokens.append(n)
    for c in codes:
        c6 = c.split('.', 1)[0]
        if c6 and c6 not in tokens:
            tokens.append(c6)
        if c and c not in tokens:
            tokens.append(c)
    # 额外补充用户文本中的 2-6 字中文片段，增强相关性匹配
    for x in re.findall(r'[\u4e00-\u9fff]{2,6}', q):
        if x not in tokens and x not in ('今天', '今日', '最新', '实时', '当前', '现在', '走势', '行情', '分析'):
            tokens.append(x)
    is_realtime = any(k in q for k in ('今天', '今日', '最新', '实时', '当前', '现在', '盘中'))
    return {
        'query': q,
        'names': names,
        'codes': codes,
        'tokens': tokens,
        'has_stock_target': bool(names or codes),
        'is_realtime': is_realtime,
    }


def _rewrite_search_query(query):
    q = str(query or '').strip()
    if not q:
        return q
    hints = _build_web_search_hints(q)
    if not hints.get('has_stock_target'):
        return q
    if not hints.get('is_realtime'):
        return q
    today_str = _now_cn().strftime('%Y-%m-%d')
    token_parts = []
    if hints.get('names'):
        token_parts.extend(hints['names'][:1])
    if hints.get('codes'):
        token_parts.extend(hints['codes'][:1])
        code6 = hints['codes'][0].split('.', 1)[0]
        if code6:
            token_parts.append(code6)
    token_parts.extend(['A股', '今日', today_str, '行情', '新闻', '公告'])
    extra = ' '.join([x for x in token_parts if x]).strip()
    if not extra:
        return q
    return (q + ' ' + extra).strip()[:320]


def _rank_filter_search_items(query, items, limit=8):
    rows = [x for x in (items or []) if isinstance(x, dict)]
    if not rows:
        return []
    max_n = max(1, int(limit or 8))
    hints = _build_web_search_hints(query)

    ranked = []
    for row in rows:
        title = str(row.get('title') or '').strip()
        summary = str(row.get('summary') or '').strip()
        source = str(row.get('source') or '').strip()
        url = str(row.get('url') or '').strip()
        published_at = str(row.get('published_at') or '').strip()
        packed = ' '.join([title, summary, source, url]).upper()

        entity_hit = False
        score = 0.0

        for c in hints.get('codes') or []:
            c_u = str(c).upper()
            c6 = c_u.split('.', 1)[0]
            if c_u and c_u in packed:
                score += 5.0
                entity_hit = True
            if c6 and c6 in packed:
                score += 4.0
                entity_hit = True

        for n in hints.get('names') or []:
            if n and n in (title + summary + source):
                score += 5.0
                entity_hit = True

        token_hits = 0
        for t in hints.get('tokens') or []:
            if t and t in (title + summary + source):
                token_hits += 1
        score += min(3.0, float(token_hits) * 0.8)

        day_key = _infer_item_date_yyyymmdd(row)
        days_old = _days_old_yyyymmdd(day_key)
        if days_old is None:
            score += 0.2
        elif days_old <= 0:
            score += 3.0
        elif days_old <= 1:
            score += 2.4
        elif days_old <= 3:
            score += 1.8
        elif days_old <= 7:
            score += 1.2
        elif days_old <= 30:
            score += 0.6
        elif days_old > 365:
            score -= 0.8

        if source:
            score += 0.2
        if url.startswith('http'):
            score += 0.2

        ranked.append({
            'score': score,
            'entity_hit': entity_hit,
            'days_old': days_old if isinstance(days_old, int) else 999999,
            'row': {
                'title': title,
                'url': url,
                'source': source,
                'published_at': published_at,
                'summary': summary,
            }
        })

    ranked.sort(key=lambda x: (-float(x.get('score') or 0), int(x.get('days_old') or 999999)))
    ordered = ranked

    if hints.get('has_stock_target'):
        with_entity = [x for x in ranked if x.get('entity_hit')]
        if with_entity:
            ordered = with_entity

    if hints.get('is_realtime'):
        fresh = [x for x in ordered if int(x.get('days_old') or 999999) <= 7]
        if fresh:
            ordered = fresh + [x for x in ordered if x not in fresh]
        if hints.get('has_stock_target'):
            # “今日/实时+个股”场景：尽量只保留近期来源，避免陈旧旧闻污染结论
            recent = [x for x in ordered if int(x.get('days_old') or 999999) <= 14]
            if recent:
                ordered = recent

    out = []
    seen = set()
    for item in ordered:
        row = dict(item.get('row') or {})
        key = (str(row.get('url') or '').lower(), str(row.get('title') or ''))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
        if len(out) >= max_n:
            break

    # 若筛选过严导致空结果，回退到原始排序前 max_n，避免“有结果却全被过滤”
    if not out:
        for item in ranked[:max_n]:
            row = dict(item.get('row') or {})
            if row:
                out.append(row)
    return out


def _collect_web_search_snapshot_native(query, timeout_s=45, max_items=6):
    if not QWEN_API_KEY:
        return {'items': [], 'model': '', 'raw': '', 'error': 'missing_qwen_api_key'}
    url = _dashscope_native_generation_url()
    headers = {
        'Authorization': 'Bearer ' + QWEN_API_KEY,
        'Content-Type': 'application/json',
    }
    payload = {
        'model': AI_WEB_SEARCH_MODEL,
        'input': {
            'messages': [
                {'role': 'system', 'content': '你是 AlphaFin 联网检索器。必须联网并返回可核验来源。'},
                {'role': 'user', 'content': str(query or '')},
            ]
        },
        'parameters': {
            'result_format': 'message',
            'enable_search': True,
            'search_options': {
                'forced_search': True,
                'enable_source': True
            }
        }
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_s)
        resp.raise_for_status()
        data = resp.json()
        output = data.get('output') or {}
        choices = output.get('choices') or []
        raw_text = ''
        if choices:
            msg = (choices[0] or {}).get('message') or {}
            raw_text = _extract_content_text(msg.get('content'))
        search_info = output.get('search_info') or {}
        rows = search_info.get('search_results') or []
        items = _as_dashscope_search_items(rows, limit=max_items)
        if (not items) and raw_text:
            items = _extract_urls_from_text(raw_text, limit=max_items)
        return {
            'items': items,
            'model': AI_WEB_SEARCH_MODEL,
            'raw': raw_text,
            'error': '' if items else 'empty_search_items',
        }
    except Exception as e:
        err = str(e)
        if ('timed out' in err.lower()) or ('timeout' in err.lower()):
            err = 'web_search_timeout'
        return {'items': [], 'model': '', 'raw': '', 'error': err}


def _chat_choice_to_plain_dict(choice_obj):
    msg = {}
    finish_reason = ''
    if isinstance(choice_obj, dict):
        msg = dict(choice_obj.get('message') or {})
        finish_reason = str(choice_obj.get('finish_reason') or '')
    else:
        try:
            msg = getattr(choice_obj, 'message', None) or {}
        except Exception:
            msg = {}
        if not isinstance(msg, dict):
            try:
                msg = {
                    'role': getattr(msg, 'role', 'assistant'),
                    'content': getattr(msg, 'content', ''),
                    'tool_calls': getattr(msg, 'tool_calls', None),
                }
            except Exception:
                msg = {'role': 'assistant', 'content': ''}
        try:
            finish_reason = str(getattr(choice_obj, 'finish_reason', '') or '')
        except Exception:
            finish_reason = ''

    role = str(msg.get('role') or 'assistant')
    content = msg.get('content', '')
    if content is None:
        content = ''
    tool_calls_raw = msg.get('tool_calls') or []
    tool_calls = []
    for tc in tool_calls_raw:
        if isinstance(tc, dict):
            fid = str(tc.get('id') or '')
            fn = tc.get('function') or {}
            f_name = str(fn.get('name') or '')
            f_args = fn.get('arguments')
            if not isinstance(f_args, str):
                try:
                    f_args = json.dumps(f_args, ensure_ascii=False)
                except Exception:
                    f_args = '{}'
        else:
            try:
                fid = str(getattr(tc, 'id', '') or '')
                fn = getattr(tc, 'function', None)
                f_name = str(getattr(fn, 'name', '') or '')
                f_args = getattr(fn, 'arguments', '{}')
                if not isinstance(f_args, str):
                    f_args = json.dumps(f_args, ensure_ascii=False)
            except Exception:
                fid, f_name, f_args = '', '', '{}'
        tool_calls.append({
            'id': fid,
            'type': 'function',
            'function': {'name': f_name, 'arguments': f_args or '{}'}
        })

    return {
        'finish_reason': finish_reason,
        'message': {
            'role': role,
            'content': content,
            'tool_calls': tool_calls,
        }
    }


def _extract_search_items_from_payload(payload, limit=8):
    found = []
    seen = set()

    def _push(row):
        if not isinstance(row, dict):
            return
        title = str(row.get('title') or row.get('name') or '').strip()
        url = str(row.get('url') or row.get('link') or '').strip()
        source = str(row.get('source') or row.get('site') or row.get('media') or '').strip()
        published_at = str(
            row.get('published_at') or row.get('time') or row.get('datetime') or row.get('date') or ''
        ).strip()
        summary = str(
            row.get('summary') or row.get('snippet') or row.get('content') or row.get('desc') or ''
        ).strip()
        if not url:
            return
        key = (url.lower(), title)
        if key in seen:
            return
        seen.add(key)
        found.append({
            'title': title or url,
            'url': url,
            'source': source,
            'published_at': published_at,
            'summary': summary,
        })

    def _walk(node):
        if len(found) >= max(2, int(limit or 8)) * 3:
            return
        if isinstance(node, dict):
            _push(node)
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(payload)
    return _normalize_web_items(found, limit=max(2, int(limit or 8)) * 2)


def _collect_web_search_snapshot(user_message, model_name='', timeout_s=None, retries=None, max_items=None):
    """
    统一联网检索入口：固定走 Kimi $web_search 工具调用链。
    返回: {'items': [...], 'model': str, 'raw': str, 'error': str}
    """
    query_raw = str(user_message or '').strip()
    if not query_raw:
        return {'items': [], 'model': '', 'raw': '', 'error': 'empty query'}

    # 与本地脚本一致：原问题不做改写，原样透传
    query = query_raw
    max_items = int(max_items if max_items is not None else AI_WEB_SEARCH_MAX_ITEMS)
    # 与本地脚本一致：默认单次执行（不隐式重试）
    retries = int(max(1, (retries if retries is not None else 1)))
    timeout_s = int(max(6, (timeout_s if timeout_s is not None else AI_WEB_SEARCH_TIMEOUT_SECONDS)))
    selected_model = str(KIMI_WEB_SEARCH_MODEL or 'kimi-k2.5').strip() or 'kimi-k2.5'
    if not KIMI_WEB_SEARCH_API_KEY:
        return {
            'items': [],
            'model': selected_model,
            'raw': '',
            'error': 'Kimi联网搜索未配置API密钥',
        }

    # 与本地测试脚本保持同构：
    # - chat.completions + builtin_function($web_search)
    # - finish_reason == tool_calls 时回填 tool message
    # - 最终直接返回模型内容，并提取来源链接
    last_error = ''
    for attempt in range(retries):
        try:
            client = _get_kimi_openai_client()
            if client is None:
                raise RuntimeError('Kimi客户端初始化失败')

            messages = [
                {'role': 'system', 'content': '你是 Kimi。'},
                # 与本地验证脚本严格一致：用户原问题原样透传
                {'role': 'user', 'content': query},
            ]
            tool_payloads = []
            final_text = ''
            model_used = selected_model
            finish_reason = None
            turn_cap = max(2, int(KIMI_WEB_SEARCH_MAX_TURNS))
            turn_idx = 0

            while (finish_reason is None or finish_reason == 'tool_calls') and turn_idx < turn_cap:
                turn_idx += 1
                completion = client.chat.completions.create(
                    model=selected_model,
                    messages=messages,
                    max_tokens=32768,
                    extra_body={'thinking': {'type': 'disabled'}},
                    tools=[
                        {
                            'type': 'builtin_function',
                            'function': {'name': '$web_search'},
                        }
                    ],
                    timeout=timeout_s,
                )
                try:
                    model_used = str(getattr(completion, 'model', '') or selected_model)
                except Exception:
                    model_used = selected_model

                choices = getattr(completion, 'choices', None) or []
                if not choices:
                    raise RuntimeError('empty_search_choices')
                choice = choices[0]
                finish_reason = str(getattr(choice, 'finish_reason', '') or '')
                msg_obj = getattr(choice, 'message', None)
                if msg_obj is None:
                    raise RuntimeError('empty_choice_message')
                tool_calls = list(getattr(msg_obj, 'tool_calls', None) or [])

                # 与本地脚本同构：先把 assistant(tool_calls) 原样加入上下文
                if finish_reason == 'tool_calls' and tool_calls:
                    messages.append(msg_obj)
                    for tc in tool_calls:
                        fn_obj = getattr(tc, 'function', None)
                        fn_name = str(getattr(fn_obj, 'name', '') or '')
                        fn_args_raw = getattr(fn_obj, 'arguments', '{}')
                        try:
                            fn_args = json.loads(fn_args_raw) if isinstance(fn_args_raw, str) else (fn_args_raw or {})
                        except Exception:
                            fn_args = {}
                        if fn_name == '$web_search':
                            tool_result = fn_args
                            tool_payloads.append(fn_args)
                        else:
                            tool_result = {'error': 'unknown_tool', 'name': fn_name}
                        messages.append({
                            'role': 'tool',
                            'tool_call_id': str(getattr(tc, 'id', '') or ''),
                            'name': fn_name,
                            # 与本地脚本一致（默认 ensure_ascii=True）
                            'content': json.dumps(tool_result),
                        })
                    continue

                # 与本地脚本一致：最终以模型内容作为联网输出主文本
                final_text = _extract_content_text(getattr(msg_obj, 'content', None)) or str(getattr(msg_obj, 'content', '') or '')
                break

            if (not final_text) and tool_payloads:
                # 回退：即便模型最终文本为空，也尽量从工具负载抽取来源
                final_text = json.dumps(tool_payloads[-1], ensure_ascii=False)

            items = []
            for payload in tool_payloads:
                items.extend(_extract_search_items_from_payload(payload, limit=max_items))
            if not items and final_text:
                parsed = _extract_json_payload(final_text)
                items = _as_search_items(parsed)
            if not items and final_text:
                items = _extract_urls_from_text(final_text, limit=max_items)
            items = _normalize_web_items(items, limit=max_items)

            if final_text or items:
                return {
                    'items': items[:max_items],
                    'model': model_used,
                    'raw': str(final_text or '')[:8000],
                    'error': '',
                }
            last_error = 'empty_search_items'
        except Exception as e:
            err = str(e)
            low = err.lower()
            if ('timed out' in low) or ('timeout' in low):
                err = 'web_search_timeout'
            last_error = err

        if attempt < retries - 1 and AI_WEB_SEARCH_RETRY_BACKOFF > 0:
            time.sleep(min(2.5, AI_WEB_SEARCH_RETRY_BACKOFF * (attempt + 1)))

    return {
        'items': [],
        'model': selected_model,
        'raw': '',
        'error': last_error or 'web_search_failed',
    }


def _get_pro():
    global _PRO_CLIENT
    if _PRO_CLIENT is not None:
        return _PRO_CLIENT
    try:
        import tushare as ts
        _PRO_CLIENT = ts.pro_api(TUSHARE_TOKEN)
    except Exception:
        _PRO_CLIENT = None
    return _PRO_CLIENT


def _collect_internal_market_snapshot(user_message, enable_search=True, model_name=''):
    """
    采集内部可验证数据快照，供模型推理使用。
    返回: dict{generated_at, context_text, evidence}
    """
    generated_at = _now_cn().strftime('%Y-%m-%d %H:%M:%S')
    evidence = []
    lines = []
    idx_codes = _extract_index_codes(user_message)
    stock_entities = _resolve_stock_entities(user_message, limit=2)
    need_news = _need_intraday_news(user_message)
    # 用户勾选“联网搜索”时，强制对每个问题执行一次网络检索
    need_web_search = bool(enable_search)

    try:
        from AlphaFin.ai_team.services.tushare_watch_service import (
            fetch_intraday_index,
            fetch_intraday_news,
            fetch_intraday_stock_price,
            fetch_intraday_stock_quote,
            get_market_clock,
        )
    except Exception as e:
        return {
            'generated_at': generated_at,
            'context_text': '【内部实时数据快照】\n初始化失败: %s' % str(e),
            'evidence': [],
        }

    # 1) 市场时钟
    clock = {}
    try:
        clock = get_market_clock() or {}
        lines.append(
            '- 市场时钟: %s | phase=%s | trade_date=%s'
            % (clock.get('datetime', generated_at), clock.get('phase', 'unknown'), clock.get('trade_date', ''))
        )
        evidence.append({'source': 'get_market_clock', 'data': clock})
    except Exception as e:
        lines.append('- 市场时钟获取失败: %s' % str(e))

    # 2) 复用“智能团队分析”skill 快照（与团队模块同源）
    skill_lines, skill_evidence = _collect_skill_snapshot(
        user_message=user_message,
        idx_codes=idx_codes,
        stock_entities=stock_entities,
        need_news=need_news,
    )
    if skill_lines:
        lines.append('- 智能团队 skill 快照:')
        lines.extend(['  ' + x for x in skill_lines])
    if skill_evidence:
        evidence.extend(skill_evidence)

    # 2.5) 网络搜索快照（公开信源，补足突发与跨市场信息）
    if need_web_search:
        ws = _collect_web_search_snapshot(user_message, model_name=model_name)
        ws_items = ws.get('items') or []
        if ws_items:
            lines.append('- 联网搜索快照(前3):')
            for item in ws_items[:3]:
                lines.append(
                    '  * %s | %s | %s | %s'
                    % (
                        (item.get('published_at') or '-'),
                        (item.get('source') or '-'),
                        (item.get('title') or '-')[:120],
                        (item.get('url') or '-'),
                    )
                )
            evidence.append({
                'source': 'web_search_snapshot',
                'model': ws.get('model', ''),
                'items': ws_items,
            })
        else:
            err = ws.get('error') or ''
            if err:
                lines.append('- 联网搜索快照失败: %s' % err)
                evidence.append({
                    'source': 'web_search_snapshot',
                    'error': err,
                })
            else:
                raw_text = str(ws.get('raw') or '').strip()
                if raw_text:
                    lines.append('- 联网搜索快照(原始): %s' % raw_text[:220].replace('\n', ' '))
                    evidence.append({
                        'source': 'web_search_snapshot',
                        'model': ws.get('model', ''),
                        'raw': raw_text[:2000],
                    })

    # 3) 指数分时快照（结构化原始数据）
    if idx_codes:
        try:
            idx_rows = fetch_intraday_index(ts_codes=idx_codes, freq='1MIN') or []
            if idx_rows:
                lines.append('- 指数分时快照:')
                for row in idx_rows:
                    lines.append(
                        '  * %s close=%s intraday_pct=%s%% time=%s source=%s'
                        % (
                            row.get('ts_code', ''),
                            row.get('close', ''),
                            row.get('intraday_pct', ''),
                            row.get('time', ''),
                            row.get('source', ''),
                        )
                    )
                evidence.append({'source': 'fetch_intraday_index', 'data': idx_rows})
            else:
                lines.append('- 指数分时快照: 无数据')
        except Exception as e:
            lines.append('- 指数分时获取失败: %s' % str(e))

    # 4) 个股实时分钟 + 日线最新（支持中文名解析）
    if stock_entities:
        pro = _get_pro()
        today = _now_cn()
        start_20d = (today - datetime.timedelta(days=30)).strftime('%Y%m%d')
        end_date = today.strftime('%Y%m%d')
        lines.append('- 个股快照:')
        for ent in stock_entities:
            code = ent.get('ts_code', '')
            name = ent.get('name', '')
            row = {'ts_code': code, 'name': name}

            try:
                rt_quote = fetch_intraday_stock_quote(code, freq='1MIN')
                if rt_quote:
                    row['rt_quote'] = rt_quote
                    row['rt_price'] = rt_quote.get('price')
                else:
                    row['rt_price'] = fetch_intraday_stock_price(code)
            except Exception as e:
                row['rt_error'] = str(e)

            if pro is not None:
                # 分时原始快照（尽量提供“今天走势”证据）
                try:
                    df_rt = pro.rt_min(ts_code=code, freq='1MIN')
                except Exception:
                    df_rt = None
                if df_rt is not None and not df_rt.empty:
                    if 'time' in df_rt.columns:
                        try:
                            df_rt = df_rt.sort_values('time')
                        except Exception:
                            pass
                    first = df_rt.iloc[0]
                    last = df_rt.iloc[-1]
                    open0 = first.get('open')
                    close_last = last.get('close')
                    intraday_pct = None
                    try:
                        if open0 not in (None, 0) and close_last is not None:
                            intraday_pct = (float(close_last) - float(open0)) / float(open0) * 100.0
                    except Exception:
                        intraday_pct = None
                    row['rt_min'] = {
                        'time': last.get('time'),
                        'close': float(close_last) if close_last is not None else None,
                        'high': float(last.get('high')) if last.get('high') is not None else None,
                        'low': float(last.get('low')) if last.get('low') is not None else None,
                        'intraday_pct': intraday_pct,
                        'source': 'tushare_rt_min',
                    }

                try:
                    daily = pro.daily(
                        ts_code=code,
                        start_date=start_20d,
                        end_date=end_date,
                        fields='trade_date,open,high,low,close,pct_chg,vol,amount'
                    )
                except Exception:
                    daily = None
                if daily is not None and not daily.empty:
                    latest = daily.sort_values('trade_date').iloc[-1]
                    row['latest_daily'] = {
                        'trade_date': latest.get('trade_date'),
                        'close': float(latest.get('close')) if latest.get('close') is not None else None,
                        'pct_chg': float(latest.get('pct_chg')) if latest.get('pct_chg') is not None else None,
                        'vol': float(latest.get('vol')) if latest.get('vol') is not None else None,
                    }

                try:
                    basic = pro.daily_basic(
                        ts_code=code,
                        start_date=start_20d,
                        end_date=end_date,
                        fields='trade_date,pe_ttm,pb,total_mv,turnover_rate'
                    )
                except Exception:
                    basic = None
                if basic is not None and not basic.empty:
                    b = basic.sort_values('trade_date').iloc[-1]
                    row['latest_basic'] = {
                        'trade_date': b.get('trade_date'),
                        'pe_ttm': float(b.get('pe_ttm')) if b.get('pe_ttm') is not None else None,
                        'pb': float(b.get('pb')) if b.get('pb') is not None else None,
                        'total_mv': float(b.get('total_mv')) if b.get('total_mv') is not None else None,
                        'turnover_rate': float(b.get('turnover_rate')) if b.get('turnover_rate') is not None else None,
                    }

            # 本地数据库兜底
            try:
                local_daily = _get_local_daily_snapshot(code)
                if local_daily:
                    row['local_daily'] = local_daily
            except Exception:
                pass

            # 分时失败时，用本地最新收盘价兜底，避免“完全无价可用”
            if row.get('rt_price') in (None, ''):
                local_latest = _get_local_latest_close(code)
                if local_latest:
                    row['rt_price_fallback'] = local_latest

            lines.append(
                '  * %s(%s) rt_quote=%s rt_min=%s rt_price=%s rt_fallback=%s daily=%s local_daily=%s basic=%s'
                % (
                    name or '-',
                    code,
                    row.get('rt_quote', {}),
                    row.get('rt_min', {}),
                    row.get('rt_price', 'NA'),
                    row.get('rt_price_fallback', {}),
                    row.get('latest_daily', {}),
                    row.get('local_daily', {}),
                    row.get('latest_basic', {}),
                )
            )
            evidence.append({'source': 'stock_snapshot', 'data': row})

    # 5) 盘中快讯（结构化原始数据）
    if need_news:
        try:
            news = fetch_intraday_news(hours=3, src='cls', limit=5) or []
            if news:
                lines.append('- 最近3小时快讯(前5):')
                for n in news:
                    lines.append(
                        '  * %s | %s'
                        % (n.get('datetime', ''), (n.get('title', '') or '')[:80])
                    )
                evidence.append({'source': 'fetch_intraday_news', 'data': news})
            else:
                lines.append('- 最近3小时快讯: 无数据')
        except Exception as e:
            lines.append('- 盘中快讯获取失败: %s' % str(e))

    if not lines:
        lines.append('- 未命中内部快照规则，可补充股票代码（如 600519.SH）提升准确性。')

    context_text = '【内部实时数据快照】生成时间: %s\n%s' % (generated_at, '\n'.join(lines))
    return {'generated_at': generated_at, 'context_text': context_text, 'evidence': evidence}


def _persist_trace(trace_id, payload):
    try:
        day = _now_cn().strftime('%Y%m%d')
        day_dir = os.path.join(_TRACE_ROOT, day)
        os.makedirs(day_dir, exist_ok=True)
        fpath = os.path.join(day_dir, '%s.json' % trace_id)
        with open(fpath, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _get_local_latest_trade_date():
    """读取本地数据库最新交易日。"""
    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return ''
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute('SELECT MAX(trade_date) FROM daily_kline').fetchone()
        conn.close()
        if row and row[0]:
            return str(row[0])
    except Exception:
        return ''
    return ''


def get_realtime_data_status():
    """
    获取 AI 智能分析的数据源状态，用于前端状态灯展示。
    mode:
    - realtime: 实时分钟源可用
    - degraded: 实时源受限，已降级（例如日线兜底或本地库）
    - offline: 数据源不可用
    """
    checked_at = _now_cn().strftime('%Y-%m-%d %H:%M:%S')
    status = {
        'checked_at': checked_at,
        'mode': 'offline',
        'summary': '数据源状态未知',
        'clock_phase': '',
        'intraday_index_ok': False,
        'intraday_index_source': '',
        'intraday_index_time': '',
        'intraday_news_ok': False,
        'stock_quote_ok': False,
        'stock_quote_source': '',
        'stock_quote_time': '',
        'local_db_latest_trade_date': '',
        'errors': [],
    }

    try:
        from AlphaFin.ai_team.services.tushare_watch_service import (
            get_market_clock, fetch_intraday_index, fetch_intraday_news, fetch_intraday_stock_quote
        )
    except Exception as e:
        status['errors'].append('初始化实时服务失败: %s' % str(e))
        status['local_db_latest_trade_date'] = _get_local_latest_trade_date()
        if status['local_db_latest_trade_date']:
            status['mode'] = 'degraded'
            status['summary'] = '实时服务不可用，已降级为本地数据库'
        else:
            status['mode'] = 'offline'
            status['summary'] = '实时服务不可用，且本地数据库不可读'
        return status

    try:
        clock = get_market_clock() or {}
        status['clock_phase'] = clock.get('phase', '')
    except Exception as e:
        status['errors'].append('市场时钟失败: %s' % str(e))

    try:
        idx_rows = fetch_intraday_index(ts_codes=['000001.SH'], freq='1MIN') or []
        if idx_rows:
            row = idx_rows[0]
            status['intraday_index_ok'] = True
            status['intraday_index_source'] = str(row.get('source', '') or '')
            status['intraday_index_time'] = str(row.get('time', '') or '')
    except Exception as e:
        status['errors'].append('指数分时失败: %s' % str(e))

    try:
        news_rows = fetch_intraday_news(hours=3, src='cls', limit=1) or []
        status['intraday_news_ok'] = bool(news_rows)
    except Exception as e:
        status['errors'].append('盘中快讯失败: %s' % str(e))

    try:
        quote = fetch_intraday_stock_quote('600000.SH', freq='1MIN') or {}
        if quote and quote.get('price') is not None:
            status['stock_quote_ok'] = True
            status['stock_quote_source'] = str(quote.get('source', '') or '')
            status['stock_quote_time'] = str(quote.get('time', '') or '')
    except Exception as e:
        status['errors'].append('个股实时价失败: %s' % str(e))

    status['local_db_latest_trade_date'] = _get_local_latest_trade_date()

    src = status['intraday_index_source']
    qsrc = status['stock_quote_source']
    if (
        (status['intraday_index_ok'] and src == 'rt_idx_min')
        or (status['stock_quote_ok'] and qsrc in ('tushare_rt_min', 'tushare_stk_mins', 'qq_quote', 'sina_quote'))
    ):
        status['mode'] = 'realtime'
        status['summary'] = '实时分钟数据正常'
    elif (status['intraday_index_ok'] and src) or (status['stock_quote_ok'] and qsrc):
        status['mode'] = 'degraded'
        status['summary'] = '实时接口受限，当前使用兜底数据(%s)' % (src or qsrc)
    elif status['local_db_latest_trade_date']:
        status['mode'] = 'degraded'
        status['summary'] = '实时接口不可用，已降级本地数据库(%s)' % status['local_db_latest_trade_date']
    else:
        status['mode'] = 'offline'
        status['summary'] = '实时与本地数据均不可用'

    return status


def get_ai_chat_trace(trace_id):
    """按 trace_id 查询回溯记录。"""
    if not re.fullmatch(r'[A-Za-z0-9_-]{6,64}', str(trace_id or '')):
        return None
    if not os.path.isdir(_TRACE_ROOT):
        return None

    trace_name = '%s.json' % trace_id
    try:
        days = sorted(os.listdir(_TRACE_ROOT), reverse=True)
    except Exception:
        return None

    for day in days:
        fpath = os.path.join(_TRACE_ROOT, day, trace_name)
        if not os.path.isfile(fpath):
            continue
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return None
    return None


def _clean_grounded_answer(raw_text):
    """
    默认回复仅保留最终答案，不展示推理/验证细节。
    详细证据链保留在 trace 中供前端展开查看。
    """
    text = str(raw_text or '').strip()
    if not text:
        return ''

    # 清理模型可能返回的思维标签与代码块包裹
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.IGNORECASE | re.DOTALL).strip()
    text = re.sub(r'```(?:json|markdown|text)?\s*', '', text, flags=re.IGNORECASE)
    text = text.replace('```', '').strip()

    cut_markers = [
        '\n**推理过程',
        '\n推理过程：',
        '\n### 推理过程',
        '\n**验证过程',
        '\n验证过程：',
        '\n### 验证过程',
        '\n**可验证步骤',
        '\n可验证步骤：',
        '\n### 可验证步骤',
        '\n**证据链',
        '\n证据链：',
        '\n### 证据链',
        '\n✅ 验证完成',
    ]
    cut = len(text)
    for marker in cut_markers:
        idx = text.find(marker)
        if idx >= 0:
            cut = min(cut, idx)
    if cut < len(text):
        text = text[:cut].rstrip()

    # 轻量清洗标题前缀
    text = re.sub(r'^\s*\*{0,2}核心结论[:：]\*{0,2}\s*', '', text)
    text = re.sub(r'^\s*\*{0,2}结论[:：]\*{0,2}\s*', '', text)
    text = text.replace('**', '')

    # 压缩多余空行
    lines = [ln.rstrip() for ln in text.splitlines()]
    compact = []
    blank = False
    for line in lines:
        if line.strip():
            compact.append(line.strip())
            blank = False
        elif not blank:
            compact.append('')
            blank = True
    text = '\n'.join(compact).strip()

    return text or str(raw_text or '').strip()


def _should_use_direct_web_reply(user_message, ws_items=None, ws_raw=''):
    """
    对事实型实时问题，直接返回联网答案，避免二次模型改写后丢失关键数值。
    """
    text = str(user_message or '').strip()
    if not text or not str(ws_raw or '').strip():
        return False
    hints = [
        '天气', '温度', '多少度', '几度', '现在多少', '当前多少',
        '今日股价', '现在股价', '最新股价', '最新价格',
        '汇率', '油价', 'shibor', 'do007', '现在几点', '今天日期'
    ]
    if any(x in text for x in hints):
        return True
    short_fact = len(text) <= 26 and any(x in text for x in ['多少', '几', '最新', '现在', '今日'])
    if short_fact and (ws_items or []):
        return True
    return False


def ai_chat_direct(
        user_message,
        history=None,
        enable_search=True,
        context_text='',
        context_file_ids=None,
        model_name=''
):
    """
    直连 Qwen 对话（与板块热点分析同链路）。
    返回结构化对象，便于前端展示模型与模式。
    """
    if not QWEN_API_KEY:
        return {
            'reply': '请在 config.py 中设置 QWEN_API_KEY 后使用此功能。',
            'model': '',
            'enable_search': bool(enable_search),
            'mode': 'qwen_direct',
        }

    resolved_model = normalize_model_name(
        model_name or get_module_model('ai_chat', default=AI_CHAT_MODEL)
    )
    direct_prompt = get_prompt(AI_CHAT_PROMPT_MODULE, 'direct', SYSTEM_PROMPT)
    messages = [{'role': 'system', 'content': direct_prompt}]

    if history:
        for h in history[-24:]:
            messages.append({'role': h.get('role', 'user'), 'content': h.get('content', '')})

    extra_context = str(context_text or '').strip()
    if extra_context:
        messages.append({
            'role': 'system',
            'content': (
                '你收到了用户上传的外部上下文材料。回答时请优先结合这些材料，'
                '并明确区分“用户上传材料”与“联网搜索信息”。\n\n' + extra_context
            )
        })

    file_ids = []
    for fid in (context_file_ids or []):
        s = str(fid or '').strip()
        if s and s not in file_ids:
            file_ids.append(s)
    file_mode = bool(file_ids)
    if file_mode:
        refs = ['fileid://%s' % fid for fid in file_ids]
        messages.append({
            'role': 'system',
            'content': ','.join(refs)
        })
        messages.append({
            'role': 'system',
            'content': (
                '以上 fileid 是用户上传并通过官方 file-extract 解析的材料。'
                '请严格基于这些材料回答，并标注“来源: fileid://...”'
            )
        })

    ws_items = []
    ws_error = ''
    ws_raw = ''
    if bool(enable_search):
        ws = _collect_web_search_snapshot(user_message, model_name=resolved_model)
        ws_items = _normalize_web_items((ws or {}).get('items') or [], limit=6)
        ws_error = str((ws or {}).get('error') or '').strip()
        ws_raw = str((ws or {}).get('raw') or '').strip()
        if (not ws_items) and ws_raw:
            ws_items = _normalize_web_items(_extract_urls_from_text(ws_raw, limit=6), limit=6)
        if (not file_mode) and _should_use_direct_web_reply(user_message, ws_items, ws_raw):
            direct_reply = _append_web_sources_to_reply(ws_raw, ws_items, limit=6)
            return {
                'reply': direct_reply,
                'model': str((ws or {}).get('model') or resolved_model),
                'enable_search': True,
                'mode': 'web_search_direct',
                'search_links': ws_items,
                'search_error': ws_error,
            }
        ws_packet = _format_web_search_packet_for_prompt(ws_raw, ws_items, limit=6, raw_limit=3200)
        messages.append({
            'role': 'system',
            'content': (
                '已开启联网搜索。请优先依据“系统联网检索结果”回答，不要凭记忆臆测。'
                '回答中需包含“信息来源与时间”小节，并附带可点击链接。'
            )
        })
        if ws_packet:
            messages.append({
                'role': 'system',
                'content': '【系统联网检索结果包】\n' + ws_packet
            })
        elif ws_error:
            messages.append({
                'role': 'system',
                'content': (
                    '【系统联网检索状态】检索失败: %s\n'
                    '若证据不足请明确写“联网数据不足，待补充验证”。'
                ) % ws_error
            })

    messages.append({'role': 'user', 'content': user_message})

    try:
        result = _call_qwen_chat(
            messages=messages,
            temperature=0.7,
            enable_search=False,
            timeout=AI_CHAT_TIMEOUT_SECONDS,
            model_candidates=[AI_CHAT_FILE_MODEL] if file_mode else [resolved_model],
            model_name=(AI_CHAT_FILE_MODEL if file_mode else resolved_model),
        )
        reply_text = result.get('content', '')
        if bool(enable_search):
            reply_text = _append_web_sources_to_reply(reply_text, ws_items, limit=6)
        return {
            'reply': reply_text,
            'model': result.get('model', AI_CHAT_MODEL),
            'enable_search': bool(enable_search) and (not file_mode),
            'mode': 'qwen_direct',
            'search_links': ws_items,
            'search_error': ws_error,
        }
    except requests.exceptions.Timeout:
        return {
            'reply': 'AI 请求超时，请稍后重试。',
            'model': '',
            'enable_search': bool(enable_search),
            'mode': 'qwen_direct',
        }
    except Exception as e:
        return {
            'reply': 'AI 调用失败: ' + str(e),
            'model': '',
            'enable_search': bool(enable_search),
            'mode': 'qwen_direct',
        }


def ai_chat(
        user_message,
        history=None,
        enable_search=True,
        context_text='',
        context_file_ids=None,
        model_name=''
):
    """
    独立 AI 对话（支持联网搜索）。
    兼容旧调用：返回纯文本。
    """
    return (
        ai_chat_direct(
            user_message, history, enable_search,
            context_text=context_text,
            context_file_ids=context_file_ids,
            model_name=model_name,
        ) or {}
    ).get('reply', '')


def ai_chat_grounded(
        user_message,
        history=None,
        enable_search=True,
        context_text='',
        context_file_ids=None,
        model_name=''
):
    """
    带内部数据证据链的 AI 对话：
    - 先采集 AlphaFin 内部实时数据
    - 再让模型基于证据回答
    - 生成可回溯 trace_id
    """
    if not QWEN_API_KEY:
        return '请在 config.py 中设置 QWEN_API_KEY 后使用此功能。'

    trace_id = uuid.uuid4().hex[:12]
    resolved_model = normalize_model_name(
        model_name or get_module_model('ai_chat', default=AI_CHAT_MODEL)
    )
    snapshot = _collect_internal_market_snapshot(
        user_message,
        enable_search=bool(enable_search),
        model_name=resolved_model
    )
    generated_at = snapshot.get('generated_at') or _now_cn().strftime('%Y-%m-%d %H:%M:%S')
    snapshot_text = snapshot.get('context_text', '')
    extra_context = str(context_text or '').strip()
    web_ctx = _extract_snapshot_web_context(snapshot, limit=6, raw_limit=3600) if bool(enable_search) else {
        'items': [], 'raw': '', 'packet': '', 'model': ''
    }
    web_items = list(web_ctx.get('items') or [])
    web_raw = str(web_ctx.get('raw') or '').strip()
    web_packet = str(web_ctx.get('packet') or '').strip()

    if bool(enable_search) and _should_use_direct_web_reply(user_message, web_items, web_raw):
        direct_reply = _append_web_sources_to_reply(web_raw, web_items, limit=6)
        _persist_trace(trace_id, {
            'trace_id': trace_id,
            'created_at': generated_at,
            'user_message': user_message,
            'history_len': len(history or []),
            'enable_search': True,
            'web_search_forced': True,
            'snapshot': snapshot,
            'uploaded_context': extra_context,
            'uploaded_file_ids': list(context_file_ids or []),
            'answer': direct_reply,
            'raw_answer': web_raw,
            'model': str(web_ctx.get('model') or resolved_model),
            'mode': 'web_search_direct',
        })
        return {
            'reply': direct_reply,
            'trace_id': trace_id,
            'trace_url': '/api/ai_chat/trace/%s' % trace_id,
            'snapshot_time': generated_at,
            'model': str(web_ctx.get('model') or resolved_model),
            'enable_search': True,
            'search_links': web_items,
            'mode': 'web_search_direct',
        }

    grounded_prompt = get_prompt(AI_CHAT_PROMPT_MODULE, 'grounded', GROUNDED_SYSTEM_PROMPT)
    messages = [{'role': 'system', 'content': grounded_prompt}]
    if history:
        # 防止上下文过长，保留最近 12 轮
        for h in history[-24:]:
            messages.append({'role': h['role'], 'content': h['content']})
    if snapshot_text:
        messages.append({'role': 'system', 'content': snapshot_text})
    if web_packet:
        messages.append({
            'role': 'system',
            'content': (
                '【系统联网检索结果包】\n' + web_packet +
                '\n\n请优先把它作为外部事实底稿使用，避免遗漏检索到的关键数字、时间和网址。'
            )
        })
    if extra_context:
        messages.append({
            'role': 'system',
            'content': (
                '以下是用户额外上传的上下文材料，可作为补充证据：\n' + extra_context
            )
        })
    file_ids = []
    for fid in (context_file_ids or []):
        s = str(fid or '').strip()
        if s and s not in file_ids:
            file_ids.append(s)
    file_mode = bool(file_ids)
    if file_mode:
        refs = ['fileid://%s' % fid for fid in file_ids]
        messages.append({'role': 'system', 'content': ','.join(refs)})
        messages.append({
            'role': 'system',
            'content': '上述 fileid 为官方解析材料，请将其作为优先证据源。'
        })
    messages.append({
        'role': 'user',
        'content': (
            str(user_message or '') +
            '\n\n请严格基于“内部实时数据快照”回答，并写明关键依据（时间戳+来源）。'
            '\n只输出给用户的最终结论与建议，不要展示“推理过程/验证过程/可验证步骤/证据链”。'
        )
    })

    try:
        result = _call_qwen_chat(
            messages=messages,
            temperature=0.2,
            enable_search=False,
            timeout=AI_CHAT_TIMEOUT_SECONDS,
            model_candidates=[AI_CHAT_FILE_MODEL] if file_mode else [resolved_model],
            model_name=(AI_CHAT_FILE_MODEL if file_mode else resolved_model),
        )
        raw_answer = result.get('content', '')
        model_used = result.get('model', AI_CHAT_MODEL)
        answer = _clean_grounded_answer(raw_answer)
        if bool(enable_search):
            answer = _append_web_sources_to_reply(answer, web_items, limit=6)
        _persist_trace(trace_id, {
            'trace_id': trace_id,
            'created_at': generated_at,
            'user_message': user_message,
            'history_len': len(history or []),
            'enable_search': bool(enable_search),
            'web_search_forced': bool(enable_search),
            'snapshot': snapshot,
            'uploaded_context': extra_context,
            'uploaded_file_ids': file_ids,
            'answer': answer,
            'raw_answer': raw_answer,
            'model': model_used,
        })
        return {
            'reply': answer,
            'trace_id': trace_id,
            'trace_url': '/api/ai_chat/trace/%s' % trace_id,
            'snapshot_time': generated_at,
            'model': model_used,
            'enable_search': bool(enable_search) and (not file_mode),
            'search_links': web_items,
        }
    except requests.exceptions.Timeout:
        _persist_trace(trace_id, {
            'trace_id': trace_id,
            'created_at': generated_at,
            'user_message': user_message,
            'snapshot': snapshot,
            'uploaded_context': extra_context,
            'uploaded_file_ids': file_ids,
            'error': 'timeout',
        })
        return {
            'reply': 'AI 请求超时，请稍后重试。',
            'trace_id': trace_id,
            'trace_url': '/api/ai_chat/trace/%s' % trace_id,
            'snapshot_time': generated_at,
        }
    except Exception as e:
        _persist_trace(trace_id, {
            'trace_id': trace_id,
            'created_at': generated_at,
            'user_message': user_message,
            'snapshot': snapshot,
            'uploaded_context': extra_context,
            'uploaded_file_ids': file_ids,
            'error': str(e),
        })
        return {
            'reply': 'AI 调用失败: %s' % str(e),
            'trace_id': trace_id,
            'trace_url': '/api/ai_chat/trace/%s' % trace_id,
            'snapshot_time': generated_at,
        }
