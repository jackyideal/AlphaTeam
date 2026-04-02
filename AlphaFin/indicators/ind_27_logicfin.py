"""
ind_27 - LogicFin 策略（predict_index 全链路复现版）
复现目标文件: third_party/stockagent/predict_index.py
"""
import base64
import os
import re
import time
from datetime import datetime, timedelta

os.environ.setdefault('MPLCONFIGDIR', '/tmp/matplotlib-cache')
import matplotlib
matplotlib.use('Agg')
import akshare as ak
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle
import numpy as np
import pandas as pd
import requests
import tushare as ts

from AlphaFin.config import (
    BASE_DIR,
    CHART_DIR,
    QWEN_API_KEY,
    QWEN_BASE_URL,
    QWEN_MODEL,
    TUSHARE_TOKEN,
)

plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'Heiti TC']
plt.rcParams['axes.unicode_minus'] = False

pro = ts.pro_api(TUSHARE_TOKEN)


INDICATOR_META = {
    'id': 'ind_27_logicfin',
    'name': 'LogicFin 策略',
    'group': '策略模型',
    'description': '面向指数与市场环境的全链路智能决策引擎：标的识别、技术图谱解析、实时情报聚合、资金结构研判、分阶段决策校准、推理链路可视化与多期限目标区间输出。',
    'input_type': 'query',
    'default_query': '请帮我分析一下上证指数，是否值得投资？',
    'requires_db': False,
    'slow': True,
    'chart_count': 12,
    'chart_descriptions': [
        '深度思考逻辑链路图',
        '最终决策结果',
        '标的行情分析',
        '研究报告分析',
        '资金流分析报告',
        '技术面透析',
        '日K线图展示',
        '买卖决策与目标价格',
        '市场政策信息汇总',
        '市场热点信息汇总',
        '策略构建蓝图',
        '策略效果展示',
    ],
}


_INDEX_FALLBACK = {
    '上证指数': '000001.SH',
    '深证成指': '399001.SZ',
    '创业板指': '399006.SZ',
    '沪深300': '000300.SH',
    '上证50': '000016.SH',
    '中证500': '000905.SH',
}

_TEXT_MODEL = os.getenv('LOGICFIN_TEXT_MODEL', 'qwen3.5-plus')
_TEXT_FALLBACK_MODEL = os.getenv('LOGICFIN_TEXT_FALLBACK_MODEL', 'qwen-plus')
_VISION_MODEL = os.getenv('LOGICFIN_VISION_MODEL', QWEN_MODEL or 'qwen-vl-plus')
_STOCK_BASIC_CACHE = None
_INDEX_BASIC_CACHE = None


def _call_qwen(messages, model=None, enable_search=False, temperature=0.1, max_tokens=6000, timeout=150):
    if not QWEN_API_KEY:
        return {'ok': False, 'content': '未配置 QWEN_API_KEY', 'raw': None, 'error': 'missing_key'}
    url = QWEN_BASE_URL.rstrip('/') + '/chat/completions'
    model_candidates = []
    for m in (model or _TEXT_MODEL, _TEXT_FALLBACK_MODEL):
        mm = str(m or '').strip()
        if mm and mm not in model_candidates:
            model_candidates.append(mm)
    headers = {
        'Authorization': 'Bearer ' + QWEN_API_KEY,
        'Content-Type': 'application/json',
    }
    last_err = ''
    for idx, use_model in enumerate(model_candidates):
        payload = {
            'model': use_model,
            'messages': messages,
            'temperature': temperature,
            'max_tokens': max_tokens,
        }
        if enable_search:
            payload['enable_search'] = True
            payload['search_options'] = {'forced_search': True}
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
            if not (200 <= resp.status_code < 300):
                txt = (resp.text or '').replace('\n', ' ')[:260]
                last_err = f'HTTP {resp.status_code}: {txt}'
                if idx < len(model_candidates) - 1:
                    continue
                return {'ok': False, 'content': f'LLM调用失败({last_err})', 'raw': None, 'error': last_err}
            data = resp.json()
            choices = data.get('choices') or []
            if not choices:
                last_err = 'empty_choices'
                if idx < len(model_candidates) - 1:
                    continue
                return {'ok': False, 'content': 'LLM返回空 choices', 'raw': data, 'error': 'empty_choices'}
            msg = choices[0].get('message') or {}
            content = msg.get('content', '')
            if isinstance(content, list):
                texts = []
                for c in content:
                    if isinstance(c, dict) and c.get('text'):
                        texts.append(str(c.get('text')))
                    elif isinstance(c, str):
                        texts.append(c)
                content = '\n'.join(texts).strip()
            elif not isinstance(content, str):
                content = str(content)
            return {'ok': True, 'content': content.strip(), 'raw': data, 'error': ''}
        except Exception as e:
            last_err = str(e)
            if idx < len(model_candidates) - 1:
                continue
            return {'ok': False, 'content': f'LLM调用异常: {str(e)}', 'raw': None, 'error': str(e)}
    return {'ok': False, 'content': f'LLM调用失败: {last_err}', 'raw': None, 'error': last_err}


def _load_stock_basic():
    global _STOCK_BASIC_CACHE
    if _STOCK_BASIC_CACHE is None:
        try:
            df = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name')
        except Exception:
            df = pd.DataFrame(columns=['ts_code', 'symbol', 'name'])
        if df is None:
            df = pd.DataFrame(columns=['ts_code', 'symbol', 'name'])
        _STOCK_BASIC_CACHE = df.fillna('')
    return _STOCK_BASIC_CACHE


def _load_index_basic():
    global _INDEX_BASIC_CACHE
    if _INDEX_BASIC_CACHE is None:
        frames = []
        for market in ('SSE', 'SZSE'):
            try:
                df = pro.index_basic(market=market)
            except Exception:
                df = pd.DataFrame()
            if df is not None and not df.empty:
                frames.append(df[['ts_code', 'name']].copy())
        if frames:
            merged = pd.concat(frames, ignore_index=True).drop_duplicates(subset=['ts_code', 'name'])
        else:
            merged = pd.DataFrame(
                [{'ts_code': code, 'name': name} for name, code in _INDEX_FALLBACK.items()]
            )
        _INDEX_BASIC_CACHE = merged.fillna('')
    return _INDEX_BASIC_CACHE


def _default_target():
    return {
        'kind': 'index',
        'name': '上证指数',
        'ts_code': '000001.SH',
        'symbol': '000001',
        'display_name': '上证指数(000001.SH)',
    }


def _build_target(kind, name, ts_code, symbol=''):
    safe_name = str(name or '').strip() or str(ts_code or '').strip()
    safe_code = str(ts_code or '').upper().strip()
    safe_symbol = str(symbol or safe_code.split('.')[0]).strip()
    return {
        'kind': kind,
        'name': safe_name,
        'ts_code': safe_code,
        'symbol': safe_symbol,
        'display_name': f'{safe_name}({safe_code})' if safe_code else safe_name,
    }


def _pick_name_match(df, question):
    if df is None or df.empty:
        return None
    matches = df[df['name'].apply(lambda x: str(x) and str(x) in question)].copy()
    if matches.empty:
        return None
    matches['_len'] = matches['name'].astype(str).str.len()
    matches = matches.sort_values(['_len', 'name'], ascending=[False, True])
    return matches.iloc[0]


def _resolve_target(task):
    q = str(task or '').strip()
    if not q:
        return _default_target()

    upper_q = q.upper()
    stock_df = _load_stock_basic()
    index_df = _load_index_basic()

    # 1) 优先识别完整 ts_code
    code_match = re.search(r'\b(\d{6}\.(?:SH|SZ))\b', upper_q)
    if code_match:
        ts_code = code_match.group(1).upper()
        m = stock_df[stock_df['ts_code'].astype(str).str.upper() == ts_code]
        if not m.empty:
            row = m.iloc[0]
            return _build_target('stock', row['name'], row['ts_code'], row['symbol'])
        m = index_df[index_df['ts_code'].astype(str).str.upper() == ts_code]
        if not m.empty:
            row = m.iloc[0]
            return _build_target('index', row['name'], row['ts_code'])

    # 2) 识别 6 位代码
    symbol_match = re.search(r'\b(\d{6})\b', upper_q)
    if symbol_match:
        symbol = symbol_match.group(1)
        m = stock_df[stock_df['symbol'].astype(str) == symbol]
        if not m.empty:
            row = m.iloc[0]
            return _build_target('stock', row['name'], row['ts_code'], row['symbol'])

    # 3) 优先识别股票名称，避免回退到上证指数
    stock_match = _pick_name_match(stock_df, q)
    if stock_match is not None:
        return _build_target('stock', stock_match['name'], stock_match['ts_code'], stock_match['symbol'])

    # 4) 常见指数别名
    for name, ts_code in _INDEX_FALLBACK.items():
        if name in q:
            return _build_target('index', name, ts_code)

    # 5) 指数名称兜底
    index_match = _pick_name_match(index_df, q)
    if index_match is not None:
        return _build_target('index', index_match['name'], index_match['ts_code'])

    return _default_target()


def _render_text_figure(text, title, figsize=(22, 12), font_size=15):
    fig, ax = plt.subplots(figsize=figsize, facecolor='white')
    ax.axis('off')
    ax.set_title(title, fontsize=max(20, int(font_size + 6)), fontweight='bold', pad=14)
    src = str(text or '（无内容）')
    lines = []
    for line in src.split('\n'):
        if len(line) <= 64:
            lines.append(line)
        else:
            while len(line) > 64:
                lines.append(line[:64])
                line = line[64:]
            if line:
                lines.append(line)
    if len(lines) > 90:
        lines = lines[:90] + ['...（内容过长，已截断）']
    ax.text(0.02, 0.97, '\n'.join(lines), transform=ax.transAxes,
            va='top', ha='left', fontsize=font_size, linespacing=1.65)
    fig.tight_layout(rect=(0.01, 0.01, 0.99, 0.96))
    return fig


def _image_to_figure(image_path, title, figsize=(18, 10)):
    if not image_path or not os.path.exists(image_path):
        return _render_text_figure(f'图片不存在: {image_path}', title, figsize=figsize)
    try:
        from PIL import Image
        img = Image.open(image_path)
        fig, ax = plt.subplots(figsize=figsize, facecolor='white')
        ax.imshow(np.array(img))
        ax.axis('off')
        ax.set_title(title, fontsize=20, fontweight='bold', pad=12)
        fig.tight_layout(rect=(0.01, 0.01, 0.99, 0.95))
        return fig
    except Exception as e:
        return _render_text_figure(f'图片加载失败: {str(e)}', title, figsize=figsize)


def _build_dynamic_kline(target):
    """为股票或指数实时生成最新日线K线图。"""
    today = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=520)).strftime('%Y%m%d')

    try:
        if target['kind'] == 'stock':
            df = ts.pro_bar(
                ts_code=target['ts_code'],
                start_date=start_date,
                end_date=today,
                freq='D',
                adj='qfq',
            )
        else:
            df = ts.pro_bar(
                ts_code=target['ts_code'],
                asset='I',
                start_date=start_date,
                end_date=today,
            )
    except Exception:
        df = pd.DataFrame()

    if df is None or df.empty:
        return ''

    df = df.sort_values('trade_date').reset_index(drop=True).tail(220).copy()
    for col in ('open', 'high', 'low', 'close', 'vol', 'amount'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    if 'vol' not in df.columns or df['vol'].isna().all():
        df['vol'] = pd.to_numeric(df.get('amount'), errors='coerce')
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.dropna(subset=['open', 'high', 'low', 'close']).reset_index(drop=True)
    if df.empty:
        return ''

    df['ma5'] = df['close'].rolling(5).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()
    ema_fast = df['close'].ewm(span=12, adjust=False).mean()
    ema_slow = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = ema_fast - ema_slow
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal']
    low_min = df['low'].rolling(9).min()
    high_max = df['high'].rolling(9).max()
    spread = (high_max - low_min).replace(0, np.nan)
    rsv = (df['close'] - low_min) / spread * 100
    df['k'] = rsv.ewm(alpha=1/3, adjust=False).mean()
    df['d'] = df['k'].ewm(alpha=1/3, adjust=False).mean()
    df['j'] = 3 * df['k'] - 2 * df['d']

    fig = plt.figure(figsize=(18, 13), facecolor='white')
    gs = fig.add_gridspec(4, 1, height_ratios=[3.6, 1.2, 1.2, 1.2], hspace=0.06)
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    ax3 = fig.add_subplot(gs[2], sharex=ax1)
    ax4 = fig.add_subplot(gs[3], sharex=ax1)

    x = np.arange(len(df))
    up_color = '#ef4444'
    down_color = '#10b981'
    candle_colors = np.where(df['close'] >= df['open'], up_color, down_color)

    for idx, row in df.iterrows():
        color = up_color if row['close'] >= row['open'] else down_color
        ax1.vlines(x[idx], row['low'], row['high'], color=color, linewidth=1.0, alpha=0.95)
        lower = min(row['open'], row['close'])
        height = max(abs(row['close'] - row['open']), 0.01)
        ax1.add_patch(Rectangle((x[idx] - 0.32, lower), 0.64, height, facecolor=color, edgecolor=color, alpha=0.82))

    ax1.plot(x, df['ma5'], label='MA5', color='#2563eb', linewidth=1.4)
    ax1.plot(x, df['ma20'], label='MA20', color='#f59e0b', linewidth=1.4)
    ax1.plot(x, df['ma60'], label='MA60', color='#7c3aed', linewidth=1.4)
    ax1.set_title(f"{target['display_name']} 最新日线K线图", fontsize=18, fontweight='bold', pad=10)
    ax1.legend(loc='upper left', fontsize=10)
    ax1.grid(alpha=0.18)

    ax2.bar(x, df['vol'].fillna(0), color=candle_colors, alpha=0.7)
    ax2.set_ylabel('VOL', fontsize=10)
    ax2.grid(alpha=0.15)

    macd_bar_colors = np.where(df['macd_hist'] >= 0, '#fca5a5', '#86efac')
    ax3.bar(x, df['macd_hist'].fillna(0), color=macd_bar_colors, alpha=0.7, label='MACD Hist')
    ax3.plot(x, df['macd'], color='#2563eb', linewidth=1.4, label='MACD')
    ax3.plot(x, df['macd_signal'], color='#f97316', linewidth=1.4, label='Signal')
    ax3.set_ylabel('MACD', fontsize=10)
    ax3.legend(loc='upper left', fontsize=9)
    ax3.grid(alpha=0.15)

    ax4.plot(x, df['k'], color='#0ea5e9', linewidth=1.4, label='K')
    ax4.plot(x, df['d'], color='#a855f7', linewidth=1.4, label='D')
    ax4.plot(x, df['j'], color='#ef4444', linewidth=1.2, label='J')
    ax4.set_ylabel('KDJ', fontsize=10)
    ax4.legend(loc='upper left', fontsize=9)
    ax4.grid(alpha=0.15)

    tick_count = min(8, len(df))
    tick_idx = sorted(set(np.linspace(0, len(df) - 1, num=tick_count, dtype=int).tolist()))
    tick_labels = [df['trade_date'].dt.strftime('%Y-%m-%d').iloc[i] for i in tick_idx]
    ax4.set_xticks(tick_idx)
    ax4.set_xticklabels(tick_labels, rotation=35, ha='right', fontsize=9)
    plt.setp(ax1.get_xticklabels(), visible=False)
    plt.setp(ax2.get_xticklabels(), visible=False)
    plt.setp(ax3.get_xticklabels(), visible=False)

    os.makedirs(CHART_DIR, exist_ok=True)
    save_path = os.path.join(CHART_DIR, f"logicfin_kline_{target['ts_code'].replace('.', '_')}_{int(time.time())}.png")
    fig.subplots_adjust(left=0.06, right=0.98, top=0.96, bottom=0.10)
    fig.savefig(save_path, dpi=170, bbox_inches='tight')
    plt.close(fig)
    return save_path


def _vision_kline_read(target):
    """实时生成最新日线图，并调用视觉模型做技术面解读。"""
    image_path = _build_dynamic_kline(target)
    if not image_path or not os.path.exists(image_path):
        return '', 'K线图生成失败，无法执行技术面解读。'

    try:
        with open(image_path, 'rb') as f:
            base64_image = base64.b64encode(f.read()).decode('utf-8')
    except Exception as e:
        return image_path, f'K线图读取失败: {str(e)}'

    content = [
        {'type': 'image_url', 'image_url': {'url': f'data:image/png;base64,{base64_image}'}},
        {
            'type': 'text',
            'text': (
                f"请根据提供的{target['kind'] == 'stock' and '股票' or '指数'} "
                f"{target['display_name']} 最新日线走势图，进行详细技术分析和走势预判。"
                '请覆盖K线形态、均线、成交量、MACD、KDJ，并给出短中期风险与机会结论。'
            )
        }
    ]
    resp = _call_qwen(
        messages=[{'role': 'system', 'content': '你是专业金融技术分析师。'}, {'role': 'user', 'content': content}],
        model=_VISION_MODEL,
        enable_search=False,
        temperature=0.1,
        max_tokens=3000,
        timeout=120,
    )
    return os.path.abspath(image_path), resp.get('content', '')


def _kimi_web_search_report(task, target):
    prompt = (
        f'今天的日期是{datetime.now().strftime("%Y%m%d")}。\n'
        f"请搜索{target['display_name']}最近一个季度的研究观点、券商解读或市场评论，展示链接与发布时间；"
        '仅保留最近三个月内的内容，并给出详细总结。'
    )
    resp = _call_qwen(
        messages=[{'role': 'system', 'content': '你是金融分析师。'}, {'role': 'user', 'content': prompt}],
        enable_search=True,
        model=_TEXT_MODEL,
        temperature=0.1,
        max_tokens=5000,
        timeout=150,
    )
    return resp.get('content', '')


def _kimi_web_highlight(target):
    prompt = (
        f'今天日期是{datetime.now().strftime("%Y%m%d")}。'
        f"请围绕{target['display_name']}搜索A股市场热点板块、相关新闻与资金流向，"
        '给出新闻链接、板块联动关系和后续关注方向。'
    )
    resp = _call_qwen(
        messages=[{'role': 'system', 'content': '你是金融分析师。'}, {'role': 'user', 'content': prompt}],
        enable_search=True,
        model=_TEXT_MODEL,
        temperature=0.1,
        max_tokens=4500,
        timeout=150,
    )
    return resp.get('content', '')


def _kimi_web_market_feeling(task):
    prompt = (
        f'今天日期是{datetime.now().strftime("%Y%m%d")}。'
        '请搜索中国资本市场最新信息（政策、宏观、地缘、行业），'
        '并给出链接与市场情绪判断（积极/中性/消极）。'
    )
    resp = _call_qwen(
        messages=[{'role': 'system', 'content': '你是金融分析师。'}, {'role': 'user', 'content': prompt}],
        enable_search=True,
        model=_TEXT_MODEL,
        temperature=0.1,
        max_tokens=4500,
        timeout=150,
    )
    return resp.get('content', '')


def _fetch_money_data(target):
    """股票走个股+上证联合资金面；指数走上证融资融券资金面。"""
    today = datetime.now().strftime('%Y%m%d')
    if target['kind'] == 'stock':
        try:
            stock_data = ts.pro_bar(ts_code=target['ts_code'], start_date='20240601', end_date=today, freq='D', adj='qfq')
            stock_data = (stock_data if stock_data is not None else pd.DataFrame()).sort_values('trade_date').reset_index(drop=True)
            stock_margin = pro.margin_detail(ts_code=target['ts_code'], start_date='20240601', end_date=today)
            stock_margin = (stock_margin if stock_margin is not None else pd.DataFrame()).sort_values('trade_date').reset_index(drop=True)

            index_data = pro.index_daily(ts_code='000001.SH', start_date='20240601', end_date=today)
            index_data = (index_data if index_data is not None else pd.DataFrame()).sort_values('trade_date').reset_index(drop=True)
            index_margin = pro.margin(start_date='20240601', end_date=today)
            index_margin = index_margin if index_margin is not None else pd.DataFrame()
            if not index_margin.empty and 'exchange_id' in index_margin.columns:
                index_margin = index_margin[index_margin['exchange_id'] == 'SSE'].drop(columns=['exchange_id'])

            stock_block = pd.DataFrame()
            if not stock_data.empty and not stock_margin.empty:
                stock_block = pd.merge(
                    stock_data[['trade_date', 'close', 'amount']],
                    stock_margin[['trade_date', 'rzye', 'rzmre', 'rqye', 'rzche']],
                    on='trade_date',
                    how='left'
                ).tail(60)

            index_block = pd.DataFrame()
            if not index_data.empty and not index_margin.empty:
                index_block = pd.merge(
                    index_data[['trade_date', 'close']],
                    index_margin[['trade_date', 'rzye', 'rzmre', 'rqye', 'rzche']],
                    on='trade_date',
                    how='left'
                ).tail(60)
        except Exception as e:
            return f"资金面数据获取失败: {str(e)}"

        prompt = f"你是一个优秀的金融分析师。以下是{target['display_name']}与上证指数的近期资金面数据：\n"
        if stock_block.empty:
            prompt += f"{target['display_name']}近期没有可用的融资融券明细，请在分析中明确说明数据不足。\n"
        else:
            for _, row in stock_block.iterrows():
                prompt += (
                    f"个股 日期{row['trade_date']}，收盘{row['close']}，成交额{row.get('amount', np.nan)}，"
                    f"融资余额{row.get('rzye', np.nan)}，融资买入额{row.get('rzmre', np.nan)}，"
                    f"融券余额{row.get('rqye', np.nan)}，融券卖出量{row.get('rzche', np.nan)}。\n"
                )
        if index_block.empty:
            prompt += "上证指数近期没有可用的融资融券汇总，请在分析中明确说明数据不足。\n"
        else:
            for _, row in index_block.iterrows():
                prompt += (
                    f"上证指数 日期{row['trade_date']}，收盘{row['close']}，融资余额{row.get('rzye', np.nan)}，"
                    f"融资买入额{row.get('rzmre', np.nan)}，融券余额{row.get('rqye', np.nan)}，"
                    f"融券卖出量{row.get('rzche', np.nan)}。\n"
                )
        prompt += "请对个股与市场的近期资金面强弱、是否过热、是否存在风险背离做出清晰分析。"
    else:
        try:
            index_data = pro.index_daily(ts_code='000001.SH', start_date='20221201', end_date=today)
            margin = pro.margin(start_date='20221201', end_date=today)
            margin = margin[margin['exchange_id'] == 'SSE'].drop(columns=['exchange_id'])
            merged = pd.merge(
                index_data[['trade_date', 'close']],
                margin[['trade_date', 'rzye', 'rzmre', 'rqye', 'rzche']],
                on='trade_date', how='left'
            ).sort_values('trade_date').reset_index(drop=True)
        except Exception as e:
            return f'资金面数据获取失败: {str(e)}'

        prompt = "你是一个优秀的金融分析师。以下是上证指数资金面数据：\n"
        for _, row in merged.tail(120).iterrows():
            prompt += (
                f"日期{row['trade_date']}，收盘{row['close']}，融资余额{row['rzye']}，"
                f"融资买入额{row['rzmre']}，融券余额{row['rqye']}，融券卖出量{row['rzche']}。\n"
            )
        prompt += "请分析近期资金面是否过热，并给出风险与机会。"

    resp = _call_qwen(
        messages=[{'role': 'user', 'content': prompt}],
        enable_search=False,
        model=_TEXT_MODEL,
        temperature=0.1,
        max_tokens=3500,
        timeout=120,
    )
    return resp.get('content', '')


def _fetch_latest_daily_basic(ts_code):
    today = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y%m%d')
    try:
        df = pro.daily_basic(
            ts_code=ts_code,
            start_date=start_date,
            end_date=today,
            fields='ts_code,trade_date,pe,pb,turnover_rate,volume_ratio,total_mv,circ_mv',
        )
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty:
        return pd.Series(dtype=object)
    return df.sort_values('trade_date').iloc[-1]


def _analyse_a_market(target):
    """
    兼容股票/指数的标的分析：
    - 指数：输出市场环境分析
    - 股票：输出个股 + 行业 + 市场相对强弱分析
    """
    today = datetime.now().strftime('%Y%m%d')
    start_date = '20220101'
    if target['kind'] == 'stock':
        try:
            trade = ts.pro_bar(ts_code=target['ts_code'], start_date='20240101', end_date=today, freq='D', adj='qfq')
            trade = (trade if trade is not None else pd.DataFrame()).sort_values('trade_date').reset_index(drop=True)
            if trade.empty:
                return f"{target['display_name']} 标的数据获取失败。"

            for col in ('close', 'amount'):
                if col in trade.columns:
                    trade[col] = pd.to_numeric(trade[col], errors='coerce')
            trade['trade_date'] = pd.to_datetime(trade['trade_date'], format='%Y%m%d')
            trade['10_day_return'] = trade['close'].pct_change(10).fillna(0)
            trade['30_day_return'] = trade['close'].pct_change(30).fillna(0)
            trade['60_day_return'] = trade['close'].pct_change(60).fillna(0)
            trade['ma_5'] = trade['close'].rolling(window=5).mean()
            trade['ma_20'] = trade['close'].rolling(window=20).mean()
            trade['ma_60'] = trade['close'].rolling(window=60).mean()
            latest = trade.iloc[-1]

            basic = _fetch_latest_daily_basic(target['ts_code'])

            index_daily = pro.index_daily(ts_code='000001.SH', start_date='20240101', end_date=today)
            index_daily = (index_daily if index_daily is not None else pd.DataFrame()).sort_values('trade_date').reset_index(drop=True)
            relative_20d = np.nan
            if not index_daily.empty:
                index_daily['trade_date'] = pd.to_datetime(index_daily['trade_date'], format='%Y%m%d')
                index_daily['close'] = pd.to_numeric(index_daily['close'], errors='coerce')
                merged = pd.merge(
                    trade[['trade_date', 'close']],
                    index_daily[['trade_date', 'close']].rename(columns={'close': 'index_close'}),
                    on='trade_date',
                    how='inner'
                )
                if len(merged) > 20:
                    relative_20d = (
                        merged['close'].pct_change(20).iloc[-1] - merged['index_close'].pct_change(20).iloc[-1]
                    ) * 100

            industry_name = '未知行业'
            industry_summary = '行业数据不足'
            try:
                members = pro.index_member_all(l1_code='')
                if members is not None and not members.empty:
                    row = members[members['ts_code'] == target['ts_code']]
                    if not row.empty:
                        row = row.iloc[0]
                        industry_name = str(row.get('l1_name') or '未知行业')
                        l1_code = str(row.get('l1_code') or '')
                        if l1_code:
                            sw = pro.sw_daily(ts_code=l1_code, fields='')
                            sw = (sw if sw is not None else pd.DataFrame()).sort_values('trade_date').reset_index(drop=True)
                            if not sw.empty and 'pct_change' in sw.columns:
                                sw['pct_change'] = pd.to_numeric(sw['pct_change'], errors='coerce')
                                industry_summary = (
                                    f"行业近10日累计涨跌幅 {sw['pct_change'].tail(10).sum():.2f}% , "
                                    f"近30日累计涨跌幅 {sw['pct_change'].tail(30).sum():.2f}%"
                                )
            except Exception:
                pass
        except Exception as e:
            return f'标的分析失败: {str(e)}'

        prompt = (
            f"你是一个优秀的金融分析师。以下是股票 {target['display_name']} 的近期核心数据：\n"
            f"最新收盘价: {latest['close']:.2f}\n"
            f"最近10天涨幅: {latest['10_day_return'] * 100:.2f}%\n"
            f"最近30天涨幅: {latest['30_day_return'] * 100:.2f}%\n"
            f"最近60天涨幅: {latest['60_day_return'] * 100:.2f}%\n"
            f"均线: MA5={latest['ma_5']:.2f}, MA20={latest['ma_20']:.2f}, MA60={latest['ma_60']:.2f}\n"
            f"近20日相对上证超额收益: {relative_20d:.2f}%\n"
            f"所属一级行业: {industry_name}\n"
            f"行业观察: {industry_summary}\n"
            f"最新估值: PE={basic.get('pe', np.nan)}, PB={basic.get('pb', np.nan)}, 换手率={basic.get('turnover_rate', np.nan)}, 量比={basic.get('volume_ratio', np.nan)}\n"
            "请基于以上真实数据，对该股票后续走势、相对强弱、估值位置和主要风险做出清晰判断。"
        )
        resp = _call_qwen(
            messages=[{'role': 'user', 'content': prompt}],
            enable_search=False,
            model=_TEXT_MODEL,
            temperature=0.1,
            max_tokens=3500,
            timeout=120,
        )
        return resp.get('content', '')

    code = '000001.SH'
    try:
        df1 = pro.index_dailybasic(ts_code=code, start_date=start_date, end_date=today, fields='ts_code,trade_date,pe')
        df2 = ts.pro_bar(ts_code=code, asset='I', start_date=start_date, end_date=today)
        df1 = df1[['ts_code', 'trade_date', 'pe']]
        df2 = df2[['ts_code', 'trade_date', 'close', 'amount']]
        df1['trade_date'] = pd.to_datetime(df1['trade_date'], format='%Y%m%d')
        df2['trade_date'] = pd.to_datetime(df2['trade_date'], format='%Y%m%d')
        df3 = pd.merge(df1, df2, on=['ts_code', 'trade_date']).sort_values('trade_date').reset_index(drop=True)

        dd1 = pro.index_dailybasic(ts_code='000001.SH')
        dd1['trade_date'] = pd.to_datetime(dd1['trade_date'], format='%Y%m%d')
        dd1 = dd1.sort_values('trade_date').reset_index(drop=True)
        dd1['1/PE'] = 1 / pd.to_numeric(dd1['pe_ttm'], errors='coerce')

        dd2 = ak.bond_zh_us_rate()
        dd2 = dd2[['日期', '中国国债收益率10年', '美国国债收益率10年']].sort_values('日期').reset_index(drop=True)
        dd2['trade_date'] = pd.to_datetime(dd2['日期'], format='%Y-%m-%d')
        dd2 = dd2.ffill()

        dd = pd.concat([
            dd1[['1/PE', 'trade_date']].set_index('trade_date'),
            dd2[['中国国债收益率10年', '美国国债收益率10年', 'trade_date']].set_index('trade_date')
        ], axis=1)
        dd['上证估值与中国国债收益率差值'] = dd['1/PE'] - pd.to_numeric(dd['中国国债收益率10年'], errors='coerce') / 100
        dd['上证估值与美国国债收益率差值'] = dd['1/PE'] - pd.to_numeric(dd['美国国债收益率10年'], errors='coerce') / 100
        dd = dd[['1/PE', '上证估值与中国国债收益率差值', '上证估值与美国国债收益率差值']].astype(float).bfill()
        dd = dd[dd.index >= pd.Timestamp('2013-01-01')]

        df4 = pd.merge(df3, dd.reset_index(), on=['trade_date']).sort_values('trade_date').reset_index(drop=True)
        df4['10_day_return'] = df4['close'].pct_change(10).fillna(0)
        df4['30_day_return'] = df4['close'].pct_change(30).fillna(0)
        df4['60_day_return'] = df4['close'].pct_change(60).fillna(0)
        df4['ma_5'] = df4['close'].rolling(window=5).mean()
        df4['ma_10'] = df4['close'].rolling(window=10).mean()
        df4['ma_20'] = df4['close'].rolling(window=20).mean()
        df4['ma_60'] = df4['close'].rolling(window=60).mean()
        df4['amount_5'] = df4['amount'].rolling(window=5).mean()
        df4['amount_10'] = df4['amount'].rolling(window=10).mean()
        df4['amount_60'] = df4['amount'].rolling(window=60).mean()
        latest = df4.iloc[-1]
        avg_diff = dd['上证估值与中国国债收益率差值'].mean()
        valuation_status = "偏高" if latest['上证估值与中国国债收益率差值'] > avg_diff else "偏低"
        cross_status = "金叉" if latest['ma_5'] > latest['ma_20'] else ("死叉" if latest['ma_5'] < latest['ma_20'] else "无明显趋势")
    except Exception as e:
        return f'大盘数据分析失败: {str(e)}'

    prompt = (
        "你是一个优秀的金融分析师。以下是当前A股市场环境（以上证指数为代表）的近期数据：\n"
        f"当前点位: {latest['close']}\n"
        f"最近10天涨幅: {latest['10_day_return'] * 100:.2f}%\n"
        f"最近30天涨幅: {latest['30_day_return'] * 100:.2f}%\n"
        f"最近60天涨幅: {latest['60_day_return'] * 100:.2f}%\n"
        f"均线: MA5={latest['ma_5']:.2f}, MA10={latest['ma_10']:.2f}, MA20={latest['ma_20']:.2f}, MA60={latest['ma_60']:.2f}\n"
        f"均线状态: {cross_status}\n"
        f"成交额: 当日={latest['amount']:.2f}, 5日均值={latest['amount_5']:.2f}, 10日均值={latest['amount_10']:.2f}, 60日均值={latest['amount_60']:.2f}\n"
        f"股债差值: 当前={latest['上证估值与中国国债收益率差值']:.4f}, 历史均值={avg_diff:.4f}, 估值状态={valuation_status}\n"
        "请给出明确的风险/机会判断，并说明核心依据。"
    )
    resp = _call_qwen(
        messages=[{'role': 'user', 'content': prompt}],
        enable_search=False,
        model=_TEXT_MODEL,
        temperature=0.1,
        max_tokens=3500,
        timeout=120,
    )
    return resp.get('content', '')


def _decision_tool(target, target_analysis, k_line_prompt, a_share_report, market_feeling, industry_prefrence, money_report):
    prompt = (
        f"你是专业金融分析师。请基于以下信息给出对{target['display_name']}的买入/卖出/观望决策并解释推理：\n\n"
        f"标的分析: {target_analysis}\n\n"
        f"资金面: {money_report}\n\n"
        f"K线技术面: {k_line_prompt}\n\n"
        f"研究报告: {a_share_report}\n\n"
        f"市场热点: {industry_prefrence}\n\n"
        f"政策与市场情绪: {market_feeling}\n\n"
        "请按步骤推理并给明确结论。"
    )
    resp = _call_qwen(
        messages=[{'role': 'user', 'content': prompt}],
        enable_search=False,
        model=_TEXT_MODEL,
        temperature=0.1,
        max_tokens=4500,
        timeout=120,
    )
    return resp.get('content', '')


def _final_reflection(decision_text):
    prompt = (
        "请对以下初步决策进行反思：\n"
        f"{decision_text}\n\n"
        "1) 评估优点与不足；2) 预测未来一个月走势；3) 给出改进建议。"
    )
    resp = _call_qwen(
        messages=[{'role': 'user', 'content': prompt}],
        model=_TEXT_MODEL,
        temperature=0.1,
        max_tokens=3500,
        timeout=120,
    )
    return resp.get('content', '')


def _final_decision1(target, target_analysis, k_line_prompt, full_result, a_share_report, market_feeling,
                     industry_prefrence, money_report, reflection, task):
    prompt = (
        f"基于以下信息与反思意见，请重构对{target['display_name']}的综合决策，并给出目标价格或目标点位：\n"
        f"市场新闻: {market_feeling}\n"
        f"标的分析: {target_analysis}\n"
        f"研究报告: {a_share_report}\n"
        f"资金面: {money_report}\n"
        f"热点: {industry_prefrence}\n"
        f"K线: {k_line_prompt}\n"
        f"初步决策: {full_result}\n"
        f"反思意见: {reflection}\n\n"
        "请输出更完善的最终分析文本。"
    )
    resp = _call_qwen(
        messages=[{'role': 'user', 'content': prompt}],
        model=_TEXT_MODEL,
        temperature=0.1,
        max_tokens=5000,
        timeout=140,
    )
    content = resp.get('content', '')
    reasoning = content  # Qwen 无统一 reasoning_content 字段时，用内容本体替代
    raw = resp.get('raw') or {}
    try:
        msg = ((raw.get('choices') or [{}])[0].get('message') or {})
        reasoning = str(msg.get('reasoning_content') or content)
    except Exception:
        pass
    return content, reasoning


def _final_decision2(target, final_decision_text):
    prompt = (
        f"基于以下最终分析报告，判断{target['display_name']}短期/中期/长期是否买入，并给出目标价格或目标点位。\n"
        f"{final_decision_text}\n\n"
        "请严格按格式输出：\n"
        "短期买卖决策：\"\"，短期目标价格：\"\"；\n"
        "中期买卖决策：\"\"，中期目标价格：\"\"；\n"
        "长期买卖决策：\"\"，长期目标价格：\"\"；"
    )
    resp = _call_qwen(
        messages=[{'role': 'user', 'content': prompt}],
        model=_TEXT_MODEL,
        temperature=0.1,
        max_tokens=1200,
        timeout=90,
    )
    return resp.get('content', '')


def _normalize_decision_text(src):
    txt = str(src or '').strip()
    if not txt:
        return ''
    return (
        txt.replace('“', '"').replace('”', '"')
        .replace('‘', "'").replace('’', "'")
        .replace('：', ':').replace('；', ';').replace('，', ',')
        .replace('—', '-').replace('–', '-')
    )


def _extract_horizon_segment(text, horizon):
    labels = ['短期', '中期', '长期']
    start = text.find(horizon)
    if start < 0:
        return text
    end = len(text)
    for lb in labels:
        if lb == horizon:
            continue
        p = text.find(lb, start + len(horizon))
        if p >= 0:
            end = min(end, p)
    return text[start:end]


def _infer_decision_from_text(text):
    src = str(text or '')
    if any(k in src for k in ['卖出', '减仓', '止损', '做空']):
        return '卖出/减仓'
    if any(k in src for k in ['买入', '增持', '加仓', '做多']):
        return '买入/增持'
    if any(k in src for k in ['持有', '观望', '等待']):
        return '观望/持有'
    return '观望/持有'


def _infer_target_from_text(text):
    src = str(text or '').strip()
    if not src:
        return '未给出'
    # 先抓区间，再抓单值
    m_range = re.search(r'(-?\d+(?:\.\d+)?)\s*[-~到至]\s*(-?\d+(?:\.\d+)?)', src)
    if m_range:
        return f"{m_range.group(1)}-{m_range.group(2)}"
    m_one = re.search(r'-?\d+(?:\.\d+)?', src)
    if m_one:
        return m_one.group(0)
    return '未给出'


def _extract_decision_and_target_price(final_decision_response):
    cleaned = _normalize_decision_text(final_decision_response)
    if not cleaned:
        cleaned = '未返回决策文本'

    # 优先尝试标准格式（兼容中英文引号）
    strict_pattern = re.compile(
        r'短期(?:买卖)?(?:决策)?[: ]*["\']?(.*?)["\']?,\s*短期目标(?:价格|点位|区间)?[: ]*["\']?(.*?)["\']?\s*[;。\n]'
        r'\s*中期(?:买卖)?(?:决策)?[: ]*["\']?(.*?)["\']?,\s*中期目标(?:价格|点位|区间)?[: ]*["\']?(.*?)["\']?\s*[;。\n]'
        r'\s*长期(?:买卖)?(?:决策)?[: ]*["\']?(.*?)["\']?,\s*长期目标(?:价格|点位|区间)?[: ]*["\']?(.*?)["\']?',
        re.DOTALL
    )
    m = strict_pattern.search(cleaned + '\n')
    if m and all((m.group(i) or '').strip() for i in [1, 2, 3, 4, 5, 6]):
        return {
            "short_term": {"decision": m.group(1).strip() or '观望/持有', "target_price": m.group(2).strip() or '未给出'},
            "medium_term": {"decision": m.group(3).strip() or '观望/持有', "target_price": m.group(4).strip() or '未给出'},
            "long_term": {"decision": m.group(5).strip() or '观望/持有', "target_price": m.group(6).strip() or '未给出'},
        }

    # 容错解析：分期限抽取，避免模型格式轻微偏离时失败
    result = {}
    for label, key in [('短期', 'short_term'), ('中期', 'medium_term'), ('长期', 'long_term')]:
        seg = _extract_horizon_segment(cleaned, label)

        # 决策：优先抓“决策:”字段，否则从文本语义推断
        dm = re.search(r'(?:买卖决策|交易决策|决策|策略)[: ]*([^,;\n。]+)', seg)
        if dm:
            decision = dm.group(1).strip().strip('"').strip("'")
        else:
            decision = _infer_decision_from_text(seg)

        # 目标位：优先抓“目标:”字段，否则从文本提取数字
        tm = re.search(r'(?:目标价格|目标点位|目标区间|目标)[: ]*([^;\n。]+)', seg)
        if tm:
            target_text = tm.group(1).strip().strip('"').strip("'")
        else:
            target_text = _infer_target_from_text(seg)

        result[key] = {
            'decision': decision or '观望/持有',
            'target_price': target_text or '未给出',
        }

    return result


def _summarize_reasoning_to_steps(task, reasoning_content):
    prompt = (
        "请将以下分析过程压缩成不超过8个核心步骤，每步不超过10个字，格式如下：\n"
        "1: 步骤1\n2: 步骤2\n...\n\n"
        f"用户问题:\n{task}\n\n"
        f"分析过程:\n{reasoning_content}"
    )
    resp = _call_qwen(
        messages=[{'role': 'user', 'content': prompt}],
        model=_TEXT_MODEL,
        temperature=0.1,
        max_tokens=500,
        timeout=90,
    )
    text = resp.get('content', '')
    steps = re.findall(r'\d+\s*[:：]\s*(.+)', text)
    if not steps:
        # 兜底：按句号切分
        cands = [x.strip() for x in re.split(r'[。；;\n]', str(reasoning_content or '')) if x.strip()]
        steps = cands[:8] if cands else ['数据汇总', '技术面判断', '资金面判断', '综合决策']
    return steps[:8], text


def _build_logic_graph_figure(task, reasoning_content):
    steps, summary_text = _summarize_reasoning_to_steps(task, reasoning_content)
    fig, ax = plt.subplots(figsize=(18, 10), facecolor='white')
    ax.axis('off')
    ax.set_title('核心投资分析逻辑链路', fontsize=22, fontweight='bold', pad=12)

    # 手绘流程图：task -> step1 -> step2...
    n = len(steps) + 1
    ys = np.linspace(0.85, 0.10, n)
    x = 0.5

    # 起点
    ax.text(x, ys[0], f'问题: {task[:42]}', ha='center', va='center',
            bbox=dict(boxstyle='round,pad=0.45', fc='#fde68a', ec='#b45309', lw=1.4),
            fontsize=13, transform=ax.transAxes)

    for i, st in enumerate(steps, 1):
        ax.annotate(
            '',
            xy=(x, ys[i] + 0.03),
            xytext=(x, ys[i - 1] - 0.035),
            arrowprops=dict(arrowstyle='->', lw=1.5, color='#4b5563'),
            xycoords=ax.transAxes,
        )
        ax.text(x, ys[i], f'{i}. {st}', ha='center', va='center',
                bbox=dict(boxstyle='round,pad=0.4', fc='#dbeafe', ec='#1d4ed8', lw=1.2),
                fontsize=13, transform=ax.transAxes)
    fig.tight_layout(rect=(0.01, 0.01, 0.99, 0.95))
    return fig, summary_text


def _format_decision_message(parsed):
    if isinstance(parsed, dict):
        return (
            f"【短期】操作建议: {parsed['short_term']['decision']} | 目标位: {parsed['short_term']['target_price']}\n"
            f"【中期】操作建议: {parsed['medium_term']['decision']} | 目标位: {parsed['medium_term']['target_price']}\n"
            f"【长期】操作建议: {parsed['long_term']['decision']} | 目标位: {parsed['long_term']['target_price']}\n\n"
            "执行提示: 先遵循风控上限，再按分批节奏执行。"
        )
    return str(parsed)


def _decision_style(decision_text):
    src = str(decision_text or '')
    if any(k in src for k in ['卖出', '减仓', '止损', '做空']):
        return {'label': '偏防守', 'face': '#fee2e2', 'edge': '#ef4444', 'action': '#b91c1c'}
    if any(k in src for k in ['买入', '增持', '加仓', '做多']):
        return {'label': '偏进攻', 'face': '#dcfce7', 'edge': '#22c55e', 'action': '#15803d'}
    return {'label': '均衡', 'face': '#fef3c7', 'edge': '#f59e0b', 'action': '#b45309'}


def _build_decision_dashboard_figure(target, parsed_decision):
    if not isinstance(parsed_decision, dict):
        parsed_decision = {
            'short_term': {'decision': '观望/持有', 'target_price': '未给出'},
            'medium_term': {'decision': '观望/持有', 'target_price': '未给出'},
            'long_term': {'decision': '观望/持有', 'target_price': '未给出'},
        }

    blocks = [
        ('短期策略', parsed_decision.get('short_term', {})),
        ('中期策略', parsed_decision.get('medium_term', {})),
        ('长期策略', parsed_decision.get('long_term', {})),
    ]

    fig, ax = plt.subplots(figsize=(24, 9), facecolor='white')
    ax.axis('off')
    ax.text(0.5, 0.93, f"{target['display_name']} 买卖决策与目标价格", ha='center', va='center',
            fontsize=30, fontweight='bold', color='#0f172a', transform=ax.transAxes)

    x0 = 0.04
    y0 = 0.14
    card_w = 0.28
    card_h = 0.68
    gap = 0.03

    for i, (name, info) in enumerate(blocks):
        decision = str(info.get('decision', '观望/持有') or '观望/持有')
        target = str(info.get('target_price', '未给出') or '未给出')
        sty = _decision_style(decision)
        left = x0 + i * (card_w + gap)

        rect = FancyBboxPatch(
            (left, y0), card_w, card_h,
            boxstyle='round,pad=0.012,rounding_size=0.02',
            linewidth=2.4, edgecolor=sty['edge'], facecolor=sty['face'],
            transform=ax.transAxes
        )
        ax.add_patch(rect)

        ax.text(left + 0.02, y0 + card_h - 0.10, name, ha='left', va='center',
                fontsize=21, fontweight='bold', color='#0f172a', transform=ax.transAxes)
        ax.text(left + 0.02, y0 + card_h - 0.20, f'仓位风格: {sty["label"]}', ha='left', va='center',
                fontsize=15, color='#334155', transform=ax.transAxes)

        ax.text(left + 0.02, y0 + card_h - 0.39, '操作建议', ha='left', va='center',
                fontsize=14, color='#475569', transform=ax.transAxes)
        ax.text(left + 0.02, y0 + card_h - 0.50, decision, ha='left', va='center',
                fontsize=27, fontweight='bold', color=sty['action'], transform=ax.transAxes)

        ax.text(left + 0.02, y0 + card_h - 0.63, '目标位', ha='left', va='center',
                fontsize=14, color='#475569', transform=ax.transAxes)
        ax.text(left + 0.02, y0 + card_h - 0.72, target, ha='left', va='center',
                fontsize=22, fontweight='bold', color='#1d4ed8', transform=ax.transAxes)

    ax.text(0.5, 0.06, '执行建议: 先按风控上限确定仓位，再采用分批成交，避免单笔冲击。', ha='center', va='center',
            fontsize=15, color='#334155', transform=ax.transAxes)
    fig.tight_layout(rect=(0.01, 0.01, 0.99, 0.98))
    return fig


def _clip_text(src, max_len=240):
    text = str(src or '').strip()
    if len(text) <= max_len:
        return text
    return text[:max_len] + '...'


def _decision_score(decision_text):
    text = str(decision_text or '')
    if any(k in text for k in ['买入', '加仓', '增持', '做多']):
        return 1
    if any(k in text for k in ['卖出', '减仓', '止损', '做空']):
        return -1
    return 0


def _extract_first_number(src):
    m = re.search(r'-?\d+(?:\.\d+)?', str(src or ''))
    if not m:
        return np.nan
    try:
        return float(m.group(0))
    except Exception:
        return np.nan


def _build_strategy_blueprint(target, parsed_decision, target_analysis, money_report,
                              k_line_prompt, market_feeling, industry_preference):
    short_decision = '待补充'
    medium_decision = '待补充'
    long_decision = '待补充'
    if isinstance(parsed_decision, dict):
        short_decision = parsed_decision.get('short_term', {}).get('decision', '待补充')
        medium_decision = parsed_decision.get('medium_term', {}).get('decision', '待补充')
        long_decision = parsed_decision.get('long_term', {}).get('decision', '待补充')

    return (
        f"【策略定位】\n"
        f"标的: {target['display_name']}\n"
        f"目标: 在可控回撤下，完成短中长期一致性的交易执行。\n\n"
        f"【信号框架】\n"
        f"1) 宏观与政策层: {_clip_text(market_feeling, 170)}\n"
        f"2) 资金结构层: {_clip_text(money_report, 170)}\n"
        f"3) 技术形态层: {_clip_text(k_line_prompt, 170)}\n"
        f"4) 热点映射层: {_clip_text(industry_preference, 170)}\n"
        f"5) 标的锚定层: {_clip_text(target_analysis, 170)}\n\n"
        f"【执行规则】\n"
        f"- 短期动作: {short_decision}\n"
        f"- 中期动作: {medium_decision}\n"
        f"- 长期动作: {long_decision}\n"
        f"- 若资金与技术结论冲突，优先降低仓位并等待二次确认。\n\n"
        f"【风控与复核】\n"
        f"- 触发条件: 趋势破位、资金显著反转、政策预期突变。\n"
        f"- 复核节奏: 日内跟踪 + 周度复盘 + 月度参数再校准。"
    )


def _build_strategy_effect_figure(target, parsed_decision):
    horizons = ['短期', '中期', '长期']
    decisions = []
    scores = []
    targets = []

    if isinstance(parsed_decision, dict):
        for key in ['short_term', 'medium_term', 'long_term']:
            info = parsed_decision.get(key, {})
            dec = str(info.get('decision', '观望'))
            decisions.append(dec)
            scores.append(_decision_score(dec))
            targets.append(_extract_first_number(info.get('target_price', '')))
    else:
        decisions = ['观望', '观望', '观望']
        scores = [0, 0, 0]
        targets = [np.nan, np.nan, np.nan]

    fig, axes = plt.subplots(1, 2, figsize=(20, 8), facecolor='white')
    ax1, ax2 = axes

    # 子图1：分期限动作强度
    colors = ['#22c55e' if s > 0 else ('#ef4444' if s < 0 else '#f59e0b') for s in scores]
    y = np.arange(len(horizons))
    ax1.barh(y, scores, color=colors, alpha=0.88)
    ax1.set_yticks(y)
    ax1.set_yticklabels(horizons, fontsize=14)
    ax1.set_xlim(-1.35, 1.35)
    ax1.axvline(0, color='#64748b', lw=1, alpha=0.7)
    ax1.set_xticks([-1, 0, 1])
    ax1.set_xticklabels(['偏空', '中性', '偏多'], fontsize=13)
    ax1.grid(axis='x', alpha=0.2)
    ax1.set_title('分期限动作倾向', fontsize=17, fontweight='bold')
    for i, dec in enumerate(decisions):
        ax1.text(scores[i] + (0.08 if scores[i] >= 0 else -0.08), i, dec,
                 va='center', ha='left' if scores[i] >= 0 else 'right', fontsize=13, color='#0f172a')

    # 子图2：目标位可视化
    x = np.arange(len(horizons))
    numeric_targets = np.array(targets, dtype=float)
    valid_mask = ~np.isnan(numeric_targets)
    if valid_mask.any():
        ax2.plot(x[valid_mask], numeric_targets[valid_mask], marker='o', markersize=9,
                 linewidth=3.0, color='#2563eb')
        for i in np.where(valid_mask)[0]:
            ax2.text(x[i], numeric_targets[i], f'{numeric_targets[i]:.2f}', fontsize=13,
                     ha='center', va='bottom', color='#1d4ed8')
        ax2.set_xticks(x)
        ax2.set_xticklabels(horizons, fontsize=14)
        ax2.grid(alpha=0.25)
        ax2.set_title('目标位区间（可数值化）', fontsize=17, fontweight='bold')
        ax2.set_ylabel('目标价格/点位', fontsize=13, color='#334155')
    else:
        ax2.axis('off')
        ax2.text(
            0.5, 0.64, '目标位未提供明确数值\n已回退为文本策略展示',
            ha='center', va='center', fontsize=18, color='#334155', linespacing=1.6
        )
        ax2.text(
            0.5, 0.28,
            '\n'.join([f'{h}: {d}' for h, d in zip(horizons, decisions)]),
            ha='center', va='center', fontsize=14, color='#475569', linespacing=1.7
        )

    fig.suptitle(f"{target['display_name']} 策略执行映射", fontsize=21, fontweight='bold', y=1.02)
    fig.tight_layout(rect=(0.01, 0.01, 0.99, 0.96))
    return fig


def generate(query='请帮我分析一下上证指数，是否值得投资？', progress_callback=None, **kwargs):
    """
    完整复现 predict_index.py 的执行顺序（适配到 AlphaFin 指标框架）。
    """
    total = 12
    t0 = time.time()

    def _progress(step, msg):
        if progress_callback:
            elapsed = time.time() - t0
            progress_callback(step, total, f'{msg} (已用时 {elapsed:.0f}s)')

    task = str(query or '').strip() or INDICATOR_META['default_query']
    _progress(0, '初始化 LogicFin 复现流程...')

    # Step 1: 识别标的（股票 / 指数）
    target = _resolve_target(task)
    _progress(1, f"识别标的完成: {target['kind']} {target['display_name']}")

    # Step 2: K线图 + 技术解读
    k_line_image_path, k_line_prompt = _vision_kline_read(target)
    _progress(2, 'K线解读完成')

    # Step 3-5: 联网检索三段
    a_share_report = _kimi_web_search_report(task, target)
    _progress(3, '研究报告检索完成')

    industry_preference = _kimi_web_highlight(target)
    _progress(4, '热点板块检索完成')

    market_feeling = _kimi_web_market_feeling(task)
    _progress(5, '市场政策检索完成')

    # Step 6: 资金面
    money_report = _fetch_money_data(target)
    _progress(6, '资金面分析完成')

    # Step 7: 标的分析
    target_analysis = _analyse_a_market(target)
    _progress(7, '标的分析完成')

    # Step 8: 初步决策
    decision_response = _decision_tool(
        target=target,
        target_analysis=target_analysis,
        k_line_prompt=k_line_prompt,
        a_share_report=a_share_report,
        market_feeling=market_feeling,
        industry_prefrence=industry_preference,
        money_report=money_report
    )
    _progress(8, '初步决策完成')

    # Step 9: 反思 + 再决策 + 逻辑图
    final_reflection_response = _final_reflection(decision_response)
    final_decision_response, reasoning_content = _final_decision1(
        target=target,
        target_analysis=target_analysis,
        k_line_prompt=k_line_prompt,
        full_result=decision_response,
        a_share_report=a_share_report,
        market_feeling=market_feeling,
        industry_prefrence=industry_preference,
        money_report=money_report,
        reflection=final_reflection_response,
        task=task,
    )
    logic_fig, summary_text = _build_logic_graph_figure(task, reasoning_content)
    _progress(9, '反思与逻辑链路完成')

    # Step 10: 最终决策 + 目标价提取
    final_decision_response_2 = _final_decision2(target, final_decision_response)
    parsed_decision = _extract_decision_and_target_price(final_decision_response_2)
    decision_message = _format_decision_message(parsed_decision)
    decision_dashboard_fig = _build_decision_dashboard_figure(target, parsed_decision)
    _progress(10, '最终买卖决策与目标价完成')

    strategy_analysis_text = _build_strategy_blueprint(
        target=target,
        parsed_decision=parsed_decision,
        target_analysis=target_analysis,
        money_report=money_report,
        k_line_prompt=k_line_prompt,
        market_feeling=market_feeling,
        industry_preference=industry_preference,
    )
    strategy_effect_fig = _build_strategy_effect_figure(target, parsed_decision)

    # Step 11: 组织图表输出（映射 Gradio 的 12 个面板）
    figures = []
    figures.append((logic_fig, '深度思考逻辑链路图'))
    figures.append((_render_text_figure(final_decision_response, '最终决策结果', figsize=(24, 14), font_size=16), '最终决策结果'))
    figures.append((_render_text_figure(target_analysis, '标的行情分析', figsize=(22, 13), font_size=15), '标的行情分析'))
    figures.append((_render_text_figure(a_share_report, '研究报告分析', figsize=(22, 13), font_size=15), '研究报告分析'))
    figures.append((_render_text_figure(money_report, '资金流分析报告', figsize=(22, 13), font_size=15), '资金流分析报告'))
    figures.append((_render_text_figure(k_line_prompt, '技术面透析', figsize=(22, 13), font_size=15), '技术面透析'))
    figures.append((_image_to_figure(k_line_image_path, '日K线图展示', figsize=(20, 11)), '日K线图展示'))
    figures.append((decision_dashboard_fig, '买卖决策与目标价格'))
    figures.append((_render_text_figure(market_feeling, '市场政策信息汇总', figsize=(22, 13), font_size=15), '市场政策信息汇总'))
    figures.append((_render_text_figure(industry_preference, '市场热点信息汇总', figsize=(22, 13), font_size=15), '市场热点信息汇总'))
    figures.append((_render_text_figure(strategy_analysis_text, '策略构建', figsize=(24, 12), font_size=16), '策略构建'))
    figures.append((strategy_effect_fig, '策略效果展示'))

    _progress(11, '图表组装完成')
    _progress(12, 'LogicFin 复现流程完成')
    return figures
