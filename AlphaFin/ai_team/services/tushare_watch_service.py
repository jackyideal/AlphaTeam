"""
盘中盯盘的 TuShare 实时数据服务。
"""
import datetime
import re

import requests

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


_PRO = None


def _now_cn():
    if ZoneInfo:
        return datetime.datetime.now(ZoneInfo('Asia/Shanghai'))
    return datetime.datetime.now()


def _get_pro():
    global _PRO
    if _PRO is not None:
        return _PRO
    import tushare as ts
    from AlphaFin.config import TUSHARE_TOKEN
    _PRO = ts.pro_api(TUSHARE_TOKEN)
    return _PRO


def _normalize_ts_code(ts_code):
    code = str(ts_code or '').strip().upper()
    if not code:
        return ''
    if len(code) == 6 and code.isdigit():
        return code + ('.SH' if code.startswith(('5', '6', '9')) else '.SZ')
    if re.fullmatch(r'\d{6}\.(SH|SZ)', code):
        return code
    return ''


def _to_market_symbol(ts_code):
    code = _normalize_ts_code(ts_code)
    if not code:
        return ''
    symbol, market = code.split('.', 1)
    prefix = 'sh' if market == 'SH' else 'sz'
    return prefix + symbol


def _parse_qq_quote_time(raw_text):
    s = str(raw_text or '').strip()
    if re.fullmatch(r'\d{14}', s):
        return '%s-%s-%s %s:%s:%s' % (
            s[0:4], s[4:6], s[6:8], s[8:10], s[10:12], s[12:14]
        )
    return ''


def _fetch_quote_from_qq(ts_code):
    symbol = _to_market_symbol(ts_code)
    if not symbol:
        return None
    url = 'https://qt.gtimg.cn/q=%s' % symbol
    try:
        resp = requests.get(url, timeout=4)
        if resp.status_code != 200:
            return None
        resp.encoding = 'gbk'
        text = resp.text or ''
    except Exception:
        return None

    if '"' not in text or '~' not in text:
        return None
    try:
        payload = text.split('"', 1)[1].rsplit('"', 1)[0]
        parts = payload.split('~')
    except Exception:
        return None
    if len(parts) < 5:
        return None
    try:
        price = float(parts[3])
    except Exception:
        price = None
    try:
        prev_close = float(parts[4])
    except Exception:
        prev_close = None
    if price is None or price <= 0:
        return None
    quote_time = _parse_qq_quote_time(parts[30] if len(parts) > 30 else '')
    return {
        'price': price,
        'prev_close': prev_close,
        'time': quote_time,
        'source': 'qq_quote',
    }


def _fetch_quote_from_sina(ts_code):
    symbol = _to_market_symbol(ts_code)
    if not symbol:
        return None
    url = 'https://hq.sinajs.cn/list=%s' % symbol
    try:
        resp = requests.get(
            url,
            timeout=4,
            headers={'Referer': 'https://finance.sina.com.cn'}
        )
        if resp.status_code != 200:
            return None
        resp.encoding = 'gbk'
        text = resp.text or ''
    except Exception:
        return None

    if '"' not in text:
        return None
    try:
        payload = text.split('"', 1)[1].rsplit('"', 1)[0]
        parts = payload.split(',')
    except Exception:
        return None
    if len(parts) < 32:
        return None
    try:
        price = float(parts[3])
    except Exception:
        price = None
    try:
        prev_close = float(parts[2])
    except Exception:
        prev_close = None
    if price is None or price <= 0:
        return None
    date_part = str(parts[30] or '').strip()
    time_part = str(parts[31] or '').strip()
    quote_time = (date_part + ' ' + time_part).strip() if date_part else time_part
    return {
        'price': price,
        'prev_close': prev_close,
        'time': quote_time,
        'source': 'sina_quote',
    }


def fetch_intraday_stock_quote(ts_code, freq='1MIN'):
    """
    获取个股实时分钟最新报价（价格+时间+来源）。

    数据源优先级：
    1) Tushare rt_min
    2) Tushare stk_mins
    3) 腾讯公开行情接口
    4) 新浪公开行情接口

    返回值：{'price': float, 'prev_close': float|None, 'time': str, 'source': str} 或 None
    """
    code = _normalize_ts_code(ts_code)
    if not code:
        return None

    freq = (freq or '1MIN').upper()
    if freq not in ('1MIN', '5MIN', '15MIN', '30MIN', '60MIN'):
        freq = '1MIN'

    pro = _get_pro()
    df_source = ''
    try:
        df = pro.rt_min(ts_code=code, freq=freq)
        if df is not None and not df.empty:
            df_source = 'tushare_rt_min'
    except Exception:
        df = None

    # 兜底：部分账号对 rt_min 权限受限，尝试 stk_mins 接口
    if df is None or df.empty:
        candidates = [
            {'ts_code': code, 'freq': freq, 'limit': 240},
            {'ts_code': code, 'freq': freq},
            {
                'ts_code': code,
                'start_date': _now_cn().strftime('%Y-%m-%d 09:30:00'),
                'end_date': _now_cn().strftime('%Y-%m-%d %H:%M:%S'),
                'freq': freq,
                'limit': 240
            },
        ]
        for kwargs in candidates:
            try:
                df = pro.stk_mins(**kwargs)
            except Exception:
                df = None
            if df is not None and not df.empty:
                df_source = 'tushare_stk_mins'
                break

    if df is not None and not df.empty:
        time_col = None
        if 'time' in df.columns:
            time_col = 'time'
        elif 'trade_time' in df.columns:
            time_col = 'trade_time'
        elif 'datetime' in df.columns:
            time_col = 'datetime'

        try:
            if time_col:
                df = df.sort_values(time_col)
        except Exception:
            pass

        row = df.iloc[-1]
        close_val = row.get('close')
        if close_val is not None:
            try:
                prev_close = None
                for k in ('pre_close', 'prev_close', 'last_close'):
                    if row.get(k) is not None:
                        prev_close = float(row.get(k))
                        break
                return {
                    'price': float(close_val),
                    'prev_close': prev_close,
                    'time': str(row.get(time_col) or '') if time_col else '',
                    'source': df_source or 'tushare_rt_min',
                }
            except Exception:
                pass

        open_val = row.get('open')
        if open_val is not None:
            try:
                return {
                    'price': float(open_val),
                    'prev_close': None,
                    'time': str(row.get(time_col) or '') if time_col else '',
                    'source': df_source or 'tushare_rt_min',
                }
            except Exception:
                pass

    # 公开行情兜底：无 token 权限时仍可返回盘中价
    qq_quote = _fetch_quote_from_qq(code)
    if qq_quote:
        return qq_quote

    sina_quote = _fetch_quote_from_sina(code)
    if sina_quote:
        return sina_quote

    return None


def fetch_intraday_stock_price(ts_code, freq='1MIN'):
    """兼容旧调用：仅返回实时价格（float）。"""
    quote = fetch_intraday_stock_quote(ts_code, freq=freq)
    if not quote:
        return None
    price = quote.get('price')
    if price is None:
        return None
    try:
        return float(price)
    except Exception:
        return None


def get_market_clock():
    """返回北京时间与交易时段状态。"""
    now = _now_cn()
    minute = now.hour * 60 + now.minute
    is_weekday = now.weekday() < 5

    if not is_weekday:
        phase = 'closed_weekend'
    elif minute < 9 * 60 + 15:
        phase = 'pre_open'
    elif minute < 9 * 60 + 30:
        phase = 'call_auction'
    elif minute < 11 * 60 + 30:
        phase = 'morning_session'
    elif minute < 13 * 60:
        phase = 'lunch_break'
    elif minute < 15 * 60:
        phase = 'afternoon_session'
    else:
        phase = 'after_close'

    return {
        'datetime': now.strftime('%Y-%m-%d %H:%M:%S'),
        'trade_date': now.strftime('%Y%m%d'),
        'weekday': now.weekday(),
        'is_weekday': is_weekday,
        'phase': phase,
    }


def fetch_intraday_index(ts_codes=None, freq='1MIN'):
    """
    获取指数实时分钟数据快照（当日首分钟到最新分钟的波动）。
    """
    codes = ts_codes or ['000001.SH', '000300.SH', '399001.SZ', '399006.SZ']
    if isinstance(codes, str):
        codes = [c.strip().upper() for c in codes.split(',') if c.strip()]
    if not codes:
        return []

    freq = (freq or '1MIN').upper()
    if freq not in ('1MIN', '5MIN', '15MIN', '30MIN', '60MIN'):
        freq = '1MIN'

    pro = _get_pro()
    try:
        df = pro.rt_idx_min(ts_code=','.join(codes), freq=freq)
    except Exception:
        df = None
    if df is None or df.empty:
        # 权限受限时兜底到 index_daily（日线快照）
        today = _now_cn().strftime('%Y%m%d')
        fallback_rows = []
        for code in codes:
            try:
                dfi = pro.index_daily(ts_code=code, start_date=today, end_date=today)
            except Exception:
                dfi = None
            if dfi is None or dfi.empty:
                continue
            r = dfi.iloc[0]
            fallback_rows.append({
                'ts_code': code,
                'time': today + ' 15:00:00',
                'close': float(r['close']) if r.get('close') is not None else None,
                'high': float(r['high']) if r.get('high') is not None else None,
                'low': float(r['low']) if r.get('low') is not None else None,
                'vol': float(r['vol']) if r.get('vol') is not None else None,
                'amount': float(r['amount']) if r.get('amount') is not None else None,
                'intraday_pct': float(r['pct_chg']) if r.get('pct_chg') is not None else None,
                'source': 'index_daily_fallback',
            })
        return fallback_rows

    if 'time' in df.columns:
        df['time'] = df['time'].astype(str)
    result = []
    for code in codes:
        sub = df[df['ts_code'] == code]
        if sub.empty:
            continue
        sub = sub.sort_values('time')
        first_row = sub.iloc[0]
        last_row = sub.iloc[-1]
        open0 = float(first_row['open']) if first_row.get('open') is not None else None
        close_last = float(last_row['close']) if last_row.get('close') is not None else None
        intraday_pct = None
        if open0 and close_last:
            intraday_pct = (close_last - open0) / open0 * 100.0
        result.append({
            'ts_code': code,
            'time': str(last_row.get('time') or ''),
            'close': close_last,
            'high': float(last_row['high']) if last_row.get('high') is not None else None,
            'low': float(last_row['low']) if last_row.get('low') is not None else None,
            'vol': float(last_row['vol']) if last_row.get('vol') is not None else None,
            'amount': float(last_row['amount']) if last_row.get('amount') is not None else None,
            'intraday_pct': intraday_pct,
            'source': 'rt_idx_min',
        })
    return result


def fetch_intraday_index_series(ts_codes=None, freq='1MIN', limit=180):
    """
    获取指数分时曲线序列。
    返回: {ts_code: [{'time': str, 'value': float}, ...], ...}
    value 为相对当日首个开盘值的涨跌幅(%)
    """
    codes = ts_codes or ['000001.SH', '000300.SH', '399001.SZ', '399006.SZ']
    if isinstance(codes, str):
        codes = [c.strip().upper() for c in codes.split(',') if c.strip()]
    if not codes:
        return {}

    freq = (freq or '1MIN').upper()
    if freq not in ('1MIN', '5MIN', '15MIN', '30MIN', '60MIN'):
        freq = '1MIN'

    pro = _get_pro()
    try:
        df = pro.rt_idx_min(ts_code=','.join(codes), freq=freq)
    except Exception:
        df = None
    if df is None or df.empty:
        return {}

    if 'time' in df.columns:
        df['time'] = df['time'].astype(str)

    out = {}
    for code in codes:
        sub = df[df['ts_code'] == code]
        if sub is None or sub.empty:
            continue
        if 'time' in sub.columns:
            sub = sub.sort_values('time')
        if int(limit) > 0 and len(sub) > int(limit):
            sub = sub.tail(int(limit))

        first_row = sub.iloc[0]
        base_val = first_row.get('open')
        if base_val is None:
            base_val = first_row.get('close')
        try:
            base_val = float(base_val)
        except Exception:
            base_val = None

        items = []
        for _, r in sub.iterrows():
            try:
                close_val = float(r.get('close'))
            except Exception:
                close_val = None
            if close_val is None:
                continue
            pct = None
            if base_val not in (None, 0):
                pct = (close_val - base_val) / base_val * 100.0
            if pct is None:
                continue
            items.append({
                'time': str(r.get('time') or ''),
                'value': float(pct),
            })
        if items:
            out[code] = items
    return out


def fetch_intraday_stock_series(ts_code, freq='1MIN', limit=180):
    """
    获取个股分时曲线序列。
    返回: [{'time': str, 'value': float}, ...]
    value 为相对当日首个开盘值的涨跌幅(%)
    """
    code = _normalize_ts_code(ts_code)
    if not code:
        return []

    freq = (freq or '1MIN').upper()
    if freq not in ('1MIN', '5MIN', '15MIN', '30MIN', '60MIN'):
        freq = '1MIN'

    pro = _get_pro()
    try:
        df = pro.rt_min(ts_code=code, freq=freq)
    except Exception:
        df = None

    if df is None or df.empty:
        candidates = [
            {'ts_code': code, 'freq': freq, 'limit': max(120, int(limit))},
            {'ts_code': code, 'freq': freq},
            {
                'ts_code': code,
                'start_date': _now_cn().strftime('%Y-%m-%d 09:30:00'),
                'end_date': _now_cn().strftime('%Y-%m-%d %H:%M:%S'),
                'freq': freq,
                'limit': max(120, int(limit))
            },
        ]
        for kwargs in candidates:
            try:
                df = pro.stk_mins(**kwargs)
            except Exception:
                df = None
            if df is not None and not df.empty:
                break

    if df is None or df.empty:
        return []

    time_col = None
    if 'time' in df.columns:
        time_col = 'time'
    elif 'trade_time' in df.columns:
        time_col = 'trade_time'
    elif 'datetime' in df.columns:
        time_col = 'datetime'

    if time_col:
        try:
            df[time_col] = df[time_col].astype(str)
            df = df.sort_values(time_col)
        except Exception:
            pass

    if int(limit) > 0 and len(df) > int(limit):
        df = df.tail(int(limit))

    first_row = df.iloc[0]
    base_val = first_row.get('open')
    if base_val is None:
        base_val = first_row.get('close')
    try:
        base_val = float(base_val)
    except Exception:
        base_val = None

    items = []
    for _, r in df.iterrows():
        try:
            close_val = float(r.get('close'))
        except Exception:
            close_val = None
        if close_val is None:
            continue
        pct = None
        if base_val not in (None, 0):
            pct = (close_val - base_val) / base_val * 100.0
        if pct is None:
            continue
        items.append({
            'time': str(r.get(time_col) or '') if time_col else '',
            'value': float(pct),
        })
    return items


def fetch_intraday_sector_heat(limit=10):
    """获取申万行业实时涨跌幅排行。"""
    pro = _get_pro()
    try:
        df = pro.rt_sw_k()
    except Exception:
        df = None
    if df is None or df.empty:
        # 权限受限时兜底：使用同花顺行业日线（type='I'）
        today = _now_cn().strftime('%Y%m%d')
        try:
            idx = pro.ths_index(exchange='A', type='I')
            daily = pro.ths_daily(start_date=today, end_date=today)
        except Exception:
            idx, daily = None, None
        if idx is None or idx.empty or daily is None or daily.empty:
            return []
        idx_map = {
            str(r['ts_code']): str(r['name'])
            for _, r in idx[['ts_code', 'name']].iterrows()
        }
        daily = daily[daily['ts_code'].isin(idx_map.keys())].copy()
        if daily.empty:
            return []
        if 'pct_change' in daily.columns:
            daily['pct_change'] = daily['pct_change'].astype(float)
        else:
            daily['pct_change'] = 0.0
        daily = daily.sort_values('pct_change', ascending=False)
        top = daily.head(max(1, int(limit)))
        rows = []
        for _, r in top.iterrows():
            code = str(r.get('ts_code', ''))
            rows.append({
                'ts_code': code,
                'name': idx_map.get(code, ''),
                'trade_time': str(r.get('trade_date', '')) + ' 15:00:00',
                'close': float(r['close']) if r.get('close') is not None else None,
                'pct_change': float(r['pct_change']) if r.get('pct_change') is not None else None,
                'amount': float(r['amount']) if r.get('amount') is not None else None,
                'source': 'ths_daily_fallback',
            })
        return rows

    if 'pct_change' in df.columns:
        df['pct_change'] = df['pct_change'].astype(float)
    else:
        df['pct_change'] = 0.0

    df = df.sort_values('pct_change', ascending=False)
    top = df.head(max(1, int(limit)))
    rows = []
    for _, r in top.iterrows():
        rows.append({
            'ts_code': r.get('ts_code', ''),
            'name': r.get('name', ''),
            'trade_time': r.get('trade_time', ''),
            'close': float(r['close']) if r.get('close') is not None else None,
            'pct_change': float(r['pct_change']) if r.get('pct_change') is not None else None,
            'amount': float(r['amount']) if r.get('amount') is not None else None,
            'source': 'rt_sw_k',
        })
    return rows


def fetch_intraday_hotrank(market='热股', limit=20, latest_only=True):
    """获取同花顺热榜（盘中/盘后）。"""
    pro = _get_pro()
    now = _now_cn()
    trade_date = now.strftime('%Y%m%d')
    is_new = 'Y' if latest_only else 'N'

    df = pro.ths_hot(trade_date=trade_date, market=market, is_new=is_new)
    if (df is None or df.empty) and is_new == 'Y':
        # 兜底：部分时段可能没有最新快照，改用分时采样数据
        df = pro.ths_hot(trade_date=trade_date, market=market, is_new='N')
    if df is None or df.empty:
        return []

    if 'rank' in df.columns:
        try:
            df['rank'] = df['rank'].astype(float)
            df = df.sort_values('rank', ascending=True)
        except Exception:
            pass

    top = df.head(max(1, int(limit)))
    rows = []
    for _, r in top.iterrows():
        rows.append({
            'trade_date': r.get('trade_date', ''),
            'rank_time': r.get('rank_time', ''),
            'market': r.get('market', market),
            'ts_code': r.get('ts_code', ''),
            'name': r.get('name', ''),
            'rank': r.get('rank', ''),
            'hot': r.get('hot', ''),
            'pct_change': r.get('pct_change', ''),
        })
    return rows


def fetch_intraday_news(hours=2, src='cls', limit=20):
    """获取最近N小时快讯。"""
    h = int(hours) if hours is not None else 2
    h = max(1, min(h, 24))
    limit = max(1, min(int(limit), 100))

    now = _now_cn()
    start = now - datetime.timedelta(hours=h)

    pro = _get_pro()
    df = pro.news(
        src=src,
        start_date=start.strftime('%Y-%m-%d %H:%M:%S'),
        end_date=now.strftime('%Y-%m-%d %H:%M:%S'),
    )
    if df is None or df.empty:
        return []

    if 'datetime' in df.columns:
        df = df.sort_values('datetime', ascending=False)
    top = df.head(limit)
    rows = []
    for _, r in top.iterrows():
        rows.append({
            'datetime': r.get('datetime', ''),
            'title': r.get('title', ''),
            'content': r.get('content', ''),
            'channels': r.get('channels', ''),
        })
    return rows
