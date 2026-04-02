# -*- coding: utf-8 -*-
"""
行业轮动图谱数据服务

核心口径:
1) 行业层级: L1 / L2 / L3（同级横向比较）
2) Y轴: 起始交易日 -> 结束交易日累计涨幅(%)
3) X轴: 结束交易日当日指标（涨跌幅/成交额/PE/PB/总市值/流通市值）
4) 叠加对照: 青松建化(600425.SH)或指定个股
"""

from __future__ import annotations

import math
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
import tushare as ts

from AlphaFin.config import TUSHARE_TOKEN

pro = ts.pro_api(TUSHARE_TOKEN)

_CACHE: Dict[tuple, dict] = {}
_CACHE_TTL_SECONDS = 600

_METRIC_META = {
    'pct_change': {'label': '最近交易日涨跌幅(%)', 'fmt': 'pct'},
    'x_interval_return': {'label': '指定区间累计涨跌幅(%)', 'fmt': 'pct'},
    'amount': {'label': '最近交易日成交额', 'fmt': 'num'},
    'pe': {'label': '市盈率(PE)', 'fmt': 'num'},
    'pb': {'label': '市净率(PB)', 'fmt': 'num'},
    'total_mv': {'label': '总市值', 'fmt': 'num'},
    'float_mv': {'label': '流通市值', 'fmt': 'num'},
}

_METRIC_ALIASES = {
    'pct_change': ['pct_change', 'pct_chg', 'change_pct'],
    'amount': ['amount'],
    'pe': ['pe', 'pe_ttm'],
    'pb': ['pb'],
    'total_mv': ['total_mv'],
    'float_mv': ['float_mv', 'circ_mv'],
    'close': ['close'],
}


def _clean_date(raw: str, default_value: str) -> str:
    s = str(raw or '').strip().replace('-', '')
    if len(s) == 8 and s.isdigit():
        return s
    return default_value


def _safe_float(v):
    try:
        x = float(v)
    except Exception:
        return None
    if pd.isna(x):
        return None
    return x


def _pick_column(df: pd.DataFrame, candidates: List[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    return ''


def _normalize_sw_snapshot(df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            'ts_code', 'name', 'trade_date', 'close',
            'pct_change', 'amount', 'pe', 'pb', 'total_mv', 'float_mv'
        ])

    d = df.copy()
    code_col = _pick_column(d, ['ts_code', 'index_code'])
    name_col = _pick_column(d, ['name', 'industry_name', 'index_name'])
    date_col = _pick_column(d, ['trade_date'])

    if not code_col:
        return pd.DataFrame(columns=[
            'ts_code', 'name', 'trade_date', 'close',
            'pct_change', 'amount', 'pe', 'pb', 'total_mv', 'float_mv'
        ])

    if code_col != 'ts_code':
        d['ts_code'] = d[code_col]
    if name_col:
        d['name'] = d[name_col]
    else:
        d['name'] = d['ts_code']
    if date_col:
        d['trade_date'] = d[date_col]
    else:
        d['trade_date'] = trade_date

    for target, cands in _METRIC_ALIASES.items():
        source_col = _pick_column(d, cands)
        if source_col and source_col != target:
            d[target] = d[source_col]
        elif source_col == target:
            continue
        elif target not in d.columns:
            d[target] = pd.NA

    d['ts_code'] = d['ts_code'].astype(str).str.strip()
    d['name'] = d['name'].astype(str).str.strip()
    d['trade_date'] = d['trade_date'].astype(str).str.strip()

    for c in ['close', 'pct_change', 'amount', 'pe', 'pb', 'total_mv', 'float_mv']:
        d[c] = pd.to_numeric(d[c], errors='coerce')

    d = d[d['ts_code'] != ''].copy()
    d = d.drop_duplicates(subset=['ts_code'], keep='last')
    return d[[
        'ts_code', 'name', 'trade_date', 'close',
        'pct_change', 'amount', 'pe', 'pb', 'total_mv', 'float_mv'
    ]]


def _get_trade_window(start_date: str, end_date: str) -> Dict[str, str]:
    requested_start = _clean_date(start_date, '20240924')
    requested_end = _clean_date(end_date, datetime.now().strftime('%Y%m%d'))
    if requested_start > requested_end:
        requested_start, requested_end = requested_end, requested_start

    try:
        cal = pro.trade_cal(
            exchange='SSE',
            start_date=requested_start,
            end_date=requested_end,
            fields='cal_date,is_open'
        )
    except TypeError:
        try:
            cal = pro.trade_cal(exchange='SSE', start_date=requested_start, end_date=requested_end)
        except Exception:
            cal = pd.DataFrame()
    except Exception:
        cal = pd.DataFrame()

    open_days = []
    if cal is not None and not cal.empty and 'cal_date' in cal.columns:
        if 'is_open' in cal.columns:
            cal = cal[cal['is_open'] == 1]
        open_days = sorted(cal['cal_date'].astype(str).tolist())

    if not open_days:
        # 当交易日历接口不可用时，不应把起止压缩成同一天，
        # 否则容易在盘中/接口抖动时导致行业快照为空。
        return {
            'requested_start': requested_start,
            'requested_end': requested_end,
            'start_trade_date': requested_start,
            'end_trade_date': requested_end,
        }

    return {
        'requested_start': requested_start,
        'requested_end': requested_end,
        'start_trade_date': open_days[0],
        'end_trade_date': open_days[-1],
    }


def _fetch_industry_universe(level: str) -> Tuple[pd.DataFrame, str]:
    level = str(level or 'L1').upper()
    if level not in ('L1', 'L2', 'L3'):
        level = 'L1'

    try:
        all_members = pro.index_member_all(src='SW2021')
    except TypeError:
        try:
            all_members = pro.index_member_all()
        except Exception:
            all_members = pd.DataFrame()
    except Exception:
        all_members = pd.DataFrame()

    if all_members is not None and not all_members.empty:
        code_col = f'{level.lower()}_code'
        name_col = f'{level.lower()}_name'
        if code_col in all_members.columns and name_col in all_members.columns:
            uni = all_members[[code_col, name_col]].dropna().copy()
            uni.columns = ['ts_code', 'name']
            uni['ts_code'] = uni['ts_code'].astype(str).str.strip()
            uni['name'] = uni['name'].astype(str).str.strip()
            uni = uni[uni['ts_code'] != ''].drop_duplicates(subset=['ts_code'], keep='last')
            if not uni.empty:
                return uni, 'index_member_all:SW2021'

    for src in ('SW2021', 'SW2014'):
        try:
            dd = pro.index_classify(level=level, src=src)
        except Exception:
            dd = pd.DataFrame()
        if dd is None or dd.empty:
            continue

        code_col = _pick_column(dd, ['index_code', 'ts_code'])
        name_col = _pick_column(dd, ['industry_name', 'name'])
        if not code_col:
            continue
        if not name_col:
            dd['name'] = dd[code_col]
            name_col = 'name'
        uni = dd[[code_col, name_col]].dropna().copy()
        uni.columns = ['ts_code', 'name']
        uni['ts_code'] = uni['ts_code'].astype(str).str.strip()
        uni['name'] = uni['name'].astype(str).str.strip()
        uni = uni[uni['ts_code'] != ''].drop_duplicates(subset=['ts_code'], keep='last')
        if not uni.empty:
            return uni, f'index_classify:{src}'

    return pd.DataFrame(columns=['ts_code', 'name']), 'none'


def _fetch_sw_snapshot(trade_date: str) -> pd.DataFrame:
    try:
        df = pro.sw_daily(
            trade_date=trade_date,
            fields='ts_code,trade_date,name,close,pct_change,amount,pe,pb,total_mv,float_mv'
        )
    except TypeError:
        try:
            df = pro.sw_daily(trade_date=trade_date)
        except Exception:
            df = pd.DataFrame()
    except Exception:
        df = pd.DataFrame()
    return _normalize_sw_snapshot(df, trade_date)


def _shift_ymd(date_str: str, delta_days: int) -> str:
    try:
        dt = datetime.strptime(str(date_str), '%Y%m%d')
        return (dt + timedelta(days=delta_days)).strftime('%Y%m%d')
    except Exception:
        return str(date_str)


def _fetch_sw_snapshot_near(trade_date: str, prefer: str = 'backward', max_days: int = 15) -> Tuple[pd.DataFrame, str]:
    """
    获取离目标日期最近的可用行业快照。
    prefer:
      - backward: 优先向前回退（适合 end_date，盘中常无当日收盘快照）
      - forward:  优先向后查找（适合 start_date）
    """
    base = _clean_date(trade_date, datetime.now().strftime('%Y%m%d'))
    if prefer not in ('backward', 'forward'):
        prefer = 'backward'

    direction_order = [-1, 1] if prefer == 'backward' else [1, -1]
    tried = set()

    for sign in direction_order:
        for i in range(0, max_days + 1):
            d = _shift_ymd(base, sign * i)
            if d in tried:
                continue
            tried.add(d)
            snap = _fetch_sw_snapshot(d)
            if snap is not None and not snap.empty:
                return snap, d

    return pd.DataFrame(), base


def _build_industry_frame(level: str, start_trade_date: str, end_trade_date: str) -> Tuple[pd.DataFrame, str]:
    universe, source = _fetch_industry_universe(level)
    start_df, start_used = _fetch_sw_snapshot_near(start_trade_date, prefer='forward', max_days=12)
    end_df, end_used = _fetch_sw_snapshot_near(end_trade_date, prefer='backward', max_days=20)

    if end_df.empty:
        return pd.DataFrame(), source

    use_universe = False
    if not universe.empty:
        code_set = set(universe['ts_code'].tolist())
        start_filtered = start_df[start_df['ts_code'].isin(code_set)].copy()
        end_filtered = end_df[end_df['ts_code'].isin(code_set)].copy()
        if not end_filtered.empty:
            start_df = start_filtered
            end_df = end_filtered
            use_universe = True
        else:
            source = str(source) + '|universe_filter_skipped'

    if end_df.empty:
        return pd.DataFrame(), source

    # 若起始快照缺失，回退为终点快照，确保累计收益可计算（结果为0附近）
    if start_df.empty:
        start_df = end_df[['ts_code', 'close']].copy()
        start_df['trade_date'] = start_used or end_used
        source = str(source) + '|start_snapshot_fallback'

    d0 = start_df[['ts_code', 'close']].rename(columns={'close': 'start_close'})
    d1 = end_df.copy()
    d = d1.merge(d0, on='ts_code', how='left')

    if use_universe:
        d = d.merge(universe, on='ts_code', how='left', suffixes=('', '_uni'))
        if 'name_uni' in d.columns:
            d['name'] = d['name_uni'].fillna(d['name'])
            d = d.drop(columns=['name_uni'])

    d['cum_return'] = (d['close'] / d['start_close'] - 1.0) * 100.0
    d = d.drop_duplicates(subset=['ts_code'], keep='last').reset_index(drop=True)
    return d, source


def _fetch_target_stock(target_code: str, start_trade_date: str, end_trade_date: str) -> dict:
    code = str(target_code or '600425.SH').strip().upper()
    if not code:
        code = '600425.SH'

    try:
        daily = pro.daily(
            ts_code=code,
            start_date=start_trade_date,
            end_date=end_trade_date,
            fields='ts_code,trade_date,close,pct_chg,amount'
        )
    except TypeError:
        try:
            daily = pro.daily(ts_code=code, start_date=start_trade_date, end_date=end_trade_date)
        except Exception:
            daily = pd.DataFrame()
    except Exception:
        daily = pd.DataFrame()

    if daily is None or daily.empty:
        return {}

    daily = daily.sort_values('trade_date').reset_index(drop=True)
    pct_col = 'pct_chg' if 'pct_chg' in daily.columns else _pick_column(daily, ['pct_change'])
    start_close = _safe_float(daily.iloc[0].get('close'))
    end_close = _safe_float(daily.iloc[-1].get('close'))
    end_amount = _safe_float(daily.iloc[-1].get('amount'))
    end_pct = _safe_float(daily.iloc[-1].get(pct_col)) if pct_col else None
    end_trade = str(daily.iloc[-1].get('trade_date', end_trade_date))

    cum_return = None
    if start_close not in (None, 0) and end_close is not None:
        cum_return = (end_close / start_close - 1.0) * 100.0

    try:
        basic = pro.daily_basic(
            ts_code=code,
            start_date=end_trade_date,
            end_date=end_trade_date,
            fields='ts_code,trade_date,pe,pb,total_mv,circ_mv'
        )
    except Exception:
        basic = pd.DataFrame()

    if basic is None or basic.empty:
        st = (datetime.strptime(end_trade_date, '%Y%m%d') - timedelta(days=120)).strftime('%Y%m%d')
        try:
            basic = pro.daily_basic(
                ts_code=code,
                start_date=st,
                end_date=end_trade_date,
                fields='ts_code,trade_date,pe,pb,total_mv,circ_mv'
            )
        except Exception:
            basic = pd.DataFrame()

    pe = pb = total_mv = float_mv = None
    if basic is not None and not basic.empty:
        basic = basic.sort_values('trade_date').reset_index(drop=True)
        row = basic.iloc[-1]
        pe = _safe_float(row.get('pe'))
        pb = _safe_float(row.get('pb'))
        total_mv = _safe_float(row.get('total_mv'))
        float_mv = _safe_float(row.get('circ_mv'))

    name = '青松建化' if code == '600425.SH' else code
    try:
        info = pro.stock_basic(ts_code=code, fields='ts_code,name')
    except Exception:
        try:
            info = pro.query('stock_basic', fields='ts_code,name,list_status')
            if info is not None and not info.empty and 'ts_code' in info.columns:
                info = info[info['ts_code'] == code].copy()
        except Exception:
            info = pd.DataFrame()
    if info is not None and not info.empty and 'name' in info.columns:
        name = str(info.iloc[0]['name'] or name)

    return {
        'ts_code': code,
        'name': name,
        'kind': 'stock',
        'is_target': True,
        'trade_date': end_trade,
        'close': end_close,
        'cum_return': cum_return,
        'pct_change': end_pct,
        'amount': end_amount,
        'pe': pe,
        'pb': pb,
        'total_mv': total_mv,
        'float_mv': float_mv,
    }


def _calc_bubble_size(df: pd.DataFrame, metric: str) -> pd.Series:
    metric = metric if metric in _METRIC_META else 'amount'
    vals = pd.to_numeric(df.get(metric, pd.Series([pd.NA] * len(df))), errors='coerce')
    transformed = vals.apply(lambda x: math.log1p(x) if pd.notna(x) and x > 0 else pd.NA)

    lo = _safe_float(transformed.min())
    hi = _safe_float(transformed.max())
    if lo is None or hi is None:
        return pd.Series([20.0] * len(df), index=df.index)
    if abs(hi - lo) < 1e-9:
        return pd.Series([26.0] * len(df), index=df.index)

    def _scale(v):
        if pd.isna(v):
            return 14.0
        ratio = (float(v) - lo) / (hi - lo)
        return 12.0 + ratio * 34.0

    return transformed.apply(_scale)


def _calc_similarity(industry_df: pd.DataFrame, target: dict) -> List[dict]:
    feats = ['cum_return', 'pct_change', 'pe', 'pb', 'total_mv', 'float_mv', 'amount']
    if industry_df is None or industry_df.empty or not target:
        return []

    work = industry_df[['ts_code', 'name'] + feats].copy()
    for c in feats:
        work[c] = pd.to_numeric(work[c], errors='coerce')

    target_vals = {f: _safe_float(target.get(f)) for f in feats}
    z_scores = {}
    target_z = {}
    for f in feats:
        col = work[f]
        tv = target_vals.get(f)
        if tv is None:
            continue
        mu = _safe_float(col.mean(skipna=True))
        sigma = _safe_float(col.std(skipna=True, ddof=0))
        if mu is None or sigma is None or sigma < 1e-9:
            continue
        z_scores[f] = (col - mu) / sigma
        target_z[f] = (tv - mu) / sigma

    if not z_scores:
        return []

    out = []
    for i, row in work.iterrows():
        if str(row.get('ts_code')) == str(target.get('ts_code')):
            continue
        diffs = []
        used = []
        for f, zcol in z_scores.items():
            z_val = _safe_float(zcol.iloc[i])
            t_val = _safe_float(target_z.get(f))
            if z_val is None or t_val is None:
                continue
            diffs.append((z_val - t_val) ** 2)
            used.append(f)
        if len(diffs) < 3:
            continue
        dist = math.sqrt(sum(diffs) / len(diffs))
        out.append({
            'ts_code': str(row.get('ts_code') or ''),
            'name': str(row.get('name') or ''),
            'distance': round(dist, 4),
            'score': round(100.0 / (1.0 + dist), 2),
            'used_features': used,
        })

    out = sorted(out, key=lambda x: x['distance'])
    return out[:5]


def _point_from_row(row: dict, x_metric: str, bubble_metric: str, bubble_size: float) -> dict:
    return {
        'ts_code': str(row.get('ts_code') or ''),
        'name': str(row.get('name') or ''),
        'kind': str(row.get('kind') or 'industry'),
        'is_target': bool(row.get('is_target', False)),
        'trade_date': str(row.get('trade_date') or ''),
        'cum_return': _safe_float(row.get('cum_return')),
        'pct_change': _safe_float(row.get('pct_change')),
        'amount': _safe_float(row.get('amount')),
        'pe': _safe_float(row.get('pe')),
        'pb': _safe_float(row.get('pb')),
        'total_mv': _safe_float(row.get('total_mv')),
        'float_mv': _safe_float(row.get('float_mv')),
        'close': _safe_float(row.get('close')),
        'x_value': _safe_float(row.get(x_metric)),
        'bubble_value': _safe_float(row.get(bubble_metric)),
        'bubble_size': _safe_float(bubble_size),
    }


def get_industry_rotation_map(
    level='L1',
    start_date='20240924',
    end_date='',
    x_metric='pct_change',
    x_start_date='',
    x_end_date='',
    bubble_metric='amount',
    target_code='600425.SH',
):
    level = str(level or 'L1').upper()
    if level not in ('L1', 'L2', 'L3'):
        level = 'L1'

    x_metric = x_metric if x_metric in _METRIC_META else 'pct_change'
    bubble_metric = bubble_metric if bubble_metric in _METRIC_META else 'amount'
    target_code = str(target_code or '600425.SH').strip().upper() or '600425.SH'
    if '.' not in target_code and target_code.isdigit():
        target_code = target_code + '.SH'

    cache_key = (
        level,
        str(start_date),
        str(end_date),
        x_metric,
        str(x_start_date),
        str(x_end_date),
        bubble_metric,
        target_code,
    )
    now_ts = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now_ts - cached.get('_ts', 0)) <= _CACHE_TTL_SECONDS:
        payload = dict(cached['data'])
        payload['cache_hit'] = True
        return payload

    window = _get_trade_window(start_date=start_date, end_date=end_date)
    start_trade = window['start_trade_date']
    end_trade = window['end_trade_date']
    x_window = _get_trade_window(
        start_date=(x_start_date or start_trade),
        end_date=(x_end_date or end_trade),
    )
    same_main_window = (
        x_window.get('start_trade_date') == start_trade and
        x_window.get('end_trade_date') == end_trade
    )

    industry_df, universe_source = _build_industry_frame(
        level=level,
        start_trade_date=start_trade,
        end_trade_date=end_trade,
    )

    if industry_df is None or industry_df.empty:
        return {
            'ok': False,
            'message': '未获取到行业数据（可能是当日快照尚未落库、网络/接口抖动或权限限制）。请稍后重试，或将结束日期改为最近已收盘交易日。',
            'window': window,
            'x_window': x_window,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

    industry_df['kind'] = 'industry'
    industry_df['is_target'] = False
    industry_df['x_interval_return'] = pd.NA

    target_x_interval_return = None
    warnings = []
    if x_metric == 'x_interval_return':
        if same_main_window:
            # Deterministic alignment: when X-window equals the main window, force X==Y.
            industry_df['x_interval_return'] = pd.to_numeric(industry_df['cum_return'], errors='coerce')
        else:
            x_df, _ = _build_industry_frame(
                level=level,
                start_trade_date=x_window['start_trade_date'],
                end_trade_date=x_window['end_trade_date'],
            )
            if x_df is not None and not x_df.empty:
                x_map = x_df.set_index('ts_code')['cum_return'].to_dict()
                industry_df['x_interval_return'] = industry_df['ts_code'].map(x_map)
            else:
                warnings.append('X轴区间行业数据缺失，建议调整X区间日期后重试。')

    industry_df['bubble_size'] = _calc_bubble_size(industry_df, bubble_metric)

    target = _fetch_target_stock(
        target_code=target_code,
        start_trade_date=start_trade,
        end_trade_date=end_trade,
    )
    if x_metric == 'x_interval_return':
        if same_main_window:
            target_x_interval_return = _safe_float(target.get('cum_return')) if target else None
        else:
            target_x = _fetch_target_stock(
                target_code=target_code,
                start_trade_date=x_window['start_trade_date'],
                end_trade_date=x_window['end_trade_date'],
            )
            if target_x:
                target_x_interval_return = _safe_float(target_x.get('cum_return'))

    target_size = 40.0
    if target:
        target['bubble_size'] = target_size
        target['x_interval_return'] = target_x_interval_return
        if x_metric == 'x_interval_return' and target['x_interval_return'] is None:
            # Fallback: if x-window target pull fails, keep target visible using main window cum return.
            target['x_interval_return'] = _safe_float(target.get('cum_return'))
            warnings.append('对照个股在X轴区间缺少数据，已回退为主区间累计涨幅展示。')
        target['x_value'] = _safe_float(target.get(x_metric))
        if x_metric == 'x_interval_return' and target['x_value'] is None:
            target['x_value'] = _safe_float(target.get('cum_return'))
        target['bubble_value'] = _safe_float(target.get(bubble_metric))

    points = []
    for _, row in industry_df.iterrows():
        points.append(_point_from_row(row.to_dict(), x_metric, bubble_metric, row.get('bubble_size', 20.0)))
    if target:
        points.append(_point_from_row(target, x_metric, bubble_metric, target.get('bubble_size', target_size)))

    x_vals = pd.to_numeric(industry_df.get(x_metric), errors='coerce').dropna()
    y_vals = pd.to_numeric(industry_df.get('cum_return'), errors='coerce').dropna()
    x_median = _safe_float(x_vals.median()) if not x_vals.empty else None
    y_median = _safe_float(y_vals.median()) if not y_vals.empty else None

    similar = _calc_similarity(industry_df, target) if target else []
    for item in similar:
        item['used_feature_labels'] = [_METRIC_META[k]['label'] for k in item['used_features'] if k in _METRIC_META]

    payload = {
        'ok': True,
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'cache_hit': False,
        'window': window,
        'x_window': x_window,
        'level': level,
        'x_metric': x_metric,
        'bubble_metric': bubble_metric,
        'metric_meta': _METRIC_META,
        'universe_source': universe_source,
        'industry_count': int(len(industry_df)),
        'points': points,
        'target': target,
        'similar_industries': similar,
        'stats': {
            'x_median': x_median,
            'y_median': y_median,
        },
        'warnings': warnings,
    }

    _CACHE[cache_key] = {'_ts': now_ts, 'data': payload}
    return payload
