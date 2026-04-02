# -*- coding: utf-8 -*-
"""
个股通用分析 — 数据服务
提供日线/周线/月线行情、daily_basic、财务指标、神奇九转数据
"""
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
import os
import sqlite3
import math

from AlphaFin.config import TUSHARE_TOKEN

pro = ts.pro_api(TUSHARE_TOKEN)
_AKSHARE_INIT = False
_AKSHARE_MOD = None


def _get_akshare():
    """
    惰性加载 akshare，避免每次分钟周期请求重复 import 带来额外开销。
    """
    global _AKSHARE_INIT, _AKSHARE_MOD
    if _AKSHARE_INIT:
        return _AKSHARE_MOD
    _AKSHARE_INIT = True
    try:
        # 部分环境 ~/.matplotlib 不可写，提前指定可写目录以减少告警。
        os.environ.setdefault('MPLCONFIGDIR', '/tmp/matplotlib-cache')
        import akshare as ak
        _AKSHARE_MOD = ak
    except Exception:
        _AKSHARE_MOD = None
    return _AKSHARE_MOD


def _today_ymd():
    return datetime.now().strftime('%Y%m%d')


def _fetch_daily_from_local_db(ts_code, start_date='20200101', end_date=''):
    """
    从本地 daily_kline.db 读取去重后的日线数据，作为 Tushare 失败时兜底。
    """
    try:
        from AlphaFin.config import DB_ROOT
    except Exception:
        return pd.DataFrame()

    db_path = os.path.join(DB_ROOT, 'daily_kline.db')
    if not os.path.isfile(db_path):
        return pd.DataFrame()

    start = str(start_date or '20200101').replace('-', '')
    end = str(end_date or _today_ymd()).replace('-', '')
    if len(start) != 8 or not start.isdigit():
        start = '20200101'
    if len(end) != 8 or not end.isdigit():
        end = _today_ymd()
    if start > end:
        start, end = end, start

    sql = """
    WITH ranked AS (
        SELECT
            ts_code, trade_date, open, high, low, close, vol, amount,
            ROW_NUMBER() OVER (
                PARTITION BY ts_code, trade_date
                ORDER BY rowid DESC
            ) AS rn
        FROM daily_kline
        WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
    )
    SELECT ts_code, trade_date, open, high, low, close, vol, amount
    FROM ranked
    WHERE rn = 1
    ORDER BY trade_date
    """

    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(sql, conn, params=[ts_code, start, end])
        conn.close()
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()
    return df


def _fetch_dailybasic_from_local_db(ts_code, start_date='20200101', end_date=''):
    """
    从本地 dailybasic.db 读取去重后的估值数据，作为 daily_basic 的兜底。
    """
    try:
        from AlphaFin.config import DB_ROOT
    except Exception:
        return pd.DataFrame()

    db_path = os.path.join(DB_ROOT, 'dailybasic.db')
    if not os.path.isfile(db_path):
        return pd.DataFrame()

    start = str(start_date or '20200101').replace('-', '')
    end = str(end_date or _today_ymd()).replace('-', '')
    if len(start) != 8 or not start.isdigit():
        start = '20200101'
    if len(end) != 8 or not end.isdigit():
        end = _today_ymd()
    if start > end:
        start, end = end, start

    sql = """
    WITH ranked AS (
        SELECT
            ts_code, trade_date, pe, pe_ttm, pb, ps, ps_ttm, total_mv, circ_mv, turnover_rate,
            ROW_NUMBER() OVER (
                PARTITION BY ts_code, trade_date
                ORDER BY rowid DESC
            ) AS rn
        FROM dailybasic
        WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
    )
    SELECT
        ts_code, trade_date, pe, pe_ttm, pb, ps, ps_ttm, total_mv, circ_mv, turnover_rate
    FROM ranked
    WHERE rn = 1
    ORDER BY trade_date
    """

    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(sql, conn, params=[ts_code, start, end])
        conn.close()
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()
    return df


def _fetch_cyq_from_local_cache(ts_code, start_date='20180101', end_date=''):
    """
    读取本地缓存的 cyq_perf 数据。
    """
    try:
        from AlphaFin.config import DB_ROOT
    except Exception:
        return pd.DataFrame()

    db_path = os.path.join(DB_ROOT, 'cyq_cache.db')
    if not os.path.isfile(db_path):
        return pd.DataFrame()

    start = str(start_date or '20180101').replace('-', '')
    end = str(end_date or _today_ymd()).replace('-', '')
    if len(start) != 8 or not start.isdigit():
        start = '20180101'
    if len(end) != 8 or not end.isdigit():
        end = _today_ymd()
    if start > end:
        start, end = end, start

    sql = """
    SELECT
        ts_code, trade_date, his_low, his_high,
        cost_5pct, cost_15pct, cost_50pct, cost_85pct, cost_95pct,
        weight_avg, winner_rate
    FROM cyq_perf_cache
    WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
    ORDER BY trade_date
    """
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(sql, conn, params=[ts_code, start, end])
        conn.close()
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()
    return df


def _save_cyq_to_local_cache(df):
    """
    将 cyq_perf 数据写入本地缓存，便于接口抖动时回退使用。
    """
    if df is None or df.empty:
        return
    try:
        from AlphaFin.config import DB_ROOT
    except Exception:
        return

    db_path = os.path.join(DB_ROOT, 'cyq_cache.db')
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cols = [
        'ts_code', 'trade_date', 'his_low', 'his_high',
        'cost_5pct', 'cost_15pct', 'cost_50pct', 'cost_85pct', 'cost_95pct',
        'weight_avg', 'winner_rate'
    ]
    try:
        w = df[cols].copy()
    except Exception:
        return
    w = w.dropna(subset=['ts_code', 'trade_date']).drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')
    if w.empty:
        return
    w['updated_at'] = now_str

    create_sql = """
    CREATE TABLE IF NOT EXISTS cyq_perf_cache (
        ts_code TEXT NOT NULL,
        trade_date TEXT NOT NULL,
        his_low REAL,
        his_high REAL,
        cost_5pct REAL,
        cost_15pct REAL,
        cost_50pct REAL,
        cost_85pct REAL,
        cost_95pct REAL,
        weight_avg REAL,
        winner_rate REAL,
        updated_at TEXT,
        PRIMARY KEY (ts_code, trade_date)
    )
    """
    upsert_sql = """
    INSERT INTO cyq_perf_cache (
        ts_code, trade_date, his_low, his_high,
        cost_5pct, cost_15pct, cost_50pct, cost_85pct, cost_95pct,
        weight_avg, winner_rate, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ts_code, trade_date) DO UPDATE SET
        his_low=excluded.his_low,
        his_high=excluded.his_high,
        cost_5pct=excluded.cost_5pct,
        cost_15pct=excluded.cost_15pct,
        cost_50pct=excluded.cost_50pct,
        cost_85pct=excluded.cost_85pct,
        cost_95pct=excluded.cost_95pct,
        weight_avg=excluded.weight_avg,
        winner_rate=excluded.winner_rate,
        updated_at=excluded.updated_at
    """
    rows = list(w.itertuples(index=False, name=None))

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(create_sql)
        cur.executemany(upsert_sql, rows)
        conn.commit()
        conn.close()
    except Exception:
        return


def get_daily_data(ts_code, start_date='20200101', end_date=''):
    """获取日线 OHLCV + daily_basic，返回 JSON 可序列化的 dict"""
    end = str(end_date or _today_ymd()).replace('-', '')
    if len(end) != 8 or not end.isdigit():
        end = _today_ymd()

    # 日线行情
    data_source = 'tushare_daily'
    try:
        df = pro.daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end,
            fields='trade_date,open,high,low,close,vol,amount'
        )
    except Exception:
        df = pd.DataFrame()

    if df is None or df.empty:
        df = _fetch_daily_from_local_db(ts_code=ts_code, start_date=start_date, end_date=end)
        data_source = 'local_daily_kline'
    if df is None or df.empty:
        return {
            'dates': [], 'ohlc': [], 'volumes': [], 'basic': {},
            'basic_history': {'dates': [], 'pe': [], 'ps': [], 'pb': [], 'total_mv': []},
            'latest_trade_date': '',
            'latest_basic_trade_date': '',
            'basic_data_source': 'none',
            'data_source': 'none',
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

    df = df.sort_values('trade_date').reset_index(drop=True)

    # daily_basic（优先在线，失败时回退本地 dailybasic.db）
    basic_source = 'tushare_daily_basic'
    try:
        db = pro.daily_basic(
            ts_code=ts_code, start_date=start_date, end_date=end,
            fields='trade_date,pe,pe_ttm,pb,ps,ps_ttm,total_mv,circ_mv,turnover_rate'
        )
        if db is not None and not db.empty:
            db = db.sort_values('trade_date').reset_index(drop=True)
        else:
            db = pd.DataFrame()
    except Exception:
        db = pd.DataFrame()

    if db.empty:
        # 补查最近一段，避免接口临时返回空导致估值图缺失
        try:
            st = (datetime.strptime(end, '%Y%m%d') - timedelta(days=420)).strftime('%Y%m%d')
            db = pro.daily_basic(
                ts_code=ts_code, start_date=st, end_date=end,
                fields='trade_date,pe,pe_ttm,pb,ps,ps_ttm,total_mv,circ_mv,turnover_rate'
            )
            if db is not None and not db.empty:
                db = db.sort_values('trade_date').reset_index(drop=True)
            else:
                db = pd.DataFrame()
        except Exception:
            db = pd.DataFrame()

    local_db = _fetch_dailybasic_from_local_db(ts_code=ts_code, start_date=start_date, end_date=end)
    if db is None or db.empty:
        if local_db is not None and not local_db.empty:
            db = local_db
            basic_source = 'local_dailybasic'
        else:
            db = pd.DataFrame()
            basic_source = 'none'
    else:
        if local_db is not None and not local_db.empty:
            try:
                remote_latest = str(db['trade_date'].max())
                local_latest = str(local_db['trade_date'].max())
                if local_latest > remote_latest:
                    db = local_db
                    basic_source = 'local_dailybasic'
            except Exception:
                pass

    dates = df['trade_date'].tolist()
    ohlc = df[['open', 'high', 'low', 'close']].values.tolist()
    volumes = df['vol'].tolist()

    # 最新 daily_basic 数据
    basic = {}
    if not db.empty:
        latest = db.iloc[-1]
        for col in ['pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'total_mv', 'circ_mv', 'turnover_rate']:
            val = latest.get(col)
            basic[col] = round(float(val), 2) if pd.notna(val) else None

    # 完整 daily_basic 时间序列（用于估值分析图表）
    basic_history = {'dates': [], 'pe': [], 'ps': [], 'pb': [], 'total_mv': []}
    if not db.empty:
        for _, row in db.iterrows():
            basic_history['dates'].append(row['trade_date'])
            for col in ['pe', 'ps', 'pb', 'total_mv']:
                val = row.get(col)
                basic_history[col].append(round(float(val), 2) if pd.notna(val) else None)

    return {
        'dates': dates,
        'ohlc': ohlc,
        'volumes': volumes,
        'basic': basic,
        'basic_history': basic_history,
        'latest_trade_date': str(dates[-1]) if dates else '',
        'latest_basic_trade_date': str(db['trade_date'].iloc[-1]) if not db.empty else '',
        'basic_data_source': basic_source,
        'data_source': data_source,
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


def get_weekly_monthly_data(ts_code, start_date='20200101', freq='W', end_date=''):
    """获取周线或月线数据"""
    end = str(end_date or _today_ymd()).replace('-', '')
    if len(end) != 8 or not end.isdigit():
        end = _today_ymd()

    try:
        if freq == 'W':
            df = pro.weekly(ts_code=ts_code, start_date=start_date, end_date=end,
                            fields='trade_date,open,high,low,close,vol,amount')
        else:
            df = pro.monthly(ts_code=ts_code, start_date=start_date, end_date=end,
                             fields='trade_date,open,high,low,close,vol,amount')
        data_source = 'tushare_' + ('weekly' if freq == 'W' else 'monthly')
    except Exception:
        df = pd.DataFrame()
        data_source = 'none'

    # 在线接口失败时，用本地日线重采样兜底
    if df is None or df.empty:
        daily = _fetch_daily_from_local_db(ts_code=ts_code, start_date=start_date, end_date=end)
        if daily is not None and not daily.empty:
            d = daily.copy()
            d['trade_date_dt'] = pd.to_datetime(d['trade_date'])
            d = d.sort_values('trade_date_dt')
            rule = 'W-FRI' if freq == 'W' else 'M'
            g = d.set_index('trade_date_dt').resample(rule)
            out = pd.DataFrame({
                'trade_date': g['trade_date'].last(),  # 周/月周期内最后一个真实交易日
                'open': g['open'].first(),
                'high': g['high'].max(),
                'low': g['low'].min(),
                'close': g['close'].last(),
                'vol': g['vol'].sum(),
                'amount': g['amount'].sum(),
            }).dropna(subset=['trade_date', 'open', 'high', 'low', 'close']).reset_index(drop=True)
            df = out[['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']].copy()
            data_source = 'local_daily_resample_' + ('weekly' if freq == 'W' else 'monthly')

    if df is None or df.empty:
        return {
            'dates': [], 'ohlc': [], 'volumes': [], 'basic': {},
            'latest_trade_date': '',
            'data_source': 'none',
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

    df = df.sort_values('trade_date').reset_index(drop=True)

    dates = df['trade_date'].tolist()
    return {
        'dates': dates,
        'ohlc': df[['open', 'high', 'low', 'close']].values.tolist(),
        'volumes': df['vol'].tolist(),
        'basic': {},
        'latest_trade_date': str(dates[-1]) if dates else '',
        'data_source': data_source,
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


def get_fina_indicator(ts_code):
    """获取最近 N 期财务指标，返回 JSON 可序列化的 dict"""
    fields = (
        'ts_code,ann_date,end_date,eps,bps,roe,roe_waa,roa,'
        'grossprofit_margin,netprofit_margin,netprofit_yoy,or_yoy,'
        'debt_to_assets,current_ratio,quick_ratio,'
        'revenue,total_profit,n_income'
    )
    try:
        df = pro.fina_indicator(ts_code=ts_code, fields=fields)
    except Exception:
        df = None

    if df is None or df.empty:
        return {'columns': [], 'data': []}

    df = df.sort_values('end_date', ascending=False).head(20)

    # 将 NaN 替换为 None，确保 JSON 序列化
    df = df.where(pd.notna(df), None)

    # 数值列四舍五入
    num_cols = ['eps', 'bps', 'roe', 'roe_waa', 'roa', 'grossprofit_margin',
                'netprofit_margin', 'netprofit_yoy', 'or_yoy',
                'debt_to_assets', 'current_ratio', 'quick_ratio']
    for col in num_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: round(float(x), 2) if x is not None else None)

    # 大数字列（亿元）
    big_cols = ['revenue', 'total_profit', 'n_income']
    for col in big_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: round(float(x) / 1e4, 2) if x is not None else None
            )

    columns = [c for c in df.columns if c not in ('ts_code', 'ann_date')]
    data = df[columns].to_dict(orient='records')

    return {
        'columns': columns,
        'data': data,
    }


def get_nineturn(ts_code, freq='D'):
    """
    获取神奇九转信号。
    优先尝试 Tushare API，失败则用前端 JS 自行计算（返回空数据）。
    """
    # 尝试 Tushare stk_nineturn API
    try:
        if freq == 'D':
            end_type = '1'
        elif freq == 'W':
            end_type = '2'
        else:
            end_type = '3'

        df = pro.stk_nineturn(ts_code=ts_code, end_type=end_type)
        if df is not None and not df.empty:
            df = df.sort_values('trade_date').reset_index(drop=True)
            result = []
            for _, row in df.iterrows():
                item = {'trade_date': row['trade_date']}
                if 'nineturn_buy' in df.columns and pd.notna(row.get('nineturn_buy')):
                    item['buy'] = int(row['nineturn_buy'])
                if 'nineturn_sell' in df.columns and pd.notna(row.get('nineturn_sell')):
                    item['sell'] = int(row['nineturn_sell'])
                if 'buy' in item or 'sell' in item:
                    result.append(item)
            return {'signals': result, 'source': 'api'}
    except Exception:
        pass

    # API 不可用，让前端自行计算
    return {'signals': [], 'source': 'frontend'}


def get_cyq_perf(ts_code, start_date='20180101', end_date=''):
    """获取每日筹码分布及胜率数据"""
    end = str(end_date or _today_ymd()).replace('-', '')
    if len(end) != 8 or not end.isdigit():
        end = _today_ymd()

    cyq_source = 'tushare_cyq_perf'
    try:
        df = pro.cyq_perf(
            ts_code=ts_code, start_date=start_date, end_date=end,
            fields='ts_code,trade_date,his_low,his_high,cost_5pct,cost_15pct,cost_50pct,cost_85pct,cost_95pct,weight_avg,winner_rate'
        )
    except TypeError:
        try:
            df = pro.cyq_perf(
                ts_code=ts_code, start_date=start_date,
                fields='ts_code,trade_date,his_low,his_high,cost_5pct,cost_15pct,cost_50pct,cost_85pct,cost_95pct,weight_avg,winner_rate'
            )
            if df is not None and not df.empty:
                df = df[df['trade_date'] <= end].copy()
        except Exception:
            df = None
    except Exception:
        df = None

    cache_df = _fetch_cyq_from_local_cache(ts_code=ts_code, start_date=start_date, end_date=end)
    if df is not None and not df.empty:
        df = df.sort_values('trade_date').drop_duplicates(subset=['trade_date'], keep='last').reset_index(drop=True)
        _save_cyq_to_local_cache(df)
        if cache_df is not None and not cache_df.empty:
            try:
                cache_latest = str(cache_df['trade_date'].max())
                online_latest = str(df['trade_date'].max())
                if cache_latest > online_latest:
                    df = cache_df
                    cyq_source = 'local_cyq_cache'
            except Exception:
                pass
    elif cache_df is not None and not cache_df.empty:
        df = cache_df
        cyq_source = 'local_cyq_cache'
    else:
        df = pd.DataFrame()
        cyq_source = 'none'

    if df is None or df.empty:
        return {'dates': [], 'cost_5pct': [], 'cost_15pct': [], 'cost_50pct': [],
                'cost_85pct': [], 'cost_95pct': [], 'weight_avg': [], 'winner_rate': [],
                'current_price': None, 'latest_trade_date': '', 'data_source': 'none',
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    df = df.sort_values('trade_date').drop_duplicates(subset=['trade_date'], keep='last').reset_index(drop=True)

    # 获取最新收盘价
    current_price = None
    try:
        dp = pro.daily(ts_code=ts_code, start_date=df['trade_date'].iloc[-1], end_date=end,
                       fields='trade_date,close')
        if dp is not None and not dp.empty:
            current_price = float(dp.sort_values('trade_date').iloc[-1]['close'])
    except Exception:
        pass

    if current_price is None:
        local_daily = _fetch_daily_from_local_db(ts_code=ts_code, start_date=df['trade_date'].iloc[-1], end_date=end)
        if local_daily is not None and not local_daily.empty:
            try:
                current_price = float(local_daily.sort_values('trade_date').iloc[-1]['close'])
            except Exception:
                current_price = None

    result = {
        'dates': df['trade_date'].tolist(),
        'current_price': current_price,
        'latest_trade_date': str(df['trade_date'].iloc[-1]) if len(df) else '',
        'data_source': cyq_source,
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }
    for col in ['cost_5pct', 'cost_15pct', 'cost_50pct', 'cost_85pct', 'cost_95pct', 'weight_avg', 'winner_rate']:
        result[col] = [round(float(v), 4) if pd.notna(v) else None for v in df[col]]

    return result


# ────────────────────────────────
# 多周期共振系统
# ────────────────────────────────

_PERIOD_CONFIG = [
    {'key': '5MIN', 'label': '5分钟', 'kind': 'minute', 'bars_per_day': 48.0},
    {'key': '15MIN', 'label': '15分钟', 'kind': 'minute', 'bars_per_day': 16.0},
    {'key': '30MIN', 'label': '30分钟', 'kind': 'minute', 'bars_per_day': 8.0},
    {'key': '60MIN', 'label': '60分钟', 'kind': 'minute', 'bars_per_day': 4.0},
    {'key': 'D', 'label': '日线', 'kind': 'day', 'bars_per_day': 1.0},
    {'key': 'W', 'label': '周线', 'kind': 'week', 'bars_per_day': 0.2},
    {'key': 'M', 'label': '月线', 'kind': 'month', 'bars_per_day': 1.0 / 21.0},
]


def _safe_round(v, digits=4):
    try:
        n = float(v)
    except Exception:
        return None
    if pd.isna(n):
        return None
    return round(n, digits)


def _relation(a, b, eps=1e-10):
    if a is None or b is None:
        return 0
    d = float(a) - float(b)
    if abs(d) <= eps:
        return 0
    return 1 if d > 0 else -1


def _rel_to_name(rel):
    if rel > 0:
        return 'golden'
    if rel < 0:
        return 'death'
    return 'neutral'


def _cross_event(prev_rel, curr_rel):
    if prev_rel <= 0 < curr_rel:
        return 'golden'
    if prev_rel >= 0 > curr_rel:
        return 'death'
    return 'none'


def _normalize_asset_type(asset_type):
    v = str(asset_type or 'stock').strip().lower()
    return 'index' if v in ('index', 'idx', 'zs') else 'stock'


def _normalize_ts_code(ts_code):
    return str(ts_code or '').strip().upper()


def _get_recent_trade_dates(asset_type, ts_code, days=5):
    """
    获取最近 N 个交易日（升序）。
    优先用对应标的的日线接口，缺失时回退交易日历。
    """
    asset_type = _normalize_asset_type(asset_type)
    ts_code = _normalize_ts_code(ts_code)
    days = int(max(1, days))
    today = datetime.now().strftime('%Y%m%d')

    df = None
    try:
        if asset_type == 'stock':
            df = pro.daily(ts_code=ts_code, end_date=today, fields='trade_date')
        else:
            df = pro.index_daily(ts_code=ts_code, end_date=today, fields='trade_date')
    except Exception:
        df = None

    if df is not None and not df.empty and 'trade_date' in df.columns:
        vals = sorted(set([str(x) for x in df['trade_date'].tolist() if x]))
        if vals:
            return vals[-days:]

    # 日线回退失败时，用交易日历兜底
    try:
        start = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
        cal = pro.trade_cal(exchange='SSE', start_date=start, end_date=today, is_open='1', fields='cal_date,is_open')
    except Exception:
        cal = None
    if cal is not None and not cal.empty and 'cal_date' in cal.columns:
        vals = sorted(set([str(x) for x in cal['cal_date'].tolist() if x]))
        if vals:
            return vals[-days:]

    return []


def _pick_time_col(df):
    for c in ('trade_time', 'time', 'datetime', 'trade_date'):
        if c in df.columns:
            return c
    return None


def _prepare_ohlc_df(df):
    if df is None or df.empty:
        return pd.DataFrame(columns=['trade_time', 'open', 'high', 'low', 'close', 'vol'])
    out = df.copy()
    time_col = _pick_time_col(out)
    if time_col is None:
        out['trade_time'] = ''
    else:
        out['trade_time'] = out[time_col].astype(str)

    for c in ('open', 'high', 'low', 'close'):
        if c not in out.columns:
            out[c] = None
        out[c] = pd.to_numeric(out[c], errors='coerce')

    if 'vol' not in out.columns:
        out['vol'] = 0
    out['vol'] = pd.to_numeric(out['vol'], errors='coerce').fillna(0)

    # 高低价缺失时用收盘价兜底，避免 KDJ 计算中断
    out['high'] = out['high'].fillna(out['close'])
    out['low'] = out['low'].fillna(out['close'])
    out['open'] = out['open'].fillna(out['close'])

    out = out.sort_values('trade_time').drop_duplicates(subset=['trade_time'], keep='last')
    out = out[['trade_time', 'open', 'high', 'low', 'close', 'vol']].reset_index(drop=True)
    return out


def _fetch_stk_mins_by_trade_days(ts_code, freq_lower, trade_days, limit=2000):
    """
    按交易日循环调用 stk_mins（带时间戳 start/end），并拼接结果。
    用法与文档案例一致：
    pro.stk_mins(ts_code='600000.SH', freq='1min',
                 start_date='2023-08-25 09:00:00',
                 end_date='2023-08-25 19:00:00')
    """
    if not trade_days:
        return None
    chunks = []
    for day in trade_days:
        try:
            d = datetime.strptime(str(day), '%Y%m%d').strftime('%Y-%m-%d')
            s = d + ' 09:00:00'
            e = d + ' 19:00:00'
            # 兼容部分 Tushare 版本：stk_mins 可能不接受 limit 参数
            df = pro.stk_mins(ts_code=ts_code, freq=freq_lower, start_date=s, end_date=e)
            if df is not None and not df.empty:
                chunks.append(df)
        except Exception:
            continue
    if not chunks:
        return None
    out = pd.concat(chunks, ignore_index=True)
    if int(limit) > 0 and len(out) > int(limit):
        out = out.tail(int(limit)).reset_index(drop=True)
    return out


def _resample_from_1min(df_1m, target_freq='5MIN'):
    """
    把 1min 分钟数据重采样为 5/15/30/60/120 分钟。
    """
    target = str(target_freq or '5MIN').upper()
    if target == '1MIN':
        return _prepare_ohlc_df(df_1m)

    bucket_map = {
        '5MIN': 5,
        '15MIN': 15,
        '30MIN': 30,
        '60MIN': 60,
        '120MIN': 120,
    }
    bucket_size = bucket_map.get(target)
    if not bucket_size:
        return _prepare_ohlc_df(df_1m)

    base = _prepare_ohlc_df(df_1m)
    if base is None or base.empty:
        return base

    x = base.copy()
    x['dt'] = pd.to_datetime(x['trade_time'], errors='coerce')
    x = x.dropna(subset=['dt']).sort_values('dt').reset_index(drop=True)
    if x.empty:
        return _prepare_ohlc_df(x)

    x['d'] = x['dt'].dt.strftime('%Y-%m-%d')
    x['seq'] = x.groupby('d').cumcount()
    x['bucket'] = x['seq'] // int(bucket_size)

    agg = x.groupby(['d', 'bucket'], as_index=False).agg({
        'dt': 'last',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'vol': 'sum',
    })
    if agg is None or agg.empty:
        return _prepare_ohlc_df(agg)
    agg['trade_time'] = agg['dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
    out = agg[['trade_time', 'open', 'high', 'low', 'close', 'vol']]
    return _prepare_ohlc_df(out)


def _empty_ohlc_with_message(message):
    out = _prepare_ohlc_df(None)
    try:
        out.attrs['fetch_message'] = str(message or '').strip() or '暂无该周期数据'
    except Exception:
        pass
    return out


def _resample_from_source_minutes(df_src, src_minutes=5, target_minutes=30):
    """
    把 N 分钟K线聚合到更大周期（按交易日内序号分桶）。
    """
    src_minutes = int(src_minutes)
    target_minutes = int(target_minutes)
    if src_minutes <= 0 or target_minutes <= src_minutes or (target_minutes % src_minutes) != 0:
        return _prepare_ohlc_df(df_src)

    ratio = int(target_minutes // src_minutes)
    base = _prepare_ohlc_df(df_src)
    if base is None or base.empty:
        return base

    x = base.copy()
    x['dt'] = pd.to_datetime(x['trade_time'], errors='coerce')
    x = x.dropna(subset=['dt']).sort_values('dt').reset_index(drop=True)
    if x.empty:
        return _prepare_ohlc_df(x)

    x['d'] = x['dt'].dt.strftime('%Y-%m-%d')
    x['seq'] = x.groupby('d').cumcount()
    x['bucket'] = x['seq'] // ratio

    agg = x.groupby(['d', 'bucket'], as_index=False).agg({
        'dt': 'last',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'vol': 'sum',
    })
    if agg is None or agg.empty:
        return _prepare_ohlc_df(agg)
    agg['trade_time'] = agg['dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
    out = agg[['trade_time', 'open', 'high', 'low', 'close', 'vol']]
    return _prepare_ohlc_df(out)


def _build_minute_time_window_from_start(start_date='20200101'):
    """
    分钟接口窗口：
    start_date 取页面“日线起始日期”，格式化为 `YYYY-MM-DD 09:00:00`；
    end_date 取当前日期 `YYYY-MM-DD 19:00:00`。
    """
    sd = str(start_date or '20200101').replace('-', '').strip()
    if len(sd) != 8 or (not sd.isdigit()):
        sd = '20200101'
    try:
        start_dt = datetime.strptime(sd, '%Y%m%d')
    except Exception:
        start_dt = datetime(2020, 1, 1)

    now_dt = datetime.now()
    start_str = start_dt.strftime('%Y-%m-%d') + ' 09:00:00'
    end_str = now_dt.strftime('%Y-%m-%d') + ' 19:00:00'
    return start_str, end_str


def _fetch_stock_minute_bundle_limited(ts_code, start_date='20200101', limit=2000):
    """
    遵循接口限流（每分钟最多2次）：
    1) 仅调用两次 stk_mins: 5min + 15min
    2) 30/60 由已有数据本地聚合得到
    """
    code = _normalize_ts_code(ts_code)
    start_str, end_str = _build_minute_time_window_from_start(start_date)
    window_text = '窗口=' + start_str + ' ~ ' + end_str

    result = {
        '5MIN': _empty_ohlc_with_message('5分钟数据待获取'),
        '15MIN': _empty_ohlc_with_message('15分钟数据待获取'),
        '30MIN': _empty_ohlc_with_message('30分钟数据待聚合'),
        '60MIN': _empty_ohlc_with_message('60分钟数据待聚合'),
    }
    errors = []

    # 调用 1：5min
    try:
        df5_raw = pro.stk_mins(ts_code=code, freq='5min', start_date=start_str, end_date=end_str)
        df5 = _prepare_ohlc_df(df5_raw)
        if df5 is not None and not df5.empty:
            if int(limit) > 0 and len(df5) > int(limit):
                df5 = df5.tail(int(limit)).reset_index(drop=True)
            df5.attrs['fetch_message'] = 'stk_mins 5min 成功，行数=' + str(len(df5)) + '；' + window_text
            result['5MIN'] = df5
        else:
            result['5MIN'] = _empty_ohlc_with_message('5分钟数据为空；' + window_text)
            errors.append('5min:empty')
    except Exception as e:
        result['5MIN'] = _empty_ohlc_with_message('5分钟拉取失败：' + str(e)[:120] + '；' + window_text)
        errors.append('5min:' + type(e).__name__)

    # 调用 2：15min
    try:
        df15_raw = pro.stk_mins(ts_code=code, freq='15min', start_date=start_str, end_date=end_str)
        df15 = _prepare_ohlc_df(df15_raw)
        if df15 is not None and not df15.empty:
            if int(limit) > 0 and len(df15) > int(limit):
                df15 = df15.tail(int(limit)).reset_index(drop=True)
            df15.attrs['fetch_message'] = 'stk_mins 15min 成功，行数=' + str(len(df15)) + '；' + window_text
            result['15MIN'] = df15
        else:
            result['15MIN'] = _empty_ohlc_with_message('15分钟数据为空；' + window_text)
            errors.append('15min:empty')
    except Exception as e:
        result['15MIN'] = _empty_ohlc_with_message('15分钟拉取失败：' + str(e)[:120] + '；' + window_text)
        errors.append('15min:' + type(e).__name__)

    # 本地聚合：优先 5min，缺失时回退 15min
    base5 = result.get('5MIN')
    base15 = result.get('15MIN')
    if base5 is not None and not base5.empty:
        df30 = _resample_from_source_minutes(base5, src_minutes=5, target_minutes=30)
        df60 = _resample_from_source_minutes(base5, src_minutes=5, target_minutes=60)
        if df30 is not None and not df30.empty:
            if int(limit) > 0 and len(df30) > int(limit):
                df30 = df30.tail(int(limit)).reset_index(drop=True)
            df30.attrs['fetch_message'] = '30分钟由 5分钟聚合'
            result['30MIN'] = df30
        else:
            result['30MIN'] = _empty_ohlc_with_message('30分钟聚合失败（5分钟样本不足）')

        if df60 is not None and not df60.empty:
            if int(limit) > 0 and len(df60) > int(limit):
                df60 = df60.tail(int(limit)).reset_index(drop=True)
            df60.attrs['fetch_message'] = '60分钟由 5分钟聚合'
            result['60MIN'] = df60
        else:
            result['60MIN'] = _empty_ohlc_with_message('60分钟聚合失败（5分钟样本不足）')
    elif base15 is not None and not base15.empty:
        df30 = _resample_from_source_minutes(base15, src_minutes=15, target_minutes=30)
        df60 = _resample_from_source_minutes(base15, src_minutes=15, target_minutes=60)
        if df30 is not None and not df30.empty:
            if int(limit) > 0 and len(df30) > int(limit):
                df30 = df30.tail(int(limit)).reset_index(drop=True)
            df30.attrs['fetch_message'] = '30分钟由 15分钟聚合'
            result['30MIN'] = df30
        else:
            result['30MIN'] = _empty_ohlc_with_message('30分钟聚合失败（15分钟样本不足）')

        if df60 is not None and not df60.empty:
            if int(limit) > 0 and len(df60) > int(limit):
                df60 = df60.tail(int(limit)).reset_index(drop=True)
            df60.attrs['fetch_message'] = '60分钟由 15分钟聚合'
            result['60MIN'] = df60
        else:
            result['60MIN'] = _empty_ohlc_with_message('60分钟聚合失败（15分钟样本不足）')
    else:
        msg = '5/15分钟均不可用；' + '/'.join(errors[:3]) if errors else '5/15分钟均不可用'
        result['30MIN'] = _empty_ohlc_with_message('30分钟缺失：' + msg)
        result['60MIN'] = _empty_ohlc_with_message('60分钟缺失：' + msg)

    for key in ('5MIN', '15MIN', '30MIN', '60MIN'):
        try:
            if result[key] is not None and result[key].empty and 'fetch_message' not in result[key].attrs:
                result[key].attrs['fetch_message'] = key + ' 暂无数据'
        except Exception:
            pass
    return result


def _to_ak_symbol(ts_code):
    """
    Tushare 代码转 akshare 分钟接口代码:
    600000.SH -> sh600000
    000001.SZ -> sz000001
    """
    code = _normalize_ts_code(ts_code)
    if not code:
        return ''
    if '.' in code:
        symbol, ex = code.split('.', 1)
        ex = ex.lower()
        if ex in ('sh', 'sz'):
            return ex + symbol
    # 回退：按首位推断
    raw = code.replace('.', '')
    if raw.startswith(('5', '6', '9')):
        return 'sh' + raw[:6]
    return 'sz' + raw[:6]


def _fetch_ak_minute_bars(ts_code, freq='5MIN', limit=1800):
    """
    akshare 分钟线兜底：
    使用 stock_zh_a_minute，兼容列名后转为标准 OHLC 结构。
    """
    freq = str(freq or '5MIN').upper()
    period_map = {
        '1MIN': '1',
        '5MIN': '5',
        '15MIN': '15',
        '30MIN': '30',
        '60MIN': '60',
    }
    period = period_map.get(freq)
    if not period:
        return None

    symbol = _to_ak_symbol(ts_code)
    if not symbol:
        return None

    ak = _get_akshare()
    if ak is None:
        return None

    try:
        df = ak.stock_zh_a_minute(symbol=symbol, period=period, adjust='')
    except Exception:
        df = None
    if df is None or df.empty:
        return None

    x = df.copy()

    def _pick_col(candidates):
        for c in candidates:
            if c in x.columns:
                return c
        return None

    time_col = _pick_col(['day', 'time', 'datetime', 'trade_time', '日期时间', '时间'])
    open_col = _pick_col(['open', '开盘'])
    high_col = _pick_col(['high', '最高'])
    low_col = _pick_col(['low', '最低'])
    close_col = _pick_col(['close', '收盘'])
    vol_col = _pick_col(['volume', 'vol', '成交量'])

    if close_col is None:
        return None

    out = pd.DataFrame()
    out['trade_time'] = x[time_col].astype(str) if time_col else ''
    out['open'] = pd.to_numeric(x[open_col], errors='coerce') if open_col else pd.to_numeric(x[close_col], errors='coerce')
    out['high'] = pd.to_numeric(x[high_col], errors='coerce') if high_col else pd.to_numeric(x[close_col], errors='coerce')
    out['low'] = pd.to_numeric(x[low_col], errors='coerce') if low_col else pd.to_numeric(x[close_col], errors='coerce')
    out['close'] = pd.to_numeric(x[close_col], errors='coerce')
    out['vol'] = pd.to_numeric(x[vol_col], errors='coerce').fillna(0) if vol_col else 0

    out = _prepare_ohlc_df(out)
    if out is not None and not out.empty and int(limit) > 0 and len(out) > int(limit):
        out = out.tail(int(limit)).reset_index(drop=True)
    return out


def _fetch_daily_week_month(asset_type, ts_code, freq='D', start_date='20180101'):
    freq = str(freq or 'D').upper()
    asset_type = _normalize_asset_type(asset_type)
    ts_code = _normalize_ts_code(ts_code)

    df = None
    try:
        if asset_type == 'stock':
            if freq == 'D':
                df = pro.daily(
                    ts_code=ts_code, start_date=start_date,
                    fields='trade_date,open,high,low,close,vol,amount'
                )
            elif freq == 'W':
                df = pro.weekly(
                    ts_code=ts_code, start_date=start_date,
                    fields='trade_date,open,high,low,close,vol,amount'
                )
            elif freq == 'M':
                df = pro.monthly(
                    ts_code=ts_code, start_date=start_date,
                    fields='trade_date,open,high,low,close,vol,amount'
                )
        else:
            if freq == 'D':
                df = pro.index_daily(
                    ts_code=ts_code, start_date=start_date,
                    fields='trade_date,open,high,low,close,vol,amount'
                )
            elif freq == 'W':
                df = pro.index_weekly(
                    ts_code=ts_code, start_date=start_date,
                    fields='trade_date,open,high,low,close,vol,amount'
                )
            elif freq == 'M':
                df = pro.index_monthly(
                    ts_code=ts_code, start_date=start_date,
                    fields='trade_date,open,high,low,close,vol,amount'
                )
    except Exception:
        df = None

    # 指数周/月线兜底：用日线聚合
    if (df is None or df.empty) and asset_type == 'index' and freq in ('W', 'M'):
        try:
            daily = pro.index_daily(
                ts_code=ts_code, start_date=start_date,
                fields='trade_date,open,high,low,close,vol,amount'
            )
        except Exception:
            daily = None
        if daily is not None and not daily.empty:
            x = daily.copy()
            x['trade_date'] = pd.to_datetime(x['trade_date'], format='%Y%m%d', errors='coerce')
            x = x.dropna(subset=['trade_date']).sort_values('trade_date').set_index('trade_date')
            rule = 'W-FRI' if freq == 'W' else 'M'
            agg = x.resample(rule).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'vol': 'sum',
                'amount': 'sum'
            }).dropna(subset=['open', 'high', 'low', 'close'])
            if not agg.empty:
                agg['trade_date'] = agg.index.strftime('%Y%m%d')
                df = agg.reset_index(drop=True)[['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']]

    return _prepare_ohlc_df(df)


def _fetch_minute_bars(asset_type, ts_code, freq='5MIN', lookback_days=90, limit=1800):
    asset_type = _normalize_asset_type(asset_type)
    ts_code = _normalize_ts_code(ts_code)
    freq = str(freq or '5MIN').upper()
    if freq not in ('5MIN', '15MIN', '30MIN', '60MIN', '120MIN'):
        freq = '5MIN'

    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=int(max(10, lookback_days)))

    errors = []

    # 关键：分钟数据按最近5个交易日滑动窗口，不依赖“今天是否开市”
    recent_trade_days = _get_recent_trade_dates(asset_type, ts_code, days=5)
    if recent_trade_days:
        start_day = str(recent_trade_days[0])
        end_day = str(recent_trade_days[-1])
        try:
            start_fmt = datetime.strptime(start_day, '%Y%m%d').strftime('%Y-%m-%d')
            end_fmt = datetime.strptime(end_day, '%Y%m%d').strftime('%Y-%m-%d')
            start_str = start_fmt + ' 09:30:00'
            end_str = end_fmt + ' 15:00:00'
        except Exception:
            start_str = start_dt.strftime('%Y-%m-%d 09:30:00')
            end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        start_str = start_dt.strftime('%Y-%m-%d 09:30:00')
        end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        start_day = start_dt.strftime('%Y%m%d')
        end_day = end_dt.strftime('%Y%m%d')
    freq_lower = freq.lower()

    # 120分钟：按60分钟聚合（doc_id=370/374 仅到60min）
    if freq == '120MIN':
        base_60 = _fetch_minute_bars(asset_type, ts_code, freq='60MIN', lookback_days=lookback_days, limit=max(400, int(limit) * 2))
        if base_60 is None or base_60.empty:
            out_empty = _prepare_ohlc_df(base_60)
            try:
                out_empty.attrs['fetch_message'] = '120MIN 聚合失败：60MIN 数据为空'
            except Exception:
                pass
            return out_empty
        x = base_60.copy()
        x['dt'] = pd.to_datetime(x['trade_time'], errors='coerce')
        x = x.dropna(subset=['dt']).sort_values('dt').reset_index(drop=True)
        if x.empty:
            return _prepare_ohlc_df(x)
        x['d'] = x['dt'].dt.strftime('%Y-%m-%d')
        x['seq'] = x.groupby('d').cumcount()
        x['bucket'] = x['seq'] // 2
        agg = x.groupby(['d', 'bucket'], as_index=False).agg({
            'dt': 'last',
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'vol': 'sum',
        })
        agg['trade_time'] = agg['dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
        out120 = agg[['trade_time', 'open', 'high', 'low', 'close', 'vol']]
        out120 = _prepare_ohlc_df(out120)
        if not out120.empty and int(limit) > 0 and len(out120) > int(limit):
            out120 = out120.tail(int(limit)).reset_index(drop=True)
        try:
            out120.attrs['fetch_message'] = '120MIN 由 60MIN 聚合'
        except Exception:
            pass
        return out120

    # 参考官方文档:
    # - doc_id=374: rt_min（实时分钟，1/5/15/30/60）
    # - doc_id=370: stk_mins（历史分钟，1/5/15/30/60）
    df = None
    best_short_df = None
    min_rows = 40
    if asset_type == 'stock':
        candidates = [
            ('stk_mins_by_days', lambda: _fetch_stk_mins_by_trade_days(ts_code, freq_lower, recent_trade_days, limit=limit)),
            ('stk_mins_ts_range_lower', lambda: pro.stk_mins(ts_code=ts_code, start_date=start_str, end_date=end_str, freq=freq_lower)),
            ('stk_mins_ts_range_upper', lambda: pro.stk_mins(ts_code=ts_code, start_date=start_str, end_date=end_str, freq=freq)),
            ('stk_mins_day_range_lower', lambda: pro.stk_mins(ts_code=ts_code, start_date=start_day, end_date=end_day, freq=freq_lower)),
            ('stk_mins_day_range_upper', lambda: pro.stk_mins(ts_code=ts_code, start_date=start_day, end_date=end_day, freq=freq)),
            ('stk_mins_lower', lambda: pro.stk_mins(ts_code=ts_code, freq=freq_lower)),
            ('stk_mins_upper', lambda: pro.stk_mins(ts_code=ts_code, freq=freq)),
            ('rt_min_upper', lambda: pro.rt_min(ts_code=ts_code, freq=freq)),
            ('rt_min_lower', lambda: pro.rt_min(ts_code=ts_code, freq=freq_lower)),
        ]
    else:
        candidates = [
            ('stk_mins_by_days', lambda: _fetch_stk_mins_by_trade_days(ts_code, freq_lower, recent_trade_days, limit=limit)),
            ('stk_mins_day_range_lower', lambda: pro.stk_mins(ts_code=ts_code, start_date=start_day, end_date=end_day, freq=freq_lower)),
            ('stk_mins_day_range_upper', lambda: pro.stk_mins(ts_code=ts_code, start_date=start_day, end_date=end_day, freq=freq)),
            ('rt_idx_min_upper', lambda: pro.rt_idx_min(ts_code=ts_code, freq=freq)),
            ('rt_idx_min_lower', lambda: pro.rt_idx_min(ts_code=ts_code, freq=freq_lower)),
        ]
    for loader_name, loader in candidates:
        try:
            tmp_df = loader()
        except Exception as e:
            tmp_df = None
            errors.append(loader_name + ':' + type(e).__name__ + ':' + str(e)[:80])
        if tmp_df is None or tmp_df.empty:
            errors.append(loader_name + ':empty')
            continue
        tmp_out = _prepare_ohlc_df(tmp_df)
        if tmp_out is None or tmp_out.empty:
            errors.append(loader_name + ':invalid_ohlc')
            continue
        if best_short_df is None or len(tmp_out) > len(best_short_df):
            best_short_df = tmp_out
        if len(tmp_out) >= min_rows:
            df = tmp_df
            break

    # 1MIN 重采样兜底：当目标频率直接取不到时，尝试先拿 1min 再聚合
    if (df is None or df.empty) and freq != '1MIN':
        one_min_df = None
        one_min_candidates = []
        if asset_type == 'stock':
            one_min_candidates = [
                ('stk_mins_by_days_1m', lambda: _fetch_stk_mins_by_trade_days(ts_code, '1min', recent_trade_days, limit=max(4000, int(limit) * 4))),
                ('stk_mins_ts_range_1m', lambda: pro.stk_mins(ts_code=ts_code, start_date=start_str, end_date=end_str, freq='1min')),
                ('stk_mins_day_range_1m', lambda: pro.stk_mins(ts_code=ts_code, start_date=start_day, end_date=end_day, freq='1min')),
                ('rt_min_upper_1m', lambda: pro.rt_min(ts_code=ts_code, freq='1MIN')),
                ('rt_min_lower_1m', lambda: pro.rt_min(ts_code=ts_code, freq='1min')),
            ]
        else:
            one_min_candidates = [
                ('stk_mins_day_range_1m', lambda: pro.stk_mins(ts_code=ts_code, start_date=start_day, end_date=end_day, freq='1min')),
                ('rt_idx_min_upper_1m', lambda: pro.rt_idx_min(ts_code=ts_code, freq='1MIN')),
                ('rt_idx_min_lower_1m', lambda: pro.rt_idx_min(ts_code=ts_code, freq='1min')),
            ]
        for loader_name, loader in one_min_candidates:
            try:
                tmp_1m = loader()
            except Exception as e:
                tmp_1m = None
                errors.append(loader_name + ':' + type(e).__name__ + ':' + str(e)[:80])
            if tmp_1m is None or tmp_1m.empty:
                errors.append(loader_name + ':empty')
                continue
            tmp_1m_out = _prepare_ohlc_df(tmp_1m)
            if tmp_1m_out is None or tmp_1m_out.empty:
                errors.append(loader_name + ':invalid_ohlc')
                continue
            one_min_df = tmp_1m_out
            break
        if one_min_df is not None and not one_min_df.empty:
            resampled = _resample_from_1min(one_min_df, target_freq=freq)
            if resampled is not None and not resampled.empty:
                df = resampled
                try:
                    df.attrs['fetch_message'] = '由 1MIN 重采样得到 ' + str(freq)
                except Exception:
                    pass

    # 再用 pro_bar 做最后兜底
    if df is None or df.empty:
        asset_flag = 'I' if asset_type == 'index' else 'E'
        try:
            tmp_df = ts.pro_bar(
                pro_api=pro,
                ts_code=ts_code,
                asset=asset_flag,
                freq=freq_lower,
                start_date=start_day,
                end_date=end_day,
            )
        except Exception:
            tmp_df = None
        if tmp_df is not None and not tmp_df.empty:
            tmp_out = _prepare_ohlc_df(tmp_df)
            if best_short_df is None or len(tmp_out) > len(best_short_df):
                best_short_df = tmp_out
            if len(tmp_out) >= min_rows:
                df = tmp_df

    # 指数兜底（少数情况下 rt_idx_min 权限受限，可尝试 stk_mins）
    if (df is None or df.empty) and asset_type == 'index':
        try:
            tmp_df = pro.stk_mins(ts_code=ts_code, start_date=start_day, end_date=end_day, freq=freq_lower)
        except Exception as e:
            tmp_df = None
            errors.append('index_stk_mins_fallback:' + type(e).__name__ + ':' + str(e)[:80])
        if tmp_df is not None and not tmp_df.empty:
            tmp_out = _prepare_ohlc_df(tmp_df)
            if best_short_df is None or len(tmp_out) > len(best_short_df):
                best_short_df = tmp_out
            if len(tmp_out) >= min_rows:
                df = tmp_df

    # 公开源兜底（Tushare 分钟权限不足时）
    if df is None or df.empty:
        try:
            ak_df = _fetch_ak_minute_bars(ts_code, freq=freq, limit=limit)
        except Exception as e:
            ak_df = None
            errors.append('ak_minute:' + type(e).__name__ + ':' + str(e)[:80])
        if ak_df is not None and not ak_df.empty:
            df = ak_df
            try:
                df.attrs['fetch_message'] = '由 akshare 分钟接口补数'
            except Exception:
                pass
        else:
            errors.append('ak_minute:empty')

    if df is None or df.empty:
        if best_short_df is not None and not best_short_df.empty:
            out = best_short_df.copy()
        else:
            out = _prepare_ohlc_df(df)
    else:
        out = _prepare_ohlc_df(df)

    # 强制限制在最近5个交易日窗口内（用户要求）
    if recent_trade_days and out is not None and not out.empty:
        try:
            dts = pd.to_datetime(out['trade_time'], errors='coerce').dt.strftime('%Y%m%d')
            mask = dts.isin(set(recent_trade_days))
            if mask.any():
                out = out.loc[mask].reset_index(drop=True)
        except Exception:
            pass

    if not out.empty and int(limit) > 0 and len(out) > int(limit):
        out = out.tail(int(limit)).reset_index(drop=True)

    if out is not None:
        try:
            if out.empty:
                base_msg = '分钟数据为空；窗口=' + (','.join(recent_trade_days) if recent_trade_days else 'unknown')
                if errors:
                    uniq = []
                    for e in errors:
                        if e not in uniq:
                            uniq.append(e)
                    base_msg += '；原因=' + '/'.join(uniq[:6])
                out.attrs['fetch_message'] = base_msg
            elif 'fetch_message' not in out.attrs:
                out.attrs['fetch_message'] = '分钟数据获取成功，行数=' + str(len(out))
        except Exception:
            pass
    return out


def _calc_macd(close):
    s = pd.Series(close, dtype='float64')
    ema12 = s.ewm(span=12, adjust=False).mean()
    ema26 = s.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    hist = (dif - dea) * 2.0
    return pd.DataFrame({'dif': dif, 'dea': dea, 'hist': hist})


def _calc_kdj(high, low, close, period=9):
    h = pd.Series(high, dtype='float64')
    l = pd.Series(low, dtype='float64')
    c = pd.Series(close, dtype='float64')

    low_n = l.rolling(period, min_periods=1).min()
    high_n = h.rolling(period, min_periods=1).max()
    denom = (high_n - low_n).replace(0, pd.NA)
    rsv = ((c - low_n) / denom * 100.0).fillna(50.0)

    k_list = []
    d_list = []
    prev_k = 50.0
    prev_d = 50.0
    for rv in rsv.tolist():
        k = prev_k * 2.0 / 3.0 + float(rv) / 3.0
        d = prev_d * 2.0 / 3.0 + k / 3.0
        j = 3.0 * k - 2.0 * d
        k_list.append(k)
        d_list.append(d)
        prev_k, prev_d = k, d
    j_list = [3.0 * k_list[i] - 2.0 * d_list[i] for i in range(len(k_list))]
    return pd.DataFrame({'k': pd.Series(k_list), 'd': pd.Series(d_list), 'j': pd.Series(j_list)})


def _predict_cross_days(close, high, low, period_key, indicator='macd'):
    close_list = [float(x) for x in close]
    high_list = [float(x) for x in high]
    low_list = [float(x) for x in low]
    if len(close_list) < 35:
        return {'days_to_golden': None, 'days_to_death': None}

    if period_key in ('W', 'M'):
        max_steps = 72
    elif period_key == 'D':
        max_steps = 260
    else:
        max_steps = 320

    bars_per_day = 1.0
    for cfg in _PERIOD_CONFIG:
        if cfg['key'] == period_key:
            bars_per_day = cfg['bars_per_day']
            break
    if not bars_per_day or bars_per_day <= 0:
        bars_per_day = 1.0

    if indicator == 'macd':
        base_df = _calc_macd(close_list)
        prev_rel = _relation(base_df['dif'].iloc[-1], base_df['dea'].iloc[-1])
    else:
        base_df = _calc_kdj(high_list, low_list, close_list)
        prev_rel = _relation(base_df['k'].iloc[-1], base_df['d'].iloc[-1])

    last_close = close_list[-1]
    days_to_golden = None
    days_to_death = None

    c_ext = close_list[:]
    h_ext = high_list[:]
    l_ext = low_list[:]

    for step in range(1, max_steps + 1):
        c_ext.append(last_close)
        h_ext.append(last_close)
        l_ext.append(last_close)

        if indicator == 'macd':
            x = _calc_macd(c_ext)
            curr_rel = _relation(x['dif'].iloc[-1], x['dea'].iloc[-1])
        else:
            x = _calc_kdj(h_ext, l_ext, c_ext)
            curr_rel = _relation(x['k'].iloc[-1], x['d'].iloc[-1])

        if days_to_golden is None and prev_rel <= 0 < curr_rel:
            days_to_golden = _safe_round(step / bars_per_day, 2)
        if days_to_death is None and prev_rel >= 0 > curr_rel:
            days_to_death = _safe_round(step / bars_per_day, 2)

        prev_rel = curr_rel
        if days_to_golden is not None and days_to_death is not None:
            break

    return {
        'days_to_golden': days_to_golden,
        'days_to_death': days_to_death,
    }


def _build_single_period_result(df, period_key, period_label):
    if df is None or df.empty:
        detail = ''
        try:
            detail = str((df.attrs or {}).get('fetch_message') or '').strip()
        except Exception:
            detail = ''
        return {
            'period': period_key,
            'label': period_label,
            'has_data': False,
            'message': detail or '暂无该周期数据'
        }

    work = df[['trade_time', 'open', 'high', 'low', 'close', 'vol']].copy()
    work = work.dropna(subset=['close'])
    if work.empty:
        detail = ''
        try:
            detail = str((df.attrs or {}).get('fetch_message') or '').strip()
        except Exception:
            detail = ''
        return {
            'period': period_key,
            'label': period_label,
            'has_data': False,
            'message': detail or '该周期无有效收盘价'
        }

    macd = _calc_macd(work['close'])
    kdj = _calc_kdj(work['high'], work['low'], work['close'])
    if len(macd) < 1 or len(kdj) < 1:
        return {
            'period': period_key,
            'label': period_label,
            'has_data': False,
            'message': '样本不足，无法计算交叉'
        }

    macd_curr_rel = _relation(macd['dif'].iloc[-1], macd['dea'].iloc[-1])
    kdj_curr_rel = _relation(kdj['k'].iloc[-1], kdj['d'].iloc[-1])

    if len(macd) >= 2:
        macd_prev_rel = _relation(macd['dif'].iloc[-2], macd['dea'].iloc[-2])
        macd_cross_event = _cross_event(macd_prev_rel, macd_curr_rel)
    else:
        macd_cross_event = 'none'
    if len(kdj) >= 2:
        kdj_prev_rel = _relation(kdj['k'].iloc[-2], kdj['d'].iloc[-2])
        kdj_cross_event = _cross_event(kdj_prev_rel, kdj_curr_rel)
    else:
        kdj_cross_event = 'none'

    macd_forecast = _predict_cross_days(
        work['close'].tolist(), work['high'].tolist(), work['low'].tolist(), period_key, indicator='macd'
    )
    kdj_forecast = _predict_cross_days(
        work['close'].tolist(), work['high'].tolist(), work['low'].tolist(), period_key, indicator='kdj'
    )

    # 多周期指标曲线展示（保留最近窗口）
    if period_key in ('W', 'M'):
        tail_n = 120
    elif period_key == 'D':
        tail_n = 180
    else:
        tail_n = 200

    work_tail = work.tail(tail_n).reset_index(drop=True)
    macd_tail = macd.tail(tail_n).reset_index(drop=True)
    kdj_tail = kdj.tail(tail_n).reset_index(drop=True)

    chart_payload = {
        'times': [str(x) for x in work_tail['trade_time'].tolist()],
        'close': [_safe_round(x, 4) for x in work_tail['close'].tolist()],
        'macd': {
            'dif': [_safe_round(x, 4) for x in macd_tail['dif'].tolist()],
            'dea': [_safe_round(x, 4) for x in macd_tail['dea'].tolist()],
            'hist': [_safe_round(x, 4) for x in macd_tail['hist'].tolist()],
        },
        'kdj': {
            'k': [_safe_round(x, 2) for x in kdj_tail['k'].tolist()],
            'd': [_safe_round(x, 2) for x in kdj_tail['d'].tolist()],
            'j': [_safe_round(x, 2) for x in kdj_tail['j'].tolist()],
        }
    }

    return {
        'period': period_key,
        'label': period_label,
        'has_data': True,
        'bars': int(len(work)),
        'latest_time': str(work['trade_time'].iloc[-1]),
        'macd': {
            'dif': _safe_round(macd['dif'].iloc[-1], 4),
            'dea': _safe_round(macd['dea'].iloc[-1], 4),
            'hist': _safe_round(macd['hist'].iloc[-1], 4),
            'relation': _rel_to_name(macd_curr_rel),
            'cross_event': macd_cross_event,
            'days_to_golden': macd_forecast['days_to_golden'],
            'days_to_death': macd_forecast['days_to_death'],
        },
        'kdj': {
            'k': _safe_round(kdj['k'].iloc[-1], 2),
            'd': _safe_round(kdj['d'].iloc[-1], 2),
            'j': _safe_round(kdj['j'].iloc[-1], 2),
            'relation': _rel_to_name(kdj_curr_rel),
            'cross_event': kdj_cross_event,
            'days_to_golden': kdj_forecast['days_to_golden'],
            'days_to_death': kdj_forecast['days_to_death'],
        },
        'chart': chart_payload,
    }


def _build_resonance_summary(period_results):
    data_map = {str(x.get('period')): x for x in (period_results or []) if x.get('has_data')}
    short_keys = ['5MIN', '15MIN', '30MIN', '60MIN', 'D']

    def all_short(ind):
        for k in short_keys:
            item = data_map.get(k)
            if not item:
                return False
            rel = (((item.get(ind) or {}).get('relation')) or '')
            if rel != 'golden':
                return False
        return True

    short_macd_all_golden = all_short('macd')
    short_kdj_all_golden = all_short('kdj')
    short_dual_all_golden = short_macd_all_golden and short_kdj_all_golden

    wm_risk = []
    for key in ('W', 'M'):
        item = data_map.get(key)
        if not item:
            continue
        for ind in ('macd', 'kdj'):
            days_to_death = ((item.get(ind) or {}).get('days_to_death'))
            if days_to_death is not None and float(days_to_death) <= 30.0:
                wm_risk.append({
                    'period': key,
                    'indicator': ind,
                    'days_to_death': float(days_to_death),
                })

    if short_dual_all_golden and not wm_risk:
        bias = '短中周期共振偏多，且周/月未见近端死叉风险'
    elif short_dual_all_golden and wm_risk:
        bias = '短周期共振偏多，但周/月存在潜在死叉风险'
    else:
        bias = '当前未形成完整短周期共振，建议等待结构一致性'

    return {
        'short_macd_all_golden': short_macd_all_golden,
        'short_kdj_all_golden': short_kdj_all_golden,
        'short_dual_all_golden': short_dual_all_golden,
        'week_month_death_risk': wm_risk,
        'bias': bias,
    }


def get_multi_cycle_resonance(ts_code, asset_type='stock', start_date='20200101'):
    """
    多周期共振系统：
    - 周期：5m/15m/30m/60m/日/周/月
    - 指标：MACD、KDJ
    - 预测：假设未来价格维持当前收盘价，估算金叉/死叉所需天数
    """
    code = _normalize_ts_code(ts_code)
    if not code:
        return {'ok': False, 'message': 'ts_code 不能为空', 'periods': []}

    asset_type = _normalize_asset_type(asset_type)
    start_date = str(start_date or '20200101').replace('-', '')
    if len(start_date) != 8 or not start_date.isdigit():
        start_date = '20200101'
    recent_trade_days = _get_recent_trade_dates(asset_type, code, days=5)
    today = datetime.now().strftime('%Y%m%d')
    latest_trade_date = recent_trade_days[-1] if recent_trade_days else today

    # 顶部K线：统一用日线
    daily_df = _fetch_daily_week_month(asset_type, code, freq='D', start_date=start_date)
    kline_payload = {
        'dates': daily_df['trade_time'].tolist() if not daily_df.empty else [],
        'ohlc': daily_df[['open', 'high', 'low', 'close']].values.tolist() if not daily_df.empty else [],
        'volumes': daily_df['vol'].tolist() if not daily_df.empty else [],
    }

    # 股票分钟线严格按限频规则：仅两次 stk_mins（5min + 15min）
    minute_bundle = {}
    if asset_type == 'stock':
        minute_bundle = _fetch_stock_minute_bundle_limited(code, start_date=start_date, limit=2000)

    period_results = []
    for cfg in _PERIOD_CONFIG:
        key = cfg['key']
        label = cfg['label']
        if cfg['kind'] == 'minute':
            if asset_type == 'stock':
                df = minute_bundle.get(key)
                if df is None:
                    df = _empty_ohlc_with_message(key + ' 分钟数据未生成')
            else:
                # 指数仍走原有分钟抓取链路
                df = _fetch_minute_bars(asset_type, code, freq=key, lookback_days=90, limit=2000)
        else:
            # 周/月需要更长历史，提高预测稳定性
            his_start = '20160101' if key in ('W', 'M') else start_date
            df = _fetch_daily_week_month(asset_type, code, freq=key, start_date=his_start)
        period_results.append(_build_single_period_result(df, key, label))

    has_any = any(x.get('has_data') for x in period_results)
    summary = _build_resonance_summary(period_results)

    return {
        'ok': bool(has_any),
        'message': 'success' if has_any else '未获取到有效数据，请检查代码/网络/交易时段',
        'asset_type': asset_type,
        'ts_code': code,
        'assumption': '未来预测基于“后续价格维持当前收盘价不变”的静态情景，不构成投资建议。',
        'kline': kline_payload,
        'periods': period_results,
        'summary': summary,
        'minute_window_trade_dates': recent_trade_days,
        'latest_trade_date': latest_trade_date,
        'used_latest_trade_day_fallback': (latest_trade_date != today),
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


# ────────────────────────────────
# K线结构相似预测（个股通用分析）
# ────────────────────────────────

def _parse_positive_ints(v, default_vals):
    vals = []
    if isinstance(v, (list, tuple)):
        raw = list(v)
    elif isinstance(v, str):
        raw = [x.strip() for x in v.split(',')]
    else:
        raw = []

    for x in raw:
        try:
            n = int(float(x))
        except Exception:
            continue
        if n > 0:
            vals.append(n)

    if not vals:
        vals = [int(x) for x in (default_vals or []) if int(x) > 0]
    out = []
    seen = set()
    for n in vals:
        if n in seen:
            continue
        seen.add(n)
        out.append(int(n))
    return out


def _parse_similarity_weights(weights):
    base = {'price': 1.0, 'volume': 0.0, 'macd': 0.0, 'kdj': 0.0}
    if isinstance(weights, dict):
        for k in list(base.keys()):
            if k in weights:
                try:
                    x = float(weights.get(k))
                except Exception:
                    continue
                if pd.notna(x) and x >= 0:
                    base[k] = float(x)
    s = sum(base.values())
    if s <= 0:
        base = {'price': 1.0, 'volume': 0.0, 'macd': 0.0, 'kdj': 0.0}
        s = 1.0
    for k in list(base.keys()):
        base[k] = float(base[k]) / float(s)
    return base


def _standardize_vector(vals):
    if vals is None:
        return []
    raw = list(vals)
    if not raw:
        return []
    clean = []
    for x in raw:
        try:
            n = float(x)
        except Exception:
            n = 0.0
        if not math.isfinite(n):
            n = 0.0
        clean.append(n)
    n_len = len(clean)
    if n_len <= 0:
        return []
    mean_v = sum(clean) / float(n_len)
    var_v = 0.0
    for x in clean:
        d = x - mean_v
        var_v += d * d
    std_v = math.sqrt(var_v / float(n_len))
    if (not math.isfinite(std_v)) or std_v < 1e-12:
        return [0.0 for _ in clean]
    return [(x - mean_v) / std_v for x in clean]


def _cosine_similarity(v1, v2):
    if not v1 or not v2:
        return 0.0
    n = min(len(v1), len(v2))
    if n <= 0:
        return 0.0
    a = v1[:n]
    b = v2[:n]
    dot = 0.0
    na = 0.0
    nb = 0.0
    for i in range(n):
        x = float(a[i])
        y = float(b[i])
        dot += x * y
        na += x * x
        nb += y * y
    if na <= 1e-18 or nb <= 1e-18:
        return 0.0
    return float(dot / math.sqrt(na * nb))


def _fetch_stock_pattern_bars(ts_code, freq='D', start_date='19900101', end_date=''):
    """
    为结构匹配准备 OHLCV。
    优先在线接口，失败时回退本地 daily_kline 重采样。
    """
    code = _normalize_ts_code(ts_code)
    f = str(freq or 'D').upper()
    if f not in ('D', 'W', 'M'):
        f = 'D'

    start = str(start_date or '19900101').replace('-', '')
    if len(start) != 8 or not start.isdigit():
        start = '19900101'

    end = str(end_date or _today_ymd()).replace('-', '')
    if len(end) != 8 or not end.isdigit():
        end = _today_ymd()
    if start > end:
        start, end = end, start

    df = pd.DataFrame()
    source = 'none'

    try:
        if f == 'D':
            df = pro.daily(
                ts_code=code, start_date=start, end_date=end,
                fields='trade_date,open,high,low,close,vol,amount'
            )
            source = 'tushare_daily'
        elif f == 'W':
            df = pro.weekly(
                ts_code=code, start_date=start, end_date=end,
                fields='trade_date,open,high,low,close,vol,amount'
            )
            source = 'tushare_weekly'
        else:
            df = pro.monthly(
                ts_code=code, start_date=start, end_date=end,
                fields='trade_date,open,high,low,close,vol,amount'
            )
            source = 'tushare_monthly'
    except Exception:
        df = pd.DataFrame()
        source = 'none'

    if df is not None and not df.empty:
        return _prepare_ohlc_df(df), source

    local_daily = _fetch_daily_from_local_db(ts_code=code, start_date=start, end_date=end)
    if local_daily is None or local_daily.empty:
        return _prepare_ohlc_df(local_daily), 'none'

    if f == 'D':
        return _prepare_ohlc_df(local_daily), 'local_daily_kline'

    # W / M：本地日线重采样
    d = local_daily.copy()
    d['trade_date_dt'] = pd.to_datetime(d['trade_date'], errors='coerce')
    d = d.dropna(subset=['trade_date_dt']).sort_values('trade_date_dt')
    if d.empty:
        return _prepare_ohlc_df(d), 'none'
    rule = 'W-FRI' if f == 'W' else 'M'
    g = d.set_index('trade_date_dt').resample(rule)
    out = pd.DataFrame({
        'trade_date': g['trade_date'].last(),
        'open': g['open'].first(),
        'high': g['high'].max(),
        'low': g['low'].min(),
        'close': g['close'].last(),
        'vol': g['vol'].sum(),
        'amount': g['amount'].sum(),
    }).dropna(subset=['trade_date', 'open', 'high', 'low', 'close']).reset_index(drop=True)
    if out.empty:
        return _prepare_ohlc_df(out), 'none'

    if f == 'W':
        return _prepare_ohlc_df(out), 'local_daily_resample_weekly'
    return _prepare_ohlc_df(out), 'local_daily_resample_monthly'


def _build_segment_feature_vectors(seg_df):
    x = seg_df.copy()
    if x is None or x.empty:
        return {'price': [], 'volume': [], 'macd': [], 'kdj': []}

    o = pd.to_numeric(x['open'], errors='coerce').fillna(method='ffill').fillna(method='bfill').fillna(0.0)
    h = pd.to_numeric(x['high'], errors='coerce').fillna(o)
    l = pd.to_numeric(x['low'], errors='coerce').fillna(o)
    c = pd.to_numeric(x['close'], errors='coerce').fillna(o)
    v = pd.to_numeric(x['vol'], errors='coerce').fillna(0.0)

    base_close = float(c.iloc[0]) if len(c) else 1.0
    if abs(base_close) < 1e-12:
        base_close = 1.0

    safe_open = o.replace(0, pd.NA).fillna(method='ffill').fillna(method='bfill').fillna(base_close)
    close_rel = (c / base_close - 1.0)
    body = ((c - o) / safe_open).fillna(0.0)
    upper = ((h - pd.concat([o, c], axis=1).max(axis=1)) / base_close).fillna(0.0)
    lower = ((pd.concat([o, c], axis=1).min(axis=1) - l) / base_close).fillna(0.0)
    intraday_range = ((h - l) / base_close).fillna(0.0)

    price_vec = []
    price_vec.extend(_standardize_vector(close_rel.tolist()))
    price_vec.extend(_standardize_vector(body.tolist()))
    price_vec.extend(_standardize_vector(upper.tolist()))
    price_vec.extend(_standardize_vector(lower.tolist()))
    price_vec.extend(_standardize_vector(intraday_range.tolist()))

    vol_ma = v.rolling(5, min_periods=1).mean().replace(0, pd.NA)
    vol_ratio = (v / vol_ma).fillna(1.0)
    volume_vec = _standardize_vector(vol_ratio.tolist())

    if 'macd_dif' in x.columns:
        mdif = _standardize_vector(pd.to_numeric(x['macd_dif'], errors='coerce').fillna(0.0).tolist())
    else:
        mdif = [0.0] * len(x)
    if 'macd_dea' in x.columns:
        mdea = _standardize_vector(pd.to_numeric(x['macd_dea'], errors='coerce').fillna(0.0).tolist())
    else:
        mdea = [0.0] * len(x)
    if 'macd_hist' in x.columns:
        mhist = _standardize_vector(pd.to_numeric(x['macd_hist'], errors='coerce').fillna(0.0).tolist())
    else:
        mhist = [0.0] * len(x)
    macd_vec = mdif + mdea + mhist

    if 'kdj_k' in x.columns:
        kk = _standardize_vector((pd.to_numeric(x['kdj_k'], errors='coerce').fillna(50.0) / 100.0).tolist())
    else:
        kk = [0.0] * len(x)
    if 'kdj_d' in x.columns:
        kd = _standardize_vector((pd.to_numeric(x['kdj_d'], errors='coerce').fillna(50.0) / 100.0).tolist())
    else:
        kd = [0.0] * len(x)
    if 'kdj_j' in x.columns:
        kj = _standardize_vector((pd.to_numeric(x['kdj_j'], errors='coerce').fillna(50.0) / 100.0).tolist())
    else:
        kj = [0.0] * len(x)
    kdj_vec = kk + kd + kj

    return {
        'price': price_vec,
        'volume': volume_vec,
        'macd': macd_vec,
        'kdj': kdj_vec,
    }


def _calc_pattern_similarity(target_feat, cand_feat, weights):
    detail = {}
    score = 0.0
    for k in ('price', 'volume', 'macd', 'kdj'):
        w = float(weights.get(k, 0.0))
        if w <= 0:
            detail[k] = None
            continue
        sim = _cosine_similarity(target_feat.get(k, []), cand_feat.get(k, []))
        detail[k] = round(float(sim), 6)
        score += w * sim
    return float(score), detail


def _series_corr(v1, v2):
    if not v1 or not v2:
        return 0.0
    n = min(len(v1), len(v2))
    if n < 2:
        return 0.0
    a = [float(x) for x in v1[:n]]
    b = [float(x) for x in v2[:n]]
    ma = sum(a) / float(n)
    mb = sum(b) / float(n)
    cov = 0.0
    va = 0.0
    vb = 0.0
    for i in range(n):
        da = a[i] - ma
        db = b[i] - mb
        cov += da * db
        va += da * da
        vb += db * db
    if va <= 1e-18 or vb <= 1e-18:
        return 0.0
    return float(cov / math.sqrt(va * vb))


def _segment_regime_stats(close_seg):
    if not close_seg or len(close_seg) < 2:
        return {'vol': 0.0, 'amp': 0.0, 'slope': 0.0}

    c = []
    for x in close_seg:
        try:
            v = float(x)
        except Exception:
            v = 0.0
        if not math.isfinite(v):
            v = 0.0
        c.append(v)
    if not c:
        return {'vol': 0.0, 'amp': 0.0, 'slope': 0.0}

    base = c[0] if abs(c[0]) > 1e-12 else 1.0
    path = [(x / base - 1.0) for x in c]
    amp = max(path) - min(path) if path else 0.0

    rets = []
    for i in range(1, len(c)):
        prev = c[i - 1]
        cur = c[i]
        if abs(prev) < 1e-12:
            r = 0.0
        else:
            r = cur / prev - 1.0
        # 裁剪异常波动，降低复权跳变干扰
        if r > 0.2:
            r = 0.2
        elif r < -0.2:
            r = -0.2
        rets.append(r)
    vol = 0.0
    if rets:
        mr = sum(rets) / float(len(rets))
        vr = sum((x - mr) * (x - mr) for x in rets) / float(len(rets))
        vol = math.sqrt(max(vr, 0.0))

    n = len(path)
    slope = 0.0
    if n >= 2:
        mx = (n - 1) / 2.0
        my = sum(path) / float(n)
        num = 0.0
        den = 0.0
        for i in range(n):
            dx = float(i) - mx
            dy = path[i] - my
            num += dx * dy
            den += dx * dx
        if den > 1e-12:
            slope = num / den

    return {
        'vol': float(max(vol, 0.0)),
        'amp': float(max(amp, 0.0)),
        'slope': float(slope),
    }


def _regime_similarity(target_stats, cand_stats):
    tv = float(target_stats.get('vol', 0.0))
    cv = float(cand_stats.get('vol', 0.0))
    ta = float(target_stats.get('amp', 0.0))
    ca = float(cand_stats.get('amp', 0.0))
    ts = float(target_stats.get('slope', 0.0))
    cs = float(cand_stats.get('slope', 0.0))

    if tv <= 1e-12 and cv <= 1e-12:
        vol_score = 1.0
    else:
        vol_score = min(tv, cv) / max(tv, cv)

    if ta <= 1e-12 and ca <= 1e-12:
        amp_score = 1.0
    else:
        amp_score = min(ta, ca) / max(ta, ca)

    slope_diff = abs(ts - cs)
    slope_score = max(0.0, 1.0 - slope_diff / 0.01)

    score = 0.45 * vol_score + 0.25 * amp_score + 0.30 * slope_score
    return float(max(0.0, min(1.0, score)))


def _build_pattern_chart_payload(seg_df):
    if seg_df is None or seg_df.empty:
        return {
            'dates': [],
            'ohlc': [],
            'volumes': [],
            'macd': {'dif': [], 'dea': [], 'hist': []},
            'kdj': {'k': [], 'd': [], 'j': []},
        }

    x = seg_df.copy().reset_index(drop=True)
    macd = _calc_macd(x['close'])
    kdj = _calc_kdj(x['high'], x['low'], x['close'])

    return {
        'dates': [str(z) for z in x['trade_time'].tolist()],
        'ohlc': x[['open', 'high', 'low', 'close']].values.tolist(),
        'volumes': x['vol'].tolist(),
        'macd': {
            'dif': [_safe_round(v, 4) for v in macd['dif'].tolist()],
            'dea': [_safe_round(v, 4) for v in macd['dea'].tolist()],
            'hist': [_safe_round(v, 4) for v in macd['hist'].tolist()],
        },
        'kdj': {
            'k': [_safe_round(v, 2) for v in kdj['k'].tolist()],
            'd': [_safe_round(v, 2) for v in kdj['d'].tolist()],
            'j': [_safe_round(v, 2) for v in kdj['j'].tolist()],
        },
    }


def _build_pattern_prediction(matches, horizons):
    out = {}
    if not matches:
        return out
    for h in horizons:
        key = str(h)
        vals = []
        wts = []
        for m in matches:
            fr = m.get('future_returns') or {}
            if key not in fr:
                continue
            try:
                v = float(fr.get(key))
            except Exception:
                continue
            vals.append(v)
            try:
                w = float(m.get('similarity'))
            except Exception:
                w = 0.0
            wts.append(max(0.0, w))
        if not vals:
            continue
        s_w = sum(wts)
        if s_w <= 1e-12:
            weighted_mean = float(pd.Series(vals, dtype='float64').mean())
        else:
            weighted_mean = float(sum(vals[i] * wts[i] for i in range(len(vals))) / s_w)
        s = pd.Series(vals, dtype='float64')
        out[key] = {
            'weighted_mean_pct': _safe_round(weighted_mean, 2),
            'mean_pct': _safe_round(float(s.mean()), 2),
            'median_pct': _safe_round(float(s.median()), 2),
            'up_ratio_pct': _safe_round(float((s > 0).sum()) * 100.0 / float(len(s)), 1),
            'min_pct': _safe_round(float(s.min()), 2),
            'max_pct': _safe_round(float(s.max()), 2),
            'std_pct': _safe_round(float(s.std(ddof=0)), 2),
            'samples': int(len(s)),
        }
    return out


def get_stock_pattern_match(ts_code, freq='D', window=40, top_k=5, horizons=None, start_date='19900101', weights=None):
    """
    基于当前最近 N 根K线结构，在历史中寻找最相似的Top-K阶段，并统计其后续收益分布。
    默认特征权重：价格结构100%（成交量/MACD/KDJ为0）。
    """
    code = _normalize_ts_code(ts_code)
    if not code:
        return {'ok': False, 'message': 'ts_code 不能为空', 'matches': []}

    f = str(freq or 'D').upper()
    if f not in ('D', 'W', 'M'):
        f = 'D'
    freq_label = {'D': '日线', 'W': '周线', 'M': '月线'}.get(f, '日线')
    freq_unit = {'D': '天', 'W': '周', 'M': '月'}.get(f, '天')

    try:
        window = int(window)
    except Exception:
        window = 40
    window = max(12, min(240, window))

    try:
        top_k = int(top_k)
    except Exception:
        top_k = 5
    top_k = max(1, min(10, top_k))

    horizons = _parse_positive_ints(horizons, [5, 10, 20])
    horizons = [h for h in horizons if 1 <= int(h) <= 260]
    if not horizons:
        horizons = [5, 10, 20]

    start = str(start_date or '19900101').replace('-', '')
    if len(start) != 8 or not start.isdigit():
        start = '19900101'

    w = _parse_similarity_weights(weights)

    bars_df, data_source = _fetch_stock_pattern_bars(ts_code=code, freq=f, start_date=start, end_date=_today_ymd())
    if bars_df is None or bars_df.empty:
        return {
            'ok': False,
            'message': '未获取到K线数据，请稍后重试',
            'ts_code': code,
            'freq': f,
            'freq_label': freq_label,
            'matches': [],
        }

    work = bars_df.copy().reset_index(drop=True)
    need_min = window + max(horizons) + 20
    if len(work) < need_min:
        return {
            'ok': False,
            'message': '样本不足：当前仅 %d 根%sK线，至少需要 %d 根（窗口+预测步长+缓冲）' % (
                int(len(work)), freq_label, int(need_min)
            ),
            'ts_code': code,
            'freq': f,
            'freq_label': freq_label,
            'window': int(window),
            'top_k': int(top_k),
            'horizons': [int(x) for x in horizons],
            'available_bars': int(len(work)),
            'matches': [],
        }

    macd = _calc_macd(work['close'])
    kdj = _calc_kdj(work['high'], work['low'], work['close'])
    work['macd_dif'] = macd['dif'].values
    work['macd_dea'] = macd['dea'].values
    work['macd_hist'] = macd['hist'].values
    work['kdj_k'] = kdj['k'].values
    work['kdj_d'] = kdj['d'].values
    work['kdj_j'] = kdj['j'].values

    target_start = int(len(work) - window)
    target_end = int(len(work) - 1)
    target_seg = work.iloc[target_start:target_end + 1].copy()
    max_h = int(max(horizons))

    need_price = float(w.get('price', 0.0)) > 0.0
    need_volume = float(w.get('volume', 0.0)) > 0.0
    need_macd = float(w.get('macd', 0.0)) > 0.0
    need_kdj = float(w.get('kdj', 0.0)) > 0.0

    o_list = pd.to_numeric(work['open'], errors='coerce').fillna(0.0).tolist()
    h_list = pd.to_numeric(work['high'], errors='coerce').fillna(0.0).tolist()
    l_list = pd.to_numeric(work['low'], errors='coerce').fillna(0.0).tolist()
    c_list = pd.to_numeric(work['close'], errors='coerce').fillna(0.0).tolist()
    v_list = pd.to_numeric(work['vol'], errors='coerce').fillna(0.0).tolist()
    mdif_list = pd.to_numeric(work['macd_dif'], errors='coerce').fillna(0.0).tolist()
    mdea_list = pd.to_numeric(work['macd_dea'], errors='coerce').fillna(0.0).tolist()
    mhist_list = pd.to_numeric(work['macd_hist'], errors='coerce').fillna(0.0).tolist()
    kk_list = pd.to_numeric(work['kdj_k'], errors='coerce').fillna(50.0).tolist()
    kd_list = pd.to_numeric(work['kdj_d'], errors='coerce').fillna(50.0).tolist()
    kj_list = pd.to_numeric(work['kdj_j'], errors='coerce').fillna(50.0).tolist()

    def _build_fast_features(start_idx, end_idx):
        out = {'price': [], 'volume': [], 'macd': [], 'kdj': []}

        o_seg = o_list[start_idx:end_idx + 1]
        h_seg = h_list[start_idx:end_idx + 1]
        l_seg = l_list[start_idx:end_idx + 1]
        c_seg = c_list[start_idx:end_idx + 1]

        if need_price:
            base = float(c_seg[0]) if c_seg else 1.0
            if abs(base) < 1e-12:
                base = 1.0
            close_rel = []
            body = []
            upper = []
            lower = []
            day_range = []
            for i in range(len(c_seg)):
                oo = float(o_seg[i]) if pd.notna(o_seg[i]) else 0.0
                hh = float(h_seg[i]) if pd.notna(h_seg[i]) else oo
                ll = float(l_seg[i]) if pd.notna(l_seg[i]) else oo
                cc = float(c_seg[i]) if pd.notna(c_seg[i]) else oo
                safe_o = oo if abs(oo) > 1e-12 else base
                close_rel.append(cc / base - 1.0)
                body.append((cc - oo) / safe_o)
                upper.append((hh - max(oo, cc)) / base)
                lower.append((min(oo, cc) - ll) / base)
                day_range.append((hh - ll) / base)
            price_vec = []
            price_vec.extend(_standardize_vector(close_rel))
            price_vec.extend(_standardize_vector(body))
            price_vec.extend(_standardize_vector(upper))
            price_vec.extend(_standardize_vector(lower))
            price_vec.extend(_standardize_vector(day_range))
            out['price'] = price_vec

        if need_volume:
            v_seg = v_list[start_idx:end_idx + 1]
            out['volume'] = _standardize_vector(v_seg)

        if need_macd:
            mdif_seg = _standardize_vector(mdif_list[start_idx:end_idx + 1])
            mdea_seg = _standardize_vector(mdea_list[start_idx:end_idx + 1])
            mhist_seg = _standardize_vector(mhist_list[start_idx:end_idx + 1])
            out['macd'] = mdif_seg + mdea_seg + mhist_seg

        if need_kdj:
            kk_seg = _standardize_vector([x / 100.0 for x in kk_list[start_idx:end_idx + 1]])
            kd_seg = _standardize_vector([x / 100.0 for x in kd_list[start_idx:end_idx + 1]])
            kj_seg = _standardize_vector([x / 100.0 for x in kj_list[start_idx:end_idx + 1]])
            out['kdj'] = kk_seg + kd_seg + kj_seg

        return out

    target_feat = _build_fast_features(target_start, target_end)
    target_close_seg = c_list[target_start:target_end + 1]
    target_path = _standardize_vector(target_close_seg)
    target_regime = _segment_regime_stats(target_close_seg)

    candidates = []
    # 候选段不与当前段重叠，且必须拥有完整未来窗口用于收益统计
    for end_idx in range(window - 1, target_start):
        if int(end_idx + max_h) >= int(len(work)):
            continue
        start_idx = int(end_idx - window + 1)
        if (end_idx - start_idx + 1) != window:
            continue
        cand_feat = _build_fast_features(start_idx, end_idx)
        sim_score_raw, sim_detail = _calc_pattern_similarity(target_feat, cand_feat, w)

        cand_close_seg = c_list[start_idx:end_idx + 1]
        cand_path = _standardize_vector(cand_close_seg)
        path_corr = _series_corr(target_path, cand_path)
        cand_regime = _segment_regime_stats(cand_close_seg)
        regime_sim = _regime_similarity(target_regime, cand_regime)

        # 综合评分：
        # 1) 结构特征余弦相似（主）
        # 2) 价格路径相关性（次）
        # 3) 波动/振幅/斜率状态一致性（稳健约束）
        sim_score = (
            0.62 * float(sim_score_raw) +
            0.28 * float(path_corr) +
            0.10 * (2.0 * float(regime_sim) - 1.0)
        )
        sim_detail['path_corr'] = _safe_round(path_corr, 6)
        sim_detail['regime_sim'] = _safe_round(regime_sim, 6)

        base_close = float(work['close'].iloc[end_idx])
        if abs(base_close) < 1e-12:
            continue

        future_ret = {}
        future_end_dates = {}
        valid = True
        for h in horizons:
            f_idx = int(end_idx + int(h))
            if f_idx >= len(work):
                valid = False
                break
            future_close = float(work['close'].iloc[f_idx])
            ret_pct = (future_close / base_close - 1.0) * 100.0
            future_ret[str(int(h))] = _safe_round(ret_pct, 2)
            future_end_dates[str(int(h))] = str(work['trade_time'].iloc[f_idx])
        if not valid:
            continue

        candidates.append({
            'start_idx': int(start_idx),
            'end_idx': int(end_idx),
            'start_date': str(work['trade_time'].iloc[start_idx]),
            'end_date': str(work['trade_time'].iloc[end_idx]),
            'similarity': float(sim_score),
            'similarity_detail': sim_detail,
            'future_returns': future_ret,
            'future_end_dates': future_end_dates,
        })

    if not candidates:
        return {
            'ok': False,
            'message': '未找到可用历史匹配样本（可能因窗口过大）',
            'ts_code': code,
            'freq': f,
            'freq_label': freq_label,
            'window': int(window),
            'top_k': int(top_k),
            'horizons': [int(x) for x in horizons],
            'available_bars': int(len(work)),
            'matches': [],
        }

    candidates = sorted(candidates, key=lambda x: float(x.get('similarity', -9e9)), reverse=True)

    # 去邻近重复：避免Top结果来自几乎同一历史阶段的相邻窗口
    top_matches = []
    min_gap = max(5, int(window // 3))
    for c in candidates:
        if not top_matches:
            top_matches.append(c)
        else:
            ok = True
            for sel in top_matches:
                if abs(int(c.get('end_idx', 0)) - int(sel.get('end_idx', 0))) < min_gap:
                    ok = False
                    break
            if ok:
                top_matches.append(c)
        if len(top_matches) >= int(top_k):
            break

    # 兜底：如果去重后不足K个，用原排序补齐
    if len(top_matches) < int(top_k):
        used_keys = set((int(x.get('start_idx', -1)), int(x.get('end_idx', -1))) for x in top_matches)
        for c in candidates:
            key = (int(c.get('start_idx', -1)), int(c.get('end_idx', -1)))
            if key in used_keys:
                continue
            top_matches.append(c)
            used_keys.add(key)
            if len(top_matches) >= int(top_k):
                break

    for i, m in enumerate(top_matches):
        s_idx = int(m['start_idx'])
        e_idx = int(m['end_idx'])
        m['rank'] = int(i + 1)
        m['similarity'] = _safe_round(float(m['similarity']), 6)
        m['distance_bars_to_current'] = int(max(0, target_start - e_idx))
        m['chart'] = _build_pattern_chart_payload(work.iloc[s_idx:e_idx + 1].copy())
        m.pop('start_idx', None)
        m.pop('end_idx', None)

    prediction = _build_pattern_prediction(top_matches, horizons)
    target_chart = _build_pattern_chart_payload(target_seg)

    return {
        'ok': True,
        'message': 'success',
        'ts_code': code,
        'freq': f,
        'freq_label': freq_label,
        'freq_unit': freq_unit,
        'window': int(window),
        'top_k': int(top_k),
        'horizons': [int(x) for x in horizons],
        'weights': {
            'price': _safe_round(w.get('price', 0), 4),
            'volume': _safe_round(w.get('volume', 0), 4),
            'macd': _safe_round(w.get('macd', 0), 4),
            'kdj': _safe_round(w.get('kdj', 0), 4),
        },
        'available_bars': int(len(work)),
        'candidate_count': int(len(candidates)),
        'latest_trade_date': str(work['trade_time'].iloc[-1]) if len(work) else '',
        'target': {
            'start_date': str(target_seg['trade_time'].iloc[0]) if len(target_seg) else '',
            'end_date': str(target_seg['trade_time'].iloc[-1]) if len(target_seg) else '',
            'chart': target_chart,
        },
        'matches': top_matches,
        'prediction': prediction,
        'data_source': data_source,
        'assumption': '该结果基于历史形态相似性统计，不构成任何投资建议。',
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


# ────────────────────────────────
# 个股工具：机器学习集成（次日涨跌预测）
# ────────────────────────────────

def _ml_safe_float(v, default=None):
    try:
        x = float(v)
        if pd.isna(x) or not math.isfinite(x):
            return default
        return x
    except Exception:
        return default


def _ml_sigmoid_confidence(prob_up):
    p = _ml_safe_float(prob_up, 0.5)
    p = max(0.0, min(1.0, p))
    return abs(p - 0.5) * 2.0


def _ml_max_drawdown(equity_series):
    if not equity_series:
        return 0.0
    peak = float(equity_series[0])
    mdd = 0.0
    for x in equity_series:
        v = float(x)
        if v > peak:
            peak = v
        if peak > 0:
            dd = (v / peak) - 1.0
            if dd < mdd:
                mdd = dd
    return mdd


def _ml_rsi(close, period=14):
    c = pd.Series(close, dtype='float64')
    delta = c.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    avg_up = up.ewm(alpha=1.0 / float(period), adjust=False).mean()
    avg_down = down.ewm(alpha=1.0 / float(period), adjust=False).mean()
    rs = avg_up / avg_down.replace(0.0, pd.NA)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(50.0)


def _ml_zscore(series, window=20, min_periods=10):
    s = pd.Series(series, dtype='float64')
    mu = s.rolling(window=int(window), min_periods=int(min_periods)).mean()
    sd = s.rolling(window=int(window), min_periods=int(min_periods)).std().replace(0.0, pd.NA)
    z = (s - mu) / sd
    return z


def _ml_fetch_daily_basic_local_first(ts_code, start_date='20160101', end_date=''):
    """
    读取个股日线与估值数据（优先本地库，在线兜底），用于 ML 特征工程。
    """
    code = _normalize_ts_code(ts_code)
    start = str(start_date or '20160101').replace('-', '')
    end = str(end_date or _today_ymd()).replace('-', '')
    if len(start) != 8 or not start.isdigit():
        start = '20160101'
    if len(end) != 8 or not end.isdigit():
        end = _today_ymd()
    if start > end:
        start, end = end, start

    # price: local first
    price_source = 'local_daily_kline'
    price_df = _fetch_daily_from_local_db(code, start, end)
    if price_df is None or price_df.empty:
        price_source = 'tushare_daily'
        try:
            price_df = pro.daily(
                ts_code=code,
                start_date=start,
                end_date=end,
                fields='trade_date,open,high,low,close,vol,amount'
            )
        except Exception:
            price_df = pd.DataFrame()
    if price_df is None or price_df.empty:
        return pd.DataFrame(), price_source, 'none'
    price_df = price_df.sort_values('trade_date').drop_duplicates(subset=['trade_date'], keep='last').reset_index(drop=True)

    # basic: local first
    basic_source = 'local_dailybasic'
    basic_df = _fetch_dailybasic_from_local_db(code, start, end)
    if basic_df is None or basic_df.empty:
        basic_source = 'tushare_daily_basic'
        try:
            basic_df = pro.daily_basic(
                ts_code=code,
                start_date=start,
                end_date=end,
                fields='trade_date,pe,pe_ttm,pb,ps,ps_ttm,total_mv,circ_mv,turnover_rate'
            )
        except Exception:
            basic_df = pd.DataFrame()
    if basic_df is None:
        basic_df = pd.DataFrame()
    if not basic_df.empty:
        basic_df = basic_df.sort_values('trade_date').drop_duplicates(subset=['trade_date'], keep='last').reset_index(drop=True)

    out = price_df.copy()
    if not basic_df.empty:
        out = out.merge(
            basic_df[['trade_date', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'total_mv', 'circ_mv', 'turnover_rate']],
            on='trade_date',
            how='left'
        )
    else:
        for col in ['pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'total_mv', 'circ_mv', 'turnover_rate']:
            out[col] = pd.NA
        basic_source = 'none'

    num_cols = [
        'open', 'high', 'low', 'close', 'vol', 'amount',
        'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'total_mv', 'circ_mv', 'turnover_rate'
    ]
    for c in num_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors='coerce')
    out['trade_date'] = out['trade_date'].astype(str)
    out = out.sort_values('trade_date').drop_duplicates(subset=['trade_date'], keep='last').reset_index(drop=True)
    return out, price_source, basic_source


def _ml_build_feature_frame(raw_df):
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(), []

    x = raw_df.copy().reset_index(drop=True)
    close = pd.Series(x['close'], dtype='float64')
    open_ = pd.Series(x['open'], dtype='float64')
    high = pd.Series(x['high'], dtype='float64')
    low = pd.Series(x['low'], dtype='float64')
    vol = pd.Series(x['vol'], dtype='float64').replace(0.0, pd.NA)
    turnover = pd.Series(x['turnover_rate'], dtype='float64')
    pe_ttm = pd.Series(x['pe_ttm'], dtype='float64')
    pb = pd.Series(x['pb'], dtype='float64')
    ps = pd.Series(x['ps'], dtype='float64')
    total_mv = pd.Series(x['total_mv'], dtype='float64')

    prev_close = close.shift(1).replace(0.0, pd.NA)
    x['ret_1'] = close.pct_change(1)
    x['ret_3'] = close.pct_change(3)
    x['ret_5'] = close.pct_change(5)
    x['ret_10'] = close.pct_change(10)
    x['ret_20'] = close.pct_change(20)
    x['gap'] = (open_ / prev_close) - 1.0
    x['intraday_ret'] = (close / open_.replace(0.0, pd.NA)) - 1.0
    x['amp'] = (high - low) / prev_close

    x['vol_chg_1'] = vol.pct_change(1)
    x['vol_ratio_5'] = (vol / vol.rolling(5, min_periods=3).mean()) - 1.0
    x['vol_z_20'] = _ml_zscore(vol, 20, 8)

    ma5 = close.rolling(5, min_periods=3).mean()
    ma10 = close.rolling(10, min_periods=5).mean()
    ma20 = close.rolling(20, min_periods=10).mean()
    ma60 = close.rolling(60, min_periods=20).mean()
    x['ma5_dev'] = (close / ma5.replace(0.0, pd.NA)) - 1.0
    x['ma10_dev'] = (close / ma10.replace(0.0, pd.NA)) - 1.0
    x['ma20_dev'] = (close / ma20.replace(0.0, pd.NA)) - 1.0
    x['ma60_dev'] = (close / ma60.replace(0.0, pd.NA)) - 1.0

    x['volatility_5'] = x['ret_1'].rolling(5, min_periods=3).std()
    x['volatility_10'] = x['ret_1'].rolling(10, min_periods=5).std()
    x['volatility_20'] = x['ret_1'].rolling(20, min_periods=10).std()

    macd_df = _calc_macd(close.tolist())
    x['macd_dif'] = pd.to_numeric(macd_df['dif'], errors='coerce')
    x['macd_dea'] = pd.to_numeric(macd_df['dea'], errors='coerce')
    x['macd_hist'] = pd.to_numeric(macd_df['hist'], errors='coerce')

    kdj_df = _calc_kdj(high.tolist(), low.tolist(), close.tolist())
    x['kdj_k'] = pd.to_numeric(kdj_df['k'], errors='coerce') / 100.0
    x['kdj_d'] = pd.to_numeric(kdj_df['d'], errors='coerce') / 100.0
    x['kdj_j'] = pd.to_numeric(kdj_df['j'], errors='coerce') / 100.0

    x['rsi14'] = _ml_rsi(close, 14) / 100.0
    boll_mid = close.rolling(20, min_periods=10).mean()
    boll_std = close.rolling(20, min_periods=10).std()
    boll_up = boll_mid + 2.0 * boll_std
    boll_low = boll_mid - 2.0 * boll_std
    boll_den = (boll_up - boll_low).replace(0.0, pd.NA)
    x['boll_pos'] = (close - boll_low) / boll_den

    x['turnover_rate'] = turnover
    x['turnover_z_20'] = _ml_zscore(turnover, 20, 8)

    x['pe_ttm'] = pe_ttm
    x['pb'] = pb
    x['ps'] = ps
    x['pe_z_252'] = _ml_zscore(pe_ttm, 252, 60)
    x['pb_z_252'] = _ml_zscore(pb, 252, 60)
    x['ps_z_252'] = _ml_zscore(ps, 252, 60)
    x['mv_log'] = total_mv.apply(lambda v: math.log(v) if (pd.notna(v) and float(v) > 0) else pd.NA)

    x['next_ret'] = close.shift(-1) / close.replace(0.0, pd.NA) - 1.0
    x['target_up'] = (x['next_ret'] > 0).astype('float')
    x.loc[x['next_ret'].isna(), 'target_up'] = pd.NA

    feature_cols = [
        'ret_1', 'ret_3', 'ret_5', 'ret_10', 'ret_20',
        'gap', 'intraday_ret', 'amp',
        'vol_chg_1', 'vol_ratio_5', 'vol_z_20',
        'ma5_dev', 'ma10_dev', 'ma20_dev', 'ma60_dev',
        'volatility_5', 'volatility_10', 'volatility_20',
        'macd_dif', 'macd_dea', 'macd_hist',
        'kdj_k', 'kdj_d', 'kdj_j',
        'rsi14', 'boll_pos',
        'turnover_rate', 'turnover_z_20',
        'pe_ttm', 'pb', 'ps',
        'pe_z_252', 'pb_z_252', 'ps_z_252',
        'mv_log'
    ]
    return x, feature_cols


def _ml_extract_feature_importance(fitted_pipeline, feature_cols):
    """
    统一提取模型特征重要性，返回已归一化 dict（sum=1）。
    """
    if fitted_pipeline is None:
        return {}
    model = None
    try:
        model = fitted_pipeline.named_steps.get('model')
    except Exception:
        model = fitted_pipeline
    if model is None:
        return {}

    values = None
    try:
        if hasattr(model, 'feature_importances_'):
            values = list(model.feature_importances_)
        elif hasattr(model, 'coef_'):
            coef = model.coef_
            if hasattr(coef, 'ndim') and coef.ndim > 1:
                values = list(abs(coef[0]))
            else:
                values = list(abs(coef))
    except Exception:
        values = None
    if values is None or len(values) != len(feature_cols):
        return {}

    imp = {}
    s = 0.0
    for i, f in enumerate(feature_cols):
        v = _ml_safe_float(values[i], 0.0)
        if v < 0:
            v = abs(v)
        imp[f] = float(v)
        s += float(v)
    if s <= 1e-12:
        return {}
    for k in list(imp.keys()):
        imp[k] = imp[k] / s
    return imp


def get_stock_ml_ensemble_prediction(ts_code='600425.SH', start_date='20160101', end_date='',
                                     threshold_up=0.58, threshold_down=0.42, min_train_size=360,
                                     enabled_models=None, auto_threshold=False, progress_callback=None):
    """
    多模型集成：预测下一个交易日涨跌（上涨/下跌/观望）。
    仅使用机器学习模型，不包含规则打分。
    """
    code = _normalize_ts_code(ts_code)
    if not code:
        return {'ok': False, 'message': 'ts_code 不能为空'}

    def _progress(percent, msg):
        if not progress_callback:
            return
        try:
            p = int(max(0, min(99, round(float(percent)))))
            progress_callback(p, 100, str(msg))
        except Exception:
            pass

    _progress(3, '读取行情与估值数据...')

    up_th = _ml_safe_float(threshold_up, 0.58)
    down_th = _ml_safe_float(threshold_down, 0.42)
    if up_th is None:
        up_th = 0.58
    if down_th is None:
        down_th = 0.42
    up_th = max(0.5, min(0.9, up_th))
    down_th = max(0.1, min(0.5, down_th))
    if down_th >= up_th:
        down_th = max(0.1, up_th - 0.08)

    min_train = int(max(180, _ml_safe_float(min_train_size, 360)))

    raw_df, price_source, basic_source = _ml_fetch_daily_basic_local_first(
        ts_code=code, start_date=start_date, end_date=end_date
    )
    if raw_df is None or raw_df.empty:
        return {
            'ok': False,
            'message': '未获取到日线数据，请稍后重试。',
            'ts_code': code,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
    if len(raw_df) < 300:
        return {
            'ok': False,
            'message': '样本不足：至少需要 300 根日线，当前仅 %d。' % int(len(raw_df)),
            'ts_code': code,
            'available_bars': int(len(raw_df)),
            'latest_trade_date': str(raw_df['trade_date'].iloc[-1]) if len(raw_df) else '',
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
    _progress(12, '构建机器学习特征...')

    feat_df, feature_cols = _ml_build_feature_frame(raw_df)
    if feat_df is None or feat_df.empty:
        return {'ok': False, 'message': '特征工程失败，无法生成训练样本。', 'ts_code': code}

    # 训练集：去掉最后一行（最后一行 next_ret 未知）
    train_df = feat_df.iloc[:-1].copy()
    train_df = train_df.dropna(subset=['target_up']).reset_index(drop=True)
    if len(train_df) < min_train:
        return {
            'ok': False,
            'message': '可训练样本不足：至少需要 %d，当前 %d。' % (int(min_train), int(len(train_df))),
            'ts_code': code,
            'available_samples': int(len(train_df)),
            'latest_trade_date': str(feat_df['trade_date'].iloc[-1]) if len(feat_df) else '',
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
    # 去掉初期指标不稳定区段
    if len(train_df) > (min_train + 120):
        train_df = train_df.iloc[60:].reset_index(drop=True)

    _progress(18, '准备训练数据与滚动验证...')

    X = train_df[feature_cols].copy()
    y = train_df['target_up'].astype(int).copy()
    train_dates = train_df['trade_date'].astype(str).tolist()
    next_ret = pd.to_numeric(train_df['next_ret'], errors='coerce').fillna(0.0).tolist()
    latest_row = feat_df.iloc[-1]
    X_latest = latest_row[feature_cols].to_frame().T

    # 延迟导入 sklearn，避免其他接口启动变慢
    try:
        import numpy as np
        from sklearn.model_selection import TimeSeriesSplit
        from sklearn.pipeline import Pipeline
        from sklearn.impute import SimpleImputer
        from sklearn.preprocessing import StandardScaler
        from sklearn.linear_model import LogisticRegression
        from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, GradientBoostingClassifier, AdaBoostClassifier
        from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
        try:
            from sklearn.ensemble import HistGradientBoostingClassifier
            has_hgb = True
        except Exception:
            HistGradientBoostingClassifier = None
            has_hgb = False
    except Exception as e:
        return {
            'ok': False,
            'message': '机器学习依赖加载失败：%s' % str(e),
            'ts_code': code,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

    random_state = 42
    model_defs = {
        'logistic': {
            'label': 'LogisticRegression',
            'builder': lambda: Pipeline([
                ('imputer', SimpleImputer(strategy='median')),
                ('scaler', StandardScaler()),
                ('model', LogisticRegression(max_iter=1200, random_state=random_state))
            ])
        },
        'rf': {
            'label': 'RandomForest',
            'builder': lambda: Pipeline([
                ('imputer', SimpleImputer(strategy='median')),
                ('model', RandomForestClassifier(
                    n_estimators=280,
                    max_depth=8,
                    min_samples_leaf=5,
                    random_state=random_state,
                    n_jobs=-1
                ))
            ])
        },
        'extra_trees': {
            'label': 'ExtraTrees',
            'builder': lambda: Pipeline([
                ('imputer', SimpleImputer(strategy='median')),
                ('model', ExtraTreesClassifier(
                    n_estimators=360,
                    max_depth=10,
                    min_samples_leaf=4,
                    random_state=random_state,
                    n_jobs=-1
                ))
            ])
        },
        'gbdt': {
            'label': 'GradientBoosting',
            'builder': lambda: Pipeline([
                ('imputer', SimpleImputer(strategy='median')),
                ('model', GradientBoostingClassifier(
                    learning_rate=0.05,
                    n_estimators=220,
                    max_depth=3,
                    random_state=random_state
                ))
            ])
        },
        'adaboost': {
            'label': 'AdaBoost',
            'builder': lambda: Pipeline([
                ('imputer', SimpleImputer(strategy='median')),
                ('model', AdaBoostClassifier(
                    n_estimators=260,
                    learning_rate=0.05,
                    random_state=random_state
                ))
            ])
        },
    }
    if has_hgb:
        model_defs['hgb'] = {
            'label': 'HistGradientBoosting',
            'builder': lambda: Pipeline([
                ('imputer', SimpleImputer(strategy='median')),
                ('model', HistGradientBoostingClassifier(
                    learning_rate=0.05,
                    max_depth=6,
                    max_iter=260,
                    random_state=random_state
                ))
            ])
        }

    default_model_order = ['logistic', 'rf', 'extra_trees', 'gbdt', 'adaboost', 'hgb']
    selected_models = []
    if isinstance(enabled_models, (list, tuple, set)):
        selected_models = [str(x).strip().lower() for x in enabled_models if str(x).strip()]
    if selected_models:
        model_order = [m for m in default_model_order if m in selected_models and m in model_defs]
    else:
        model_order = [m for m in default_model_order if m in model_defs]
    if not model_order:
        model_order = [m for m in default_model_order if m in model_defs]
    active_model_defs = {k: model_defs[k] for k in model_order}
    _progress(22, '模型配置完成：%s' % ('、'.join(model_order)))

    n_samples = len(X)
    if n_samples >= 1200:
        n_splits = 6
    elif n_samples >= 900:
        n_splits = 5
    elif n_samples >= 600:
        n_splits = 4
    else:
        n_splits = 3
    splitter = TimeSeriesSplit(n_splits=n_splits)

    model_results = []
    oos_prob_map = {}
    latest_prob_map = {}
    fitted_map = {}

    total_models = max(1, len(active_model_defs))
    for m_idx, (mk, mdef) in enumerate(active_model_defs.items()):
        label = mdef.get('label', mk)
        _progress(24 + (m_idx / float(total_models)) * 42.0, '训练模型 %s (%d/%d)...' % (label, int(m_idx + 1), int(total_models)))
        oos_probs = np.full(shape=(n_samples,), fill_value=np.nan, dtype='float64')
        status = 'ok'
        err_msg = ''

        for tr_idx, te_idx in splitter.split(X):
            if len(tr_idx) < min_train:
                continue
            y_tr = y.iloc[tr_idx]
            if y_tr.nunique() < 2:
                continue
            try:
                mdl = mdef['builder']()
                mdl.fit(X.iloc[tr_idx], y_tr)
                p = mdl.predict_proba(X.iloc[te_idx])[:, 1]
                oos_probs[te_idx] = p
            except Exception as e:
                status = 'degraded'
                err_msg = str(e)[:120]

        valid_mask = ~np.isnan(oos_probs)
        valid_count = int(np.sum(valid_mask))
        metrics = {
            'accuracy': None,
            'precision': None,
            'recall': None,
            'f1': None,
            'auc': None,
            'coverage': _ml_safe_float(valid_count / float(n_samples), 0.0),
        }
        if valid_count >= 80:
            y_true = y.iloc[valid_mask]
            y_prob = oos_probs[valid_mask]
            y_pred = (y_prob >= 0.5).astype(int)
            try:
                metrics['accuracy'] = float(accuracy_score(y_true, y_pred))
            except Exception:
                metrics['accuracy'] = None
            try:
                metrics['precision'] = float(precision_score(y_true, y_pred, zero_division=0))
            except Exception:
                metrics['precision'] = None
            try:
                metrics['recall'] = float(recall_score(y_true, y_pred, zero_division=0))
            except Exception:
                metrics['recall'] = None
            try:
                metrics['f1'] = float(f1_score(y_true, y_pred, zero_division=0))
            except Exception:
                metrics['f1'] = None
            try:
                if y_true.nunique() >= 2:
                    metrics['auc'] = float(roc_auc_score(y_true, y_prob))
            except Exception:
                metrics['auc'] = None

        latest_prob = None
        feature_imp = {}
        try:
            final_mdl = mdef['builder']()
            final_mdl.fit(X, y)
            latest_prob = float(final_mdl.predict_proba(X_latest)[0, 1])
            feature_imp = _ml_extract_feature_importance(final_mdl, feature_cols)
            fitted_map[mk] = final_mdl
        except Exception as e:
            status = 'failed'
            err_msg = str(e)[:120]

        # 权重按 OOS 指标自动计算
        acc = _ml_safe_float(metrics.get('accuracy'), 0.0)
        f1 = _ml_safe_float(metrics.get('f1'), 0.0)
        auc = _ml_safe_float(metrics.get('auc'), 0.5)
        raw_w = max(0.0, 0.70 * f1 + 0.45 * max(0.0, acc - 0.5) + 0.60 * max(0.0, auc - 0.5))
        if latest_prob is not None and valid_count >= 80:
            raw_w = max(raw_w, 0.05)
        if latest_prob is None:
            raw_w = 0.0

        oos_prob_map[mk] = oos_probs
        latest_prob_map[mk] = latest_prob
        model_results.append({
            'id': mk,
            'name': label,
            'status': status,
            'error': err_msg,
            'latest_up_prob': _ml_safe_float(latest_prob, None),
            'oos_count': valid_count,
            'metrics': metrics,
            'raw_weight': raw_w,
            'feature_importance': feature_imp,
        })
        _progress(24 + ((m_idx + 1) / float(total_models)) * 42.0, '模型完成 %s (%d/%d)' % (label, int(m_idx + 1), int(total_models)))

    usable = [m for m in model_results if _ml_safe_float(m.get('latest_up_prob'), None) is not None and m.get('raw_weight', 0) > 0]
    if not usable:
        return {
            'ok': False,
            'message': '所有模型都未能产生有效预测，请稍后重试。',
            'ts_code': code,
            'latest_trade_date': str(feat_df['trade_date'].iloc[-1]) if len(feat_df) else '',
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
    _progress(68, '融合模型结果...')

    w_sum = sum([float(m['raw_weight']) for m in usable])
    if w_sum <= 1e-12:
        eq = 1.0 / float(len(usable))
        for m in usable:
            m['weight'] = eq
    else:
        for m in usable:
            m['weight'] = float(m['raw_weight']) / w_sum

    # soft voting
    soft_up = 0.0
    for m in usable:
        soft_up += float(m['weight']) * float(m['latest_up_prob'])
    soft_up = max(0.0, min(1.0, soft_up))

    # 历史 OOS soft 概率（用于回测）
    soft_oos = np.full(shape=(n_samples,), fill_value=np.nan, dtype='float64')
    for i in range(n_samples):
        vs = []
        ws = []
        for m in usable:
            p = oos_prob_map.get(m['id'])
            if p is None:
                continue
            pv = p[i]
            if np.isnan(pv):
                continue
            vs.append(float(pv))
            ws.append(float(m['weight']))
        if vs and sum(ws) > 0:
            s = sum([vs[k] * ws[k] for k in range(len(vs))]) / sum(ws)
            soft_oos[i] = s

    # stacking（第二层模型）
    stack_up = None
    stack_oos = np.full(shape=(n_samples,), fill_value=np.nan, dtype='float64')
    stack_status = 'disabled'
    stack_train_rows = 0
    _progress(72, '训练 Stacking 融合层...')
    try:
        stack_cols = [m['id'] for m in usable]
        if len(stack_cols) >= 2:
            z_df = pd.DataFrame({cid: oos_prob_map[cid] for cid in stack_cols})
            z_valid = z_df.dropna()
            stack_train_rows = int(len(z_valid))
            if stack_train_rows >= 120:
                y_stack = y.iloc[z_valid.index]
                if y_stack.nunique() >= 2:
                    from sklearn.linear_model import LogisticRegression as _StackLogit
                    stack_model = _StackLogit(max_iter=800, random_state=42)
                    stack_model.fit(z_valid.values, y_stack.values)
                    latest_vec = [float(latest_prob_map[cid]) for cid in stack_cols]
                    stack_up = float(stack_model.predict_proba([latest_vec])[0, 1])
                    # oos stacking
                    stack_prob_vals = stack_model.predict_proba(z_valid.values)[:, 1]
                    stack_oos[z_valid.index.values] = stack_prob_vals
                    stack_status = 'enabled'
                else:
                    stack_status = 'skipped_single_class'
            else:
                stack_status = 'skipped_insufficient_rows'
        else:
            stack_status = 'skipped_single_model'
    except Exception:
        stack_status = 'failed'

    final_up = soft_up
    if stack_up is not None:
        final_up = 0.55 * stack_up + 0.45 * soft_up
    final_up = max(0.0, min(1.0, final_up))
    final_down = 1.0 - final_up

    # 历史最终概率（用于回测）
    final_oos = soft_oos.copy()
    if stack_up is not None:
        for i in range(n_samples):
            sv = soft_oos[i]
            tv = stack_oos[i]
            if not np.isnan(tv) and not np.isnan(sv):
                final_oos[i] = 0.55 * tv + 0.45 * sv
            elif not np.isnan(tv):
                final_oos[i] = tv
            else:
                final_oos[i] = sv

    _progress(79, '评估阈值与生成交易信号...')

    # 生成交易信号（历史 + 最新）
    def _sig_from_prob(p, up_threshold=None, down_threshold=None):
        ut = up_th if up_threshold is None else float(up_threshold)
        dt = down_th if down_threshold is None else float(down_threshold)
        if p is None or (isinstance(p, float) and (math.isnan(p) or not math.isfinite(p))):
            return 0
        if p >= ut:
            return 1
        if p <= dt:
            return -1
        return 0

    def _evaluate_threshold_pair(up_v, down_v):
        sig = []
        strat_ret = []
        nz_cnt = 0
        hit_cnt = 0
        for i in range(n_samples):
            p = final_oos[i]
            s = _sig_from_prob(float(p), up_v, down_v) if not np.isnan(p) else 0
            sig.append(int(s))
            r = _ml_safe_float(next_ret[i], 0.0)
            strat_ret.append(float(s) * r)
            if s != 0:
                nz_cnt += 1
                truth_up = 1 if y.iloc[i] == 1 else -1
                if s == truth_up:
                    hit_cnt += 1
        eq = 1.0
        for rv in strat_ret:
            eq = eq * (1.0 + float(rv))
        total = eq - 1.0
        annual_ret = 0.0
        if n_samples > 20:
            annual_ret = pow(max(eq, 1e-9), 252.0 / float(n_samples)) - 1.0
        s_mean = float(pd.Series(strat_ret).mean())
        s_std = float(pd.Series(strat_ret).std())
        s_sharpe = (s_mean / s_std * math.sqrt(252.0)) if s_std > 1e-9 else 0.0
        cov = nz_cnt / float(max(1, n_samples))
        hit = hit_cnt / float(max(1, nz_cnt))
        score = (
            1.20 * annual_ret +
            0.80 * hit +
            0.25 * s_sharpe +
            0.15 * cov -
            0.05 * abs(cov - 0.35)
        )
        return {
            'signals': sig,
            'strategy_ret': strat_ret,
            'nonzero_count': int(nz_cnt),
            'hit_count': int(hit_cnt),
            'coverage': float(cov),
            'hit_rate': float(hit),
            'total_return': float(total),
            'annual_return': float(annual_ret),
            'sharpe': float(s_sharpe),
            'score': float(score),
        }

    threshold_info = {
        'auto': bool(auto_threshold),
        'optimized': False,
        'input_up': round(float(up_th), 4),
        'input_down': round(float(down_th), 4),
        'best_score': None,
    }
    best_eval = _evaluate_threshold_pair(up_th, down_th)
    if bool(auto_threshold):
        up_grid = [0.54, 0.56, 0.58, 0.60, 0.62, 0.64, 0.66, 0.68, 0.70, 0.72]
        down_grid = [0.30, 0.32, 0.34, 0.36, 0.38, 0.40, 0.42, 0.44, 0.46]
        best_cfg = {
            'up': float(up_th),
            'down': float(down_th),
            'eval': best_eval,
        }
        for ut in up_grid:
            for dt in down_grid:
                if dt >= ut - 0.06:
                    continue
                ev = _evaluate_threshold_pair(ut, dt)
                if ev['nonzero_count'] < 20:
                    continue
                if ev['coverage'] < 0.03:
                    continue
                if ev['score'] > best_cfg['eval']['score']:
                    best_cfg = {'up': float(ut), 'down': float(dt), 'eval': ev}
        up_th = float(best_cfg['up'])
        down_th = float(best_cfg['down'])
        best_eval = best_cfg['eval']
        threshold_info.update({
            'optimized': (abs(up_th - threshold_info['input_up']) > 1e-9 or abs(down_th - threshold_info['input_down']) > 1e-9),
            'best_score': round(float(best_eval['score']), 6),
            'opt_up': round(float(up_th), 4),
            'opt_down': round(float(down_th), 4),
        })

    hist_signal = best_eval['signals']
    strategy_ret = best_eval['strategy_ret']
    benchmark_ret = [_ml_safe_float(next_ret[i], 0.0) for i in range(n_samples)]

    latest_signal = _sig_from_prob(final_up, up_th, down_th)
    if latest_signal > 0:
        signal_text = '买入'
    elif latest_signal < 0:
        signal_text = '减仓'
    else:
        signal_text = '观望'

    # 资金曲线
    eq_strategy = [1.0]
    eq_bench = [1.0]
    for i in range(n_samples):
        eq_strategy.append(eq_strategy[-1] * (1.0 + float(strategy_ret[i])))
        eq_bench.append(eq_bench[-1] * (1.0 + float(benchmark_ret[i])))
    eq_strategy = eq_strategy[1:]
    eq_bench = eq_bench[1:]

    # 回测绩效
    nz_count = int(best_eval.get('nonzero_count', 0))
    hit_count = int(best_eval.get('hit_count', 0))
    coverage = float(best_eval.get('coverage', 0.0))
    hit_rate = float(best_eval.get('hit_rate', 0.0))
    total_ret = eq_strategy[-1] - 1.0 if eq_strategy else 0.0
    bench_ret = eq_bench[-1] - 1.0 if eq_bench else 0.0
    annual = 0.0
    annual_bench = 0.0
    if n_samples > 20 and eq_strategy:
        annual = pow(max(eq_strategy[-1], 1e-9), 252.0 / float(n_samples)) - 1.0
        annual_bench = pow(max(eq_bench[-1], 1e-9), 252.0 / float(n_samples)) - 1.0
    s_mean = float(pd.Series(strategy_ret).mean())
    s_std = float(pd.Series(strategy_ret).std())
    sharpe = (s_mean / s_std * math.sqrt(252.0)) if s_std > 1e-9 else 0.0
    mdd = _ml_max_drawdown(eq_strategy)

    # 特征重要性聚合
    agg_imp = {f: 0.0 for f in feature_cols}
    for m in usable:
        imp = m.get('feature_importance', {}) or {}
        w = float(m.get('weight', 0.0))
        for f, v in imp.items():
            if f in agg_imp:
                agg_imp[f] += w * float(v)
    top_features = sorted(
        [{'feature': k, 'score': float(v)} for k, v in agg_imp.items() if abs(v) > 1e-12],
        key=lambda x: float(x['score']),
        reverse=True
    )[:15]
    _progress(88, '生成回测与可解释性输出...')

    # 模型面板输出
    model_panel = []
    for m in model_results:
        mm = {
            'id': m['id'],
            'name': m['name'],
            'status': m.get('status', 'ok'),
            'error': m.get('error') or '',
            'latest_up_prob': _ml_safe_float(m.get('latest_up_prob'), None),
            'oos_count': int(m.get('oos_count', 0)),
            'weight': None,
            'metrics': {
                'accuracy': _ml_safe_float((m.get('metrics') or {}).get('accuracy'), None),
                'precision': _ml_safe_float((m.get('metrics') or {}).get('precision'), None),
                'recall': _ml_safe_float((m.get('metrics') or {}).get('recall'), None),
                'f1': _ml_safe_float((m.get('metrics') or {}).get('f1'), None),
                'auc': _ml_safe_float((m.get('metrics') or {}).get('auc'), None),
                'coverage': _ml_safe_float((m.get('metrics') or {}).get('coverage'), None),
            }
        }
        for u in usable:
            if u['id'] == m['id']:
                mm['weight'] = _ml_safe_float(u.get('weight'), 0.0)
                break
        model_panel.append(mm)

    # 图表数据仅保留最近 260 根，保证页面轻量
    tail_n = min(260, n_samples)
    tail_start = max(0, n_samples - tail_n)
    chart_dates = train_dates[tail_start:tail_start + tail_n]
    chart_strategy_raw = [float(v) for v in eq_strategy[tail_start:tail_start + tail_n]]
    chart_bench_raw = [float(v) for v in eq_bench[tail_start:tail_start + tail_n]]
    # 展示窗口内统一重置为 1 起点，便于用户直观看到近段相对表现
    s0 = chart_strategy_raw[0] if chart_strategy_raw and abs(chart_strategy_raw[0]) > 1e-12 else 1.0
    b0 = chart_bench_raw[0] if chart_bench_raw and abs(chart_bench_raw[0]) > 1e-12 else 1.0
    chart_strategy = [round(v / s0, 6) for v in chart_strategy_raw]
    chart_bench = [round(v / b0, 6) for v in chart_bench_raw]
    chart_prob = []
    chart_signal = []
    for p in final_oos[-tail_n:]:
        if np.isnan(p):
            chart_prob.append(None)
            chart_signal.append(0)
        else:
            chart_prob.append(round(float(p), 4))
            chart_signal.append(_sig_from_prob(float(p)))

    # 最近信号表
    recent_rows = []
    for i in range(max(0, n_samples - 30), n_samples):
        p = final_oos[i]
        if np.isnan(p):
            continue
        s = _sig_from_prob(float(p))
        recent_rows.append({
            'trade_date': str(train_dates[i]),
            'up_prob': round(float(p), 4),
            'signal': int(s),
            'next_ret_pct': round(float(next_ret[i]) * 100.0, 2),
        })

    _progress(96, '整理结果...')

    return {
        'ok': True,
        'message': 'success',
        'ts_code': code,
        'latest_trade_date': str(feat_df['trade_date'].iloc[-1]) if len(feat_df) else '',
        'pred_for': '下一交易日',
        'sample_count': int(n_samples),
        'feature_count': int(len(feature_cols)),
        'enabled_models': model_order,
        'thresholds': {
            'up': round(float(up_th), 4),
            'down': round(float(down_th), 4),
            'auto': bool(auto_threshold),
            'auto_info': threshold_info,
        },
        'prediction': {
            'up_prob': round(float(final_up), 4),
            'down_prob': round(float(final_down), 4),
            'signal': int(latest_signal),
            'signal_text': signal_text,
            'confidence': round(float(_ml_sigmoid_confidence(final_up)), 4),
            'soft_up_prob': round(float(soft_up), 4),
            'stack_up_prob': round(float(stack_up), 4) if stack_up is not None else None,
            'stack_status': stack_status,
            'stack_train_rows': int(stack_train_rows),
        },
        'models': model_panel,
        'performance': {
            'coverage': round(float(coverage), 4),
            'direction_hit_rate': round(float(hit_rate), 4),
            'strategy_total_return_pct': round(float(total_ret) * 100.0, 2),
            'benchmark_total_return_pct': round(float(bench_ret) * 100.0, 2),
            'strategy_annual_return_pct': round(float(annual) * 100.0, 2),
            'benchmark_annual_return_pct': round(float(annual_bench) * 100.0, 2),
            'strategy_sharpe': round(float(sharpe), 3),
            'strategy_max_drawdown_pct': round(float(mdd) * 100.0, 2),
        },
        'top_features': [
            {'feature': item['feature'], 'score': round(float(item['score']), 6)}
            for item in top_features
        ],
        'backtest': {
            'dates': chart_dates,
            'strategy_curve': chart_strategy,
            'benchmark_curve': chart_bench,
            'prob_up': chart_prob,
            'signal': chart_signal,
            'recent_signals': recent_rows,
        },
        'data_info': {
            'price_source': price_source,
            'basic_source': basic_source,
            'start_date': str(raw_df['trade_date'].iloc[0]) if len(raw_df) else '',
            'end_date': str(raw_df['trade_date'].iloc[-1]) if len(raw_df) else '',
        },
        'assumption': '本模块仅作机器学习统计研究，不构成投资建议。',
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }
