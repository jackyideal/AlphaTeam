"""
市场热度共享工具
- 统一生成并缓存每日上市股票数量（stock_count）
- 给市场热度/融资融券指标复用，替代外部 notebook 的 CSV 依赖
"""
import os
import sqlite3

import pandas as pd

from AlphaFin.config import BASE_DIR, DB_ROOT


DATA_DIR = os.path.join(BASE_DIR, 'AlphaFin', 'indicators', 'data')
STOCK_COUNT_CACHE_PATH = os.path.join(DATA_DIR, 'stock_count_cache.csv')


def _daily_kline_db_path():
    return os.path.join(DB_ROOT, 'daily_kline.db')


def _get_db_latest_trade_date():
    db_path = _daily_kline_db_path()
    if not os.path.exists(db_path):
        return ''
    with sqlite3.connect(db_path) as conn:
        row = conn.execute('SELECT MAX(trade_date) FROM daily_kline').fetchone()
    return str(row[0]) if row and row[0] else ''


def _query_stock_count_from_db():
    db_path = _daily_kline_db_path()
    if not os.path.exists(db_path):
        raise FileNotFoundError('未找到 daily_kline 数据库: %s' % db_path)

    sql = '''
        SELECT trade_date, COUNT(DISTINCT ts_code) AS stock_count
        FROM daily_kline
        GROUP BY trade_date
        ORDER BY trade_date
    '''
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(sql, conn)

    df['trade_date'] = df['trade_date'].astype(str)
    df['stock_count'] = df['stock_count'].astype(int)
    return df


def _load_cached_stock_count():
    if not os.path.exists(STOCK_COUNT_CACHE_PATH):
        return None
    df = pd.read_csv(STOCK_COUNT_CACHE_PATH, dtype={'trade_date': str})
    if 'trade_date' not in df.columns or 'stock_count' not in df.columns:
        return None
    return df[['trade_date', 'stock_count']].copy()


def _cache_needs_refresh(cache_df):
    if cache_df is None or cache_df.empty:
        return True
    latest_db = _get_db_latest_trade_date()
    if not latest_db:
        return False
    latest_cache = str(cache_df['trade_date'].max())
    return latest_cache < latest_db


def get_stock_count_df(start_date='20100101', refresh=False):
    """
    获取每日上市股票数量（trade_date, stock_count）。

    Args:
        start_date: 起始日期（YYYYMMDD）
        refresh: 是否强制重算缓存
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    cache_df = None if refresh else _load_cached_stock_count()
    if refresh or _cache_needs_refresh(cache_df):
        cache_df = _query_stock_count_from_db()
        cache_df.to_csv(STOCK_COUNT_CACHE_PATH, index=False)

    start = str(start_date or '19000101')
    out = cache_df[cache_df['trade_date'] >= start].copy()
    out = out.sort_values('trade_date').reset_index(drop=True)
    return out


def get_stock_count_series(start_date='20100101', refresh=False):
    """返回 datetime 索引的 stock_count 序列。"""
    df = get_stock_count_df(start_date=start_date, refresh=refresh)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    series = df.set_index('trade_date')['stock_count'].sort_index()
    return series
