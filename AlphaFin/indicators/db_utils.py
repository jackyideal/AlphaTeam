"""
数据库查询工具 - 从各种指标2/utils.py提取的核心函数
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, event
from AlphaFin.config import DB_ROOT
from .shared_utils import pro


# ── 引擎池化：同一数据库复用 engine，避免重复创建 ──
_engine_cache = {}

# 财务类表名（日期列为 end_date 而非 trade_date）
_FINANCIAL_TABLES = ('资产负债表', '利润表', '现金流量表', 'fina_indicator', 'dividend')


def _get_engine(db_path):
    """获取或创建数据库引擎（带 SQLite 性能调优）"""
    if db_path not in _engine_cache:
        engine = create_engine(db_path, connect_args={"check_same_thread": False})

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_conn, connection_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA cache_size=-64000")    # 64MB 页面缓存
            cursor.execute("PRAGMA mmap_size=268435456")   # 256MB 内存映射
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.close()

        _engine_cache[db_path] = engine
    return _engine_cache[db_path]


def get_file_path():
    """获取数据库文件路径前缀"""
    return f'sqlite:////{DB_ROOT}/'


def get_data_by_sql(file_path, db_name, table_name, codes, fields, start_date=None):
    """
    输入表名和股票篮子数组，输出从数据库中读取的数据

    Args:
        start_date: 可选，格式 'YYYYMMDD'，只加载该日期之后的数据（大幅减少IO）
    """
    codes = codes + ['随便写什么']
    # 兼容传入 file_path 是否带尾部 "/"，避免拼成错误数据库路径
    base_path = file_path.rstrip('/') + '/'
    db_path = base_path + f'{db_name}.db'
    engine = _get_engine(db_path)
    sql = f"select {fields} from {table_name} where ts_code in {tuple(codes)}"
    if start_date:
        date_col = 'end_date' if table_name in _FINANCIAL_TABLES else 'trade_date'
        sql += f" and {date_col} >= '{start_date}'"
    dd = pd.read_sql(sql, engine)
    if 'f_ann_date' in dd.columns or 'ann_date' in dd.columns:
        dd['trade_date'] = dd['end_date']
    dd['trade_date'] = pd.to_datetime(dd['trade_date'])
    dd = dd.sort_index()
    return dd


def get_pivot_data(dd, field):
    """
    指定一个字段名，然后将数据变成index是日期，columns是该字段下的变量
    """
    dd = dd.copy()
    dd = dd[['trade_date', 'ts_code', field]]
    dd['trade_date'] = pd.to_datetime(dd['trade_date'])
    dd = dd.sort_values(by=['trade_date', 'ts_code']).reset_index(drop=True)
    # 数据库存在重复写入时，先去重，避免 pivot 报错
    dd = dd.drop_duplicates(subset=['trade_date', 'ts_code'], keep='last').reset_index(drop=True)
    dd = dd.pivot(index='trade_date', columns='ts_code', values=field)
    dd = dd.sort_index()
    return dd


def get_calendar(freq, start_date, end_date):
    """获取交易日历"""
    calendar = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
    calendar = calendar[calendar['is_open'] == 1]
    calendar = calendar.sort_values(by=['cal_date']).reset_index(drop=True)
    calendar['date'] = pd.to_datetime(calendar['cal_date'])
    calendar['week'] = calendar['date'].apply(
        lambda x: '-'.join([str(x1).zfill(2) for x1 in x.isocalendar()[:2]]))
    calendar['month'] = calendar['cal_date'].apply(lambda x: x[:4] + '-' + x[4:6])

    if freq == 'weekly':
        calendar = calendar.drop_duplicates(subset=['week'], keep='last').reset_index(drop=True)
    elif freq == 'monthly':
        calendar = calendar.drop_duplicates(subset=['month'], keep='last').reset_index(drop=True)
    return calendar


def get_close_data(file_path, tp, code, start_date, end_date):
    """获取复权收盘价数据"""
    if tp == 'stock':
        codes = ['随便搞一个元素', code]
        df_adj = get_data_by_sql(file_path, 'daily_adj', 'daily_adj', codes, '*')
        df_kline = get_data_by_sql(file_path, 'daily_kline', 'daily_kline', codes, '*')

        df_adj = get_pivot_data(df_adj, 'adj_factor')
        df_close = get_pivot_data(df_kline, 'close')
        df_close = (df_close * df_adj / df_adj.loc[df_adj.index[-1]]).round(2)

        df_high = get_pivot_data(df_kline, 'high')
        df_high = (df_high * df_adj / df_adj.loc[df_adj.index[-1]]).round(2)

        df_low = get_pivot_data(df_kline, 'low')
        df_low = (df_low * df_adj / df_adj.loc[df_adj.index[-1]]).round(2)

        df_dailybasic = get_data_by_sql(file_path, 'dailybasic', 'dailybasic', codes,
                                         'trade_date,ts_code,pe_ttm')
        df_dailybasic = df_dailybasic.copy().sort_values(
            by=['ts_code', 'trade_date']).reset_index(drop=True)
        df_dailybasic = df_dailybasic.drop_duplicates(
            subset=['ts_code', 'trade_date'], keep='last').reset_index(drop=True)
        df_dailybasic = get_pivot_data(df_dailybasic, 'pe_ttm')

        df = pd.concat([df_close[code], df_high[code], df_low[code], df_dailybasic[code]], axis=1)
        df.columns = ['close', 'high', 'low', 'pe_ttm']
        df = df.sort_index()
        df = df.fillna(method='ffill')

    elif tp == 'sw':
        df = pro.sw_daily(ts_code=code, start_date=start_date, end_date=end_date)
        df.index = pd.to_datetime(df['trade_date'])
        df = df.sort_index()

    elif tp == 'index':
        df = pro.index_daily(ts_code=code, start_date=start_date, end_date=end_date)
        df.index = pd.to_datetime(df['trade_date'])
        df = df.sort_index()

    return df


def get_stocks():
    """获取所有股票列表"""
    dd1 = pro.stock_basic(list_status='L',
                          fields='ts_code,symbol,name,area,industry,list_date,delist_date')
    dd2 = pro.stock_basic(list_status='D',
                          fields='ts_code,symbol,name,area,industry,list_date,delist_date')
    dd3 = pro.stock_basic(list_status='P',
                          fields='ts_code,symbol,name,area,industry,list_date,delist_date')
    dd1['status'] = '上市'
    dd2['status'] = '退市'
    dd3['status'] = '暂停'
    dd_stocks = pd.concat([dd1, dd2, dd3])
    dd_stocks = dd_stocks[~dd_stocks['ts_code'].isin(['T00018.SH'])]
    dd_stocks = dd_stocks.reset_index(drop=True)
    dd_stocks['delist_date'] = dd_stocks['delist_date'].fillna('20991231')
    return dd_stocks


def get_index_close(index_code='000300.SZ', start_date='20100101'):
    """获取指数收盘价"""
    dd_index = pro.index_daily(ts_code=index_code, start_date=start_date)
    dd_index.index = pd.to_datetime(dd_index['trade_date'])
    dd_index = dd_index.sort_index()
    return dd_index
