"""
ind_16 - 200日均线突破
原始文件: 各种指标2/所有股票超过200日均线 的数量/200日均线突破数量指标.ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from .shared_utils import pro
from .db_utils import get_data_by_sql, get_pivot_data
from ..config import DB_ROOT

INDICATOR_META = {
    'id': 'ind_16_ma200',
    'name': '200日均线趋势突破系统',
    'group': '市场结构指标',
    'description': '统计全市场股票跌破200日均线(年线)与30日均线(月线)的占比，结合涨幅超越指数的股票比例，多角度衡量市场宽度与系统性风险，极端值常对应市场底部或顶部',
    'input_type': 'none',
    'default_code': '',
    'requires_db': True,
    'slow': True,
    'chart_count': 5,
    'chart_descriptions': [
        '全市场跌破200日均线(年线)股票占比，极高值(>70%)常对应市场底部区域',
        '跌破30日均线股票占比，短期市场恐慌程度的量化指标',
        '涨幅大于指数的股票占比，衡量个股跑赢大盘的广度',
        '20日累积涨幅差值走势，市场动量的整体方向',
        '20日涨幅大于指数的股票占比，中短期市场宽度的变化',
    ],
}


def generate(start_date='20170101', progress_callback=None, **kwargs):
    figures = []
    total = 5
    file_path = f'sqlite:////{DB_ROOT}/'

    if progress_callback:
        progress_callback(0, total, '获取股票列表与数据库数据...')

    # 获取全市场股票
    codess = pro.query('stock_basic', exchange='SSE', list_status='L',
                       fields='ts_code,symbol,name,area,industry,list_date')
    codess1 = pro.query('stock_basic', exchange='SZSE', list_status='L',
                        fields='ts_code,symbol,name,area,industry,list_date')
    codes = list(set(codess['ts_code'])) + list(set(codess1['ts_code']))

    # 从数据库获取收盘价：优先使用复权，缺失时自动降级到未复权
    df_kline = get_data_by_sql(file_path, 'daily_kline', 'daily_kline', codes, '*')
    if df_kline.empty:
        raise RuntimeError('daily_kline 数据为空，无法计算200日均线趋势突破系统')

    df_close_raw = get_pivot_data(df_kline, 'close')
    used_adjusted_close = False
    adj_error_msg = ''
    try:
        df_adj = get_data_by_sql(file_path, 'daily_adj', 'daily_adj', codes, '*')
        if df_adj.empty:
            raise RuntimeError('daily_adj 表为空')

        df_adj = get_pivot_data(df_adj, 'adj_factor')
        common_cols = sorted(set(df_close_raw.columns).intersection(set(df_adj.columns)))
        if not common_cols:
            raise RuntimeError('daily_kline 与 daily_adj 无共同股票代码')

        df_adj = df_adj[common_cols].ffill()
        df_close = df_close_raw[common_cols]
        base_adj = df_adj.iloc[-1].replace(0, np.nan)
        df_close = (df_close * df_adj / base_adj).round(2)
        used_adjusted_close = True
    except Exception as e:
        # 无复权因子时，不中断整套指标，改用原始收盘价
        adj_error_msg = str(e)
        df_close = df_close_raw.copy()

    df_close = df_close.dropna(how='all')
    if df_close.empty:
        raise RuntimeError('收盘价数据为空，无法计算200日均线趋势突破系统')

    if progress_callback and not used_adjusted_close:
        progress_callback(0, total, f'未读取到复权因子，已自动切换为未复权收盘价: {adj_error_msg}')

    # 上证指数
    df_idx = pro.index_daily(ts_code='000001.SH')
    df_idx['trade_date'] = df_idx['trade_date'].astype(str)
    df_idx = df_idx.sort_values(by=['trade_date']).reset_index(drop=True)
    df_idx.index = pd.to_datetime(df_idx['trade_date'])
    df_idx = df_idx['2017-01-01':]

    if progress_callback:
        progress_callback(1, total, '图1: 跌破年线占比...')

    # ── 图1: 跌破250日均线占比 ──
    ratio_250 = (df_close.rolling(1).mean() < df_close.rolling(250).mean()).sum(1) / df_close.notnull().sum(1)
    ratio_250 = ratio_250[ratio_250.index.isin(df_idx.index)]

    fig1 = plt.figure(figsize=(20, 6), facecolor='white')
    ax1 = fig1.add_subplot(111)
    ax1.plot(ratio_250.index, ratio_250, label='跌破年线占比')
    ax1.grid(alpha=0.3)
    ax1.set_ylabel('占比', fontsize=14)
    ax1.legend(loc='upper left', fontsize=12)
    ax2 = ax1.twinx()
    ax2.plot(df_idx.index, df_idx['close'], 'r', label='上证指数')
    ax2.set_ylabel('上证指数', fontsize=14)
    ax2.legend(loc='upper right', fontsize=12)
    plt.title('跌破年线股价占比', fontsize=16)
    fig1.tight_layout()
    figures.append((fig1, '跌破年线股价占比'))

    if progress_callback:
        progress_callback(2, total, '图2: 跌破30日线占比...')

    # ── 图2: 跌破30日均线占比 ──
    ratio_30 = (df_close.rolling(1).mean() < df_close.rolling(30).mean()).sum(1) / df_close.notnull().sum(1)
    ratio_30 = ratio_30[ratio_30.index.isin(df_idx.index)]

    fig2 = plt.figure(figsize=(20, 6), facecolor='white')
    ax1 = fig2.add_subplot(111)
    ax1.plot(ratio_30.index, ratio_30, label='跌破30日线占比')
    ax1.grid(alpha=0.3)
    ax1.set_ylabel('占比', fontsize=14)
    ax1.legend(loc='upper left', fontsize=12)
    ax2 = ax1.twinx()
    ax2.plot(df_idx.index, df_idx['close'], 'r', label='上证指数')
    ax2.set_ylabel('上证指数', fontsize=14)
    ax2.legend(loc='upper right', fontsize=12)
    plt.title('跌破30日线股价占比', fontsize=16)
    fig2.tight_layout()
    figures.append((fig2, '跌破30日线占比'))

    if progress_callback:
        progress_callback(3, total, '图3-4: 市场宽度指标...')

    # ── 图3-5: 市场宽度指标 ──
    try:
        stock_data = df_close[df_close.index >= '2020-01-01']
        index_data = df_idx[df_idx.index >= '2020-01-01']

        common_dates = stock_data.index.intersection(index_data.index)
        stock_data = stock_data.loc[common_dates]
        index_data = index_data.loc[common_dates]

        stock_returns = stock_data.pct_change()
        index_returns = index_data['close'].pct_change()

        # 指标1: 每日涨幅大于指数的股票占比
        daily_above_ratio = (stock_returns > index_returns.values.reshape(-1, 1)).sum(axis=1) / stock_returns.shape[1]
        daily_above_ratio = daily_above_ratio.rolling(20).mean()

        fig3 = plt.figure(figsize=(16, 10), facecolor='white')
        ax1 = plt.gca()
        ax2 = ax1.twinx()
        ax1.plot(index_data.index, index_data['close'], color='tab:blue', linewidth=3, label='上证指数')
        ax1.set_ylabel('上证指数', color='tab:blue', fontsize=16)
        ax1.tick_params(axis='y', labelcolor='tab:blue', labelsize=14)
        ax2.plot(daily_above_ratio.index, daily_above_ratio * 100, color='tab:red', alpha=0.8, linewidth=2)
        ax2.set_ylabel('涨幅大于指数股票占比(%)', color='tab:red', fontsize=16)
        ax2.set_ylim(0, 100)
        plt.title('每日涨幅大于上证指数的股票占比', fontsize=18)
        ax1.grid(alpha=0.3)
        ax1.legend(loc='upper left', fontsize=14)
        fig3.tight_layout()
        figures.append((fig3, '涨幅大于指数股票占比'))

        if progress_callback:
            progress_callback(4, total, '图4: 20日累积涨幅差值...')

        # 指标2: 20日累积涨幅中位数差值
        stock_cum_20d = (1 + stock_returns).rolling(window=20).apply(np.prod, raw=True) - 1
        index_cum_20d = (1 + index_returns).rolling(window=20).apply(np.prod, raw=True) - 1
        diff_median = stock_cum_20d.median(axis=1) - index_cum_20d

        fig4 = plt.figure(figsize=(16, 10), facecolor='white')
        ax1 = plt.gca()
        ax2 = ax1.twinx()
        ax1.plot(index_data.index, index_data['close'], color='tab:blue', linewidth=3, label='上证指数')
        ax1.set_ylabel('上证指数', color='tab:blue', fontsize=16)
        ax2.plot(diff_median.index, diff_median * 100, color='tab:green', alpha=0.8, linewidth=2)
        ax2.set_ylabel('累积涨幅中位数差值(%)', color='tab:green', fontsize=16)
        plt.title('20日累积涨幅中位数与上证指数差值', fontsize=18)
        ax1.grid(alpha=0.3)
        ax1.legend(loc='upper left', fontsize=14)
        fig4.tight_layout()
        figures.append((fig4, '20日累积涨幅差值'))

        # 指标3: 20日累积涨幅大于指数的股票占比
        above_count = (stock_cum_20d > index_cum_20d.values.reshape(-1, 1)).sum(axis=1)
        above_ratio = (above_count / stock_cum_20d.shape[1]).rolling(window=20).mean()

        fig5 = plt.figure(figsize=(16, 10), facecolor='white')
        ax1 = plt.gca()
        ax2 = ax1.twinx()
        ax1.plot(index_data.index, index_data['close'], color='tab:blue', linewidth=3, label='上证指数')
        ax1.set_ylabel('上证指数', color='tab:blue', fontsize=16)
        ax2.plot(above_ratio.index, above_ratio * 100, color='tab:purple', alpha=0.8, linewidth=2)
        ax2.set_ylabel('20日涨幅大于指数占比(%)', color='tab:purple', fontsize=16)
        ax2.set_ylim(0, 100)
        plt.title('20日累积涨幅大于上证指数的股票占比', fontsize=18)
        ax1.grid(alpha=0.3)
        ax1.legend(loc='upper left', fontsize=14)
        fig5.tight_layout()
        figures.append((fig5, '20日涨幅大于指数占比'))

    except Exception as e:
        print(f'绘制市场宽度指标失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
