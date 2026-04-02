"""
ind_07 - 新高低与上涨占比
原始文件: 各种指标/新高低家数占比/新高低与上涨股数占比.ipynb
注意：此指标需要遍历全市场股票，运行时间较长
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from .shared_utils import pro
from .db_utils import get_data_by_sql, get_pivot_data
from ..config import DB_ROOT

INDICATOR_META = {
    'id': 'ind_07_new_high_low',
    'name': '新高新低广度扩散指标',
    'group': '市场结构指标',
    'description': '统计全市场创新高/新低股票家数及上涨股票占比（多频率），通过市场宽度指标判断行情的广度与持续性，高占比预示普涨，低占比暗示分化',
    'input_type': 'none',
    'default_code': '',
    'requires_db': True,
    'slow': True,
    'chart_count': 4,
    'chart_descriptions': [
        '月度上涨股票占比与上证指数走势，高占比(>70%)预示普涨行情，低占比(<30%)暗示系统性下跌',
        '日频上涨股票占比的波动与上证指数对比，短期情绪的快速变化',
        '创新高股票数量与上证指数走势，新高股持续增加是趋势健康的信号',
        '新高-新低比率走势，正值且扩大表示市场广度健康，负值警示风险',
    ],
}


def _plot_ratio_vs_index(ratio_data, ratio_mean, ratio_std, df_index,
                         ratio_label, title, upper_mult=1.0, lower_mult=1.0):
    """绘制占比/指标 vs 上证指数双轴图"""
    fig, ax1 = plt.subplots(figsize=(20, 8), facecolor='white')
    ax1.plot(ratio_data.index, ratio_data.values, color='red', linewidth=1.5, label=ratio_label)
    ax1.axhline(ratio_mean - lower_mult * ratio_std, color='blue', linestyle='--', linewidth=1)
    ax1.axhline(ratio_mean, color='grey', linestyle='--', linewidth=1)
    ax1.axhline(ratio_mean + upper_mult * ratio_std, color='yellow', linestyle='--', linewidth=1)
    ax1.set_ylabel(ratio_label, fontsize=20)
    ax1.legend([ratio_label, '-1std', '均值', '+1std'], loc='upper left', fontsize=12)
    ax1.tick_params(labelsize=14)

    ax2 = ax1.twinx()
    ax2.plot(df_index.index, df_index['close'], color='orange', linewidth=2)
    ax2.set_ylabel('上证指数', fontsize=20)
    ax2.legend(['上证指数'], loc='upper right', fontsize=12)
    ax2.tick_params(labelsize=14)

    plt.title(title, fontsize=22)
    ax1.grid(alpha=0.3)
    fig.tight_layout()
    return fig


def generate(start_date='20170101', progress_callback=None, **kwargs):
    figures = []
    total = 4
    file_path = f'sqlite:////{DB_ROOT}'
    load_start = '20160101'

    if progress_callback:
        progress_callback(0, total, '加载全市场股票列表...')

    # 只取当前上市股票，避免无效数据带来的计算开销
    codess_sh = pro.query(
        'stock_basic', exchange='SSE', list_status='L',
        fields='ts_code,symbol,name,area,industry,list_date'
    )
    codess_sz = pro.query(
        'stock_basic', exchange='SZSE', list_status='L',
        fields='ts_code,symbol,name,area,industry,list_date'
    )
    codes = sorted(set(codess_sh['ts_code']).union(set(codess_sz['ts_code'])))
    if not codes:
        raise RuntimeError('未获取到股票列表')

    if progress_callback:
        progress_callback(0, total, f'从本地数据库批量加载 {len(codes)} 只股票行情...')

    # 批量读取数据库，替代逐只网络请求
    df_adj = get_data_by_sql(
        file_path, 'daily_adj', 'daily_adj', codes,
        'trade_date,ts_code,adj_factor', start_date=load_start
    )
    df_kline = get_data_by_sql(
        file_path, 'daily_kline', 'daily_kline', codes,
        'trade_date,ts_code,close', start_date=load_start
    )
    if df_adj.empty or df_kline.empty:
        raise RuntimeError('本地数据库缺少 daily_adj / daily_kline 数据，请先更新数据库')

    df_adj_pivot = get_pivot_data(df_adj, 'adj_factor').sort_index()
    df_close_raw = get_pivot_data(df_kline, 'close').sort_index()

    # 对齐后复权，避免空列导致数值扩散
    common_cols = sorted(set(df_adj_pivot.columns).intersection(set(df_close_raw.columns)))
    if not common_cols:
        raise RuntimeError('行情数据与复权因子无法对齐（无共同股票代码）')
    df_adj_pivot = df_adj_pivot[common_cols].ffill()
    df_close_raw = df_close_raw[common_cols]

    base_adj = df_adj_pivot.iloc[-1].replace(0, np.nan)
    df_close = (df_close_raw * df_adj_pivot / base_adj).replace([np.inf, -np.inf], np.nan)
    df_close = df_close.dropna(how='all')
    if df_close.empty:
        raise RuntimeError('复权后收盘价为空，无法计算新高新低指标')

    if progress_callback:
        progress_callback(1, total, '图1: 月度上涨股票占比...')

    # ── 获取月度数据 ──
    # 简化：用日线数据按月resample
    monthly_ret = df_close.resample('M').last().pct_change()
    monthly_start = '2017-01-01'
    monthly_ret = monthly_ret[monthly_ret.index >= monthly_start]

    up_ratio_m = (monthly_ret > 0).sum(axis=1) / monthly_ret.notna().sum(axis=1).replace(0, np.nan)
    up_ratio_m = up_ratio_m.rolling(1).mean()
    up_ratio_m = up_ratio_m.dropna()
    if up_ratio_m.empty:
        raise RuntimeError('月度上涨占比为空，请检查数据库行情覆盖区间')
    m_mean = up_ratio_m.mean()
    m_std = up_ratio_m.std()

    m_start = datetime.strftime(up_ratio_m.index[0], '%Y%m%d')
    df_index_m = pro.index_daily(ts_code='000001.SH', start_date=m_start)
    df_index_m = df_index_m.sort_values(by=['trade_date'])
    df_index_m.index = pd.to_datetime(df_index_m['trade_date'])

    fig1 = _plot_ratio_vs_index(up_ratio_m, m_mean, m_std, df_index_m,
                                '月度上涨股票占比', '月度上涨股票占比与上证指数', 0.9, 1.0)
    figures.append((fig1, '月度上涨股票占比与上证指数'))

    if progress_callback:
        progress_callback(2, total, '图2: 日频上涨股票占比...')

    # ── 图2: 日频上涨股票占比 ──
    daily_ret = df_close.pct_change()
    daily_ret = daily_ret[daily_ret.index >= '2024-01-01']
    if daily_ret.empty:
        raise RuntimeError('日频涨跌序列为空，请检查数据库数据或开始日期')
    d_start = datetime.strftime(daily_ret.index[0], '%Y%m%d')

    up_ratio_d = (daily_ret > 0).sum(axis=1) / daily_ret.notna().sum(axis=1).replace(0, np.nan)
    up_ratio_d = up_ratio_d.dropna()
    if up_ratio_d.empty:
        raise RuntimeError('日频上涨占比为空，请检查数据库行情覆盖区间')
    d_mean = up_ratio_d.mean()
    d_std = up_ratio_d.std()

    df_index_d = pro.index_daily(ts_code='000001.SH', start_date=d_start)
    df_index_d = df_index_d.sort_values(by=['trade_date'])
    df_index_d.index = pd.to_datetime(df_index_d['trade_date'])

    fig2 = _plot_ratio_vs_index(up_ratio_d, d_mean, d_std, df_index_d,
                                '日频上涨占比', '日频上涨股票占比与上证指数')
    figures.append((fig2, '日频上涨股票占比与上证指数'))

    if progress_callback:
        progress_callback(3, total, '图3: 新高股票数...')

    # ── 计算新高新低 ──
    NH = (df_close == df_close.rolling(250).max()).sum(axis=1)
    NL = (df_close == df_close.rolling(250).min()).sum(axis=1)

    nh_line = NH.rolling(5).mean()
    nh_line = nh_line[nh_line.index >= '2019-01-03']
    if nh_line.dropna().empty:
        raise RuntimeError('新高股票序列为空，请检查数据库历史区间')
    df_index_nh = pro.index_daily(ts_code='000001.SH', start_date='20190103')
    df_index_nh = df_index_nh.sort_values(by=['trade_date'])
    df_index_nh.index = pd.to_datetime(df_index_nh['trade_date'])

    # ── 图3: 新高股票数 ──
    fig3 = _plot_ratio_vs_index(nh_line, nh_line.mean(), nh_line.std(), df_index_nh,
                                '新高股票数', '新高股票数与上证指数', 0.95, 0.9)
    figures.append((fig3, '新高股票数与上证指数'))

    if progress_callback:
        progress_callback(4, total, '图4: 新高-新低比率...')

    # ── 图4: 新高-新低比率 ──
    nh_nl = (100 * (NH - NL) / (NH + NL + 1e-9)).rolling(60).mean()
    nh_nl = nh_nl[nh_nl.index >= '2020-01-03']
    if nh_nl.dropna().empty:
        raise RuntimeError('新高-新低比率为空，请检查数据库历史区间')
    df_index_nhnl = df_index_nh[df_index_nh.index >= '2020-01-03']

    fig4 = _plot_ratio_vs_index(nh_nl, nh_nl.mean(), nh_nl.std(), df_index_nhnl,
                                '新高-新低比率', '新高-新低比率与上证指数')
    figures.append((fig4, '新高-新低比率与上证指数'))

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
