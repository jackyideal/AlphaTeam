"""
ind_24 - 市场热度指标
原始参考: 各种指标2/市场当日热度指标/市场热度指标.ipynb
"""
import sqlite3

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from AlphaFin.config import DB_ROOT
from .market_heat_shared import get_stock_count_df
from .shared_utils import pro


INDICATOR_META = {
    'id': 'ind_24_market_heat',
    'name': '市场热度扩散指数',
    'group': '市场结构指标',
    'description': '基于全市场涨跌幅分布构建热度指数，跟踪涨停级强势股密度、下跌扩散程度和市场冷暖分位。',
    'input_type': 'none',
    'default_code': '',
    'requires_db': True,
    'slow': False,
    'chart_count': 4,
    'chart_descriptions': [
        '市场热度综合指数（60日平滑）与上证指数双轴对比，识别极端低温/过热区间。',
        '涨跌扩散指标（>5%上涨占比、<-5%下跌占比、净扩散占比）与上证指数对照。',
        '每日可交易股票数量与60日均线，观察市场广度变化。',
        '热度分位阶段标注图（低温/中性/过热）辅助择时判断。',
    ],
}


def _load_heat_panel(start_date):
    db_path = f'{DB_ROOT}/daily_kline.db'
    sql = '''
        SELECT
            trade_date,
            SUM(CASE WHEN pct_chg > 5 THEN 1 ELSE 0 END) AS up_5,
            SUM(CASE WHEN pct_chg > 7 THEN 1 ELSE 0 END) AS up_7,
            SUM(CASE WHEN pct_chg > 10 THEN 1 ELSE 0 END) AS up_10,
            SUM(CASE WHEN pct_chg < -5 THEN 1 ELSE 0 END) AS down_5,
            SUM(CASE WHEN pct_chg < -7 THEN 1 ELSE 0 END) AS down_7,
            SUM(CASE WHEN pct_chg < -10 THEN 1 ELSE 0 END) AS down_10,
            COUNT(DISTINCT ts_code) AS db_stock_count
        FROM daily_kline
        GROUP BY trade_date
        ORDER BY trade_date
    '''
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(sql, conn)

    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df[df['trade_date'] >= pd.to_datetime(start_date)].copy()
    return df


def _load_sse_index(start_date):
    idx = pro.index_daily(ts_code='000001.SH', start_date=start_date)
    idx['trade_date'] = pd.to_datetime(idx['trade_date'])
    idx = idx.sort_values('trade_date')
    idx = idx[['trade_date', 'close']].rename(columns={'close': '上证指数'})
    return idx


def _phase_label(series):
    q05 = series.quantile(0.05)
    q25 = series.quantile(0.25)
    q75 = series.quantile(0.75)
    q95 = series.quantile(0.95)

    phase = pd.Series('中性', index=series.index)
    phase[series <= q05] = '极冷'
    phase[(series > q05) & (series <= q25)] = '偏冷'
    phase[(series >= q75) & (series < q95)] = '偏热'
    phase[series >= q95] = '极热'
    return phase


def generate(start_date='20170101', progress_callback=None, **kwargs):
    if progress_callback:
        progress_callback(0, 4, '加载市场热度基础数据...')

    heat = _load_heat_panel(start_date)
    stock_count = get_stock_count_df(start_date=start_date, refresh=False)
    stock_count['trade_date'] = pd.to_datetime(stock_count['trade_date'])
    heat = heat.merge(stock_count, on='trade_date', how='left')
    heat['stock_count'] = heat['stock_count'].fillna(heat['db_stock_count'])
    heat['stock_count'] = heat['stock_count'].replace(0, np.nan).ffill().bfill()

    idx = _load_sse_index(start_date)
    df = heat.merge(idx, on='trade_date', how='inner').sort_values('trade_date')
    if df.empty:
        raise ValueError('市场热度数据为空，请调整起始日期')
    df = df.set_index('trade_date')

    # 综合热度：强势扩散 - 弱势扩散（按市场股票数量归一）
    raw = (df['up_5'] + df['up_7'] + df['up_10'] -
           df['down_5'] - df['down_7'] - df['down_10']) / df['stock_count']
    df['热度综合指数'] = raw.rolling(60, min_periods=5).mean()
    df['上涨扩散占比'] = (df['up_5'] / df['stock_count']).rolling(20, min_periods=5).mean()
    df['下跌扩散占比'] = (df['down_5'] / df['stock_count']).rolling(20, min_periods=5).mean()
    df['净扩散占比'] = df['上涨扩散占比'] - df['下跌扩散占比']
    valid_heat = df['热度综合指数'].dropna()
    if valid_heat.empty:
        df['市场阶段'] = '中性'
    else:
        df['市场阶段'] = _phase_label(valid_heat).reindex(df.index).ffill().bfill()

    figures = []

    if progress_callback:
        progress_callback(1, 4, '图1: 热度综合指数...')

    fig1, ax1 = plt.subplots(figsize=(18, 8), facecolor='white')
    ax1.plot(df.index, df['热度综合指数'], color='#dc2626', linewidth=2, label='热度综合指数(60日)')
    ax1.axhline(df['热度综合指数'].mean(), color='gray', linestyle='--', linewidth=1, label='均值')
    ax1.set_ylabel('热度指数', fontsize=14)
    ax1.legend(loc='upper left', fontsize=11)
    ax1.grid(alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(df.index, df['上证指数'], color='#1d4ed8', linewidth=2, label='上证指数')
    ax2.set_ylabel('上证指数', fontsize=14)
    ax2.legend(loc='upper right', fontsize=11)
    ax1.set_title('市场热度综合指数与上证指数', fontsize=18)
    fig1.tight_layout()
    figures.append((fig1, '市场热度综合指数与上证指数'))

    if progress_callback:
        progress_callback(2, 4, '图2: 涨跌扩散占比...')

    fig2, ax1 = plt.subplots(figsize=(18, 8), facecolor='white')
    ax1.plot(df.index, df['上涨扩散占比'], color='#16a34a', linewidth=2, label='>5%上涨占比(20日)')
    ax1.plot(df.index, df['下跌扩散占比'], color='#dc2626', linewidth=2, label='<-5%下跌占比(20日)')
    ax1.plot(df.index, df['净扩散占比'], color='#9333ea', linewidth=2, label='净扩散占比')
    ax1.axhline(0, color='gray', linestyle='--', linewidth=1)
    ax1.set_ylabel('占比', fontsize=14)
    ax1.legend(loc='upper left', fontsize=11)
    ax1.grid(alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(df.index, df['上证指数'], color='#2563eb', linewidth=1.8, alpha=0.85, label='上证指数')
    ax2.set_ylabel('上证指数', fontsize=14)
    ax2.legend(loc='upper right', fontsize=11)
    ax1.set_title('市场涨跌扩散占比与上证指数', fontsize=18)
    fig2.tight_layout()
    figures.append((fig2, '涨跌扩散占比与上证指数'))

    if progress_callback:
        progress_callback(3, 4, '图3: 市场股票数量...')

    fig3, ax1 = plt.subplots(figsize=(18, 8), facecolor='white')
    ax1.plot(df.index, df['stock_count'], color='#0f766e', linewidth=1.8, label='每日股票数量')
    ax1.plot(df.index, df['stock_count'].rolling(60, min_periods=10).mean(),
             color='#14b8a6', linewidth=2.2, label='60日均线')
    ax1.set_ylabel('股票数量', fontsize=14)
    ax1.legend(loc='upper left', fontsize=11)
    ax1.grid(alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(df.index, df['上证指数'], color='#1e40af', linewidth=1.8, alpha=0.85, label='上证指数')
    ax2.set_ylabel('上证指数', fontsize=14)
    ax2.legend(loc='upper right', fontsize=11)
    ax1.set_title('市场股票数量与上证指数', fontsize=18)
    fig3.tight_layout()
    figures.append((fig3, '市场股票数量与上证指数'))

    if progress_callback:
        progress_callback(4, 4, '图4: 热度阶段标注...')

    fig4, ax1 = plt.subplots(figsize=(18, 8), facecolor='white')
    ax1.plot(df.index, df['热度综合指数'], color='#b91c1c', linewidth=2, label='热度综合指数')
    y_min = df['热度综合指数'].min()
    y_max = df['热度综合指数'].max()
    if pd.isna(y_min) or pd.isna(y_max) or y_min == y_max:
        y_min, y_max = -1, 1
    phase_colors = {
        '极冷': '#bfdbfe',
        '偏冷': '#dbeafe',
        '中性': '#f3f4f6',
        '偏热': '#fee2e2',
        '极热': '#fecaca',
    }
    for phase, color in phase_colors.items():
        mask = (df['市场阶段'] == phase).astype(int)
        if mask.sum() == 0:
            continue
        ax1.fill_between(df.index, y_min, y_max,
                         where=mask.values.astype(bool), color=color, alpha=0.25)
    ax1.set_ylabel('热度指数', fontsize=14)
    ax1.legend(loc='upper left', fontsize=11)
    ax1.grid(alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(df.index, df['上证指数'], color='#1d4ed8', linewidth=1.8, label='上证指数')
    ax2.set_ylabel('上证指数', fontsize=14)
    ax2.legend(loc='upper right', fontsize=11)
    ax1.set_title('市场热度阶段与上证指数', fontsize=18)
    fig4.tight_layout()
    figures.append((fig4, '市场热度阶段标注图'))

    return figures
