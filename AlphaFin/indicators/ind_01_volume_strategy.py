"""
ind_01 - 大盘成交量策略
原始文件: 各种指标/前百分之5%成交额占比/大盘成交量策略，超强.ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from .shared_utils import pro

INDICATOR_META = {
    'id': 'ind_01_volume_strategy',
    'name': '成交量动量择时引擎',
    'group': '资金面指标',
    'description': '基于短期与长期成交量均线（5/20日）金叉死叉构建的大盘择时策略，通过量能趋势变化捕捉买卖信号，回测展示策略累计收益与基准对比',
    'input_type': 'none',
    'default_code': '000001.SH',
    'requires_db': False,
    'slow': False,
    'chart_count': 2,
    'chart_descriptions': [
        '展示短期(5日)与长期(20日)成交量均线金叉死叉构建的择时策略累计收益曲线，与买入持有基准对比，评估策略有效性',
        '标注成交量策略的买入(金叉)和卖出(死叉)信号在上证指数走势上的具体位置，直观展示每笔交易的时机',
    ],
}


def generate(start_date='20170101', progress_callback=None, **kwargs):
    figures = []
    today = datetime.strftime(datetime.now(), '%Y%m%d')

    if progress_callback:
        progress_callback(0, 2, '获取上证指数数据...')

    # 获取数据
    df_index = pro.index_daily(ts_code='000001.SH', start_date=start_date)
    df_index = df_index.sort_values(by=['trade_date'])
    df_index.index = pd.to_datetime(df_index['trade_date'])

    if df_index.isnull().values.any():
        df_index.fillna(method='ffill', inplace=True)
        df_index.fillna(method='bfill', inplace=True)

    # 策略参数
    best_short_window = 9
    best_long_window = 12

    # 计算策略
    df_index['short_vol'] = df_index['vol'].rolling(window=best_short_window).mean()
    df_index['long_vol'] = df_index['vol'].rolling(window=best_long_window).mean()
    df_index['position'] = 0

    for i in range(1, len(df_index)):
        if (df_index['short_vol'].iloc[i - 1] < df_index['long_vol'].iloc[i - 1] and
                df_index['short_vol'].iloc[i] > df_index['long_vol'].iloc[i]):
            df_index['position'].iloc[i] = 1
        elif (df_index['short_vol'].iloc[i - 1] > df_index['long_vol'].iloc[i - 1] and
              df_index['short_vol'].iloc[i] < df_index['long_vol'].iloc[i]):
            df_index['position'].iloc[i] = 0
        else:
            df_index['position'].iloc[i] = df_index['position'].iloc[i - 1]

    df_index['strategy_return'] = df_index['close'].pct_change() * df_index['position'].shift(1)
    df_index['cumulative_strategy_return'] = (1 + df_index['strategy_return']).cumprod() - 1
    df_index['cumulative_index_return'] = (1 + df_index['close'].pct_change()).cumprod() - 1

    if progress_callback:
        progress_callback(1, 2, '绘制累积收益对比图...')

    # ── 图1: 累积收益对比图 ──
    fig1, ax = plt.subplots(figsize=(20, 8), facecolor='white')
    ax.plot(df_index.index, df_index['cumulative_strategy_return'], label='成交量策略', linewidth=2)
    ax.plot(df_index.index, df_index['cumulative_index_return'], label='上证指数', linewidth=2)
    ax.legend(fontsize=14)
    ax.set_title('成交量策略累积收益', fontsize=20)
    ax.set_xlabel('日期', fontsize=16)
    ax.set_ylabel('累计收益', fontsize=16)
    ax.tick_params(labelsize=14)
    ax.grid(alpha=0.3)
    fig1.tight_layout()
    figures.append((fig1, '成交量策略累积收益对比'))

    if progress_callback:
        progress_callback(2, 2, '绘制成交量与买卖信号图...')

    # ── 图2: 成交量与买卖信号图（双Y轴）──
    # 筛选最近一段数据
    filtered_data = df_index.loc['2024-01-01':]
    if len(filtered_data) == 0:
        filtered_data = df_index.tail(250)

    fig2, ax1 = plt.subplots(figsize=(20, 8), facecolor='white')
    ax1.plot(filtered_data.index, filtered_data['short_vol'], label='短期成交量',
             linewidth=2, color='orange')
    ax1.plot(filtered_data.index, filtered_data['long_vol'], label='长期成交量',
             linewidth=2, color='purple')

    # 买卖信号
    buy_signals = filtered_data[
        (filtered_data['position'] == 1) & (filtered_data['position'].shift(1) == 0)]
    sell_signals = filtered_data[
        (filtered_data['position'] == 0) & (filtered_data['position'].shift(1) == 1)]

    ax1.scatter(buy_signals.index, buy_signals['short_vol'],
                marker='^', color='r', label='买入', alpha=1, s=100)
    ax1.scatter(sell_signals.index, sell_signals['short_vol'],
                marker='v', color='g', label='卖出', alpha=1, s=100)

    ax1.set_xlabel('日期', fontsize=16)
    ax1.set_ylabel('成交量指标', fontsize=16)
    ax1.legend(loc='upper left', fontsize=14)
    ax1.grid(alpha=0.3)
    ax1.tick_params(axis='both', labelsize=14)

    ax2 = ax1.twinx()
    ax2.plot(filtered_data.index, filtered_data['close'], label='上证指数',
             color='black', alpha=0.6, linewidth=2)
    ax2.set_ylabel('上证指数', fontsize=16)
    ax2.legend(loc='upper right', fontsize=14)
    ax2.tick_params(axis='both', labelsize=14)

    ax1.set_title('成交量策略和上证指数', fontsize=20)
    fig2.tight_layout()
    figures.append((fig2, '成交量策略买卖信号与上证指数'))

    return figures
