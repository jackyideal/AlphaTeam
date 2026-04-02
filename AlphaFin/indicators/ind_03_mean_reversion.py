"""
ind_03 - 均值回归策略
原始文件: 各种指标/均值回归指标/均值回归 及 青松破净买入策略.ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .shared_utils import pro

INDICATOR_META = {
    'id': 'ind_03_mean_reversion',
    'name': '价量均值回归决策模型',
    'group': '资金面指标',
    'description': '基于均值回归原理构建的「引力指标」，衡量价格偏离均线的距离与回归趋势，覆盖多指数与个股的偏离度分析、策略回测及波动率通道，适用于超买超卖判断',
    'input_type': 'stock',
    'default_code': '600425.SH',
    'requires_db': False,
    'slow': False,
    'chart_count': 10,
    'chart_descriptions': [
        '上证指数收盘价与5日均线的偏离距离，偏离过大暗示短期超买或超卖',
        '上证指数与20日引力指标的走势，引力指标反映价格向均值回归的趋势力度',
        '基于引力指标的买卖信号标注在上证指数K线上，直观展示策略入场与离场点',
        '引力策略累计收益曲线与基准对比，评估均值回归策略的超额收益表现',
        '创业板指数引力指标走势，观察成长风格指数的偏离回归特征',
        '沪深300引力指标走势，大盘蓝筹风格的均值回归节奏',
        '中证500引力指标走势，中小盘风格的偏离与回归特征',
        '中证1000引力指标走势，小盘股群体的均值回归信号',
        '个股日线级别引力指标，观察短周期偏离回归信号',
        '个股周线级别引力指标，中期趋势的偏离程度与回归时机',
    ],
}


def _plot_gravity(data, close_col, mr_col, mean_col, upper_col, lower_col,
                  right_label, title, mr_color='tab:blue', close_color='red'):
    """通用引力指标双轴图"""
    fig, ax1 = plt.subplots(figsize=(20, 8), facecolor='white')
    ax1.plot(data.index, data[mr_col], color=mr_color, label='引力指标')
    ax1.plot(data.index, data[mean_col], linestyle='--', label='均值')
    ax1.plot(data.index, data[upper_col], linestyle='--', label='+1std')
    ax1.plot(data.index, data[lower_col], linestyle='--', label='-1std')
    ax1.legend(loc='upper left', fontsize=12)
    ax1.set_ylabel('引力指标', fontsize=20)
    ax1.tick_params(labelsize=14)

    ax2 = ax1.twinx()
    ax2.plot(data.index, data[close_col], color=close_color, linewidth=3, label=right_label)
    ax2.legend(loc='upper right', fontsize=12)
    ax2.set_ylabel(right_label, fontsize=20)
    ax2.tick_params(labelsize=14)

    plt.title(title, fontsize=25)
    ax1.grid(alpha=0.3)
    fig.tight_layout()
    return fig


def _calc_mean_reversion(df, close_col, sma_period, upper_mult=1.0, lower_mult=1.0):
    """计算均值回归指标"""
    df = df.copy()
    df['SMA'] = df[close_col].rolling(sma_period).mean()
    df['均值回归'] = df[close_col] - df['SMA']
    df['mean'] = df['均值回归'].mean()
    df['均值回归+1std'] = df['均值回归'].mean() + upper_mult * df['均值回归'].std()
    df['均值回归-1std'] = df['均值回归'].mean() - lower_mult * df['均值回归'].std()
    return df


def generate(ts_code='600425.SH', start_date='20180901', progress_callback=None, **kwargs):
    figures = []
    total = 10

    if progress_callback:
        progress_callback(0, total, '获取上证指数数据...')

    # ── 上证指数数据 ──
    df_sh = pro.index_daily(ts_code='000001.SH', adj='qfq', start_date=start_date)
    df_sh = df_sh.sort_values(by=['trade_date'])
    df_sh.index = pd.to_datetime(df_sh['trade_date'])

    if progress_callback:
        progress_callback(1, total, '图1: 上证指数偏离5日线...')

    # ── 图1: 上证指数偏离5日线距离 ──
    df1 = df_sh[['close']].copy()
    df1['5日均线'] = df1['close'].rolling(5).mean()
    df1['偏离'] = df1['close'] - df1['5日均线']
    df1['偏离_mean'] = df1['偏离'].mean()
    df1['偏离-1std'] = df1['偏离'].mean() - df1['偏离'].std()
    df1['偏离+1std'] = df1['偏离'].mean() + df1['偏离'].std()

    fig1 = _plot_gravity(df1, 'close', '偏离', '偏离_mean', '偏离+1std', '偏离-1std',
                         '上证指数', '上证指数与偏离5日线距离')
    figures.append((fig1, '上证指数与偏离5日线距离'))

    if progress_callback:
        progress_callback(2, total, '图2: 上证指数引力指标(20日)...')

    # ── 图2: 上证指数引力指标（20日均值回归） ──
    df2 = _calc_mean_reversion(df_sh[['close']].copy(), 'close', 20)
    fig2 = _plot_gravity(df2, 'close', '均值回归', 'mean', '均值回归+1std', '均值回归-1std',
                         '上证指数', '上证指数与引力指标(20日)')
    figures.append((fig2, '上证指数与引力指标(20日)'))

    if progress_callback:
        progress_callback(3, total, '图3: 策略买卖信号...')

    # ── 图3: 上证指数策略买卖信号 ──
    best_s, best_m, best_n = 8, 1.2, 0.8
    df3 = df_sh[['close']].copy()
    df3['SMA'] = df3['close'].rolling(best_s).mean()
    df3['均值回归'] = df3['close'] - df3['SMA']
    df3['均值回归+mstd'] = df3['均值回归'].mean() + best_m * df3['均值回归'].std()
    df3['均值回归-nstd'] = df3['均值回归'].mean() - best_n * df3['均值回归'].std()

    buy_sigs = df3['均值回归'] < df3['均值回归-nstd']
    sell_sigs = df3['均值回归'] >= df3['均值回归+mstd']
    df3['signal'] = 0
    holding = False
    for i in range(len(df3)):
        if buy_sigs.iloc[i] and not holding:
            df3.iloc[i, df3.columns.get_loc('signal')] = 1
            holding = True
        elif sell_sigs.iloc[i] and holding:
            df3.iloc[i, df3.columns.get_loc('signal')] = 0
            holding = False
        elif i > 0:
            df3.iloc[i, df3.columns.get_loc('signal')] = df3.iloc[i - 1]['signal']

    # 买卖信号对
    buy_sell_pairs = []
    current_buy = None
    for idx in range(len(df3)):
        if df3['signal'].iloc[idx] == 1 and current_buy is None:
            current_buy = df3.index[idx]
        elif df3['signal'].iloc[idx] == 0 and current_buy is not None:
            buy_sell_pairs.append((current_buy, df3.index[idx]))
            current_buy = None

    fig3, ax1 = plt.subplots(figsize=(16, 7), facecolor='white')
    ax1.plot(df3.index, df3['close'], label='上证指数', color='tab:blue', linewidth=2)
    for buy, sell in buy_sell_pairs:
        ax1.scatter(buy, df3.loc[buy, 'close'], marker='^', color='red', s=100)
        ax1.scatter(sell, df3.loc[sell, 'close'], marker='v', color='green', s=100)
    ax1.set_ylabel('上证指数', fontsize=16)
    ax1.tick_params(labelsize=14)

    ax2 = ax1.twinx()
    ax2.plot(df3.index, df3['均值回归'], color='orange', label='均值回归', linewidth=1)
    ax2.plot(df3.index, df3['均值回归+mstd'], color='red', linestyle='--', linewidth=1)
    ax2.plot(df3.index, df3['均值回归-nstd'], color='green', linestyle='--', linewidth=1)
    ax2.set_ylabel('均线回归指标', fontsize=16)

    plt.title('上证指数及策略买卖信号', fontsize=20)
    plt.grid(alpha=0.3)
    fig3.tight_layout()
    figures.append((fig3, '上证指数策略买卖信号'))

    if progress_callback:
        progress_callback(4, total, '图4: 策略累计收益...')

    # ── 图4: 策略累计收益 ──
    df3['position'] = df3['signal'].shift().fillna(0)
    df3['strategy_return'] = df3['close'].pct_change() * df3['position']
    df3['buy_hold_return'] = df3['close'].pct_change()
    df3['strategy_cum'] = (1 + df3['strategy_return']).cumprod() - 1
    df3['hold_cum'] = (1 + df3['buy_hold_return']).cumprod() - 1

    fig4, ax = plt.subplots(figsize=(16, 7), facecolor='white')
    ax.plot(df3.index, df3['strategy_cum'], label='策略累计收益', linewidth=2)
    ax.plot(df3.index, df3['hold_cum'], label='买入持有累计收益', linewidth=2)
    ax.legend(fontsize=14)
    ax.set_title('策略累计收益 vs 买入持有', fontsize=20)
    ax.set_ylabel('累计收益', fontsize=14)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=14)
    fig4.tight_layout()
    figures.append((fig4, '策略累计收益对比'))

    if progress_callback:
        progress_callback(5, total, '图5-8: 各指数引力指标...')

    # ── 图5-8: 各指数引力指标 ──
    index_configs = [
        ('000001.SH', '上证指数', 30, 0.78, 0.6, True),
        ('399006.SZ', '创业板指数', 30, 0.78, 0.6, True),
        ('399300.SZ', '沪深300指数', 80, 1.0, 1.0, True),
        ('000016.SH', '上证50指数', 30, 1.0, 1.0, True),
    ]

    for idx, (code, name, sma, u_mult, l_mult, is_index) in enumerate(index_configs):
        if progress_callback:
            progress_callback(5 + idx, total, f'图{5 + idx}: {name}引力指标...')
        try:
            if is_index:
                df_idx = pro.index_daily(ts_code=code, adj='qfq', start_date='20180901')
            else:
                df_idx = pro.daily(ts_code=code, adj='qfq', start_date='20180901')
            df_idx = df_idx.sort_values(by=['trade_date'])
            df_idx.index = pd.to_datetime(df_idx['trade_date'])

            df_mr = _calc_mean_reversion(df_idx[['close']].copy(), 'close', sma, u_mult, l_mult)
            # 缩放倍率
            if u_mult != 1.0:
                df_mr['均值回归+1std'] = u_mult * df_mr['均值回归+1std']
                df_mr['均值回归-1std'] = l_mult * df_mr['均值回归-1std']

            fig = _plot_gravity(df_mr, 'close', '均值回归', 'mean', '均值回归+1std', '均值回归-1std',
                                name, f'{name}与引力指标')
            figures.append((fig, f'{name}与引力指标'))
        except Exception as e:
            print(f'绘制{name}引力指标失败: {e}')

    if progress_callback:
        progress_callback(9, total, '图9-10: 个股引力指标...')

    # ── 图9: 个股日线引力指标 ──
    try:
        dd = pro.daily(ts_code=ts_code, adj='qfq', start_date='20201101')
        dd = dd.sort_values(by=['trade_date'])
        dd.index = pd.to_datetime(dd['trade_date'])

        dd_mr = _calc_mean_reversion(dd[['close']].copy(), 'close', 20, 0.8, 0.8)
        fig9 = _plot_gravity(dd_mr, 'close', '均值回归', 'mean', '均值回归+1std', '均值回归-1std',
                             ts_code, f'{ts_code}日线引力指标', mr_color='orange', close_color='IndianRed')
        figures.append((fig9, f'{ts_code}日线引力指标'))
    except Exception as e:
        print(f'绘制个股日线引力指标失败: {e}')

    # ── 图10: 个股周线引力指标 ──
    try:
        dd_w = pro.weekly(ts_code=ts_code, adj='qfq', start_date='20180901')
        dd_w = dd_w.sort_values(by=['trade_date'])
        dd_w.index = pd.to_datetime(dd_w['trade_date'])

        dd_w_mr = _calc_mean_reversion(dd_w[['close']].copy(), 'close', 5)
        fig10 = _plot_gravity(dd_w_mr, 'close', '均值回归', 'mean', '均值回归+1std', '均值回归-1std',
                              ts_code, f'{ts_code}周线引力指标', mr_color='orange', close_color='red')
        figures.append((fig10, f'{ts_code}周线引力指标'))
    except Exception as e:
        print(f'绘制个股周线引力指标失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
