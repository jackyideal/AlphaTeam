"""
ind_09 - MFI货币流量指标
原始文件: 各种指标/mfi指标源码/mfi 指标.ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .shared_utils import pro

try:
    import talib as ta
    HAS_TALIB = True
except ImportError:
    HAS_TALIB = False

INDICATOR_META = {
    'id': 'ind_09_mfi',
    'name': '资金流量强度因子',
    'group': '资金面指标',
    'description': '资金流量指标(MFI)融合成交量与价格信息，类似带量RSI，用于判断超买(>80)超卖(<20)区域，含多周期参数对比、策略回测与跨指数信号验证',
    'input_type': 'stock',
    'default_code': '600425.SH',
    'requires_db': False,
    'slow': False,
    'chart_count': 8,
    'chart_descriptions': [
        '14日MFI指标与股价走势，MFI>80为超买信号、MFI<20为超卖信号',
        'MFI策略累计收益曲线与基准对比，评估量价结合策略的有效性',
        '4日短周期MFI与收盘价走势，短期交易的快速超买超卖判断',
        '基于MFI阈值的买卖信号标注在股价走势上，展示每笔交易时机',
        '上证指数MFI走势，大盘层面的资金流量判断',
        '创业板MFI走势，成长板块的资金流入流出节奏',
        '沪深300 MFI走势，蓝筹板块的量价配合状态',
        '上证50 MFI走势，权重股群体的资金流量信号',
    ],
}


def _calc_mfi(high, low, close, vol, timeperiod=14):
    """计算MFI，支持talib和纯pandas两种方式"""
    if HAS_TALIB:
        return ta.MFI(high.astype(float), low.astype(float),
                      close.astype(float), vol.astype(float), timeperiod=timeperiod)
    else:
        tp = (high + low + close) / 3
        mf = tp * vol
        pmf = mf.where(tp > tp.shift(1), 0)
        nmf = mf.where(tp < tp.shift(1), 0)
        pmf_sum = pmf.rolling(timeperiod).sum()
        nmf_sum = nmf.rolling(timeperiod).sum()
        return 100 - 100 / (1 + pmf_sum / (nmf_sum + 1e-9))


def _mfi_signal(df, buy_threshold=19, sell_threshold=79):
    """根据MFI生成买卖信号"""
    df['signal'] = 0
    holding = False
    prev_mfi = df['mfi'].shift(1)
    for i in range(1, len(df)):
        curr = df.iloc[i]['mfi']
        if holding:
            if curr > sell_threshold:
                df.iloc[i, df.columns.get_loc('signal')] = 0
                holding = False
            else:
                df.iloc[i, df.columns.get_loc('signal')] = 1
        else:
            if curr < buy_threshold and (i > 0 and prev_mfi.iloc[i] > buy_threshold):
                df.iloc[i, df.columns.get_loc('signal')] = 1
                holding = True
    return df


def _plot_index_mfi(code, name, start_date, timeperiod, high_line, low_line):
    """绘制指数MFI双子图"""
    df = pro.index_daily(ts_code=code, start_date=start_date)
    df = df.sort_values(by=['trade_date'])
    df.index = pd.to_datetime(df['trade_date'])
    df['mfi'] = _calc_mfi(df['high'], df['low'], df['close'], df['vol'], timeperiod)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), facecolor='white')
    ax1.plot(df.index, df['close'], color='r', linewidth=2)
    ax1.set_ylabel(name, fontsize=18)
    ax1.set_title(f'{name}走势', fontsize=22)
    ax1.grid(alpha=0.3)
    ax1.tick_params(labelsize=14)

    ax2.plot(df.index, df['mfi'], linewidth=1.5)
    ax2.axhline(y=high_line, color='red', linestyle='--', linewidth=1)
    ax2.axhline(y=low_line, color='red', linestyle='--', linewidth=1)
    ax2.set_ylabel('MFI', fontsize=18)
    ax2.set_title(f'MFI指标(timeperiod={timeperiod})', fontsize=22)
    ax2.grid(alpha=0.3)
    ax2.tick_params(labelsize=14)
    fig.tight_layout()
    return fig


def generate(ts_code='600425.SH', start_date='20201201', progress_callback=None, **kwargs):
    figures = []
    total = 8

    if progress_callback:
        progress_callback(0, total, '获取个股数据...')

    # 获取个股数据
    df = pro.daily(ts_code=ts_code, adj='qfq', start_date=start_date)
    df = df.sort_values(by=['trade_date'])
    df.index = pd.to_datetime(df['trade_date'])
    df['ret'] = df['close'] / df['close'].shift(1) - 1

    if progress_callback:
        progress_callback(1, total, '图1: MFI与股价(timeperiod=14)...')

    # ── 图1: MFI与股价（timeperiod=14）──
    df['mfi'] = _calc_mfi(df['high'], df['low'], df['close'], df['vol'], timeperiod=14)

    fig1, ax1 = plt.subplots(figsize=(20, 10), facecolor='white')
    ax1.plot(df.index, df['close'], color='IndianRed', linewidth=2, label='股价')
    ax1.set_ylabel('股价', fontsize=22)
    ax1.legend(loc='upper left', fontsize=15)
    ax1.tick_params(labelsize=16)

    ax2 = ax1.twinx()
    ax2.plot(df.index, df['mfi'], color='orange', linewidth=1, label='MFI')
    ax2.axhline(y=79, color='green', linestyle='--', linewidth=1)
    ax2.axhline(y=19, color='green', linestyle='--', linewidth=1)
    ax2.set_ylabel('MFI', fontsize=22)
    ax2.legend(loc='upper right', fontsize=15)
    ax2.tick_params(labelsize=16)
    ax1.set_title(f'{ts_code} MFI反转指标(14日)', fontsize=24)
    ax1.grid(alpha=0.3)
    fig1.tight_layout()
    figures.append((fig1, f'{ts_code} MFI与股价(14日)'))

    if progress_callback:
        progress_callback(2, total, '图2: MFI策略回测...')

    # 使用优化参数 timeperiod=4 计算MFI策略
    df['mfi'] = _calc_mfi(df['high'], df['low'], df['close'], df['vol'], timeperiod=4)
    df = _mfi_signal(df, buy_threshold=19, sell_threshold=79)

    df['daily_return'] = df['close'].pct_change()
    df['strategy_return'] = df['signal'].shift(1) * df['daily_return']
    df['cumulative_return_strategy'] = (1 + df['strategy_return']).cumprod()
    df['cumulative_return_hold'] = (1 + df['daily_return']).cumprod()

    # 获取上证指数作为基准
    df_idx = pro.index_daily(ts_code='000001.SH', start_date=start_date)
    df_idx = df_idx.sort_values(by=['trade_date'])
    df_idx.index = pd.to_datetime(df_idx['trade_date'])
    df_merged = df.merge(df_idx[['close']].rename(columns={'close': '上证指数'}),
                         left_index=True, right_index=True, how='left')
    df_merged['benchmark_return'] = df_merged['上证指数'].pct_change()
    df_merged['cumulative_return_benchmark'] = (1 + df_merged['benchmark_return']).cumprod()

    # ── 图2: 累计收益对比 ──
    fig2, ax = plt.subplots(figsize=(16, 8), facecolor='white')
    ax.plot(df.index, df['cumulative_return_hold'], label=f'{ts_code}持有', color='blue', linewidth=2)
    ax.plot(df.index, df['cumulative_return_strategy'], label='MFI策略', color='orange', linewidth=2)
    ax.plot(df_merged.index, df_merged['cumulative_return_benchmark'],
            label='上证指数', color='IndianRed', linewidth=2)
    ax.set_xlabel('日期', fontsize=16)
    ax.set_ylabel('累计收益', fontsize=16)
    ax.set_title('累计收益对比：持有 vs MFI策略 vs 上证指数', fontsize=20)
    ax.legend(fontsize=14)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=14)
    fig2.tight_layout()
    figures.append((fig2, '累计收益对比'))

    if progress_callback:
        progress_callback(3, total, '图3: MFI与股价(timeperiod=4)...')

    # ── 图3: MFI与股价（timeperiod=4, 截取后半段）──
    df_half = df_merged.iloc[len(df_merged) // 3:]
    date_range = df_half.index

    fig3, ax1 = plt.subplots(figsize=(20, 10), facecolor='white')
    ax1.set_ylabel('收盘价', color='tab:blue', fontsize=22)
    ax1.plot(date_range, df_half['close'], color='tab:blue', label='收盘价', linewidth=2)
    ax1.tick_params(axis='y', labelcolor='tab:blue', labelsize=18)
    ax1.tick_params(axis='x', labelsize=14)

    ax2 = ax1.twinx()
    ax2.plot(date_range, df_half['mfi'], color='tab:orange', label='MFI', linewidth=1)
    ax2.axhline(y=79, color='green', linestyle='--')
    ax2.axhline(y=19, color='green', linestyle='--')
    ax2.set_ylabel('MFI', fontsize=22)
    ax2.tick_params(labelsize=18)

    ax1.set_title(f'{ts_code} 收盘价与MFI(4日)', fontsize=22)
    ax1.grid(alpha=0.3)
    fig3.tight_layout()
    figures.append((fig3, f'{ts_code} 收盘价与MFI(4日)'))

    if progress_callback:
        progress_callback(4, total, '图4: 买卖信号...')

    # ── 图4: 买卖信号图 ──
    buy_sigs = df_half[(df_half['signal'] == 1) & (df_half['signal'].shift(1) != 1)]
    sell_sigs = df_half[(df_half['signal'] == 0) & (df_half['signal'].shift(1) == 1)]

    fig4, ax = plt.subplots(figsize=(18, 8), facecolor='white')
    ax.plot(date_range, df_half['close'], label='收盘价', color='blue', linewidth=2)

    for date in buy_sigs.index:
        ax.annotate('买', (date, df_half.loc[date, 'close']),
                    textcoords="offset points", xytext=(0, 10),
                    ha='center', color='red', fontsize=14, fontweight='bold')
        ax.scatter(date, df_half.loc[date, 'close'], marker='^', color='red', s=100)

    for date in sell_sigs.index:
        ax.annotate('卖', (date, df_half.loc[date, 'close']),
                    textcoords="offset points", xytext=(0, 10),
                    ha='center', color='green', fontsize=14, fontweight='bold')
        ax.scatter(date, df_half.loc[date, 'close'], marker='v', color='green', s=100)

    ax.set_xlabel('日期', fontsize=16)
    ax.set_ylabel('收盘价', fontsize=16)
    ax.set_title(f'{ts_code} MFI买卖信号', fontsize=20)
    ax.legend(fontsize=14)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=14)
    fig4.tight_layout()
    figures.append((fig4, f'{ts_code} MFI买卖信号'))

    if progress_callback:
        progress_callback(5, total, '图5: 上证指数MFI...')

    # ── 图5-8: 各指数MFI ──
    index_configs = [
        ('000001.SH', '上证指数', 60, 64, 43),
        ('399006.SZ', '创业板指数', 30, 65, 38),
        ('399300.SZ', '沪深300指数', 20, 70, 38),
        ('000016.SH', '上证50指数', 20, 70, 35),
    ]

    for idx, (code, name, tp, hi, lo) in enumerate(index_configs):
        if progress_callback:
            progress_callback(5 + idx, total, f'图{5 + idx}: {name}MFI...')
        try:
            fig = _plot_index_mfi(code, name, '20180701', tp, hi, lo)
            figures.append((fig, f'{name} MFI指标'))
        except Exception as e:
            print(f'绘制{name}MFI失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
