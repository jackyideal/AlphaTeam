"""
ind_12 - 换手率与股价
原始文件: 各种指标/股价与换手率关系/青松建化 换手率与股价关系.ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .shared_utils import pro

INDICATOR_META = {
    'id': 'ind_12_turnover',
    'name': '换手率-价格共振监测',
    'group': '资金面指标',
    'description': '通过个股换手率的滚动均值、标准差通道及成交额比值分析量价背离与共振，异常换手率突破通道上轨常预示变盘，适用于个股交易时机判断',
    'input_type': 'stock',
    'default_code': '600425.SH',
    'requires_db': False,
    'slow': False,
    'chart_count': 3,
    'chart_descriptions': [
        '个股换手率滚动均值与股价走势叠加，量价共振时趋势更可靠',
        '换手率均值与标准差通道，突破上轨预示量能异常放大，常对应变盘',
        '个股成交额与大盘成交额比值走势，比值突然放大反映该股获得超额关注',
    ],
}


def generate(ts_code='600425.SH', start_date='20210201', progress_callback=None, **kwargs):
    figures = []
    total = 3

    if progress_callback:
        progress_callback(0, total, '获取个股与换手率数据...')

    # ── 获取数据 ──
    df = pro.daily(ts_code=ts_code, adj='qfq', start_date=start_date)
    df = df.sort_values(by=['trade_date']).reset_index(drop=True)
    df2 = pro.daily_basic(ts_code=ts_code, fields='trade_date,turnover_rate',
                          start_date=start_date)
    df2 = df2.sort_values(by=['trade_date']).reset_index(drop=True)
    df = pd.merge(df, df2, on=['trade_date'])

    m = 20  # 滚动窗口
    df['rolling1'] = df['turnover_rate'].rolling(m).mean()
    df['rolling_mean'] = df['rolling1'].mean()
    df['turnover_rate_mean+1std'] = df['rolling_mean'] + 0.8 * df['rolling1'].std()
    df['turnover_rate_mean-1std'] = df['rolling_mean'] - 0.8 * df['rolling1'].std()
    df.index = pd.to_datetime(df['trade_date'])

    if progress_callback:
        progress_callback(1, total, '图1: 换手率与股价...')

    # ── 图1: 换手率滚动均值 + 均值/标准差通道 vs 股价 ──
    fig1 = plt.figure(figsize=(20, 8), facecolor='white')
    ax1 = fig1.add_subplot(111)
    ax1.plot(df.index, df['close'], label='股价', color='red', linewidth=2)
    ax1.legend(loc='upper left', fontsize=15)
    ax1.set_ylabel('股价', fontsize=25)
    ax1.tick_params(labelsize=15)

    ax2 = ax1.twinx()
    ax2.plot(df.index, df['rolling1'], color='purple', label='换手率(20日均值)')
    ax2.plot(df.index, df['rolling_mean'], color='orange', label='均值', linestyle='--')
    ax2.plot(df.index, df['turnover_rate_mean+1std'], color='blue', label='均值+0.8std', linestyle='--')
    ax2.plot(df.index, df['turnover_rate_mean-1std'], color='green', label='均值-0.8std', linestyle='--')
    ax2.set_ylabel('换手率(%)', fontsize=25)
    ax2.tick_params(labelsize=15)
    ax2.legend(loc='upper right', fontsize=12)

    plt.title(f'{ts_code} 换手率与股价', fontsize=25)
    ax1.grid(alpha=0.3)
    fig1.tight_layout()
    figures.append((fig1, f'{ts_code}换手率与股价'))

    if progress_callback:
        progress_callback(2, total, '图2: 5/10日换手率均线...')

    # ── 图2: 5/10日换手率均线 vs 股价 ──
    df['turnover_rate_5'] = df['turnover_rate'].rolling(5).mean()
    df['turnover_rate_10'] = df['turnover_rate'].rolling(10).mean()

    fig2, ax1 = plt.subplots(figsize=(20, 12), facecolor='white')
    ax1.plot(df.index, df['close'], color='IndianRed', linewidth=3)
    ax1.legend(['股价'], loc='upper left', fontsize=17)
    ax1.set_ylabel('股价', fontsize=30)
    ax1.tick_params(labelsize=15)

    ax2 = ax1.twinx()
    ax2.plot(df.index, df['turnover_rate_5'], color='black', label='5日换手率')
    ax2.plot(df.index, df['turnover_rate_10'], color='SkyBlue', label='10日换手率')
    ax2.set_ylabel('换手率', fontsize=30)
    ax2.legend(loc='upper right', fontsize=17)
    ax2.tick_params(labelsize=15)

    plt.title(f'{ts_code}与换手率指标', fontsize=30)
    ax1.grid(alpha=0.3)
    fig2.tight_layout()
    figures.append((fig2, f'{ts_code}换手率均线'))

    if progress_callback:
        progress_callback(3, total, '图3: 成交额比值...')

    # ── 图3: 个股/上证指数成交额比值 vs 股价 ──
    try:
        szzs = pro.index_daily(ts_code='000001.SH', start_date=start_date)
        szzs = szzs.sort_values('trade_date')
        szzs.index = pd.to_datetime(szzs['trade_date'])

        merged = pd.merge(szzs[['close', 'amount', 'pct_chg']],
                          df[['close', 'amount', 'pct_chg']],
                          left_index=True, right_index=True, suffixes=('_szzs', '_stock'))
        merged['ratio'] = (merged['amount_stock'] / merged['amount_szzs']).rolling(10).mean()

        fig3, ax1 = plt.subplots(figsize=(20, 10), facecolor='white')
        line1 = ax1.plot(merged.index, merged['close_stock'], color='tab:red', label=f'{ts_code}收盘价')
        ax1.set_ylabel(f'{ts_code}收盘价', color='tab:red', fontsize=16)
        ax1.tick_params(axis='y', labelcolor='tab:red', labelsize=14)
        ax1.tick_params(axis='x', labelsize=14)

        ax2 = ax1.twinx()
        line2 = ax2.plot(merged.index, merged['ratio'], color='tab:blue', label='成交额比值')
        ax2.set_ylabel('成交额比值(10日均)', color='tab:blue', fontsize=16)
        ax2.tick_params(axis='y', labelcolor='tab:blue', labelsize=14)

        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, loc='upper left', fontsize=14)
        plt.title(f'{ts_code}收盘价与成交额比值关系图', fontsize=20)
        ax1.grid(alpha=0.3)
        fig3.tight_layout()
        figures.append((fig3, f'{ts_code}成交额比值'))
    except Exception as e:
        print(f'绘制成交额比值失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
