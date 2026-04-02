"""
ind_13 - 人民币汇率与上证指数
原始文件: 各种指标/人民币汇率/人民币汇率与上证指数.ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .shared_utils import pro

INDICATOR_META = {
    'id': 'ind_13_cny',
    'name': '人民币汇率压力指数',
    'group': '宏观与估值指标',
    'description': '人民币离岸汇率(USD/CNH)与A股的多维联动分析，涵盖汇率倒数映射、差分动量因子、同比增速对比及引力回归策略，揭示汇率变动对A股的传导机制',
    'input_type': 'none',
    'default_code': '',
    'requires_db': False,
    'slow': False,
    'chart_count': 9,
    'chart_descriptions': [
        '上证指数与人民币汇率倒数(CNH/USD)关系图，两者通常正相关',
        '上证指数与汇率差分(diff)关系图，汇率变化速度对股市的影响',
        '上证指数与汇率平滑差分关系图，消除噪声后的趋势性联动',
        '上证平滑涨跌幅与汇率平滑diff叠加，节奏同步性分析',
        '上证指数与人民币同比增速关系图，年度视角的汇率-股市联动',
        '汇率收盘价与人民币动量因子走势，短期汇率趋势的量化度量',
        '汇率收盘价与引力指标叠加，汇率偏离均值的回归信号',
        '基于汇率引力指标的回归策略净值曲线',
        '汇率买卖信号与Score综合评分指标',
    ],
}


def _dual_axis_chart(left_data, right_data, left_label, right_label, title,
                     left_color='blue', right_color='red'):
    """通用双轴图"""
    fig = plt.figure(figsize=(20, 8), facecolor='white')
    ax1 = fig.add_subplot(111)
    ax1.plot(left_data, label=left_label, linewidth=1, color=left_color)
    ax1.set_ylabel(left_label, fontsize=20)
    ax1.legend(loc='upper right', fontsize=15)
    ax1.tick_params(labelsize=18)

    ax2 = ax1.twinx()
    ax2.plot(right_data, color=right_color, label=right_label, linewidth=2)
    ax2.set_ylabel(right_label, fontsize=20)
    ax2.legend(loc='upper left', fontsize=15)
    ax2.tick_params(labelsize=18)

    ax1.set_title(title, fontsize=22)
    ax1.grid(alpha=0.3)
    fig.tight_layout()
    return fig


def generate(start_date='20100101', progress_callback=None, **kwargs):
    figures = []
    total = 9

    if progress_callback:
        progress_callback(0, total, '获取外汇与指数数据...')

    # 获取数据
    df3 = pro.fx_daily(ts_code='USDCNH.FXCM', start_date=start_date)
    df5 = pro.index_daily(ts_code='000001.SH', start_date=start_date)
    df5 = df5.sort_values(by=['trade_date']).reset_index(drop=True)
    df3 = df3.sort_values(by=['trade_date']).reset_index(drop=True)

    data = pd.merge(df3, df5, on=['trade_date'])
    data = data[['trade_date', 'ts_code_x', 'bid_close', 'ts_code_y', 'close']]
    data.columns = ['日期', '美元兑人民币', '汇率收盘价', '上证指数', '收盘价']
    data.index = pd.to_datetime(data['日期'])
    data['汇率收盘价---1'] = 1 / data['汇率收盘价']

    if progress_callback:
        progress_callback(1, total, '图1: 汇率倒数与上证指数...')

    # ── 图1: 汇率倒数与上证指数 ──
    fig1 = _dual_axis_chart(data['汇率收盘价---1'], data['收盘价'],
                            '中美汇率收盘价倒数', '上证指数',
                            '上证指数与中美汇率倒数关系图')
    figures.append((fig1, '上证指数与中美汇率倒数关系图'))

    if progress_callback:
        progress_callback(2, total, '图2-3: 汇率diff分析...')

    # 计算diff
    n = 150
    data['diff'] = data['汇率收盘价'] - data['汇率收盘价'].shift(1)
    data['diffrolling'] = data['diff'].rolling(n).mean()

    # ── 图2: 汇率diff与上证 ──
    fig2 = _dual_axis_chart(data['diff'], data['收盘价'],
                            'diff', '上证指数',
                            '上证指数与中美汇率diff关系图')
    figures.append((fig2, '上证指数与中美汇率diff关系图'))

    # ── 图3: 平滑diff与上证 ──
    fig3 = _dual_axis_chart(data['diffrolling'], data['收盘价'],
                            f'平滑{n}期diff', '上证指数',
                            '上证指数与中美汇率平滑diff关系图')
    figures.append((fig3, '上证指数与中美汇率平滑diff关系图'))

    if progress_callback:
        progress_callback(3, total, '图4: 平滑涨跌幅...')

    # 计算涨跌幅
    data['ret2'] = data['收盘价'].pct_change().fillna(0)
    data['retwrolling'] = data['ret2'].rolling(60).mean().fillna(0)
    data['diffrolling'] = data['diffrolling'].fillna(0)

    # ── 图4: 平滑涨跌幅与平滑diff ──
    fig4 = _dual_axis_chart(data['diffrolling'], data['retwrolling'],
                            f'平滑{n}期diff', '上证指数平滑涨跌幅',
                            '上证指数平滑涨跌幅与中美汇率平滑diff关系图',
                            right_color='red')
    figures.append((fig4, '上证指数平滑涨跌幅与汇率平滑diff关系图'))

    if progress_callback:
        progress_callback(4, total, '图5: 人民币同比增速...')

    # 计算动量因子
    data['人民币同比增速'] = (data['汇率收盘价'] / data['汇率收盘价'].shift(12) - 1).rolling(5).mean()
    data['人民币涨跌幅'] = data['汇率收盘价'].pct_change()
    data['短时人民币动量因子'] = data['人民币涨跌幅'].rolling(20).mean()
    data['中时人民币动量因子'] = data['人民币涨跌幅'].rolling(60).mean()
    data['长时人民币动量因子'] = data['人民币涨跌幅'].rolling(120).mean()
    data['mutil_scale_人民币动量因子'] = (1 / 3 * (data['短时人民币动量因子'] +
                                              data['中时人民币动量因子'] +
                                              data['长时人民币动量因子'])).rolling(20).mean()

    data1 = data[data['日期'] >= '20180101'].copy()
    n_sma = 60
    data1['sma'] = data1['汇率收盘价'].rolling(n_sma).mean()
    data1['引力因子'] = data1['汇率收盘价'] - data1['sma']
    data1['mean'] = data1['引力因子'].mean()
    data1['均值回归+1std'] = data1['mean'] + 1 * data1['引力因子'].std()
    data1['均值回归-1std'] = data1['mean'] - 1 * data1['引力因子'].std()

    # ── 图5: 人民币同比增速 ──
    fig5 = _dual_axis_chart(data1['人民币同比增速'], data1['收盘价'],
                            '人民币同比增速', '上证指数',
                            '上证指数与人民币同比增速关系图')
    figures.append((fig5, '上证指数与人民币同比增速关系图'))

    if progress_callback:
        progress_callback(5, total, '图6: 人民币动量因子...')

    # ── 图6: 多尺度人民币动量因子 ──
    fig6 = _dual_axis_chart(data1['mutil_scale_人民币动量因子'], data1['汇率收盘价'],
                            'Multi-scale 人民币动量因子', '汇率收盘价',
                            '汇率收盘价与人民币动量因子关系图',
                            left_color='orange')
    figures.append((fig6, '汇率收盘价与人民币动量因子关系图'))

    if progress_callback:
        progress_callback(6, total, '图7: 引力指标...')

    # ── 图7: 引力指标与汇率 ──
    fig7, ax1 = plt.subplots(figsize=(20, 8), facecolor='white')
    ax1.plot(data1['引力因子'], color='orange', linewidth=2, label='引力指标')
    ax1.plot(data1['mean'], linestyle='--', linewidth=2, label='均值')
    ax1.plot(1.6 * data1['均值回归+1std'], linestyle='--', linewidth=2, label='引力指标+1std')
    ax1.plot(1.6 * data1['均值回归-1std'], linestyle='--', linewidth=2, label='引力指标-1std')
    ax1.legend(loc='upper left', fontsize=15)
    ax1.set_ylabel('引力指标', fontsize=20)
    ax1.tick_params(labelsize=18)

    ax2 = ax1.twinx()
    ax2.plot(data1['汇率收盘价'], color='red', linewidth=4, label='汇率收盘价')
    ax2.legend(loc='upper right', fontsize=15)
    ax2.set_ylabel('汇率收盘价', fontsize=20)
    ax2.tick_params(labelsize=18)
    ax1.set_title('汇率收盘价与引力指标', fontsize=22)
    ax1.grid(alpha=0.3)
    fig7.tight_layout()
    figures.append((fig7, '汇率收盘价与引力指标'))

    if progress_callback:
        progress_callback(7, total, '图8-9: 汇率回归策略...')

    # 计算Score策略
    best_sma = 3
    best_multiplier = 0.5
    df_best = _calculate_score_strategy(data.copy(), best_sma, best_multiplier)

    # ── 图8: 策略净值与汇率 ──
    fig8, ax1 = plt.subplots(figsize=(20, 8), facecolor='white')
    ax2 = ax1.twinx()
    ax1.plot(df_best.index, df_best['net_value'], 'g-', label='策略净值', linewidth=1)
    ax2.plot(df_best.index, df_best['汇率收盘价'], 'b-', label='汇率收盘价', linewidth=1)
    ax1.set_xlabel('时间', fontsize=18)
    ax1.set_ylabel('策略净值', color='g', fontsize=18)
    ax2.set_ylabel('汇率收盘价', color='b', fontsize=18)
    ax1.legend(loc='upper left', fontsize=18)
    ax2.legend(loc='upper right', fontsize=18)
    ax1.tick_params(labelsize=14)
    ax2.tick_params(labelsize=14)
    ax1.grid(alpha=0.3)
    fig8.tight_layout()
    figures.append((fig8, '汇率回归策略净值'))

    # ── 图9: 最近一段的买卖信号 + Score ──
    df_recent = df_best[df_best.index >= '2024-01-01'].copy()
    if len(df_recent) == 0:
        df_recent = df_best.tail(250)

    fig9, (ax1, ax3) = plt.subplots(2, 1, figsize=(20, 16), facecolor='white')
    ax2 = ax1.twinx()
    ax2.plot(df_recent.index, df_recent['汇率收盘价'], 'b-', label='汇率收盘价', linewidth=1)

    buy_signals = df_recent[(df_recent['signal'] == 1) & (df_recent['signal'].shift(1) != 1)]
    sell_signals = df_recent[(df_recent['signal'] == 0) & (df_recent['signal'].shift(1) != 0)]
    ax2.scatter(buy_signals.index, buy_signals['汇率收盘价'],
                marker='^', color='g', label='买入信号', s=200, alpha=1)
    ax2.scatter(sell_signals.index, sell_signals['汇率收盘价'],
                marker='v', color='r', label='卖出信号', s=200, alpha=1)

    ax1.set_ylabel('上证指数', fontsize=18)
    ax2.set_ylabel('汇率收盘价', fontsize=18)
    ax1.legend(loc='upper left', fontsize=14)
    ax2.legend(loc='upper right', fontsize=14)
    ax1.tick_params(labelsize=14)
    ax2.tick_params(labelsize=14)
    ax1.set_title('汇率买卖信号', fontsize=20)
    ax1.grid(alpha=0.3)

    ax3.plot(df_recent.index, df_recent['score'], label='Score', color='purple', linewidth=1)
    ax3.plot(df_recent.index, df_recent['score_mean'], label='Mean Score', color='orange', linewidth=1)
    ax3.plot(df_recent.index, df_recent['score+std'], label='Score + STD', color='red', linewidth=1)
    ax3.plot(df_recent.index, df_recent['score-std'], label='Score - STD', color='blue', linewidth=1)
    ax3.set_title(f'汇率Score（标准差乘数={best_multiplier:.1f}）', fontsize=20)
    ax3.legend(fontsize=14)
    ax3.tick_params(labelsize=14)
    ax3.grid(alpha=0.3)
    fig9.tight_layout()
    figures.append((fig9, '汇率买卖信号与Score指标'))

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures


def _calculate_score_strategy(df, sma, std_multiplier, initial_balance=10000):
    """计算Score策略"""
    ret = df['汇率收盘价'].rolling(sma).mean()
    std = df['汇率收盘价'].rolling(sma).std()
    score = (df['汇率收盘价'] - ret) / (std + 1e-9)
    df['score'] = score
    df['score_mean'] = score.mean()
    df['score+std'] = score.mean() + std_multiplier * score.std()
    df['score-std'] = score.mean() - std_multiplier * score.std()

    balance = initial_balance
    shares = 0
    net_values = []
    signal = []
    buy_flag = True
    sell_flag = False

    for i in range(len(df)):
        net_values.append(balance + shares * df.iloc[i]['汇率收盘价'])
        if i > 0:
            prev_score = df.iloc[i - 1]['score']
            prev_low = df.iloc[i - 1]['score-std']
            prev_high = df.iloc[i - 1]['score+std']
            curr_score = df.iloc[i]['score']

            if prev_score > prev_low and curr_score < prev_low:
                if shares == 0 and buy_flag:
                    shares += balance / df.iloc[i]['汇率收盘价']
                    balance = 0
                    signal.append(1)
                    buy_flag = False
                    sell_flag = True
                else:
                    signal.append(signal[-1] if signal else 0)
            elif prev_score < prev_high and curr_score > prev_high:
                if shares > 0 and sell_flag:
                    balance += shares * df.iloc[i]['汇率收盘价']
                    shares = 0
                    signal.append(0)
                    sell_flag = False
                    buy_flag = True
                else:
                    signal.append(signal[-1] if signal else 0)
            else:
                signal.append(signal[-1] if signal else 0)
        else:
            signal.append(0)

    df['net_value'] = net_values
    df['signal'] = signal
    return df
