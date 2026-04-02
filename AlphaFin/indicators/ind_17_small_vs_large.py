"""
ind_17 - 小市值vs大市值
原始文件: 各种指标2/市场小市值与大市值对应指标/jacky_1110.ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .shared_utils import pro

INDICATOR_META = {
    'id': 'ind_17_small_vs_large',
    'name': '大小盘风格轮动因子',
    'group': '市场结构指标',
    'description': '构建小市值/大市值、亏损股/绩优股两组对照因子，通过收益率差值追踪市场风格轮动，叠加边际成交量变化分析，识别风格切换的早期信号与市场投机度',
    'input_type': 'none',
    'default_code': '',
    'requires_db': False,
    'slow': False,
    'chart_count': 4,
    'chart_descriptions': [
        '个股股价与边际成交量变化走势，量价关系的微观视角',
        '上证指数与边际成交量变化走势，大盘层面的量能边际信号',
        '亏损股vs绩优股收益率对比指标与上证指数叠加，高值表示投机氛围浓厚',
        '小市值vs大市值综合指标与上证指数叠加，正值表示小盘风格占优',
    ],
}


def _plot_zscore_chart(price_data, indicator_data, price_label, ind_label, title):
    """绘制价格vs zscore指标的双轴图"""
    fig = plt.figure(figsize=(20, 8), facecolor='white')
    ax0 = fig.add_subplot(111)
    ax0.plot(price_data.index, price_data, linewidth=2)
    ax0.legend([price_label], loc='upper left', fontsize=15)
    ax0.set_ylabel(price_label, fontsize=20)
    ax0.tick_params(labelsize=15)

    ax1 = ax0.twinx()
    ax1.plot(indicator_data.index, indicator_data, color='orange', linewidth=1.5)
    mean_val = indicator_data.mean()
    std_val = indicator_data.std()
    ax1.axhline(mean_val, color='red', linewidth=1)
    ax1.axhline(mean_val + std_val, color='yellow', linewidth=1)
    ax1.axhline(mean_val - std_val, color='blue', linewidth=1)
    ax1.set_ylabel(ind_label, fontsize=20)
    ax1.legend([ind_label, '均值', '+1标准差', '-1标准差'], loc='upper right', fontsize=12)
    ax1.tick_params(labelsize=15)

    plt.title(title, fontsize=22)
    plt.grid(alpha=0.3)
    fig.tight_layout()
    return fig


def generate(start_date='20200101', progress_callback=None, **kwargs):
    figures = []
    total = 4
    一年交易日 = 60

    if progress_callback:
        progress_callback(0, total, '获取上证指数数据...')

    # ── 数据1: 上证指数成交量边际变化 ──
    dd1 = pro.index_daily(ts_code='000001.SH')
    dd1.index = pd.to_datetime(dd1['trade_date'])
    dd1 = dd1.sort_index()
    dd1['x'] = dd1['amount'].rolling(20).mean() - dd1['amount'].rolling(60).mean()
    zscore_1 = (dd1['x'] - dd1['x'].rolling(一年交易日).mean()) / dd1['x'].rolling(一年交易日).std()
    zscore_1 = zscore_1[start_date[:4] + '-01-01':]
    index_df = pd.merge(zscore_1.rename('x'), dd1['close'], on='trade_date').dropna()

    # ── 数据2: 个股成交量边际变化 ──
    dd = pro.daily(ts_code='600425.SH', adj='qfq', start_date='20190101')
    dd.index = pd.to_datetime(dd['trade_date'])
    dd = dd.sort_index()
    dd['x'] = dd['amount'].rolling(30).mean() - dd['amount'].rolling(60).mean()
    zscore_0 = (dd['x'] - dd['x'].rolling(一年交易日).mean()) / dd['x'].rolling(一年交易日).std()
    zscore_0 = zscore_0[start_date[:4] + '-01-01':]
    qsjh = pd.merge(zscore_0.rename('x'), dd['close'], on='trade_date').dropna()

    if progress_callback:
        progress_callback(1, total, '图1: 个股边际成交量...')

    # ── 图1: 个股(青松建化)股价与边际成交量变化 ──
    fig1 = _plot_zscore_chart(qsjh['close'], qsjh['x'],
                              '青松建化', '边际成交量指标',
                              '青松建化股价与边际成交量变化')
    figures.append((fig1, '青松建化股价与边际成交量变化'))

    if progress_callback:
        progress_callback(2, total, '图2: 上证指数边际成交量...')

    # ── 图2: 上证指数与边际成交量变化 ──
    fig2 = _plot_zscore_chart(index_df['close'], index_df['x'],
                              '上证指数', '边际成交量指标',
                              '上证指数与边际成交量变化')
    figures.append((fig2, '上证指数与边际成交量变化'))

    if progress_callback:
        progress_callback(3, total, '图3: 亏损股vs绩优股...')

    # ── 数据3: 亏损股vs绩优股 ──
    try:
        dd4 = pro.sw_daily(ts_code='801851.SI')
        dd4.index = pd.to_datetime(dd4['trade_date'])
        dd4 = dd4.sort_index()
        dd4['收益率'] = dd4['pct_change'] / 100

        dd5 = pro.sw_daily(ts_code='801853.SI')
        dd5.index = pd.to_datetime(dd5['trade_date'])
        dd5 = dd5.sort_index()
        dd5['收益率'] = dd5['pct_change'] / 100

        dd6 = pd.concat([dd4['收益率'], dd5['收益率']], axis=1)
        dd6.columns = ['亏损股', '绩优股']
        dd6['x'] = (dd6['亏损股'] > dd6['绩优股']) * 1
        dd6['x_ratio'] = dd6['x'].rolling(120).mean()
        zscore_5 = (dd6['x_ratio'] - dd6['x_ratio'].rolling(60).mean()) / dd6['x_ratio'].rolling(60).std()
        zscore_5 = zscore_5[start_date[:4] + '-01-01':].dropna()

        # 对应上证指数
        dd_idx = pro.index_daily(ts_code='000001.SH')
        dd_idx.index = pd.to_datetime(dd_idx['trade_date'])
        dd_idx = dd_idx.sort_index()
        dd_idx = dd_idx[dd_idx.index.isin(zscore_5.index)]

        fig3 = _plot_zscore_chart(dd_idx['close'], zscore_5,
                                  '上证指数', '亏损股胜于绩优股',
                                  '上证指数与亏损股vs绩优股指标')
        figures.append((fig3, '上证指数与亏损股vs绩优股指标'))
    except Exception as e:
        print(f'绘制亏损股vs绩优股失败: {e}')

    if progress_callback:
        progress_callback(4, total, '图4: 小市值vs大市值...')

    # ── 数据4: 小市值vs大市值 ──
    try:
        dd_small = pro.index_daily(ts_code='000852.SH')  # 中证1000
        dd_small.index = pd.to_datetime(dd_small['trade_date'])
        dd_small = dd_small.sort_index()
        dd_small['收益率'] = dd_small['pct_chg'] / 100

        dd_large = pro.index_daily(ts_code='000903.SH')  # 中证100
        dd_large.index = pd.to_datetime(dd_large['trade_date'])
        dd_large = dd_large.sort_index()
        dd_large['收益率'] = dd_large['pct_chg'] / 100

        dd_cmp = pd.concat([dd_small['收益率'], dd_large['收益率']], axis=1)
        dd_cmp.columns = ['小市值', '大市值']
        dd_cmp['x'] = (dd_cmp['小市值'] > dd_cmp['大市值']) * 1
        dd_cmp['x_ratio'] = dd_cmp['x'].rolling(120).mean()
        zscore_2 = (dd_cmp['x_ratio'] - dd_cmp['x_ratio'].rolling(3 * 一年交易日).mean()) / \
                   dd_cmp['x_ratio'].rolling(3 * 一年交易日).std()
        zscore_2 = zscore_2[start_date[:4] + '-01-01':].dropna()

        # 综合指标 = (zscore_1 + zscore_2) / 2
        combined = pd.concat([zscore_1.rename('z1'), zscore_2.rename('z2')], axis=1).dropna()
        final_score = (combined['z1'] + combined['z2']) / 2

        dd_idx2 = pro.index_daily(ts_code='000001.SH')
        dd_idx2.index = pd.to_datetime(dd_idx2['trade_date'])
        dd_idx2 = dd_idx2.sort_index()
        dd_idx2 = dd_idx2[dd_idx2.index.isin(final_score.index)]

        fig4 = _plot_zscore_chart(dd_idx2['close'], final_score,
                                  '上证指数', '大市值指标',
                                  '上证指数与小市值vs大市值综合指标')
        figures.append((fig4, '上证指数与小市值vs大市值综合指标'))
    except Exception as e:
        print(f'绘制小市值vs大市值失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
