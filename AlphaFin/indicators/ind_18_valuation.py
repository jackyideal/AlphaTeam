"""
ind_18 - 市场估值指标
原始文件: 各种指标2/市场破净率情况/一系列评估市场估值指标.ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .shared_utils import pro
from .db_utils import get_data_by_sql, get_pivot_data
from ..config import DB_ROOT

INDICATOR_META = {
    'id': 'ind_18_valuation',
    'name': '全市场估值温度计',
    'group': '宏观与估值指标',
    'description': '多维度市场估值全景扫描：破净率、低PE占比、涨跌比率、新高新低家数差、均线突破占比、翻倍股比例等11项宽基指标，综合评估市场估值所处历史分位与极端状态',
    'input_type': 'none',
    'default_code': '',
    'requires_db': True,
    'slow': True,
    'chart_count': 11,
    'chart_descriptions': [
        '全市场PB<1(破净)股票占比走势，极高值预示市场估值底部',
        '全市场PE<100股票占比走势，低PE股票比例反映市场整体估值重心',
        '涨跌比率(ADR)走势，上涨家数/下跌家数的比值，极端值有反转意义',
        '全市场创新低家数占比，持续高比例暗示系统性风险',
        '全市场创新高家数占比，持续高比例印证趋势行情',
        '全市场所有均线突破占比，综合多周期均线的市场强度评估',
        '全市场10分位PE走势，边际估值水平的分布特征',
        '全市场平均成交额走势，反映市场活跃度与资金参与程度',
        '翻倍股占比走势，极高值通常出现在牛市中后期',
        '翻倍股数量绝对值走势',
        '新高-新低家数差值走势，正差扩大表示市场健康',
    ],
}


def _calculate_value_ratio(df_pivot, threshold, below=True):
    """计算低于(或高于)阈值的股票占比序列"""
    if below:
        count = (df_pivot < threshold).sum(axis=1)
    else:
        count = (df_pivot > threshold).sum(axis=1)
    total = df_pivot.notnull().sum(axis=1)
    ratio = count / total
    return ratio


def _plot_dual(metric_series, metric_label, szzs, start_date, title, figsize=(16, 8)):
    """绘制指标与上证指数双轴图"""
    fig, ax1 = plt.subplots(figsize=figsize, facecolor='white')

    # 指标
    data = metric_series[metric_series.index >= start_date]
    ax1.plot(data.index, data, label=metric_label, color='orange', linewidth=2)
    ax1.set_ylabel(metric_label, fontsize=16)
    ax1.tick_params(axis='y', labelsize=13)
    ax1.legend(loc='upper right', fontsize=13)

    # 上证指数
    ax2 = ax1.twinx()
    idx = szzs[szzs.index >= start_date]
    ax2.plot(idx.index, idx['close'], color='IndianRed', linewidth=2, label='上证指数')
    ax2.set_ylabel('上证指数', fontsize=16)
    ax2.tick_params(axis='y', labelsize=13)
    ax2.legend(loc='upper left', fontsize=13)

    ax1.grid(alpha=0.3)
    plt.title(title, fontsize=18)
    fig.tight_layout()
    return fig


def generate(start_date='20100101', progress_callback=None, **kwargs):
    figures = []
    total = 11
    file_path = f'sqlite:////{DB_ROOT}'

    if progress_callback:
        progress_callback(0, total, '获取股票列表与数据库数据...')

    # ── 获取全市场股票 ──
    codess = pro.query('stock_basic', exchange='SSE', list_status='L',
                       fields='ts_code,symbol,name,area,industry,list_date')
    codess1 = pro.query('stock_basic', exchange='SZSE', list_status='L',
                        fields='ts_code,symbol,name,area,industry,list_date')
    codes = list(set(codess['ts_code'])) + list(set(codess1['ts_code']))

    # ── 上证指数 ──
    szzs = pro.index_daily(ts_code='000001.SH')
    szzs['trade_date'] = pd.to_datetime(szzs['trade_date'])
    szzs = szzs.sort_values(by='trade_date').reset_index(drop=True)
    szzs.index = szzs['trade_date']

    if progress_callback:
        progress_callback(0, total, '加载数据库（dailybasic, kline, adj）...')

    # ── 从数据库加载数据 ──
    df_dailybasic = get_data_by_sql(file_path, 'dailybasic', 'dailybasic', codes, '*')

    df_adj = get_data_by_sql(file_path, 'daily_adj', 'daily_adj', codes, '*')
    df_kline = get_data_by_sql(file_path, 'daily_kline', 'daily_kline', codes, '*')

    # 复权收盘价
    df_adj_pivot = get_pivot_data(df_adj, 'adj_factor')
    df_close = get_pivot_data(df_kline, 'close')
    df_close = (df_close * df_adj_pivot / df_adj_pivot.loc[df_adj_pivot.index[-1]]).round(2)
    df_close = df_close.dropna(how='all')

    # 估值pivot
    PB = get_pivot_data(df_dailybasic, 'pb')
    PE = get_pivot_data(df_dailybasic, 'pe_ttm')

    if progress_callback:
        progress_callback(1, total, '图1: PB<1占比...')

    # ── 图1: PB<1 占比与上证指数 ──
    pb_ratio = _calculate_value_ratio(PB, 1, below=True)
    fig1 = _plot_dual(pb_ratio, 'PB<1占比', szzs, start_date,
                      '市净率低于1的占比与上证指数关系图')
    figures.append((fig1, 'PB低于1占比'))

    if progress_callback:
        progress_callback(2, total, '图2: PE<100占比...')

    # ── 图2: PE<100 占比与上证指数 ──
    # 只统计PE>0的（排除亏损股）
    PE_positive = PE.where(PE > 0)
    pe_ratio = _calculate_value_ratio(PE_positive, 100, below=True)
    fig2 = _plot_dual(pe_ratio, 'PE<100占比', szzs, start_date,
                      'PE低于100的占比与上证指数关系图')
    figures.append((fig2, 'PE低于100占比'))

    if progress_callback:
        progress_callback(3, total, '图3: 涨跌比率...')

    # ── 图3: 上证指数与涨跌比率 ──
    try:
        df_ret = df_close.pct_change()
        daily_up_count = (df_ret > 0).sum(axis=1)
        daily_down_count = (df_ret < 0).sum(axis=1)
        up_down_ratio = (daily_up_count / (daily_down_count + 1)).rolling(60).mean()

        fig3 = _plot_dual(up_down_ratio, '涨跌比率(60日均)', szzs, '20100101',
                          '上证指数与市场涨跌比率', figsize=(16, 8))
        figures.append((fig3, '涨跌比率'))
    except Exception as e:
        print(f'绘制涨跌比率失败: {e}')

    if progress_callback:
        progress_callback(4, total, '图4-5: 新高低家数...')

    # ── 计算市场宽度指标 ──
    try:
        # 新高新低
        rolling_max_250 = df_close.rolling(250).max()
        rolling_min_250 = df_close.rolling(250).min()
        new_high = (df_close >= rolling_max_250).sum(axis=1)
        new_low = (df_close <= rolling_min_250).sum(axis=1)
        total_stocks = df_close.notnull().sum(axis=1)

        new_high_ratio = (new_high / total_stocks).rolling(60).mean()
        new_low_ratio = (new_low / total_stocks).rolling(60).mean()
        new_high_low_diff = (new_high - new_low).rolling(60).mean()

        # 均线突破
        ma_250 = df_close.rolling(250).mean()
        ma_60 = df_close.rolling(60).mean()
        ma_20 = df_close.rolling(20).mean()
        ma_5 = df_close.rolling(5).mean()

        break_250 = (df_close > ma_250).sum(axis=1) / total_stocks
        break_60 = (df_close > ma_60).sum(axis=1) / total_stocks
        break_20 = (df_close > ma_20).sum(axis=1) / total_stocks
        break_5 = (df_close > ma_5).sum(axis=1) / total_stocks
        break_all = ((df_close > ma_250) & (df_close > ma_60) &
                     (df_close > ma_20) & (df_close > ma_5)).sum(axis=1) / total_stocks

        # 翻倍股
        rolling_ret_250 = df_close / df_close.shift(250) - 1
        double_stocks = (rolling_ret_250 > 1).sum(axis=1)
        double_stocks_ratio = double_stocks / total_stocks

        # 平均成交额
        szzs_amount = szzs['amount']
        szse = pro.index_daily(ts_code='399001.SZ')
        szse['trade_date'] = pd.to_datetime(szse['trade_date'])
        szse = szse.sort_values('trade_date')
        szse.index = szse['trade_date']
        total_amount = szzs_amount.add(szse['amount'], fill_value=0)
        avg_amount = total_amount / total_stocks

        # 分位数PE
        quantile_10_pe = PE.quantile(0.1, axis=1)

        # ── 图4: 新低家数占比 ──
        fig4 = _plot_dual(new_low_ratio, '新低家数占比(60日均)', szzs, '20180101',
                          '250日新低家数占比', figsize=(20, 10))
        figures.append((fig4, '新低家数占比'))

        if progress_callback:
            progress_callback(5, total, '图5: 新高家数占比...')

        # ── 图5: 新高家数占比 ──
        fig5 = _plot_dual(new_high_ratio, '新高家数占比(60日均)', szzs, '20180101',
                          '250日新高家数占比', figsize=(20, 10))
        figures.append((fig5, '新高家数占比'))

        if progress_callback:
            progress_callback(6, total, '图6: 均线全突破占比...')

        # ── 图6: 同时突破所有均线占比 ──
        fig6 = _plot_dual(break_all, '全均线突破占比', szzs, '20180101',
                          '同时突破5/20/60/250日均线的股票占比', figsize=(20, 10))
        figures.append((fig6, '全均线突破占比'))

        if progress_callback:
            progress_callback(7, total, '图7: 10分位PE...')

        # ── 图7: 10分位PE ──
        fig7 = _plot_dual(quantile_10_pe, '10分位PE', szzs, '20100101',
                          '全市场10分位PE与上证指数', figsize=(20, 10))
        figures.append((fig7, '10分位PE'))

        if progress_callback:
            progress_callback(8, total, '图8: 平均成交额...')

        # ── 图8: 平均成交额 ──
        fig8 = _plot_dual(avg_amount, '平均每只股票成交额', szzs, '20180101',
                          '平均每只股票成交额与上证指数', figsize=(20, 10))
        figures.append((fig8, '平均成交额'))

        if progress_callback:
            progress_callback(9, total, '图9: 翻倍股占比...')

        # ── 图9: 翻倍股占比 ──
        fig9 = _plot_dual(double_stocks_ratio, '翻倍股占比', szzs, '20180101',
                          '年内翻倍股票占比与上证指数', figsize=(20, 10))
        figures.append((fig9, '翻倍股占比'))

        if progress_callback:
            progress_callback(10, total, '图10: 翻倍股数量...')

        # ── 图10: 翻倍股数量 ──
        fig10 = _plot_dual(double_stocks, '翻倍股数量', szzs, '20180101',
                           '年内翻倍股票数量与上证指数', figsize=(20, 10))
        figures.append((fig10, '翻倍股数量'))

        # ── 图11: 新高-新低差值 ──
        fig11 = _plot_dual(new_high_low_diff, '新高-新低(60日均)', szzs, '20180101',
                           '250日新高与新低家数差值', figsize=(20, 10))
        figures.append((fig11, '新高新低差值'))

    except Exception as e:
        print(f'绘制市场宽度指标失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
