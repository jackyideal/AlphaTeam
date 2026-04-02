"""
ind_08 - 总市值与M2比值
原始文件: 各种指标/总市值与中国gdp/总市值与m2比值.ipynb
注意：此指标需要遍历每月交易日获取全市场市值，运行时间较长
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import time
from .shared_utils import pro

INDICATOR_META = {
    'id': 'ind_08_mcap_m2',
    'name': '股市总市值/M2估值锚',
    'group': '宏观与估值指标',
    'description': '巴菲特指标的中国版本：全市场总市值与M2货币供应量的比值走势，叠加主要指数PE估值通道（均值±标准差），多维度评估市场整体估值水位',
    'input_type': 'none',
    'default_code': '',
    'requires_db': False,
    'slow': True,
    'chart_count': 4,
    'chart_descriptions': [
        '中国版巴菲特指标：A股总市值与M2余额的比值走势，高于历史均值+1std预示高估',
        '上证指数PE估值通道（均值±1/2倍标准差），当前PE所处分位一目了然',
        '创业板PE估值通道，成长板块估值周期的历史参照',
        '沪深300 PE估值通道，蓝筹板块估值的安全边际判断',
    ],
}


def _plot_mv_m2(dff, set_ylabel, set_ylabel2, title):
    """总市值/M2 vs 指数 双轴图"""
    fig, ax1 = plt.subplots(figsize=(20, 10), facecolor='white')
    ax1.plot(dff.index, dff['total_mv/m2'], color='SkyBlue', linewidth=5, label=set_ylabel)
    ax1.plot(dff.index, dff['total_mv/m2_mean'], color='black', linestyle='--', linewidth=5, label='Mean')
    ax1.plot(dff.index, dff['total_mv/m2_mean+1std'], color='green', linestyle='--', linewidth=5, label='Mean ± 1 Std')
    ax1.plot(dff.index, dff['total_mv/m2_mean-1std'], color='green', linestyle='--', linewidth=5)
    ax1.set_ylabel(set_ylabel, fontsize=25)
    ax1.tick_params(axis='y', labelsize=15)
    ax1.legend(loc='upper left', fontsize=15)

    ax2 = ax1.twinx()
    ax2.plot(dff.index, dff['close'], color='IndianRed', linewidth=2, label=set_ylabel2)
    ax2.set_ylabel(set_ylabel2, fontsize=25)
    ax2.tick_params(axis='y', labelsize=15)
    ax2.legend(loc='upper right', fontsize=15)

    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=12))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax1.grid(True)
    plt.title(title, fontsize=30)
    fig.tight_layout()
    return fig


def _mean_std(dd):
    """计算PE/PB均值和标准差"""
    dd = dd.copy()
    dd['pe_mean'] = dd['pe'].mean()
    dd['pe_mean+1std'] = dd['pe'].mean() + 1.2 * dd['pe'].std()
    dd['pe_mean-1std'] = dd['pe'].mean() - 1.2 * dd['pe'].std()
    return dd


def _plot_ind(dd, title, ind):
    """绘制PE/PB带均值标准差通道的图表"""
    dd = dd.sort_values(by=['trade_date']).reset_index(drop=True)
    dd.index = pd.to_datetime(dd['trade_date'])
    fig, ax = plt.subplots(figsize=(20, 8), facecolor='white')
    ax.plot(dd.index, dd[ind], label=ind)
    ax.plot(dd.index, dd[f'{ind}_mean'], linestyle='--', label='均值')
    ax.plot(dd.index, dd[f'{ind}_mean+1std'], linestyle='--', label='+1.2std')
    ax.plot(dd.index, dd[f'{ind}_mean-1std'], linestyle='--', label='-1.2std')
    ax.set_title(title, fontsize=20)
    ax.set_xlabel('时间', fontsize=20)
    ax.tick_params(labelsize=15)
    ax.legend(fontsize=17)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    return fig


def generate(start_date='20010101', progress_callback=None, **kwargs):
    figures = []
    total = 4

    if progress_callback:
        progress_callback(0, total, '获取交易日历和M2数据...')

    # ── 获取交易日历，每月最后一个交易日 ──
    end_date = datetime.strftime(datetime.now() - timedelta(days=1), '%Y%m%d')
    df_calender = pro.trade_cal(exchange='', start_date=start_date,
                                end_date=end_date, is_open=1)
    df_calender = df_calender.sort_values(by=['cal_date']).reset_index(drop=True)
    df_calender['month'] = df_calender['cal_date'].apply(lambda x: x[:6])
    df_calender = df_calender.drop_duplicates(subset=['month'], keep='last')
    df_calender = df_calender.reset_index(drop=True)

    # M2数据
    df_m2 = pro.cn_m()
    df_m2 = df_m2[df_m2['month'] >= start_date[:6]].sort_values(by=['month']).reset_index(drop=True)

    if progress_callback:
        progress_callback(0, total, f'获取{len(df_calender)}个月的全市场市值数据（耗时较长）...')

    # ── 获取每月末全市场总市值 ──
    mvs = []
    for i in range(len(df_calender)):
        try:
            dd = pro.daily_basic(ts_code='',
                                 trade_date=df_calender.loc[i, 'cal_date'],
                                 fields='ts_code,trade_date,total_mv')
            mvs.append(dd['total_mv'].sum() / 10000)
            time.sleep(20)
        except Exception:
            mvs.append(np.nan)

        if progress_callback and i % 10 == 0:
            progress_callback(0, total, f'获取市值数据 {i}/{len(df_calender)}...')

    df_calender['total_mv'] = mvs

    if progress_callback:
        progress_callback(1, total, '图1: 总市值/M2比值...')

    # ── 合并数据 ──
    df = pd.merge(df_calender, df_m2, on=['month'], how='outer')
    df = df.fillna(method='ffill')
    df = df[['month', 'm2', 'total_mv']]
    df.columns = ['trade_date', 'm2', 'total_mv']
    df.index = df['trade_date'].apply(lambda x: datetime.strptime(x, '%Y%m'))
    df = df.drop(['trade_date'], axis=1)

    # 获取上证指数
    df1 = pro.index_daily(ts_code='000001.SH', start_date='20000101',
                          fields='ts_code,trade_date,close,vol,amount')
    df1 = df1.sort_values(by=['trade_date']).reset_index(drop=True)
    df1.index = pd.to_datetime(df1['trade_date'])
    df1 = df1.drop(['trade_date', 'ts_code'], axis=1, errors='ignore')

    df = pd.merge(df1, df, on=['trade_date'], how='outer')
    df = df.sort_index()
    df['total_mv/m2'] = df['total_mv'] / df['m2']
    df['total_mv/m2_mean'] = df['total_mv/m2'].mean()
    df['total_mv/m2_mean+1std'] = df['total_mv/m2'].mean() + 0.5 * df['total_mv/m2'].std()
    df['total_mv/m2_mean-1std'] = df['total_mv/m2'].mean() - 0.4 * df['total_mv/m2'].std()
    df = df.fillna(method='ffill')

    cyb = df[df.index >= '2001-06-01']

    # ── 图1: 总市值/M2比值 ──
    fig1 = _plot_mv_m2(cyb, '上证指数市值/m2余额', '上证指数', '上证指数市值与m2余额')
    figures.append((fig1, '上证指数市值与M2余额'))

    if progress_callback:
        progress_callback(2, total, '图2: 上证指数PE...')

    # ── 图2: 上证指数PE ──
    try:
        szzs = pro.index_dailybasic(ts_code='000001.SH',
                                     fields='ts_code,trade_date,pe,pb')
        szzs = _mean_std(szzs)
        fig2 = _plot_ind(szzs, '上证指数PE', 'pe')
        figures.append((fig2, '上证指数PE'))
    except Exception as e:
        print(f'绘制上证指数PE失败: {e}')

    if progress_callback:
        progress_callback(3, total, '图3: 创业板PE...')

    # ── 图3: 创业板PE ──
    try:
        df_cyb = pro.index_dailybasic(ts_code='399006.SZ',
                                       fields='ts_code,trade_date,pe,pb')
        df_cyb = _mean_std(df_cyb)
        fig3 = _plot_ind(df_cyb, '创业板PE', 'pe')
        figures.append((fig3, '创业板PE'))
    except Exception as e:
        print(f'绘制创业板PE失败: {e}')

    # ── 图4: 沪深300PE ──
    try:
        df_hs300 = pro.index_dailybasic(ts_code='000300.SH',
                                         fields='ts_code,trade_date,pe,pb')
        df_hs300 = _mean_std(df_hs300)
        fig4 = _plot_ind(df_hs300, '沪深300PE', 'pe')
        figures.append((fig4, '沪深300PE'))
    except Exception as e:
        print(f'绘制沪深300PE失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
