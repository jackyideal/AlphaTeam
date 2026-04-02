"""
ind_10 - SHIBOR/DR007利率
原始文件: 各种指标/shibor 和 dr007…/利率.ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .shared_utils import pro

try:
    import akshare as ak
    HAS_AK = True
except ImportError:
    HAS_AK = False

INDICATOR_META = {
    'id': 'ind_10_shibor',
    'name': 'SHIBOR/DR007流动性中枢',
    'group': '宏观与估值指标',
    'description': '上海银行间同业拆放利率(SHIBOR)与银行间质押式回购利率(DR007)的多维分析，展示利率走廊结构、期限利差变化及其对股市和个股的领先/滞后传导关系',
    'input_type': 'none',
    'default_code': '',
    'requires_db': False,
    'slow': False,
    'chart_count': 8,
    'chart_descriptions': [
        'SHIBOR各期限（隔夜/1周/1月/3月/6月/1年）利率走势全景图',
        'SHIBOR 1年期利率长期走势，反映中长期资金面松紧',
        'SHIBOR(1年)与上证指数的叠加走势，观察利率对大盘的领先效应',
        'SHIBOR(1月)与个股走势的联动关系',
        'DR007与上证指数走势，银行间流动性对股市的传导',
        'DR007与个股走势的关联分析',
        '利率走廊结构图：SLF/MLF/OMO等政策利率的层级关系',
        '利率波动极限区间分析，识别流动性紧张的极端时刻',
    ],
}


def _dual_rate_chart(rate_data, price_data, rate_label, price_label, title):
    """通用利率vs价格双轴图"""
    fig = plt.figure(figsize=(20, 8), facecolor='white')
    ax1 = fig.add_subplot(111)
    ax1.plot(rate_data.index, rate_data, color='red', linewidth=1.5)
    ax1.set_ylabel(rate_label, fontsize=20)
    ax1.legend([rate_label], loc='upper left', fontsize=15)
    ax1.tick_params(labelsize=14)

    ax2 = ax1.twinx()
    ax2.plot(price_data.index, price_data, color='black', linewidth=3)
    ax2.set_ylabel(price_label, fontsize=20)
    ax2.legend([price_label], loc='upper right', fontsize=15)
    ax2.tick_params(labelsize=14)

    plt.title(title, fontsize=25)
    plt.grid(alpha=0.3)
    fig.tight_layout()
    return fig


def generate(start_date='20100101', progress_callback=None, **kwargs):
    figures = []
    total = 8

    if progress_callback:
        progress_callback(0, total, '获取SHIBOR数据...')

    # ── 图1: SHIBOR多期利率 ──
    df_shibor = pro.shibor(start_date=start_date)
    df_shibor = df_shibor.sort_values(by=['date']).reset_index(drop=True)
    df_shibor.index = pd.to_datetime(df_shibor['date'])
    df_shibor = df_shibor.drop(['date'], axis=1)

    cols = ['3m', '6m', '9m', '1y']
    fig1, ax = plt.subplots(figsize=(20, 12), facecolor='white')
    for col in cols:
        if col in df_shibor.columns:
            ax.plot(df_shibor.index, df_shibor[col], label=col, linewidth=1.5)
    ax.set_title('Shibor 各期利率走势（%）', fontsize=25)
    ax.set_ylabel('Shibor 利率（%）', fontsize=20)
    ax.legend(loc='upper right', fontsize=15)
    ax.tick_params(labelsize=15)
    ax.grid(alpha=0.3)
    fig1.tight_layout()
    figures.append((fig1, 'Shibor各期利率走势'))

    if progress_callback:
        progress_callback(1, total, '图2: SHIBOR 1年期...')

    # ── 图2: SHIBOR 1年期 ──
    fig2, ax = plt.subplots(figsize=(20, 12), facecolor='white')
    if '1y' in df_shibor.columns:
        ax.plot(df_shibor.index, df_shibor['1y'], linewidth=1.5)
    ax.set_title('Shibor 1年期利率走势（%）', fontsize=25)
    ax.set_ylabel('Shibor 利率（%）', fontsize=20)
    ax.legend(['1y'], loc='upper right', fontsize=15)
    ax.tick_params(labelsize=15)
    ax.grid(alpha=0.3)
    fig2.tight_layout()
    figures.append((fig2, 'Shibor 1年期利率'))

    if progress_callback:
        progress_callback(2, total, '图3: SHIBOR与上证指数...')

    # ── 图3: SHIBOR(1y) vs 上证指数 ──
    df1 = pro.shibor(start_date='20160301')
    df1 = df1.sort_values(by=['date']).reset_index(drop=True)
    df1 = df1[['date', '1y']].rename(columns={'date': 'trade_date', '1y': 'rate'})
    df2 = pro.index_daily(ts_code='000001.SH')
    df2 = df2.sort_values(by=['trade_date']).reset_index(drop=True)
    df2 = df2[['trade_date', 'close']].rename(columns={'close': 'index_close'})
    df = pd.merge(df1, df2, on=['trade_date'])
    df.index = pd.to_datetime(df['trade_date'])

    fig3 = _dual_rate_chart(df['rate'], df['index_close'],
                            'Shibor（1y）', '上证指数', 'Shibor（1y）和上证指数')
    figures.append((fig3, 'Shibor(1y)与上证指数'))

    if progress_callback:
        progress_callback(3, total, '图4: SHIBOR与青松建化...')

    # ── 图4: SHIBOR(1m) vs 600425.SH ──
    df1 = pro.shibor(start_date='20150101')
    df1 = df1.sort_values(by=['date']).reset_index(drop=True)
    df1 = df1[['date', '1m']].rename(columns={'date': 'trade_date', '1m': 'rate'})
    df2 = pro.daily(ts_code='600425.SH')
    df2 = df2.sort_values(by=['trade_date']).reset_index(drop=True)
    df2 = df2[['trade_date', 'close']].rename(columns={'close': 'index_close'})
    df = pd.merge(df1, df2, on=['trade_date'])
    df.index = pd.to_datetime(df['trade_date'])

    fig4 = _dual_rate_chart(df['rate'], df['index_close'],
                            'Shibor（1m）', '青松建化', 'Shibor与青松建化')
    figures.append((fig4, 'Shibor(1m)与青松建化'))

    if progress_callback:
        progress_callback(4, total, '图5: DR007与上证指数...')

    # ── 图5: DR007 vs 上证指数 ──
    df1 = pro.repo_daily(ts_code='DR007.IB', fields='trade_date,close')
    df1 = df1.sort_values(by=['trade_date']).reset_index(drop=True)
    df1['close'] = df1['close'].rolling(45).mean()
    df1.columns = ['trade_date', 'rate']
    df2 = pro.index_daily(ts_code='000001.SH', start_date='20160101')
    df2 = df2.sort_values(by=['trade_date']).reset_index(drop=True)
    df2 = df2[['trade_date', 'close']].rename(columns={'close': 'index_close'})
    df = pd.merge(df1, df2, on=['trade_date'])
    df.index = pd.to_datetime(df['trade_date'])

    fig5 = _dual_rate_chart(df['rate'], df['index_close'],
                            'DR007（%）', '上证指数', 'DR007（%）和上证指数')
    figures.append((fig5, 'DR007与上证指数'))

    if progress_callback:
        progress_callback(5, total, '图6: DR007与青松建化...')

    # ── 图6: DR007 vs 600425.SH ──
    df1 = pro.repo_daily(ts_code='DR007.IB', fields='trade_date,close')
    df1 = df1.sort_values(by=['trade_date']).reset_index(drop=True)
    df1['close'] = df1['close'].rolling(60).mean()
    df1.columns = ['trade_date', 'rate']
    df2 = pro.daily(ts_code='600425.SH', start_date='20150101')
    df2 = df2.sort_values(by=['trade_date']).reset_index(drop=True)
    df2 = df2[['trade_date', 'close']].rename(columns={'close': 'index_close'})
    df = pd.merge(df1, df2, on=['trade_date'])
    df.index = pd.to_datetime(df['trade_date'])

    fig6 = _dual_rate_chart(df['rate'], df['index_close'],
                            'DR007', '青松建化', 'DR007与青松建化')
    figures.append((fig6, 'DR007与青松建化'))

    if progress_callback:
        progress_callback(6, total, '图7: 利率走廊...')

    # ── 图7: 利率走廊 ──
    try:
        if HAS_AK:
            china_bond = ak.bond_zh_us_rate()
            china_bond = china_bond[['日期', '中国国债收益率2年', '中国国债收益率5年',
                                     '中国国债收益率10年', '中国国债收益率30年']]
            china_bond.columns = ['trade_date', 'yield_2y', 'yield_5y', 'yield_10y', 'yield_30y']
            china_bond['trade_date'] = pd.to_datetime(china_bond['trade_date'])
            china_bond.index = china_bond['trade_date']
            china_bond = china_bond.drop(['trade_date'], axis=1)

            # 回购利率
            ts_codes = ['DR007.IB', 'DR021.IB', 'DR3M.IB', 'DR6M.IB', '204091.SH', '204182.SH']
            df_all = pd.DataFrame()
            for code in ts_codes:
                df_temp = pro.repo_daily(ts_code=code, fields='trade_date,ts_code,close')
                df_temp['close'] = df_temp['close'].rolling(60).mean()
                df_temp['trade_date'] = pd.to_datetime(df_temp['trade_date'])
                df_all = pd.concat([df_all, df_temp], ignore_index=True)

            df_pivot = df_all.pivot(index='trade_date', columns='ts_code', values='close')
            df_pivot = df_pivot.ffill()
            merged = pd.merge(china_bond, df_pivot, on='trade_date', how='inner')

            # LPR
            lpr = pro.shibor_lpr(start_date='20180101', fields='*')
            lpr = lpr.rename(columns={'DATE': 'trade_date'})
            lpr['trade_date'] = pd.to_datetime(lpr['trade_date'])
            lpr.index = lpr['trade_date']
            lpr = lpr.sort_index()
            lpr = lpr[['1Y', '5Y']]
            merged = pd.merge(lpr, merged, on='trade_date', how='inner')

            # SHIBOR
            shibor2 = pro.shibor(start_date='2000-01-01')
            shibor2 = shibor2.rename(columns={'date': 'trade_date'})
            shibor2 = shibor2.sort_values(by=['trade_date']).reset_index(drop=True)
            shibor2['trade_date'] = pd.to_datetime(shibor2['trade_date'])
            shibor2.index = shibor2['trade_date']
            shibor2 = shibor2.drop(['trade_date'], axis=1, errors='ignore')
            if 'on' in shibor2.columns:
                shibor2 = shibor2.drop(['on'], axis=1)
            shibor2 = shibor2.add_prefix('shibor_')
            merged = pd.merge(shibor2, merged, on='trade_date', how='inner')
            merged = merged.bfill()

            # 计算短期平均利率
            rate_cols = [c for c in merged.columns if c.startswith('shibor_') or c in
                         ['204091.SH', '204182.SH', 'DR007.IB', 'DR021.IB', 'DR3M.IB', 'DR6M.IB']]
            merged[rate_cols] = merged[rate_cols].apply(pd.to_numeric, errors='coerce')
            merged['short_term_rate_avg'] = merged[rate_cols].mean(axis=1, skipna=True)

            display_cols = ['1Y', '5Y', 'yield_2y', 'yield_5y', 'yield_10y', 'yield_30y', 'short_term_rate_avg']
            df_display = merged[display_cols].copy()

            fig7, ax = plt.subplots(figsize=(20, 12), facecolor='white')
            for col in df_display.columns:
                if col == 'short_term_rate_avg':
                    ax.plot(df_display.index, df_display[col], label='短期平均利率', linewidth=3, color='brown')
                else:
                    ax.plot(df_display.index, df_display[col], label=col, linestyle='-', alpha=0.7)
            ax.set_title('利率走廊', fontsize=30)
            ax.set_xlabel('日期', fontsize=20)
            ax.set_ylabel('利率', fontsize=20)
            ax.legend(loc='best', fontsize=12)
            ax.tick_params(labelsize=15)
            ax.grid(alpha=0.3)
            fig7.tight_layout()
            figures.append((fig7, '利率走廊'))

            if progress_callback:
                progress_callback(7, total, '图8: 利率波动极限...')

            # ── 图8: 利率波动极限区间 ──
            df_display['gap'] = df_display['yield_30y'] - df_display['short_term_rate_avg']
            fig8, ax = plt.subplots(figsize=(20, 12), facecolor='white')
            ax.plot(df_display.index, df_display['gap'], color='red', linewidth=1.5)
            ax.set_title('利率波动极限区间（30Y国债 - 短期平均利率）', fontsize=25)
            ax.set_xlabel('日期', fontsize=20)
            ax.set_ylabel('利差', fontsize=20)
            ax.tick_params(labelsize=15)
            ax.grid(alpha=0.3)
            fig8.tight_layout()
            figures.append((fig8, '利率波动极限区间'))
        else:
            print('akshare未安装，跳过利率走廊图表')
    except Exception as e:
        print(f'绘制利率走廊失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
