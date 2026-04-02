"""
ind_02 - 美元黄金加息关系图
原始文件: 各种指标/美元指数…/美元黄金加息关系图.ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from .shared_utils import pro

try:
    import akshare as ak
    HAS_AK = True
except ImportError:
    HAS_AK = False

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False

INDICATOR_META = {
    'id': 'ind_02_usd_gold',
    'name': '美元-黄金-利率传导矩阵',
    'group': '宏观与估值指标',
    'description': '美元指数(DXY)、黄金价格与美联储利率政策的多维联动分析，涵盖经济指标综合面板、利率走廊结构、跨资产相关性矩阵，揭示全球宏观传导路径',
    'input_type': 'none',
    'default_code': '',
    'requires_db': False,
    'slow': False,
    'chart_count': 6,
    'chart_descriptions': [
        '美元指数(DXY)与黄金价格的长期走势对比，观察两者的负相关性及阶段性偏离',
        '美联储利率政策变化对美元指数的影响传导，加息周期中美元走强的典型模式',
        '美联储利率政策变化与黄金的联动，低利率环境利好黄金、加息压制金价的宏观逻辑',
        '经济指标综合面板：CPI、PPI、PMI等核心宏观指标走势，快速概览经济基本面',
        '美联储利率走廊结构图，展示政策利率、隔夜逆回购利率等关键利率的层次关系',
        '各经济指标之间的相关性矩阵热力图，识别宏观因子间的联动与分化',
    ],
}


def _plot_dual_with_bands(df, name1, name2, title):
    """双轴图：左轴name1(红色)，右轴name2(橙色)+均值/标准差"""
    fig = plt.figure(figsize=(20, 8), facecolor='white')
    ax1 = fig.add_subplot(111)
    ax1.plot(df.index, df[name1], label=name1, color='IndianRed', linewidth=4)
    ax1.legend(loc='upper right', fontsize=15)
    ax1.set_ylabel(name1, fontsize=20)
    ax1.tick_params(labelsize=15)

    ax2 = ax1.twinx()
    ax2.plot(df.index, df[name2], label=name2, color='orange', linewidth=2)
    mean_val = df[name2].mean()
    std_val = df[name2].std()
    ax2.axhline(mean_val + std_val, color='green', linestyle='--', linewidth=2)
    ax2.axhline(mean_val, color='red', linestyle='--')
    ax2.axhline(mean_val - std_val, color='green', linestyle='--', linewidth=2)
    ax2.legend(loc='upper left', fontsize=15)
    ax2.tick_params(labelsize=15)

    plt.title(title, fontsize=25)
    plt.grid(alpha=0.3)
    fig.tight_layout()
    return fig


def generate(start_date='20100101', progress_callback=None, **kwargs):
    figures = []
    total = 6

    if progress_callback:
        progress_callback(0, total, '获取美元指数与黄金数据...')

    from datetime import datetime
    today = datetime.strftime(datetime.now(), '%Y%m%d')

    # 获取美元指数
    us = pro.fx_daily(ts_code='USDOLLAR.FXCM', start_date='20010101', end_date=today)
    us = us[['ts_code', 'trade_date', 'bid_close']]
    us = us.sort_values(by=['trade_date']).reset_index(drop=True)

    # 获取黄金
    gold = pro.fx_daily(ts_code='XAUUSD.FXCM', start_date='20010101', end_date=today)
    gold = gold[['ts_code', 'trade_date', 'bid_close']]
    gold = gold.sort_values(by=['trade_date']).reset_index(drop=True)

    # 合并
    data = pd.merge(us, gold, on=['trade_date'])
    data.index = pd.to_datetime(data['trade_date'])
    data = data.rename(columns={'bid_close_x': '美元指数', 'bid_close_y': '黄金'})

    # 获取美联储利率
    if HAS_AK:
        try:
            us_rate_df = ak.macro_bank_usa_interest_rate()
            us_rate = pd.DataFrame(us_rate_df).reset_index(drop=True)
            if 'index' in us_rate.columns:
                us_rate = us_rate.rename(columns={'index': '日期'})
            us_rate = us_rate.sort_values(by=['日期']).reset_index(drop=True)
            us_rate = us_rate.rename(columns={'今值': 'usa_interest_rate'})
            us_rate['日期'] = pd.to_datetime(us_rate['日期'])
            us_rate.index = us_rate['日期']
            us_rate = us_rate[['usa_interest_rate']]
            us_rate['usa_interest_rate'] = pd.to_numeric(us_rate['usa_interest_rate'], errors='coerce')

            all_data = pd.merge(data[['美元指数', '黄金']], us_rate, left_index=True, right_index=True, how='outer')
            all_data = all_data.sort_index().ffill().dropna()
            all_data = all_data.loc[all_data.index >= '2001-01-03']
        except Exception:
            all_data = data[['美元指数', '黄金']].copy()
            all_data['usa_interest_rate'] = np.nan
    else:
        all_data = data[['美元指数', '黄金']].copy()
        all_data['usa_interest_rate'] = np.nan

    if progress_callback:
        progress_callback(1, total, '图1: 美元指数与黄金对比图...')

    # ── 图1: 美元指数与黄金 ──
    fig1 = _plot_dual_with_bands(all_data, '美元指数', '黄金', '美元指数与黄金对比图')
    figures.append((fig1, '美元指数与黄金对比图'))

    if progress_callback:
        progress_callback(2, total, '图2: 美联储加息与美元指数...')

    # ── 图2: 美联储加息与美元指数 ──
    fig2 = _plot_dual_with_bands(all_data, 'usa_interest_rate', '美元指数', '美联储加息与美元指数对比图')
    figures.append((fig2, '美联储加息与美元指数对比图'))

    if progress_callback:
        progress_callback(3, total, '图3: 美联储加息与黄金...')

    # ── 图3: 美联储加息与黄金 ──
    fig3 = _plot_dual_with_bands(all_data, 'usa_interest_rate', '黄金', '美联储加息与黄金对比图')
    figures.append((fig3, '美联储加息与黄金对比图'))

    if progress_callback:
        progress_callback(4, total, '图4: 经济指标6面板...')

    # ── 获取额外数据 ──
    # 人民币汇率
    df3 = pro.fx_daily(ts_code='USDCNH.FXCM', start_date=start_date)
    df5 = pro.index_daily(ts_code='000001.SH', start_date=start_date)
    df5 = df5.sort_values(by=['trade_date']).reset_index(drop=True)
    df3 = df3.sort_values(by=['trade_date']).reset_index(drop=True)
    data1 = pd.merge(df3, df5, on=['trade_date'])
    data1 = data1[['trade_date', 'bid_close', 'close']]
    data1.columns = ['日期', '汇率收盘价', '收盘价']
    data1.index = pd.to_datetime(data1['日期'])

    rmb_exchange_rate = data1[['汇率收盘价']]

    # SHIBOR
    shibor = pro.shibor(start_date=start_date)
    shibor['date'] = pd.to_datetime(shibor['date'])
    shibor = shibor.sort_values(by=['date'])
    shibor.index = shibor['date']

    # 国债收益率
    if HAS_AK:
        try:
            dd2 = ak.bond_zh_us_rate()
            dd2 = dd2[['日期', '中国国债收益率10年', '美国国债收益率10年']]
            dd2 = dd2.sort_values(by=['日期']).reset_index(drop=True)
            dd2 = dd2.ffill()
            dd2['日期'] = pd.to_datetime(dd2['日期'])
            dd2.index = dd2['日期']
        except Exception:
            dd2 = pd.DataFrame()
    else:
        dd2 = pd.DataFrame()

    # 美联储利率子集
    if HAS_AK and 'usa_interest_rate' in all_data.columns:
        usd_interest_rate = all_data[['usa_interest_rate']].rename(columns={'usa_interest_rate': '今值'})
    else:
        usd_interest_rate = pd.DataFrame({'今值': [np.nan]}, index=[pd.Timestamp('2020-01-01')])

    # 美元指数(yfinance)
    if HAS_YF:
        try:
            dxy = yf.Ticker("DX-Y.NYB")
            dollar_index = dxy.history(period="max")
            dollar_index.index = pd.to_datetime(dollar_index.index).tz_localize(None)
            dollar_index = dollar_index[['Close']].rename(columns={'Close': '美元指数'})
        except Exception:
            dollar_index = data[['美元指数']].copy()
    else:
        dollar_index = data[['美元指数']].copy()

    # ── 图4: 经济指标6面板 ──
    common_start = max(
        rmb_exchange_rate.index.min(),
        shibor.index.min(),
        usd_interest_rate.index.min(),
        dollar_index.index.min(),
    )
    if len(dd2) > 0:
        common_start = max(common_start, dd2.index.min())

    fig4, axs = plt.subplots(6, 1, figsize=(20, 42), sharex=True, facecolor='white')

    # 子图1: 美元利率
    usd_sub = usd_interest_rate[usd_interest_rate.index >= common_start]
    axs[0].plot(usd_sub.index, usd_sub['今值'], color='tab:blue', linewidth=2)
    axs[0].set_ylabel('美元加息', fontsize=20, color='tab:blue')
    axs[0].set_title('美元利率走势', fontsize=22)
    axs[0].grid(alpha=0.3)

    # 子图2: SHIBOR
    shibor_sub = shibor[shibor.index >= common_start]
    axs[1].plot(shibor_sub.index, shibor_sub['1y'], color='black', linewidth=2)
    axs[1].set_ylabel('shibor', fontsize=20)
    axs[1].set_title('shibor利率走势', fontsize=22)
    axs[1].grid(alpha=0.3)

    # 子图3: 人民币汇率
    rmb_sub = rmb_exchange_rate[rmb_exchange_rate.index >= common_start]
    axs[2].plot(rmb_sub.index, rmb_sub['汇率收盘价'], color='tab:green', linewidth=2)
    axs[2].set_ylabel('人民币汇率', fontsize=20, color='tab:green')
    axs[2].set_title('人民币汇率走势', fontsize=22)
    axs[2].grid(alpha=0.3)

    # 子图4: 中国10年国债
    if len(dd2) > 0:
        dd2_sub = dd2[dd2.index >= common_start]
        axs[3].plot(dd2_sub.index, dd2_sub['中国国债收益率10年'], color='tab:purple', linewidth=2)
    axs[3].set_ylabel('中国国债收益率', fontsize=20, color='tab:purple')
    axs[3].set_title('中国国债收益率10年', fontsize=22)
    axs[3].grid(alpha=0.3)

    # 子图5: 美国10年国债
    if len(dd2) > 0:
        axs[4].plot(dd2_sub.index, dd2_sub['美国国债收益率10年'], color='tab:brown', linewidth=2)
    axs[4].set_ylabel('美国国债收益率', fontsize=20, color='tab:brown')
    axs[4].set_title('美国国债收益率10年', fontsize=22)
    axs[4].grid(alpha=0.3)

    # 子图6: 上证指数
    data1_sub = data1[data1.index >= common_start]
    axs[5].plot(data1_sub.index, data1_sub['收盘价'], color='IndianRed', linewidth=2)
    axs[5].set_ylabel('上证指数', fontsize=20, color='IndianRed')
    axs[5].set_title('上证指数走势', fontsize=22)
    axs[5].grid(alpha=0.3)

    for ax in axs:
        ax.tick_params(axis='x', labelsize=14, rotation=45)
        ax.tick_params(axis='y', labelsize=14)

    fig4.suptitle('美元利率，黄金，人民币汇率，美元指数，国债收益率，上证指数', fontsize=20)
    fig4.tight_layout(rect=[0, 0, 1, 0.97])
    figures.append((fig4, '经济指标综合6面板'))

    if progress_callback:
        progress_callback(5, total, '图5: 利率走廊...')

    # ── 图5: 利率走廊 ──
    try:
        china_30y = ak.bond_zh_us_rate() if HAS_AK else pd.DataFrame()
        if len(china_30y) > 0:
            china_30y = china_30y[['日期', '中国国债收益率30年']].rename(
                columns={'日期': 'trade_date', '中国国债收益率30年': '30年期国债收益率'})
            china_30y['trade_date'] = pd.to_datetime(china_30y['trade_date'])

            lpr = pro.shibor_lpr(start_date='20180101', fields='*')
            lpr = lpr.rename(columns={'DATE': 'trade_date'})
            lpr['trade_date'] = pd.to_datetime(lpr['trade_date'])
            lpr = lpr.sort_values('trade_date')
            lpr.index = lpr['trade_date']
            lpr = lpr[['1Y', '5Y']].rename(columns={'1Y': '1年-LPR利率', '5Y': '5年-LPR利率'})

            shibor1 = pro.shibor(start_date='2010-01-01')
            shibor1 = shibor1.rename(columns={'date': 'trade_date'})
            shibor1 = shibor1.sort_values(by=['trade_date']).reset_index(drop=True)
            shibor1['trade_date'] = pd.to_datetime(shibor1['trade_date'])
            shibor1.index = shibor1['trade_date']
            shibor1 = shibor1.drop(['trade_date'], axis=1, errors='ignore')
            if 'on' in shibor1.columns:
                shibor1 = shibor1.drop(['on'], axis=1)

            rate_cols = [c for c in shibor1.columns if c not in ['trade_date']]
            shibor1[rate_cols] = shibor1[rate_cols].apply(pd.to_numeric, errors='coerce')
            shibor1['短期平均利率'] = shibor1[rate_cols].mean(axis=1, skipna=True)

            merged_rates = pd.merge(lpr, shibor1[['短期平均利率']], left_index=True, right_index=True, how='inner')
            merged_rates = pd.merge(merged_rates, china_30y.set_index('trade_date')[['30年期国债收益率']],
                                    left_index=True, right_index=True, how='inner')
            merged_rates = merged_rates.bfill()
            merged_rates = merged_rates[merged_rates.index >= '2018-01-01']

            fig5, ax = plt.subplots(figsize=(16, 10), facecolor='white')
            for col in merged_rates.columns:
                if col == '30年期国债收益率':
                    ax.plot(merged_rates.index, merged_rates[col], label=col, linewidth=3, color='brown')
                else:
                    ax.plot(merged_rates.index, merged_rates[col], label=col, linestyle='-', alpha=0.7)
            ax.set_title('利率走廊', fontsize=30)
            ax.set_xlabel('日期', fontsize=20)
            ax.set_ylabel('利率', fontsize=20)
            ax.legend(loc='best', fontsize=12)
            ax.tick_params(labelsize=16)
            ax.grid(alpha=0.3)
            fig5.tight_layout()
            figures.append((fig5, '利率走廊'))
        else:
            raise ValueError("无数据")
    except Exception as e:
        print(f'绘制利率走廊失败: {e}')

    if progress_callback:
        progress_callback(6, total, '图6: 相关性矩阵...')

    # ── 图6: 相关性矩阵 ──
    try:
        import seaborn as sns
        corr_data = pd.DataFrame()
        corr_data['Shibor_Rate'] = shibor['1y']
        corr_data['USD_Interest_Rate'] = usd_interest_rate['今值']
        corr_data['RMB_Exchange'] = rmb_exchange_rate['汇率收盘价']
        corr_data['USD_Index'] = dollar_index['美元指数']
        if len(dd2) > 0:
            corr_data['China_Bond_10Y'] = dd2['中国国债收益率10年']
            corr_data['US_Bond_10Y'] = dd2['美国国债收益率10年']
        corr_data['A_Index'] = data1['收盘价']
        corr_data['A_Return'] = data1['收盘价'].pct_change() * 100
        corr_data = corr_data.dropna()

        if len(corr_data) > 10:
            corr_matrix = corr_data.corr()
            fig6, ax = plt.subplots(figsize=(12, 8), facecolor='white')
            sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt='.2f',
                        linewidths=0.5, center=0, ax=ax)
            ax.set_title('经济指标相关性矩阵', fontsize=20)
            fig6.tight_layout()
            figures.append((fig6, '经济指标相关性矩阵'))
    except Exception as e:
        print(f'绘制相关性矩阵失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
