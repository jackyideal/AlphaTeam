"""
ind_04 - 市盈率倒数风险程度
原始文件: 各种指标/指数风险程度（市盈率倒数）/市盈率倒数---指数风险程度.ipynb
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
    'id': 'ind_04_pe_risk',
    'name': '盈利收益率风险溢价模型',
    'group': '宏观与估值指标',
    'description': '以市盈率倒数（盈利收益率E/P）与国债收益率差值构建的股债性价比模型，量化评估当前指数估值的相对风险水平，数值越低表示股市相对债市吸引力越弱',
    'input_type': 'none',
    'default_code': '',
    'requires_db': False,
    'slow': False,
    'chart_count': 3,
    'chart_descriptions': [
        '上证指数盈利收益率(E/P)与中国国债收益率差值走势，差值为负表示股市估值偏贵',
        '沪深300股债利差走势，蓝筹股估值相对债市的吸引力变化',
        '创业板股债利差走势，成长股估值风险的动态评估',
    ],
}


def _get_index(code, start_date):
    """获取指数PE和收盘价"""
    df1 = pro.index_dailybasic(ts_code=code, start_date=start_date,
                                fields='ts_code,trade_date,turnover_rate,pe')
    df2 = pro.index_daily(ts_code=code)
    df1 = df1[['ts_code', 'trade_date', 'pe']]
    df2 = df2[['ts_code', 'trade_date', 'close']]
    df3 = pd.merge(df1, df2, on=['ts_code', 'trade_date'])
    df3['市盈率倒数'] = 1 / df3['pe']
    df3.index = df3['trade_date']
    df3 = df3.sort_index()
    del df3['trade_date']
    df3 = df3.reset_index()
    df3['trade_date'] = df3['trade_date'].astype(str)
    return df3


def _get_valuation_with_bond():
    """获取上证估值与国债收益率差值"""
    dd1 = pro.index_dailybasic(ts_code='000001.SH')
    dd1 = dd1.sort_values(by=['trade_date']).reset_index(drop=True)
    dd1.index = pd.to_datetime(dd1['trade_date'])
    dd1['1/PE'] = 1 / dd1['pe_ttm']

    if HAS_AK:
        dd2 = ak.bond_zh_us_rate()
        dd2 = dd2[['日期', '中国国债收益率10年', '美国国债收益率10年',
                    '美国国债收益率10年-2年', '中国国债收益率10年-2年']]
        dd2 = dd2.sort_values(by=['日期']).reset_index(drop=True)
        dd2['trade_date'] = dd2['日期']
        dd2 = dd2.ffill()
        dd2.index = pd.to_datetime(dd2['日期'])

        dd = pd.concat([dd1['1/PE'], dd2['中国国债收益率10年'], dd2['美国国债收益率10年'],
                         dd2['美国国债收益率10年-2年'], dd2['中国国债收益率10年-2年']], axis=1)
        dd['上证估值与中国国债收益率差值'] = dd['1/PE'] - dd['中国国债收益率10年'] / 100
        dd['上证估值与美国国债收益率差值'] = dd['1/PE'] - dd['美国国债收益率10年'] / 100
        dd = dd[['1/PE', '上证估值与中国国债收益率差值', '上证估值与美国国债收益率差值',
                 '美国国债收益率10年-2年', '中国国债收益率10年-2年']].astype(float)
    else:
        dd = pd.DataFrame({'1/PE': dd1['1/PE']})
        dd['上证估值与中国国债收益率差值'] = dd['1/PE']
        dd['美国国债收益率10年-2年'] = np.nan
        dd['中国国债收益率10年-2年'] = np.nan

    return dd


def _plot_risk(df, title):
    """绘制估值风险图：收盘价 + 估值因子 + 均值/标准差"""
    df = df.copy()
    df['均值'] = df['估值ratio'].mean()
    df['均值-1倍标准差'] = df['估值ratio'].mean() - df['估值ratio'].std()
    df['均值+1倍标准差'] = df['估值ratio'].mean() + df['估值ratio'].std()
    df.index = pd.to_datetime(df['trade_date'])

    fig, ax1 = plt.subplots(figsize=(20, 10), facecolor='white')
    ax1.plot(df.index, df['close'], linewidth=2)
    ax1.set_ylabel(title, fontsize=25)
    ax1.tick_params(labelsize=14)

    ax2 = ax1.twinx()
    ax2.plot(df.index, df['估值ratio'], color='r', linewidth=1.5, label='估值因子')
    ax2.plot(df.index, df['均值'], color='g', linestyle='--', label='均值')
    ax2.plot(df.index, 0.95 * df['均值+1倍标准差'], color='r', linestyle='--', label='+1std')
    ax2.plot(df.index, 1.05 * df['均值-1倍标准差'], color='r', linestyle='--', label='-1std')
    ax2.set_ylabel('估值因子', fontsize=20)
    ax2.tick_params(labelsize=14)
    ax2.legend(loc='upper left', fontsize=14)

    plt.title(title + '估值变化', fontsize=30)
    plt.grid(alpha=0.3)
    fig.tight_layout()
    return fig


def generate(start_date='20010101', progress_callback=None, **kwargs):
    figures = []
    total = 3

    if progress_callback:
        progress_callback(0, total, '获取估值与国债收益率数据...')

    # 获取SHIBOR
    df_shibor = pro.shibor()
    df_shibor = df_shibor.rename(columns={'date': 'trade_date'})
    df_shibor = df_shibor.sort_values(by=['trade_date']).reset_index(drop=True)
    df_shibor['trade_date'] = df_shibor['trade_date'].astype(str)
    df_shibor = df_shibor[['trade_date', '6m']]
    df_shibor['trade_date'] = pd.to_datetime(df_shibor['trade_date'])

    # 获取估值与国债收益率
    rate = _get_valuation_with_bond()
    rate = rate.reset_index().ffill()
    rate = rate.rename(columns={'index': 'trade_date'})
    rate = pd.merge(rate, df_shibor, on=['trade_date'])
    rate = rate.rename(columns={'6m': 'shibor6m'})

    # 获取指数数据
    index_dict = {
        '上证综指': '000001.SH', '深证成指': '399001.SZ', '沪深300': '000300.SH',
        '创业板指': '399006.SZ', '上证50': '000016.SH', '中证500': '000905.SH',
    }

    df = _get_index(code='000001.SH', start_date=start_date)
    df['trade_date'] = pd.to_datetime(df['trade_date'])

    chart_configs = [
        ('上证估值与中国国债收益率差值', '上证综指(中国国债收益率差值)'),
        ('中国国债收益率10年-2年', '上证综指(中国国债10Y-2Y)'),
        ('美国国债收益率10年-2年', '上证综指(美国国债10Y-2Y)'),
    ]

    for idx, (ratio_name, chart_title) in enumerate(chart_configs):
        if progress_callback:
            progress_callback(idx, total, f'图{idx + 1}: {chart_title}...')

        try:
            all_data = pd.merge(df, rate, on=['trade_date'])
            all_data['估值ratio'] = all_data[ratio_name]
            fig = _plot_risk(all_data, chart_title)
            figures.append((fig, chart_title + '估值变化'))
        except Exception as e:
            print(f'绘制{chart_title}失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
