"""
ind_26 - 市场波动率指标（世界不确定性）
原始参考: 各种指标2/世界不确定性指标/世界不确定性指标.ipynb
"""
import matplotlib.pyplot as plt
import pandas as pd

from .shared_utils import pro


INDICATOR_META = {
    'id': 'ind_26_market_volatility',
    'name': '市场波动率状态监测',
    'group': '宏观与估值指标',
    'description': '用中美核心指数波动率构建世界不确定性指数，跟踪全球风险偏好变化及其对A股的传导。',
    'input_type': 'none',
    'default_code': '',
    'requires_db': False,
    'slow': False,
    'chart_count': 4,
    'chart_descriptions': [
        '上证指数与世界不确定性指数（60日滚动波动率合成）双轴对比。',
        '上证指数与上证自身波动率对比，识别本土波动扩张阶段。',
        '创业板指数与国内不确定性指数（上证+创业板波动）对比。',
        '上证/道琼斯/纳斯达克波动率成分拆解，观察全球风险来源。',
    ],
}


def _fetch_cn_index(ts_code, start_date, name):
    df = pro.index_daily(ts_code=ts_code, start_date=start_date)
    if df is None or df.empty:
        return pd.DataFrame(columns=['trade_date', name, name + '涨跌幅'])
    df = df[['trade_date', 'close', 'pct_chg']].copy()
    df.columns = ['trade_date', name, name + '涨跌幅']
    return df


def _fetch_global_index(ts_code, start_date, name):
    df = pro.index_global(ts_code=ts_code, start_date=start_date)
    if df is None or df.empty:
        return pd.DataFrame(columns=['trade_date', name, name + '涨跌幅'])
    swing_col = 'swing' if 'swing' in df.columns else 'pct_chg'
    df = df[['trade_date', 'close', swing_col]].copy()
    df.columns = ['trade_date', name, name + '涨跌幅']
    return df


def generate(start_date='20150101', progress_callback=None, **kwargs):
    total = 4

    if progress_callback:
        progress_callback(0, total, '加载中美核心指数数据...')

    szzs = _fetch_cn_index('000001.SH', start_date, '上证指数')
    cyb = _fetch_cn_index('399006.SZ', start_date, '创业板指数')
    dji = _fetch_global_index('DJI', start_date, '道琼斯工业指数')
    ixic = _fetch_global_index('IXIC', start_date, '纳斯达克指数')

    data = szzs.merge(cyb, on='trade_date', how='outer')
    data = data.merge(dji, on='trade_date', how='outer')
    data = data.merge(ixic, on='trade_date', how='outer')
    data['trade_date'] = pd.to_datetime(data['trade_date'])
    data = data.sort_values('trade_date').reset_index(drop=True)
    data = data.set_index('trade_date')
    data = data.ffill().dropna(subset=['上证指数'])
    if data.empty:
        raise ValueError('市场波动率数据为空，请调整起始日期')

    for col in ['上证指数涨跌幅', '创业板指数涨跌幅', '道琼斯工业指数涨跌幅', '纳斯达克指数涨跌幅']:
        data[col] = pd.to_numeric(data[col], errors='coerce')

    data['上证指数波动率'] = data['上证指数涨跌幅'].rolling(window=60, min_periods=20).std()
    data['创业板指数波动率'] = data['创业板指数涨跌幅'].rolling(window=60, min_periods=20).std()
    data['道琼斯工业指数波动率'] = data['道琼斯工业指数涨跌幅'].rolling(window=60, min_periods=20).std()
    data['纳斯达克指数波动率'] = data['纳斯达克指数涨跌幅'].rolling(window=60, min_periods=20).std()

    data['世界不确定性指数'] = (
        data['上证指数波动率'] + data['道琼斯工业指数波动率'] + data['纳斯达克指数波动率']
    )
    data['国内不确定性指数'] = data['上证指数波动率'] + data['创业板指数波动率']

    figures = []

    if progress_callback:
        progress_callback(1, total, '图1: 世界不确定性指数...')

    fig1, ax1 = plt.subplots(figsize=(16, 8), facecolor='white')
    ax1.plot(data.index, data['上证指数'], color='#1d4ed8', linewidth=2, label='上证指数')
    ax1.set_ylabel('上证指数', fontsize=14)
    ax1.tick_params(axis='y', labelcolor='#1d4ed8')
    ax1.grid(alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(data.index, data['世界不确定性指数'], color='#dc2626', linewidth=2, label='世界不确定性指数')
    ax2.set_ylabel('世界不确定性指数', fontsize=14)
    ax2.tick_params(axis='y', labelcolor='#dc2626')

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=11)
    ax1.set_title('上证指数与世界不确定性指数', fontsize=18)
    fig1.tight_layout()
    figures.append((fig1, '上证指数与世界不确定性指数'))

    if progress_callback:
        progress_callback(2, total, '图2: 上证波动率...')

    fig2, ax1 = plt.subplots(figsize=(16, 8), facecolor='white')
    ax1.plot(data.index, data['上证指数'], color='#1e40af', linewidth=2, label='上证指数')
    ax1.set_ylabel('上证指数', fontsize=14)
    ax1.tick_params(axis='y', labelcolor='#1e40af')
    ax1.grid(alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(data.index, data['上证指数波动率'], color='#b91c1c', linewidth=2, label='上证指数波动率')
    ax2.set_ylabel('波动率', fontsize=14)
    ax2.tick_params(axis='y', labelcolor='#b91c1c')

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=11)
    ax1.set_title('上证指数与上证波动率', fontsize=18)
    fig2.tight_layout()
    figures.append((fig2, '上证指数与上证波动率'))

    if progress_callback:
        progress_callback(3, total, '图3: 国内不确定性...')

    fig3, ax1 = plt.subplots(figsize=(16, 8), facecolor='white')
    ax1.plot(data.index, data['创业板指数'], color='#0f766e', linewidth=2, label='创业板指数')
    ax1.set_ylabel('创业板指数', fontsize=14)
    ax1.tick_params(axis='y', labelcolor='#0f766e')
    ax1.grid(alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(data.index, data['国内不确定性指数'], color='#ea580c', linewidth=2, label='国内不确定性指数')
    ax2.set_ylabel('国内不确定性指数', fontsize=14)
    ax2.tick_params(axis='y', labelcolor='#ea580c')

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=11)
    ax1.set_title('创业板指数与国内不确定性指数', fontsize=18)
    fig3.tight_layout()
    figures.append((fig3, '创业板指数与国内不确定性指数'))

    if progress_callback:
        progress_callback(4, total, '图4: 波动率成分拆解...')

    fig4, ax = plt.subplots(figsize=(16, 8), facecolor='white')
    ax.plot(data.index, data['上证指数波动率'], linewidth=2, label='上证波动率')
    ax.plot(data.index, data['道琼斯工业指数波动率'], linewidth=2, label='道琼斯波动率')
    ax.plot(data.index, data['纳斯达克指数波动率'], linewidth=2, label='纳斯达克波动率')
    ax.set_ylabel('60日滚动波动率', fontsize=14)
    ax.set_title('世界不确定性指数成分拆解', fontsize=18)
    ax.legend(loc='upper left', fontsize=11)
    ax.grid(alpha=0.3)
    fig4.tight_layout()
    figures.append((fig4, '世界不确定性指数成分拆解'))

    return figures
