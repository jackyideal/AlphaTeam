"""
ind_25 - 融资融券指标
原始参考: 各种指标/融资融券与股价:指数关系/融资融券（指数与个股）.ipynb
"""
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .market_heat_shared import get_stock_count_df
from .shared_utils import pro


INDICATOR_META = {
    'id': 'ind_25_margin_financing',
    'name': '融资融券杠杆景气度',
    'group': '资金面指标',
    'description': '跟踪全市场融资净买入强度、融资融券余额变化及个股融资行为，与指数/股价联动观察杠杆资金风险偏好。',
    'input_type': 'stock',
    'default_code': '600425.SH',
    'requires_db': False,
    'slow': False,
    'chart_count': 6,
    'chart_descriptions': [
        '全市场滚动净融资买入（按股票数量标准化）与上证指数走势。',
        '融资余额/融券余额增速Z分数与上证指数，刻画杠杆资金风险偏好变化。',
        '个股融资余额与股价双轴走势。',
        '个股20日净融资流入与股价双轴走势。',
        '个股“融资融比”(融资余额/融券余额)增速与股价对比，观察杠杆结构变化。',
        '个股累计融资净买入额（52日滚动）与股价关系图，刻画中期杠杆资金方向。',
    ],
}


def _safe_pct_zscore(series, smooth=10, lookback=120):
    """
    对“余额增速”做稳健标准化：
    1) pct_change 后清理 inf/-inf（余额从0跳变会产生极值）
    2) 1%~99% 分位截尾，降低异常点主导
    3) 使用滚动窗口 z-score，避免全样本均值/方差失真
    """
    s = pd.to_numeric(series, errors='coerce')
    s = s.replace([np.inf, -np.inf], np.nan)

    ret = s.pct_change(fill_method=None)
    ret = ret.replace([np.inf, -np.inf], np.nan)

    valid = ret.dropna()
    if not valid.empty:
        lo = valid.quantile(0.01)
        hi = valid.quantile(0.99)
        if pd.notna(lo) and pd.notna(hi) and lo < hi:
            ret = ret.clip(lo, hi)

    roll_mean = ret.rolling(lookback, min_periods=max(20, lookback // 6)).mean()
    roll_std = ret.rolling(lookback, min_periods=max(20, lookback // 6)).std(ddof=0)
    roll_std = roll_std.replace(0, np.nan)
    z = (ret - roll_mean) / roll_std
    z = z.replace([np.inf, -np.inf], np.nan)
    return z.rolling(smooth, min_periods=max(2, smooth // 3)).mean()


def _fetch_market_data(start_date):
    margin = pro.margin(start_date=start_date)
    if margin is None or margin.empty:
        raise ValueError('未获取到融资融券市场数据')

    margin['trade_date'] = pd.to_datetime(margin['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
    margin = margin.dropna(subset=['trade_date'])
    margin = margin.sort_values('trade_date')
    grouped = margin.groupby('trade_date', as_index=False).sum(numeric_only=True)
    grouped = grouped.sort_values('trade_date')
    for col in ['rzmre', 'rzche', 'rzye', 'rqye']:
        if col not in grouped.columns:
            grouped[col] = np.nan
        grouped[col] = pd.to_numeric(grouped[col], errors='coerce')
    grouped[['rzmre', 'rzche']] = grouped[['rzmre', 'rzche']].fillna(0.0)
    grouped[['rzye', 'rqye']] = grouped[['rzye', 'rqye']].ffill().bfill()

    grouped['net_rz'] = grouped['rzmre'] - grouped['rzche']
    grouped['cum_net_rz_52'] = grouped['net_rz'].rolling(52, min_periods=10).sum()

    stock_count = get_stock_count_df(start_date=start_date, refresh=False)
    stock_count['trade_date'] = pd.to_datetime(stock_count['trade_date'])
    grouped = grouped.merge(stock_count, on='trade_date', how='left')
    grouped['stock_count'] = grouped['stock_count'].replace(0, np.nan).ffill().bfill()
    grouped['cum_net_rz_per_stock'] = grouped['cum_net_rz_52'] / grouped['stock_count']
    grouped['cum_net_rz_per_stock'] = grouped['cum_net_rz_per_stock'] / 1e4  # 元/家 -> 万元/家

    idx = pro.index_daily(ts_code='000001.SH', start_date=start_date)
    idx['trade_date'] = pd.to_datetime(idx['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
    idx = idx.dropna(subset=['trade_date'])
    idx = idx.sort_values('trade_date')[['trade_date', 'close']].rename(columns={'close': '上证指数'})

    merged = grouped.merge(idx, on='trade_date', how='inner').sort_values('trade_date')
    return merged


def _fetch_stock_data(ts_code, start_date):
    stock = pro.daily(ts_code=ts_code, start_date=start_date)
    margin = pro.margin_detail(ts_code=ts_code, start_date=start_date)
    if stock is None or stock.empty or margin is None or margin.empty:
        return None

    stock = stock[['trade_date', 'close']].copy()
    stock['trade_date'] = pd.to_datetime(stock['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
    stock['close'] = pd.to_numeric(stock['close'], errors='coerce')
    stock = stock.dropna(subset=['trade_date']).drop_duplicates(subset=['trade_date'], keep='last')

    margin_cols = [c for c in ['trade_date', 'rzye', 'rqye', 'rzmre', 'rzche'] if c in margin.columns]
    margin = margin[margin_cols].copy()
    margin['trade_date'] = pd.to_datetime(margin['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
    for col in ['rzye', 'rqye', 'rzmre', 'rzche']:
        if col not in margin.columns:
            margin[col] = 0.0
        margin[col] = pd.to_numeric(margin[col], errors='coerce')
    margin = margin.dropna(subset=['trade_date']).drop_duplicates(subset=['trade_date'], keep='last')

    # 以个股交易日为主轴，避免因为融资明细缺失交易日导致时间轴出现大段空档
    merged = stock.merge(margin, on='trade_date', how='left')
    if merged.empty:
        return None

    merged = merged.sort_values('trade_date')
    merged[['rzmre', 'rzche']] = merged[['rzmre', 'rzche']].fillna(0.0)
    merged[['rzye', 'rqye']] = merged[['rzye', 'rqye']].ffill().bfill()
    merged['net_rz_20'] = (merged['rzmre'] - merged['rzche']).rolling(20, min_periods=5).sum() / 1e8
    merged['cum_net_rz_52'] = (merged['rzmre'] - merged['rzche']).rolling(52, min_periods=10).sum() / 1e8
    merged['rzrq_ratio'] = merged['rzye'] / merged['rqye'].replace(0, np.nan)
    ratio_growth = merged['rzrq_ratio'].pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan)
    valid = ratio_growth.dropna()
    if not valid.empty:
        lo = valid.quantile(0.01)
        hi = valid.quantile(0.99)
        if pd.notna(lo) and pd.notna(hi) and lo < hi:
            ratio_growth = ratio_growth.clip(lo, hi)
    merged['rzrq_ratio_growth_20'] = ratio_growth.rolling(20, min_periods=5).mean()
    merged['rzye_yi'] = merged['rzye'] / 1e8
    return merged


def generate(ts_code='600425.SH', start_date='20170101', progress_callback=None, **kwargs):
    total = 6
    figures = []

    if progress_callback:
        progress_callback(0, total, '加载融资融券市场数据...')

    market = _fetch_market_data(start_date)
    if market.empty:
        raise ValueError('融资融券市场数据为空，请调整起始日期')

    if progress_callback:
        progress_callback(1, total, '图1: 全市场净融资强度...')

    fig1, ax1 = plt.subplots(figsize=(18, 8), facecolor='white')
    ax1.plot(market['trade_date'], market['cum_net_rz_per_stock'],
             color='#dc2626', linewidth=2, label='滚动净融资买入(万元/家)')
    ax1.axhline(market['cum_net_rz_per_stock'].mean(), color='gray', linestyle='--', linewidth=1, label='均值')
    ax1.set_ylabel('万元/家', fontsize=14)
    ax1.legend(loc='upper left', fontsize=11)
    ax1.grid(alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(market['trade_date'], market['上证指数'], color='#1d4ed8', linewidth=2, label='上证指数')
    ax2.set_ylabel('上证指数', fontsize=14)
    ax2.legend(loc='upper right', fontsize=11)
    ax1.set_title('全市场净融资买入强度与上证指数', fontsize=18)
    fig1.tight_layout()
    figures.append((fig1, '全市场净融资买入强度与上证指数'))

    if progress_callback:
        progress_callback(2, total, '图2: 融资融券余额增速...')

    market['rzye_z'] = _safe_pct_zscore(market['rzye'])
    market['rqye_z'] = _safe_pct_zscore(market['rqye'])
    market['rzye_z_smooth'] = market['rzye_z'].rolling(5, min_periods=1).mean()
    market['rqye_z_smooth'] = market['rqye_z'].rolling(5, min_periods=1).mean()
    first_close = market['上证指数'].iloc[0] if len(market) else np.nan
    if pd.notnull(first_close) and first_close != 0:
        market['上证指数基准化'] = market['上证指数'] / first_close * 100.0
    else:
        market['上证指数基准化'] = np.nan

    fig2, ax1 = plt.subplots(figsize=(18, 8), facecolor='white')
    ax1.set_facecolor('#f8fbff')
    ax1.plot(
        market['trade_date'], market['rzye_z'],
        color='#7dd3fc', linewidth=1.0, alpha=0.35
    )
    ax1.plot(
        market['trade_date'], market['rqye_z'],
        color='#fdba74', linewidth=1.0, alpha=0.35
    )
    ax1.plot(
        market['trade_date'], market['rzye_z_smooth'],
        color='#0284c7', linewidth=2.6, label='融资余额增速滚动Z分数(120日,5日平滑)'
    )
    ax1.plot(
        market['trade_date'], market['rqye_z_smooth'],
        color='#ea580c', linewidth=2.4, linestyle='--', label='融券余额增速滚动Z分数(120日,5日平滑)'
    )
    ax1.axhline(0, color='#94a3b8', linestyle='--', linewidth=1.2)
    ax1.set_ylabel('Z-Score', fontsize=14)
    ax1.tick_params(axis='y', colors='#0f172a')
    ax1.legend(loc='upper left', fontsize=11)
    ax1.grid(alpha=0.28, linestyle='--', linewidth=0.8)
    ax1.spines['top'].set_visible(False)

    ax2 = ax1.twinx()
    ax2.plot(
        market['trade_date'], market['上证指数基准化'],
        color='#7c3aed', linewidth=2.5, alpha=0.92, label='上证指数(基准化=100)'
    )
    ax2.set_ylabel('上证指数(基准化)', fontsize=14)
    ax2.tick_params(axis='y', colors='#581c87')
    ax2.spines['top'].set_visible(False)
    ax2.legend(loc='upper right', fontsize=11)
    ax1.set_title('融资融券余额增速与上证指数', fontsize=18)
    fig2.tight_layout()
    figures.append((fig2, '融资融券余额增速与上证指数'))

    if progress_callback:
        progress_callback(3, total, '图3-4: 个股融资行为...')

    stock = _fetch_stock_data(ts_code, start_date)
    if stock is not None and not stock.empty:
        fig3, ax1 = plt.subplots(figsize=(18, 8), facecolor='white')
        ax1.plot(stock['trade_date'], stock['rzye_yi'], color='#b91c1c', linewidth=2, label='融资余额(亿元)')
        ax1.set_ylabel('融资余额(亿元)', fontsize=14)
        ax1.legend(loc='upper left', fontsize=11)
        ax1.grid(alpha=0.3)

        ax2 = ax1.twinx()
        ax2.plot(stock['trade_date'], stock['close'], color='#1e3a8a', linewidth=2, label='股价')
        ax2.set_ylabel('股价', fontsize=14)
        ax2.legend(loc='upper right', fontsize=11)
        ax1.set_title('%s 融资余额与股价' % ts_code, fontsize=18)
        fig3.tight_layout()
        figures.append((fig3, '%s 融资余额与股价' % ts_code))

        fig4, ax1 = plt.subplots(figsize=(18, 8), facecolor='white')
        ax1.plot(stock['trade_date'], stock['net_rz_20'], color='#d97706', linewidth=2, label='20日净融资流入(亿元)')
        ax1.axhline(stock['net_rz_20'].mean(), color='gray', linestyle='--', linewidth=1, label='均值')
        ax1.set_ylabel('亿元', fontsize=14)
        ax1.legend(loc='upper left', fontsize=11)
        ax1.grid(alpha=0.3)

        ax2 = ax1.twinx()
        ax2.plot(stock['trade_date'], stock['close'], color='#1d4ed8', linewidth=2, label='股价')
        ax2.set_ylabel('股价', fontsize=14)
        ax2.legend(loc='upper right', fontsize=11)
        ax1.set_title('%s 20日净融资流入与股价' % ts_code, fontsize=18)
        fig4.tight_layout()
        figures.append((fig4, '%s 20日净融资流入与股价' % ts_code))

        if progress_callback:
            progress_callback(4, total, '图5: 股价与融资融比增速...')

        fig5, ax1 = plt.subplots(figsize=(18, 8), facecolor='white')
        ax1.plot(
            stock['trade_date'], stock['rzrq_ratio_growth_20'] * 100.0,
            color='#a21caf', linewidth=2, label='融资融比增速(20日平滑, %)'
        )
        ax1.axhline(0, color='gray', linestyle='--', linewidth=1)
        ax1.set_ylabel('融资融比增速(%)', fontsize=14)
        ax1.legend(loc='upper left', fontsize=11)
        ax1.grid(alpha=0.3)

        ax2 = ax1.twinx()
        ax2.plot(stock['trade_date'], stock['close'], color='#1d4ed8', linewidth=2, label='股价')
        ax2.set_ylabel('股价', fontsize=14)
        ax2.legend(loc='upper right', fontsize=11)
        ax1.set_title('%s 股价与融资融比增速对比图' % ts_code, fontsize=18)
        fig5.tight_layout()
        figures.append((fig5, '%s 股价与融资融比增速对比图' % ts_code))

        if progress_callback:
            progress_callback(5, total, '图6: 股价与累计融资净买入额...')

        fig6, ax1 = plt.subplots(figsize=(18, 8), facecolor='white')
        ax1.plot(
            stock['trade_date'], stock['cum_net_rz_52'],
            color='#b45309', linewidth=2, label='累计融资净买入额(52日滚动, 亿元)'
        )
        ax1.axhline(stock['cum_net_rz_52'].mean(), color='gray', linestyle='--', linewidth=1, label='均值')
        ax1.set_ylabel('亿元', fontsize=14)
        ax1.legend(loc='upper left', fontsize=11)
        ax1.grid(alpha=0.3)

        ax2 = ax1.twinx()
        ax2.plot(stock['trade_date'], stock['close'], color='#1e3a8a', linewidth=2, label='股价')
        ax2.set_ylabel('股价', fontsize=14)
        ax2.legend(loc='upper right', fontsize=11)
        ax1.set_title('%s 股价与累计融资净买入额关系图' % ts_code, fontsize=18)
        fig6.tight_layout()
        figures.append((fig6, '%s 股价与累计融资净买入额关系图' % ts_code))
    else:
        # 回退：若个股融资数据缺失，仍保持图表数量
        fig3, ax = plt.subplots(figsize=(12, 5), facecolor='white')
        ax.text(0.5, 0.5, '%s 暂无可用融资明细数据' % ts_code,
                ha='center', va='center', fontsize=14)
        ax.axis('off')
        figures.append((fig3, '%s 融资明细数据缺失' % ts_code))

        fig4, ax = plt.subplots(figsize=(12, 5), facecolor='white')
        ax.text(0.5, 0.5, '请更换股票代码或调整起始日期',
                ha='center', va='center', fontsize=14)
        ax.axis('off')
        figures.append((fig4, '个股融资分析提示'))

        fig5, ax = plt.subplots(figsize=(12, 5), facecolor='white')
        ax.text(0.5, 0.5, '无法生成“股价与融资融比增速对比图”',
                ha='center', va='center', fontsize=14)
        ax.axis('off')
        figures.append((fig5, '融资融比增速图缺失提示'))

        fig6, ax = plt.subplots(figsize=(12, 5), facecolor='white')
        ax.text(0.5, 0.5, '无法生成“股价与累计融资净买入额关系图”',
                ha='center', va='center', fontsize=14)
        ax.axis('off')
        figures.append((fig6, '累计融资净买入额图缺失提示'))

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
