"""
ind_15 - 巨无霸企业占比
原始文件: 各种指标2/平均价格指数 巨无霸企业/巨无霸企业占比指数.ipynb
"""
import os
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .shared_utils import pro
from .db_utils import get_data_by_sql, get_pivot_data, get_file_path
from ..config import DB_ROOT

INDICATOR_META = {
    'id': 'ind_15_mega_cap',
    'name': '超大市值集中度指标',
    'group': '市场结构指标',
    'description': '对齐“平均价格指数/等权价格指数”与“巨无霸企业占比指数”两套口径，联合展示等权-市值加权、大小盘风格、指数及个股对照的集中度演化。',
    'input_type': 'none',
    'default_code': '',
    'requires_db': True,
    'slow': True,
    'chart_count': 4,
    'chart_descriptions': [
        '参考“平均价格指数.ipynb”：青松建化、简单平均、市值加权、创业板、上证指数累计收益对比',
        '参考“巨无霸企业占比指数.ipynb”：小盘(5%分位) / 大盘(90%分位) 与指数及个股累计收益对比',
        '参考“巨无霸企业占比指数.ipynb”：大盘-小盘差值、均值与±1σ阈值，并给出风格切换信号',
        '参考“平均价格指数.ipynb”：市值加权与简单平均的累计差值（集中度扩张/收敛）',
    ],
}


def _normalize_date_str(start_date):
    s = str(start_date)
    return s.replace('-', '')


def _to_datetime_start(start_date):
    s = _normalize_date_str(start_date)
    return pd.to_datetime(s, format='%Y%m%d', errors='coerce')


def _cumprod_norm(series):
    s = pd.to_numeric(series, errors='coerce').fillna(0.0)
    c = (1.0 + s).cumprod()
    if len(c) == 0:
        return c
    first = c.iloc[0]
    if pd.isna(first) or first == 0:
        return c
    return c / first


def _safe_index_pct(ts_code, name, start_date):
    try:
        df = pro.index_daily(ts_code=ts_code, start_date=start_date)
        if df is None or df.empty:
            return pd.Series(dtype=float, name=name)
        df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df = df.dropna(subset=['trade_date']).sort_values('trade_date')
        s = df.set_index('trade_date')['close'].pct_change(fill_method=None)
        s.name = name
        return s
    except Exception:
        return pd.Series(dtype=float, name=name)


def _get_all_codes():
    # 优先走 Tushare，失败时回退到本地数据库
    try:
        codess = pro.query(
            'stock_basic', exchange='SSE', list_status='L',
            fields='ts_code,symbol,name,area,industry,list_date'
        )
        codess1 = pro.query(
            'stock_basic', exchange='SZSE', list_status='L',
            fields='ts_code,symbol,name,area,industry,list_date'
        )
        codes = sorted(set(codess['ts_code']).union(set(codess1['ts_code'])))
        if codes:
            return codes
    except Exception:
        pass

    db_path = os.path.join(DB_ROOT, 'dailybasic.db')
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql('select distinct ts_code from dailybasic', conn)
    return sorted(df['ts_code'].dropna().astype(str).unique().tolist())


def generate(start_date='20240101', progress_callback=None, **kwargs):
    figures = []
    total = 4
    file_path = get_file_path()
    start_dt = _to_datetime_start(start_date)
    start_date_ymd = _normalize_date_str(start_date)
    if pd.isna(start_dt):
        raise ValueError(f'无效起始日期: {start_date}')

    if progress_callback:
        progress_callback(0, total, '加载股票池与数据库数据...')

    codes = _get_all_codes()
    if not codes:
        raise ValueError('未获取到股票代码列表')

    # 参考“平均价格指数.ipynb”：复权收益 + 市值权重
    df_adj = get_data_by_sql(
        file_path, 'daily_adj', 'daily_adj', codes,
        'trade_date,ts_code,adj_factor', start_date=start_date_ymd
    )
    df_kline = get_data_by_sql(
        file_path, 'daily_kline', 'daily_kline', codes,
        'trade_date,ts_code,close', start_date=start_date_ymd
    )
    df_dailybasic = get_data_by_sql(
        file_path, 'dailybasic', 'dailybasic', codes,
        'ts_code,trade_date,close,total_mv', start_date=start_date_ymd
    )

    if df_dailybasic.empty:
        raise ValueError('dailybasic 数据为空，请检查数据库更新')
    if df_adj.empty or df_kline.empty:
        raise ValueError('daily_adj 或 daily_kline 数据为空，请检查数据库更新')

    df_adj_pivot = get_pivot_data(df_adj, 'adj_factor').sort_index()
    df_close_raw = get_pivot_data(df_kline, 'close').sort_index()
    mv = get_pivot_data(df_dailybasic, 'total_mv').sort_index()

    common_codes = sorted(set(df_adj_pivot.columns) & set(df_close_raw.columns) & set(mv.columns))
    if not common_codes:
        raise ValueError('无可用共同股票代码，无法计算指标')

    df_adj_pivot = df_adj_pivot[common_codes].ffill()
    df_close_raw = df_close_raw[common_codes]
    mv = mv[common_codes]

    base_adj = df_adj_pivot.iloc[-1].replace(0, np.nan)
    df_close_adj = (df_close_raw * df_adj_pivot / base_adj).replace([np.inf, -np.inf], np.nan)
    df_return = df_close_adj.pct_change(fill_method=None)
    df_return = df_return[df_return.index >= start_dt]
    mv = mv[mv.index >= start_dt]

    equal_ret = df_return.mean(axis=1, skipna=True)
    mv_sum = mv.sum(axis=1).replace(0, np.nan)
    cap_ret = (df_return * mv).sum(axis=1, min_count=1) / mv_sum

    qsjh_ret = df_return['600425.SH'] if '600425.SH' in df_return.columns else pd.Series(index=df_return.index, dtype=float)
    qsjh_ret.name = '青松建化'
    cyb_ret = _safe_index_pct('399006.SZ', '创业板综', start_date_ymd)
    sse_ret = _safe_index_pct('000001.SH', '上证指数', start_date_ymd)

    panel1 = pd.concat(
        [
            qsjh_ret.rename('青松建化'),
            equal_ret.rename('简单平均'),
            cap_ret.rename('市值加权'),
            cyb_ret,
            sse_ret,
        ],
        axis=1
    ).sort_index()
    panel1 = panel1[panel1.index >= start_dt]
    cum1 = panel1.apply(_cumprod_norm, axis=0)

    if progress_callback:
        progress_callback(1, total, '图1: 平均价格指数/等权价格指数体系...')

    fig1, ax = plt.subplots(figsize=(18, 9), facecolor='white')
    for col in cum1.columns:
        if cum1[col].dropna().empty:
            continue
        ax.plot(cum1.index, cum1[col], linewidth=2, label=col)
    ax.set_xlabel('日期', fontsize=13)
    ax.set_ylabel('累计净值(起点=1)', fontsize=13)
    ax.set_title('平均价格指数 / 等权价格指数对照图', fontsize=17)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=11, loc='best')
    fig1.tight_layout()
    figures.append((fig1, '平均价格指数与等权价格指数对照'))

    # 参考“巨无霸企业占比指数.ipynb”：大小盘累计收益与差值
    stock_data = df_dailybasic[['ts_code', 'trade_date', 'close', 'total_mv']].copy()
    stock_data['trade_date'] = pd.to_datetime(stock_data['trade_date'].astype(str), errors='coerce')
    stock_data['close'] = pd.to_numeric(stock_data['close'], errors='coerce')
    stock_data['total_mv'] = pd.to_numeric(stock_data['total_mv'], errors='coerce')
    stock_data = stock_data.dropna(subset=['trade_date', 'close', 'total_mv'])
    stock_data = stock_data[stock_data['trade_date'] >= start_dt]
    stock_data = stock_data.sort_values(by=['ts_code', 'trade_date'])
    stock_data['pct_chg'] = stock_data.groupby('ts_code')['close'].pct_change(fill_method=None)

    small_cap_threshold = stock_data['total_mv'].quantile(0.05)
    large_cap_threshold = stock_data['total_mv'].quantile(0.9)
    small_cap = stock_data[stock_data['total_mv'] < small_cap_threshold].groupby('trade_date')['pct_chg'].mean().fillna(0).cumsum()
    large_cap = stock_data[stock_data['total_mv'] > large_cap_threshold].groupby('trade_date')['pct_chg'].mean().fillna(0).cumsum()

    hold_600425 = stock_data[stock_data['ts_code'] == '600425.SH'].copy()
    hold_600425 = hold_600425.sort_values('trade_date')
    hold_600425['持有股票累计收益'] = hold_600425['close'].pct_change(fill_method=None).fillna(0).cumsum()

    hold_002307 = stock_data[stock_data['ts_code'] == '002307.SZ'].copy()
    hold_002307 = hold_002307.sort_values('trade_date')
    hold_002307['持有股票累计收益'] = hold_002307['close'].pct_change(fill_method=None).fillna(0).cumsum()

    sse_cum_lin = sse_ret.fillna(0).cumsum()
    sse_cum_lin.name = '上证指数累计收益'

    if progress_callback:
        progress_callback(2, total, '图2: 巨无霸口径大小盘与个股对照...')

    fig2, ax = plt.subplots(figsize=(18, 9), facecolor='white')
    ax.plot(small_cap.index, small_cap, linewidth=2, label='小盘累计收益(市值5%分位以下)')
    ax.plot(large_cap.index, large_cap, linewidth=2, label='大盘累计收益(市值90%分位以上)')
    if not sse_cum_lin.dropna().empty:
        ax.plot(sse_cum_lin.index, sse_cum_lin, linewidth=2, label='上证指数累计收益')
    if not hold_600425.empty:
        ax.plot(hold_600425['trade_date'], hold_600425['持有股票累计收益'], linewidth=2, label='600425.SH累计收益')
    if not hold_002307.empty:
        ax.plot(hold_002307['trade_date'], hold_002307['持有股票累计收益'], linewidth=2, label='002307.SZ累计收益')
    ax.set_xlabel('日期', fontsize=13)
    ax.set_ylabel('累计收益(线性累计)', fontsize=13)
    ax.set_title('巨无霸口径：大小盘 / 指数 / 个股累计收益对照', fontsize=17)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=10, loc='best')
    fig2.tight_layout()
    figures.append((fig2, '大小盘与指数个股累计收益对照'))

    if progress_callback:
        progress_callback(3, total, '图3: 大小盘差值与阈值...')

    difference = (large_cap - small_cap).dropna()
    if difference.empty:
        raise ValueError('大小盘差值为空，请检查输入数据范围')
    cutoff_date = pd.to_datetime(f'{start_dt.year}-09-02')
    pre_cutoff_data = difference.loc[difference.index > cutoff_date]
    if pre_cutoff_data.empty:
        pre_cutoff_data = difference
    threshold = pre_cutoff_data.std()
    mean_difference = pre_cutoff_data.mean()
    upper = mean_difference + threshold
    lower = mean_difference - threshold

    signals = pd.DataFrame(index=difference.index)
    signals['difference'] = difference
    signals['1+std'] = upper
    signals['1-std'] = lower
    signals['investment'] = np.where(
        signals['difference'] > signals['1+std'], 'small_cap',
        np.where(signals['difference'] < signals['1-std'], 'large_cap', 'hold')
    )

    fig3, ax = plt.subplots(figsize=(18, 8), facecolor='white')
    ax.plot(signals.index, signals['difference'], linewidth=2, label='大盘-小盘差值')
    ax.plot(signals.index, signals['1+std'], color='r', linestyle='--', linewidth=1.5, label='+1σ阈值')
    ax.plot(signals.index, signals['1-std'], color='b', linestyle='--', linewidth=1.5, label='-1σ阈值')
    buy_signals = signals[signals['investment'] == 'small_cap']
    sell_signals = signals[signals['investment'] == 'large_cap']
    if not buy_signals.empty:
        ax.scatter(buy_signals.index, buy_signals['difference'], marker='^', s=24, color='g', alpha=0.7, label='偏向小盘')
    if not sell_signals.empty:
        ax.scatter(sell_signals.index, sell_signals['difference'], marker='v', s=24, color='darkred', alpha=0.7, label='偏向大盘')
    ax.set_xlabel('日期', fontsize=13)
    ax.set_ylabel('差值', fontsize=13)
    ax.set_title('大盘与小盘差值及风格信号', fontsize=17)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=10, loc='best')
    fig3.tight_layout()
    figures.append((fig3, '大小盘差值与阈值信号'))

    # 参考“平均价格指数.ipynb”：市值加权 vs 简单平均差值
    spread = (cum1['市值加权'] - cum1['简单平均']).dropna() if {'市值加权', '简单平均'}.issubset(cum1.columns) else pd.Series(dtype=float)
    if spread.empty:
        fig4, ax = plt.subplots(figsize=(14, 6), facecolor='white')
        ax.text(0.5, 0.5, '市值加权与简单平均数据不足，无法计算差值', ha='center', va='center', fontsize=13)
        ax.axis('off')
        figures.append((fig4, '市值加权与简单平均差值提示'))
    else:
        spread_mean = spread.mean()
        spread_std = spread.std()
        fig4, ax = plt.subplots(figsize=(18, 8), facecolor='white')
        ax.plot(spread.index, spread, linewidth=2.2, color='#8b5cf6', label='市值加权 - 简单平均')
        ax.axhline(spread_mean, color='gray', linestyle='--', linewidth=1.2, label='均值')
        if pd.notna(spread_std):
            ax.axhline(spread_mean + spread_std, color='r', linestyle='--', linewidth=1.0, label='+1σ')
            ax.axhline(spread_mean - spread_std, color='b', linestyle='--', linewidth=1.0, label='-1σ')
        ax.set_xlabel('日期', fontsize=13)
        ax.set_ylabel('累计差值', fontsize=13)
        ax.set_title('市值加权与简单平均累计差值（集中度）', fontsize=17)
        ax.grid(alpha=0.3)
        ax.legend(fontsize=10, loc='best')
        fig4.tight_layout()
        figures.append((fig4, '市值加权与简单平均累计差值'))

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
