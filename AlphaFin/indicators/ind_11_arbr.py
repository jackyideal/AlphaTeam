"""
ind_11 - ARBR情绪指标
原始文件: 各种指标/arbr指标/情绪指标 arbr指标.ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .shared_utils import pro

try:
    import talib as ta
    HAS_TALIB = True
except ImportError:
    HAS_TALIB = False

INDICATOR_META = {
    'id': 'ind_11_arbr',
    'name': '市场情绪扩散指数',
    'group': '资金面指标',
    'description': 'AR(人气指标)衡量开盘价位势能，BR(买卖意愿指标)衡量多空力量对比，双指标协同判断市场情绪极端状态，含阈值信号策略回测与多标的对比分析',
    'input_type': 'stock',
    'default_code': '600425.SH',
    'requires_db': False,
    'slow': False,
    'chart_count': 11,
    'chart_descriptions': [
        'AR/BR情绪策略累计收益曲线与基准对比',
        '个股走势与BR指标的叠加，BR>300极度乐观、BR<50极度悲观',
        'BR指标买卖信号标注在个股走势上',
        '上证指数BR指标走势',
        '创业板BR指标走势',
        '沪深300 BR指标走势',
        '中证500 BR指标走势',
        'AR指标买卖信号与个股走势',
        'AR策略净值曲线',
        'BR策略买卖信号详情',
        'BR策略净值曲线',
    ],
}

INDEX_MAP = {
    '上证综指': '000001.SH', '深证成指': '399001.SZ',
    '沪深300': '000300.SH', '创业板指': '399006.SZ',
    '上证50': '000016.SH', '中证500': '000905.SH',
}


def _calc_arbr(df, timeperiod=12):
    """计算AR/BR指标"""
    df = df.copy()
    df['HO'] = df['high'] - df['open']
    df['OL'] = df['open'] - df['low']
    df['HCY'] = df['high'] - df['close'].shift(1)
    df['CYL'] = df['close'].shift(1) - df['low']

    if HAS_TALIB:
        df['AR'] = ta.SUM(df['HO'], timeperiod=timeperiod) / ta.SUM(df['OL'], timeperiod=timeperiod) * 100
        df['BR'] = ta.SUM(df['HCY'], timeperiod=timeperiod) / ta.SUM(df['CYL'], timeperiod=timeperiod) * 100
    else:
        df['AR'] = df['HO'].rolling(timeperiod).sum() / df['OL'].rolling(timeperiod).sum() * 100
        df['BR'] = df['HCY'].rolling(timeperiod).sum() / df['CYL'].rolling(timeperiod).sum() * 100

    return df[['close', 'AR', 'BR']].dropna()


def _fetch_stock(ts_code, start_date='20180701'):
    """获取个股数据"""
    df = pro.daily(ts_code=ts_code, adj='qfq', start_date=start_date)
    df = df.sort_values(by='trade_date')
    df.index = pd.to_datetime(df['trade_date'])
    return df


def _fetch_index(ts_code, start_date='20180701'):
    """获取指数数据"""
    df = pro.index_daily(ts_code=ts_code, start_date=start_date)
    df = df.sort_values(by='trade_date')
    df.index = pd.to_datetime(df['trade_date'])
    return df


def _plot_arbr_dual(df, name, line_upper=150, line_lower=63):
    """绘制BR指标与股价双轴图"""
    fig = plt.figure(figsize=(20, 8), facecolor='white')
    ax1 = fig.add_subplot(111)
    ax1.plot(df.index, df['close'], color='SkyBlue', linewidth=3)
    ax1.set_ylabel(f'{name}股价', fontsize=20)
    ax1.legend([f'{name}股价'], loc='upper left', fontsize=15)
    ax1.tick_params(labelsize=15)

    ax2 = ax1.twinx()
    ax2.plot(df.index, df['AR'], color='Indianred', linewidth=2)
    ax2.set_ylabel('AR/BR', fontsize=20)
    ax2.legend(['AR'], loc='upper right', fontsize=15)
    ax2.plot(df.index, [line_upper] * len(df), color='green', linestyle='--')
    ax2.plot(df.index, [line_lower] * len(df), color='green', linestyle='--')
    ax2.tick_params(labelsize=15)

    plt.title(f'{name} BR 指标', fontsize=25)
    plt.grid(alpha=0.3)
    fig.tight_layout()
    return fig


def _calc_performance(df, strategy_col, stock_col='return'):
    """计算策略绩效指标"""
    total_ret = df[[strategy_col, '青松自身净值']].iloc[-1] - 1
    try:
        annual_ret = pow(1 + total_ret, 250 / len(df)) - 1
    except Exception:
        annual_ret = total_ret
    dd = (df[[strategy_col, '青松自身净值']].cummax() - df[[strategy_col, '青松自身净值']]) / \
         df[[strategy_col, '青松自身净值']].cummax()
    max_dd = dd.max()
    return total_ret, annual_ret, max_dd


def generate(ts_code='600425.SH', start_date='20180701', progress_callback=None, **kwargs):
    figures = []
    total = 11
    best_timeperiod = 12
    best_buy_signal = 30
    best_sell_signal = 250

    if progress_callback:
        progress_callback(0, total, '获取个股数据...')

    # ── 获取个股数据 ──
    df_stock = _fetch_stock(ts_code, start_date)
    df_arbr = _calc_arbr(df_stock, best_timeperiod)

    # ── BR信号策略 ──
    df_arbr['signal'] = 0
    holding = False
    previous_BR = df_arbr['BR'].shift(1)
    for i in range(1, len(df_arbr)):
        current_BR = df_arbr.loc[df_arbr.index[i], 'BR']
        if holding:
            if current_BR > best_sell_signal:
                df_arbr.loc[df_arbr.index[i], 'signal'] = 0
                holding = False
            else:
                df_arbr.loc[df_arbr.index[i], 'signal'] = 1
        else:
            if current_BR < best_buy_signal and previous_BR.iloc[i] > best_buy_signal:
                df_arbr.loc[df_arbr.index[i], 'signal'] = 1
                holding = True
            elif current_BR > best_sell_signal:
                df_arbr.loc[df_arbr.index[i], 'signal'] = 0

    df_arbr['daily_return'] = df_arbr['close'].pct_change()
    df_arbr['strategy_return'] = df_arbr['signal'].shift(1) * df_arbr['daily_return']
    df_arbr['cumulative_return_strategy'] = (1 + df_arbr['strategy_return']).cumprod()
    df_arbr['cumulative_return_hold'] = (1 + df_arbr['daily_return']).cumprod()

    # 获取上证指数作为基准
    df_idx = _fetch_index('000001.SH', start_date)
    df_idx = df_idx[['close']].rename(columns={'close': '上证指数'})
    df_merged = df_arbr.merge(df_idx, left_index=True, right_index=True, how='left')
    df_merged['benchmark_return'] = df_merged['上证指数'].pct_change()
    df_merged['cumulative_return_benchmark'] = (1 + df_merged['benchmark_return']).cumprod()

    if progress_callback:
        progress_callback(1, total, '图1: 策略累积收益...')

    # ── 图1: 策略累积收益对比 ──
    fig1, ax = plt.subplots(figsize=(14, 7), facecolor='white')
    ax.plot(df_arbr.index, df_arbr['cumulative_return_hold'], label='持有股票', color='blue')
    ax.plot(df_arbr.index, df_arbr['cumulative_return_strategy'], label='BR策略', color='orange')
    ax.plot(df_merged.index, df_merged['cumulative_return_benchmark'], label='持有上证指数', color='green')
    ax.set_xlabel('日期', fontsize=14)
    ax.set_ylabel('累积收益', fontsize=14)
    ax.set_title('策略累积收益', fontsize=16)
    ax.legend(fontsize=12)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=12)
    fig1.tight_layout()
    figures.append((fig1, '策略累积收益'))

    if progress_callback:
        progress_callback(2, total, '图2: 股价与BR指标...')

    # ── 图2: 股价与BR指标双轴图 ──
    # 剔除BR极端值
    p99 = df_merged['BR'].quantile(0.99)
    p01 = df_merged['BR'].quantile(0.01)
    df_merged.loc[df_merged['BR'] > p99, 'BR'] = p99
    df_merged.loc[df_merged['BR'] < p01, 'BR'] = p01

    fig2, ax1 = plt.subplots(figsize=(14, 7), facecolor='white')
    ax1.plot(df_merged.index, df_merged['close'], color='black', label='股价', linewidth=3)
    ax1.set_ylabel('股价', fontsize=20)
    ax1.tick_params(labelsize=12)

    ax2 = ax1.twinx()
    ax2.plot(df_merged.index, df_merged['BR'], color='tab:orange', label='BR', linewidth=1)
    ax2.plot(df_merged.index, [best_sell_signal] * len(df_merged), color='green', linestyle='--')
    ax2.plot(df_merged.index, [best_buy_signal] * len(df_merged), color='green', linestyle='--')
    ax2.set_ylabel('BR', fontsize=20)
    ax2.tick_params(labelsize=12)

    plt.title(f'{ts_code}走势与反应比率', fontsize=16)
    ax1.legend(loc='upper left', fontsize=12)
    ax2.legend(loc='upper right', fontsize=12)
    ax1.grid(alpha=0.3)
    fig2.tight_layout()
    figures.append((fig2, f'{ts_code}走势与BR指标'))

    if progress_callback:
        progress_callback(3, total, '图3: BR买卖信号...')

    # ── 图3: BR买卖信号 ──
    first_buy = df_merged[(df_merged['signal'] == 1) & (df_merged['signal'].shift(1) != 1)].index
    first_sell = df_merged[(df_merged['signal'] == 0) & (df_merged['signal'].shift(1) == 1)].index

    fig3, ax = plt.subplots(figsize=(16, 7), facecolor='white')
    ax.plot(df_merged.index, df_merged['close'], label='股价', color='blue')
    for date in first_buy:
        ax.scatter(date, df_merged.loc[date, 'close'], marker='^', color='red', s=100)
        ax.annotate('买', (date, df_merged.loc[date, 'close']),
                    textcoords="offset points", xytext=(0, 10), ha='center',
                    color='red', fontsize=14, fontweight='bold')
    for date in first_sell:
        ax.scatter(date, df_merged.loc[date, 'close'], marker='v', color='green', s=100)
        ax.annotate('卖', (date, df_merged.loc[date, 'close']),
                    textcoords="offset points", xytext=(0, 10), ha='center',
                    color='green', fontsize=14, fontweight='bold')
    ax.set_title(f'{ts_code}走势和买卖信号', fontsize=16)
    ax.set_xlabel('日期', fontsize=14)
    ax.set_ylabel('股价', fontsize=14)
    ax.legend(fontsize=12)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=12)
    fig3.tight_layout()
    figures.append((fig3, f'{ts_code} BR买卖信号'))

    if progress_callback:
        progress_callback(4, total, '图4-7: 各标的BR指标...')

    # ── 图4-7: 各标的BR指标 ──
    targets = [
        (ts_code, ts_code, False),
        ('000001.SH', '上证综指', True),
        ('000300.SH', '沪深300', True),
        ('399006.SZ', '创业板指', True),
    ]

    for idx, (code, name, is_index) in enumerate(targets):
        if progress_callback:
            progress_callback(4 + idx, total, f'图{4 + idx}: {name}BR指标...')
        try:
            if is_index:
                df_t = _fetch_index(code, start_date)
            else:
                df_t = _fetch_stock(code, start_date)
            df_t_arbr = _calc_arbr(df_t, 12)
            fig = _plot_arbr_dual(df_t_arbr, name)
            figures.append((fig, f'{name} BR指标'))
        except Exception as e:
            print(f'绘制{name}BR指标失败: {e}')

    if progress_callback:
        progress_callback(8, total, '图8-9: AR策略...')

    # ── AR策略部分 ──
    try:
        df_ar = _calc_arbr(_fetch_stock(ts_code, start_date), 12)
        df_ar['return'] = df_ar['close'] / df_ar['close'].shift(1) - 1

        # AR信号
        for i in range(1, len(df_ar)):
            if df_ar['AR'].iloc[i] > 65 and df_ar['AR'].iloc[i - 1] < 65:
                df_ar.loc[df_ar.index[i], '收盘信号AR'] = 1
            if df_ar['AR'].iloc[i] < 140 and df_ar['AR'].iloc[i - 1] > 140:
                df_ar.loc[df_ar.index[i], '收盘信号AR'] = 0

        df_ar['当天仓位AR'] = df_ar.get('收盘信号AR', pd.Series(dtype=float)).shift(1)
        df_ar['当天仓位AR'] = df_ar['当天仓位AR'].fillna(method='ffill').fillna(0)

        # ── 图8: AR买卖信号 ──
        fig8, ax = plt.subplots(figsize=(20, 8), facecolor='white')
        ax.plot(df_ar.index, df_ar['close'], linewidth=2)
        for i in range(len(df_ar)):
            if '收盘信号AR' in df_ar.columns and pd.notna(df_ar['收盘信号AR'].iloc[i]):
                if df_ar['收盘信号AR'].iloc[i] == 1:
                    ax.annotate('买入', xy=(df_ar.index[i], df_ar['close'].iloc[i]),
                                arrowprops=dict(facecolor='r', shrink=0.05))
                elif df_ar['收盘信号AR'].iloc[i] == 0:
                    ax.annotate('卖出', xy=(df_ar.index[i], df_ar['close'].iloc[i]),
                                arrowprops=dict(facecolor='g', shrink=0.1))
        ax.set_title(f'{ts_code} AR买卖信号', fontsize=15)
        ax.grid(alpha=0.3)
        fig8.tight_layout()
        figures.append((fig8, f'{ts_code} AR买卖信号'))

        if progress_callback:
            progress_callback(9, total, '图9: AR策略净值...')

        # ── 图9: AR策略净值 ──
        df_ar['策略ar净值'] = (df_ar['return'] * df_ar['当天仓位AR'] + 1.0).cumprod()
        df_ar['青松自身净值'] = (df_ar['return'] + 1.0).cumprod()

        total_ret, annual_ret, max_dd = _calc_performance(df_ar, '策略ar净值')

        fig9, ax = plt.subplots(figsize=(20, 10), facecolor='white')
        ax.plot(df_ar.index, df_ar['策略ar净值'], label='AR策略净值')
        ax.plot(df_ar.index, df_ar['青松自身净值'], label='持有股票净值')
        ax.set_title(f'{ts_code}与AR情绪指标策略', fontsize=15)
        bbox = dict(boxstyle="round", fc="w", ec="0.5", alpha=0.9)
        try:
            text = (f"累计收益：策略{round(total_ret['策略ar净值'] * 100, 2)}%, "
                    f"股票{round(total_ret['青松自身净值'] * 100, 2)}%\n"
                    f"最大回撤：策略{round(max_dd['策略ar净值'] * 100, 2)}%, "
                    f"股票{round(max_dd['青松自身净值'] * 100, 2)}%")
            ax.text(0.02, 0.15, text, transform=ax.transAxes, fontsize=13, bbox=bbox)
        except Exception:
            pass
        ax.legend(fontsize=12)
        ax.grid(alpha=0.3)
        fig9.tight_layout()
        figures.append((fig9, f'{ts_code} AR策略净值'))
    except Exception as e:
        print(f'AR策略部分失败: {e}')

    if progress_callback:
        progress_callback(10, total, '图10-11: BR策略...')

    # ── BR策略部分 ──
    try:
        df_br = _calc_arbr(_fetch_stock(ts_code, start_date), 12)
        df_br['return'] = df_br['close'] / df_br['close'].shift(1) - 1

        # BR信号
        for i in range(1, len(df_br)):
            if df_br['BR'].iloc[i] > 70 and df_br['BR'].iloc[i - 1] < 70:
                df_br.loc[df_br.index[i], '收盘信号BR'] = 1
            if df_br['BR'].iloc[i] < 175 and df_br['BR'].iloc[i - 1] > 175:
                df_br.loc[df_br.index[i], '收盘信号BR'] = 0

        df_br['当天仓位BR'] = df_br.get('收盘信号BR', pd.Series(dtype=float)).shift(1)
        df_br['当天仓位BR'] = df_br['当天仓位BR'].fillna(method='ffill').fillna(0)

        # ── 图10: BR买卖信号 ──
        fig10, ax = plt.subplots(figsize=(20, 8), facecolor='white')
        ax.plot(df_br.index, df_br['close'], linewidth=2)
        for i in range(len(df_br)):
            if '收盘信号BR' in df_br.columns and pd.notna(df_br['收盘信号BR'].iloc[i]):
                if df_br['收盘信号BR'].iloc[i] == 1:
                    ax.annotate('买入', xy=(df_br.index[i], df_br['close'].iloc[i]),
                                arrowprops=dict(facecolor='r', shrink=0.05))
                elif df_br['收盘信号BR'].iloc[i] == 0:
                    ax.annotate('卖出', xy=(df_br.index[i], df_br['close'].iloc[i]),
                                arrowprops=dict(facecolor='g', shrink=0.1))
        ax.set_title(f'{ts_code} BR买卖信号', fontsize=15)
        ax.grid(alpha=0.3)
        fig10.tight_layout()
        figures.append((fig10, f'{ts_code} BR买卖信号'))

        # ── 图11: BR策略净值 ──
        df_br['策略BR净值'] = (df_br['return'] * df_br['当天仓位BR'] + 1.0).cumprod()
        df_br['青松自身净值'] = (df_br['return'] + 1.0).cumprod()

        total_ret, annual_ret, max_dd = _calc_performance(df_br, '策略BR净值')

        fig11, ax = plt.subplots(figsize=(20, 10), facecolor='white')
        ax.plot(df_br.index, df_br['策略BR净值'], label='BR策略净值')
        ax.plot(df_br.index, df_br['青松自身净值'], label='持有股票净值')
        ax.set_title(f'{ts_code}与BR情绪指标策略', fontsize=15)
        try:
            text = (f"累计收益：策略{round(total_ret['策略BR净值'] * 100, 2)}%, "
                    f"股票{round(total_ret['青松自身净值'] * 100, 2)}%\n"
                    f"最大回撤：策略{round(max_dd['策略BR净值'] * 100, 2)}%, "
                    f"股票{round(max_dd['青松自身净值'] * 100, 2)}%")
            ax.text(0.02, 0.15, text, transform=ax.transAxes, fontsize=13, bbox=bbox)
        except Exception:
            pass
        ax.legend(fontsize=12)
        ax.grid(alpha=0.3)
        fig11.tight_layout()
        figures.append((fig11, f'{ts_code} BR策略净值'))
    except Exception as e:
        print(f'BR策略部分失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
