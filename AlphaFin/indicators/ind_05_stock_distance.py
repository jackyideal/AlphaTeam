"""
ind_05 - 个股与指数距离
原始文件: 各种指标/证券 与 银行 差值/青松建化和各大指数距离.ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .shared_utils import pro

INDICATOR_META = {
    'id': 'ind_05_stock_distance',
    'name': '个股-指数相对强弱偏离因子',
    'group': '市场结构指标',
    'description': '度量个股与各大指数、行业板块的累计收益偏离程度，结合滚动相关性分析与Z-Score配对交易信号，识别个股相对强弱与均值回归机会',
    'input_type': 'stock',
    'default_code': '600425.SH',
    'requires_db': False,
    'slow': False,
    'chart_count': 8,
    'chart_descriptions': [
        '个股与各大指数/行业ETF的累计收益走势对比，直观展示相对强弱',
        '个股与各标的累计收益的差值走势，差值扩大说明个股跑赢/跑输',
        '个股与银行板块、上证指数的三方累计收益对比，判断个股在不同风格中的位置',
        '个股与上证指数的累计收益距离指标，衡量偏离程度及其收敛趋势',
        '个股与上证指数的滚动相关性走势，观察相关性随市场环境的动态变化',
        '个股与各标的之间的相关性矩阵热力图，识别联动最强与最弱的配对',
        '基于Z-Score的配对交易轮动通道，偏离±2倍标准差时触发均值回归信号',
        '累计收益差距与个股股价的叠加走势，观察价格与相对强弱的同步性',
    ],
}


def generate(ts_code='600425.SH', start_date='20170101', progress_callback=None, **kwargs):
    figures = []
    total = 8
    stock_name = ts_code

    if progress_callback:
        progress_callback(0, total, '获取个股与行业数据...')

    # 获取申万行业分类
    df_sw = pro.index_classify(level='L1', src='SW2014')
    sw_dict = dict(zip(df_sw['industry_name'], df_sw['index_code']))

    def fetch_sw(code, name):
        df = pro.sw_daily(ts_code=code)
        df = df.sort_values(by=['trade_date']).reset_index(drop=True)
        df.index = pd.to_datetime(df['trade_date'])
        df.rename(columns={'close': name}, inplace=True)
        return df[[name]]

    def fetch_index(code, name):
        df = pro.index_daily(ts_code=code, start_date=start_date)
        df = df.sort_values(by=['trade_date']).reset_index(drop=True)
        df.index = pd.to_datetime(df['trade_date'])
        df = df[['close']].rename(columns={'close': name})
        return df

    # 获取核心数据
    个股 = pro.daily(ts_code=ts_code, adj='qfq', start_date=start_date)
    个股 = 个股.sort_values(by=['trade_date']).reset_index(drop=True)
    个股.index = pd.to_datetime(个股['trade_date'])
    个股 = 个股[['close']].rename(columns={'close': stock_name})

    上证指数 = fetch_index('000001.SH', '上证指数')

    # 获取部分行业
    industries = {}
    for name in ['计算机', '通信', '建筑材料', '电子', '银行']:
        if name in sw_dict:
            try:
                industries[name] = fetch_sw(sw_dict[name], name)
            except Exception:
                pass

    # 合并
    merged_df = 个股.copy()
    merged_df = pd.merge(merged_df, 上证指数, on='trade_date', how='inner')
    for name, df in industries.items():
        merged_df = pd.merge(merged_df, df, on='trade_date', how='inner')

    merged_df = merged_df[merged_df.index >= start_date[:4] + '-01-01']

    # 累计收益
    基准价格 = merged_df.iloc[0]
    累计收益 = (merged_df - 基准价格) / 基准价格 * 100
    累计收益 = 累计收益.round(2)

    # 计算差值
    for col in 累计收益.columns:
        if col != stock_name:
            累计收益[f'{stock_name}-{col}'] = 累计收益[stock_name] - 累计收益[col]

    if progress_callback:
        progress_callback(1, total, '图1: 累计收益对比...')

    # ── 图1: 所有标的累计收益对比 ──
    fig1, ax = plt.subplots(figsize=(20, 8), facecolor='white')
    for col in merged_df.columns:
        lw = 3 if col == stock_name else 1.5
        ax.plot(累计收益.index, 累计收益[col], label=f'{col}累计收益', linewidth=lw)
    ax.set_title(f'{stock_name}与各标的累计收益', fontsize=16)
    ax.set_xlabel('交易日期', fontsize=14)
    ax.set_ylabel('累计收益 (%)', fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=14)
    fig1.tight_layout()
    figures.append((fig1, f'{stock_name}与各标的累计收益'))

    if progress_callback:
        progress_callback(2, total, '图2: 累计收益差异...')

    # ── 图2: 累计收益差异 ──
    diff_cols = [c for c in 累计收益.columns if c.startswith(f'{stock_name}-')]
    fig2, ax = plt.subplots(figsize=(20, 8), facecolor='white')
    for col in diff_cols:
        ax.plot(累计收益.index, 累计收益[col], label=col, linewidth=1.5)
    ax.set_title(f'{stock_name}与各标的累计收益差异', fontsize=16)
    ax.set_xlabel('交易日期', fontsize=14)
    ax.set_ylabel('累计收益差异 (%)', fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=14)
    fig2.tight_layout()
    figures.append((fig2, f'{stock_name}累计收益差异'))

    if progress_callback:
        progress_callback(3, total, '图3: 个股与银行对比...')

    # ── 图3: 个股+银行+上证指数 ──
    fig3, ax = plt.subplots(figsize=(20, 8), facecolor='white')
    ax.plot(累计收益.index, 累计收益[stock_name], label=stock_name, linewidth=2)
    ax.plot(累计收益.index, 累计收益['上证指数'], label='上证指数', linewidth=2)
    if '银行' in 累计收益.columns:
        ax.plot(累计收益.index, 累计收益['银行'], label='银行', linewidth=2)
    ax.set_title(f'{stock_name}、银行和上证指数累计收益', fontsize=16)
    ax.set_ylabel('累计收益 (%)', fontsize=14)
    ax.legend(fontsize=12)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=14)
    fig3.tight_layout()
    figures.append((fig3, f'{stock_name}银行上证指数对比'))

    if progress_callback:
        progress_callback(4, total, '图4: 更多指数距离...')

    # ── 获取更多指数 ──
    more_indices = {
        '中证1000': '000852.SH', '国证2000': '399303.SZ',
        '中证500': '000905.SH', '沪深300': '000300.SH',
        '上证50': '000016.SH', '创业板': '399006.SZ',
    }

    for name, code in more_indices.items():
        try:
            idx_df = fetch_index(code, name)
            merged_df = pd.merge(merged_df, idx_df, on='trade_date', how='inner')
        except Exception:
            pass

    基准2 = merged_df.iloc[0]
    累计收益2 = (merged_df - 基准2) / 基准2 * 100

    # ── 图4: 个股与上证指数距离 ──
    if stock_name in 累计收益2.columns and '上证指数' in 累计收益2.columns:
        累计收益2[f'{stock_name}-上证指数'] = 累计收益2[stock_name] - 累计收益2['上证指数']
        fig4, ax = plt.subplots(figsize=(20, 8), facecolor='white')
        ax.plot(累计收益2.index, 累计收益2[stock_name], label=stock_name, linewidth=2)
        ax.plot(累计收益2.index, 累计收益2[f'{stock_name}-上证指数'],
                label=f'{stock_name}-上证指数', linewidth=2)
        ax.set_title(f'{stock_name}与上证指数累计收益距离', fontsize=16)
        ax.set_ylabel('累计收益 (%)', fontsize=14)
        ax.legend(fontsize=12)
        ax.grid(alpha=0.3)
        ax.tick_params(labelsize=14)
        fig4.tight_layout()
        figures.append((fig4, f'{stock_name}与上证指数距离'))

    if progress_callback:
        progress_callback(5, total, '图5: 滚动相关性...')

    # ── 图5: 个股vs上证指数 + 滚动相关性 ──
    try:
        recent_start = pd.Timestamp(start_date[:4] + '-01-01')
        stock_recent = pro.daily(ts_code=ts_code, adj='qfq', start_date=recent_start.strftime('%Y%m%d'))
        stock_recent = stock_recent.sort_values('trade_date')
        stock_recent.index = pd.to_datetime(stock_recent['trade_date'])
        s_close = stock_recent[['close']].rename(columns={'close': stock_name})

        idx_recent = pro.index_daily(ts_code='000001.SH', start_date=recent_start.strftime('%Y%m%d'))
        idx_recent = idx_recent.sort_values('trade_date')
        idx_recent.index = pd.to_datetime(idx_recent['trade_date'])
        i_close = idx_recent[['close']].rename(columns={'close': '上证指数'})

        corr_df = pd.merge(s_close, i_close, left_index=True, right_index=True).dropna()
        corr_df['corr'] = corr_df[stock_name].rolling(window=60).corr(corr_df['上证指数'])

        fig5, ax1 = plt.subplots(figsize=(20, 10), facecolor='white')
        ax1.plot(corr_df.index, corr_df[stock_name], color='tab:red', linewidth=2, label=stock_name)
        ax1.set_ylabel(f'{stock_name}收盘价', color='tab:red', fontsize=14)
        ax1.tick_params(axis='y', labelcolor='tab:red')

        ax2 = ax1.twinx()
        ax2.plot(corr_df.index, corr_df['上证指数'], color='tab:blue', linewidth=2, alpha=0.8, label='上证指数')
        ax2.set_ylabel('上证指数', color='tab:blue', fontsize=14)

        ax3 = ax2.twinx()
        ax3.plot(corr_df['corr'].dropna().index, corr_df['corr'].dropna(),
                 color='tab:orange', linewidth=1.5, linestyle='--', label='滚动相关系数(60日)')
        ax3.set_ylabel('滚动相关系数', color='tab:orange', fontsize=14)
        ax3.spines['right'].set_position(('axes', 1.12))
        ax3.axhline(y=0, color='k', linestyle='-', alpha=0.3)

        ax1.set_title(f'{stock_name} vs 上证指数 vs 滚动相关性', fontsize=16)
        lines = ax1.get_legend_handles_labels()[0] + ax2.get_legend_handles_labels()[0] + ax3.get_legend_handles_labels()[0]
        labels = ax1.get_legend_handles_labels()[1] + ax2.get_legend_handles_labels()[1] + ax3.get_legend_handles_labels()[1]
        ax1.legend(lines, labels, loc='upper left', fontsize=12)
        ax1.grid(alpha=0.3)
        fig5.tight_layout()
        figures.append((fig5, f'{stock_name}与上证指数滚动相关性'))
    except Exception as e:
        print(f'绘制滚动相关性失败: {e}')

    if progress_callback:
        progress_callback(6, total, '图6: 相关性热力图...')

    # ── 图6: 相关性热力图 ──
    try:
        import seaborn as sns
        returns = merged_df.pct_change().dropna()
        corr_matrix = returns.corr()

        # 选择与个股相关性最高/最低的
        if stock_name in corr_matrix.columns:
            corr_series = corr_matrix[stock_name].drop(stock_name, errors='ignore')
            top5 = corr_series.sort_values(ascending=False).head(5).index.tolist()
            bot5 = corr_series.sort_values().head(5).index.tolist()
            selected = list(set([stock_name, '上证指数'] + top5 + bot5))
            selected = [c for c in selected if c in corr_matrix.columns]

            fig6, ax = plt.subplots(figsize=(14, 10), facecolor='white')
            sns.heatmap(corr_matrix.loc[selected, selected], annot=True, cmap='coolwarm',
                        fmt='.2f', linewidths=0.5, ax=ax, annot_kws={'size': 9})
            ax.set_title(f'{stock_name}相关性矩阵', fontsize=16)
            fig6.tight_layout()
            figures.append((fig6, f'{stock_name}相关性矩阵'))
    except Exception as e:
        print(f'绘制相关性热力图失败: {e}')

    if progress_callback:
        progress_callback(7, total, '图7-8: 配对交易...')

    # ── 图7: Z-score轮动通道 ──
    try:
        if stock_name in merged_df.columns and '上证指数' in merged_df.columns:
            norm_df = (merged_df - merged_df.mean()) / merged_df.std()
            ratio = norm_df[stock_name] / norm_df['上证指数'].replace(0, np.nan)
            zscore = (ratio - ratio.mean()) / ratio.std()
            zscore = zscore.dropna()

            fig7, ax = plt.subplots(figsize=(16, 8), facecolor='white')
            ax.plot(zscore.index, zscore, label='标准化价差', linewidth=1.5)
            ax.axhline(2, c='r', linestyle='--', label='+2std')
            ax.axhline(-2, c='g', linestyle='--', label='-2std')
            ax.set_title(f'{stock_name} vs 上证指数 轮动通道', fontsize=16)
            ax.legend(fontsize=12)
            ax.grid(alpha=0.3)
            ax.tick_params(labelsize=14)
            fig7.tight_layout()
            figures.append((fig7, f'{stock_name}轮动通道'))
    except Exception as e:
        print(f'绘制轮动通道失败: {e}')

    # ── 图8: 累计收益差距与股价 ──
    try:
        ret_stock = merged_df[stock_name].pct_change()
        ret_index = merged_df['上证指数'].pct_change()
        cum_stock = (1 + ret_stock).cumprod()
        cum_index = (1 + ret_index).cumprod()
        gap = cum_index - cum_stock

        fig8, ax1 = plt.subplots(figsize=(16, 8), facecolor='white')
        ax1.plot(gap.index, gap, color='tab:blue', label='累计收益率差距', linewidth=1.5)
        ax1.set_ylabel('累计收益率差距', color='tab:blue', fontsize=14)
        ax1.tick_params(axis='y', labelcolor='tab:blue')

        ax2 = ax1.twinx()
        ax2.plot(merged_df.index, merged_df[stock_name], color='tab:red', label=f'{stock_name}收盘价', linewidth=1.5)
        ax2.set_ylabel(f'{stock_name}收盘价', color='tab:red', fontsize=14)
        ax2.tick_params(axis='y', labelcolor='tab:red')

        ax1.set_title(f'{stock_name}收盘价与累计收益率差距', fontsize=16)
        ax1.legend(loc='upper left', fontsize=12)
        ax2.legend(loc='upper right', fontsize=12)
        ax1.grid(alpha=0.3)
        fig8.tight_layout()
        figures.append((fig8, f'{stock_name}累计收益差距'))
    except Exception as e:
        print(f'绘制累计收益差距失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
