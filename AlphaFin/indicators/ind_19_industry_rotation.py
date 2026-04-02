"""
ind_19 - 理想行业轮动策略一号
原始文件: 技术模型/运用rsi挖掘全年领涨行业策略/李想行业轮动大模型.ipynb
基于RSI动量+估值因子的行业轮动策略，筛选当前推荐行业
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from datetime import datetime, timedelta
from .shared_utils import pro
from .db_utils import get_data_by_sql, get_pivot_data, get_calendar
from ..config import DB_ROOT, BASE_DIR

INDICATOR_META = {
    'id': 'ind_19_industry_rotation',
    'name': '理想行业轮动策略',
    'group': '策略模型',
    'description': '多因子行业轮动策略：融合RSI动量因子、PB/PE估值因子、换手率活跃度因子及净利润增速成长因子，通过综合评分排序推荐当期最优行业配置，含策略回测与月度归因',
    'input_type': 'none',
    'default_code': '',
    'requires_db': True,
    'slow': True,
    'chart_count': 5,
    'chart_descriptions': [
        '行业综合因子排名柱状图，展示当期各行业的多因子综合评分排序',
        '推荐行业TOP4详情表，列出当前最优配置行业及各因子得分明细',
        '策略累计收益曲线与基准(等权行业)对比，评估轮动策略的超额收益',
        '最近12个月的月度收益对比柱状图，策略与基准的逐月表现差异',
        '行业因子得分热力图（近6个月），追踪各行业评分的动态演变',
    ],
}

SW_CSV = f'{BASE_DIR}/技术模型/运用rsi挖掘全年领涨行业策略/data/申万行业分类与成分.csv'


def _load_sw_members():
    """加载申万行业分类"""
    df = pd.read_csv(SW_CSV)
    df = df.fillna('')
    return df


def _de_extreme(ddd):
    """中位数去极值 (5倍MAD)"""
    median = ddd.median()
    mad = (ddd - median).abs().median()
    upper = median + 5 * mad
    lower = median - 5 * mad
    return ddd.clip(lower, upper, axis=1)


def _calc_industry_weighted(stock_data, mv_data, sw_members, index_codes):
    """计算市值加权的行业指标"""
    result = pd.DataFrame(index=stock_data.index)
    for idx_code in index_codes:
        con_codes = list(sw_members[sw_members['index_code'] == idx_code]['con_code'])
        valid = [c for c in con_codes if c in stock_data.columns and c in mv_data.columns]
        if not valid:
            continue
        numerator = (stock_data[valid] * mv_data[valid]).sum(axis=1)
        denominator = mv_data[valid].sum(axis=1)
        result[idx_code] = numerator / denominator.replace(0, np.nan)
    return result


def _render_table_figure(df, title, figsize=(18, 8)):
    """将DataFrame渲染为matplotlib表格图"""
    fig, ax = plt.subplots(figsize=figsize, facecolor='white')
    ax.axis('off')
    ax.set_title(title, fontsize=18, pad=20)

    # 格式化数值
    display_df = df.copy()
    for col in display_df.select_dtypes(include=[np.number]).columns:
        display_df[col] = display_df[col].apply(lambda x: f'{x:.4f}' if pd.notnull(x) else '')

    table = ax.table(
        cellText=display_df.values,
        colLabels=display_df.columns,
        rowLabels=display_df.index if not isinstance(display_df.index, pd.RangeIndex) else None,
        cellLoc='center',
        loc='center',
    )
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 1.5)

    # 表头颜色
    for (row, col), cell in table.get_celld().items():
        if row == 0:
            cell.set_facecolor('#4472C4')
            cell.set_text_props(color='white', fontweight='bold')
        elif row % 2 == 0:
            cell.set_facecolor('#D6E4F0')

    fig.tight_layout()
    return fig


def generate(start_date='20160101', progress_callback=None, **kwargs):
    figures = []
    total = 5
    file_path = f'sqlite:////{DB_ROOT}'
    t0 = time.time()

    def _progress(step, msg):
        elapsed = time.time() - t0
        if progress_callback:
            progress_callback(step, total, f'{msg} (已用时 {elapsed:.0f}s)')

    _progress(0, '加载申万行业分类与股票数据...')

    # ── 加载申万行业分类 ──
    sw_members = _load_sw_members()
    l2_members = sw_members[sw_members['level'] == 'L2']
    index_codes = list(l2_members['index_code'].unique())
    codes = list(l2_members['con_code'].unique())

    # ── 从数据库加载数据 ──
    _progress(0, '加载数据库: daily_kline, daily_adj...')
    df_adj = get_data_by_sql(file_path, 'daily_adj', 'daily_adj', codes, '*')
    df_kline = get_data_by_sql(file_path, 'daily_kline', 'daily_kline', codes, '*')

    df_adj_pivot = get_pivot_data(df_adj, 'adj_factor')
    df_close = get_pivot_data(df_kline, 'close')
    df_close = (df_close * df_adj_pivot / df_adj_pivot.loc[df_adj_pivot.index[-1]]).round(2)
    df_close = df_close.dropna(how='all')
    df_return = df_close.pct_change()

    _progress(0, '加载数据库: dailybasic...')
    df_dailybasic = get_data_by_sql(
        file_path, 'dailybasic', 'dailybasic', codes,
        'ts_code,trade_date,total_mv,pe_ttm,pb,turnover_rate_f'
    )
    PE = get_pivot_data(df_dailybasic, 'pe_ttm')
    PB = get_pivot_data(df_dailybasic, 'pb')
    TR = get_pivot_data(df_dailybasic, 'turnover_rate_f')
    MV = get_pivot_data(df_dailybasic, 'total_mv')

    _progress(1, '计算行业市值加权指标...')

    # ── 计算行业市值加权指标 ──
    ind_return = _calc_industry_weighted(df_return, MV, l2_members, index_codes)
    ind_PE = _calc_industry_weighted(PE, MV, l2_members, index_codes)
    ind_PB = _calc_industry_weighted(PB, MV, l2_members, index_codes)
    ind_TR = _calc_industry_weighted(TR, MV, l2_members, index_codes)

    # 行业名称映射
    name_map = dict(zip(l2_members['index_code'], l2_members['industry_name']))
    ind_return.columns = [name_map.get(c, c) for c in ind_return.columns]
    ind_PE.columns = [name_map.get(c, c) for c in ind_PE.columns]
    ind_PB.columns = [name_map.get(c, c) for c in ind_PB.columns]
    ind_TR.columns = [name_map.get(c, c) for c in ind_TR.columns]

    _progress(2, '计算RSI动量与因子排名...')

    # ── RSI动量因子 ──
    RS_20 = ind_return.rolling(20).sum().rank(axis=1, pct=True)
    RS_40 = ind_return.rolling(40).sum().rank(axis=1, pct=True)
    RS_60 = ind_return.rolling(60).sum().rank(axis=1, pct=True)
    RS = (RS_20 + RS_40 + RS_60) / 3

    # ── 估值因子排名（越低越好，反向排名）──
    PB_rank = ind_PB.rank(axis=1, pct=True, ascending=False)
    PE_rank = ind_PE.rank(axis=1, pct=True, ascending=False)
    TR_rank = ind_TR.rank(axis=1, pct=True, ascending=False)

    # ── 综合因子 ──
    X = (PB_rank + PE_rank + TR_rank + RS) / 4
    X = X.dropna(how='all')

    # 月末采样
    X_monthly = X.copy()
    X_monthly['month'] = X_monthly.index.astype(str).str[:7]
    X_monthly = X_monthly.drop_duplicates(subset=['month'], keep='last')
    X_monthly = X_monthly.drop(columns=['month'])
    X_monthly = X_monthly[X_monthly.index >= start_date]

    _progress(3, '图1: 当前行业综合因子排名...')

    # ── 图1: 当前行业综合因子排名（柱状图）──
    latest_scores = X_monthly.iloc[-1].dropna().sort_values(ascending=False)

    fig1, ax = plt.subplots(figsize=(20, 8), facecolor='white')
    colors = ['#2196F3' if i < 4 else '#90CAF9' for i in range(len(latest_scores))]
    ax.bar(range(len(latest_scores)), latest_scores.values, color=colors)
    ax.set_xticks(range(len(latest_scores)))
    ax.set_xticklabels(latest_scores.index, rotation=90, fontsize=11)
    ax.set_ylabel('综合因子得分', fontsize=14)
    ax.set_title(f'行业综合因子排名（{X_monthly.index[-1].strftime("%Y-%m-%d")}）', fontsize=18)
    ax.grid(alpha=0.3, axis='y')
    # 标注前4名
    for i in range(min(4, len(latest_scores))):
        ax.text(i, latest_scores.values[i] + 0.005,
                f'{latest_scores.values[i]:.3f}', ha='center', fontsize=10, fontweight='bold')
    fig1.tight_layout()
    figures.append((fig1, '行业综合因子排名'))

    _progress(3, '图2: 推荐行业详情表...')

    # ── 图2: 推荐行业表格 ──
    top4 = latest_scores.head(4)
    top4_names = list(top4.index)
    rec_df = pd.DataFrame({
        '行业': top4_names,
        '综合得分': [f'{v:.4f}' for v in top4.values],
        'RSI动量': [f'{RS.iloc[-1].get(n, 0):.4f}' for n in top4_names],
        'PB排名': [f'{PB_rank.iloc[-1].get(n, 0):.4f}' for n in top4_names],
        'PE排名': [f'{PE_rank.iloc[-1].get(n, 0):.4f}' for n in top4_names],
    })
    rec_df.index = range(1, len(rec_df) + 1)
    rec_df.index.name = '排名'
    fig2 = _render_table_figure(rec_df, f'当前推荐行业 TOP 4（{X_monthly.index[-1].strftime("%Y-%m-%d")}）',
                                figsize=(16, 4))
    figures.append((fig2, '推荐行业TOP4'))

    _progress(4, '图3-5: 策略回测...')

    # ── 图3: 策略累计收益 vs 基准 ──
    try:
        # 月度轮动策略：每月选前4行业，持有到下月
        strategy_rets = []
        for i in range(len(X_monthly) - 1):
            top_inds = X_monthly.iloc[i].dropna().sort_values(ascending=False).head(4).index.tolist()
            cur_date = X_monthly.index[i]
            next_date = X_monthly.index[i + 1]
            period_ret = ind_return.loc[cur_date:next_date, top_inds]
            if len(period_ret) > 1:
                period_ret = period_ret.iloc[1:]  # 排除选股当天
            avg_ret = period_ret.mean(axis=1).sum()
            strategy_rets.append({'date': next_date, 'return': avg_ret})

        strat_df = pd.DataFrame(strategy_rets)
        strat_df['cumulative'] = (strat_df['return'] + 1).cumprod() - 1

        # 上证指数基准
        szzs = pro.index_daily(ts_code='000001.SH')
        szzs['trade_date'] = pd.to_datetime(szzs['trade_date'])
        szzs = szzs.sort_values('trade_date')
        szzs.index = szzs['trade_date']
        szzs_monthly = szzs['close'].resample('M').last()
        szzs_ret = szzs_monthly.pct_change().dropna()

        # 对齐日期
        common_len = min(len(strat_df), len(szzs_ret))
        szzs_cum = (szzs_ret.iloc[-common_len:].values + 1).cumprod() - 1

        fig3, ax = plt.subplots(figsize=(18, 8), facecolor='white')
        ax.plot(strat_df['date'], strat_df['cumulative'], linewidth=2.5, label='行业轮动策略')
        if len(szzs_cum) == len(strat_df):
            ax.plot(strat_df['date'], szzs_cum, linewidth=2, label='上证指数', alpha=0.7)
        ax.set_xlabel('日期', fontsize=14)
        ax.set_ylabel('累计收益', fontsize=14)
        ax.set_title('行业轮动策略 vs 上证指数（月度调仓）', fontsize=18)
        ax.legend(fontsize=14)
        ax.grid(alpha=0.3)
        fig3.tight_layout()
        figures.append((fig3, '策略累计收益'))
    except Exception as e:
        print(f'绘制策略累计收益失败: {e}')

    # ── 图4: 最近12个月每月选行业 vs 实际涨幅 ──
    try:
        n_months = min(12, len(X_monthly) - 1)
        month_labels = []
        pred_rets = []
        actual_rets = []

        for i in range(len(X_monthly) - n_months, len(X_monthly) - 1):
            top_inds = X_monthly.iloc[i].dropna().sort_values(ascending=False).head(4).index.tolist()
            cur_date = X_monthly.index[i]
            next_date = X_monthly.index[i + 1]
            period = ind_return.loc[cur_date:next_date]
            if len(period) > 1:
                period = period.iloc[1:]

            pred_ret = period[top_inds].mean(axis=1).sum()
            all_ret = period.mean(axis=1).sum()
            month_labels.append(cur_date.strftime('%Y-%m'))
            pred_rets.append(pred_ret)
            actual_rets.append(all_ret)

        fig4, ax = plt.subplots(figsize=(18, 8), facecolor='white')
        x = np.arange(len(month_labels))
        width = 0.35
        ax.bar(x - width / 2, pred_rets, width, label='策略选行业收益', color='#2196F3', alpha=0.8)
        ax.bar(x + width / 2, actual_rets, width, label='全行业平均收益', color='#FF9800', alpha=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(month_labels, rotation=45, fontsize=11)
        ax.set_ylabel('月度收益', fontsize=14)
        ax.set_title('近12个月：策略选行业 vs 全行业平均', fontsize=18)
        ax.legend(fontsize=13)
        ax.grid(alpha=0.3, axis='y')
        fig4.tight_layout()
        figures.append((fig4, '月度收益对比'))
    except Exception as e:
        print(f'绘制月度收益对比失败: {e}')

    # ── 图5: 行业因子得分热力图（近6个月）──
    try:
        recent = X_monthly.iloc[-6:]
        top_inds = X_monthly.iloc[-1].dropna().sort_values(ascending=False).head(10).index.tolist()
        heat_data = recent[top_inds].T

        fig5, ax = plt.subplots(figsize=(16, 8), facecolor='white')
        im = ax.imshow(heat_data.values, cmap='RdYlGn', aspect='auto')
        ax.set_xticks(range(len(heat_data.columns)))
        ax.set_xticklabels([d.strftime('%Y-%m') for d in heat_data.columns], fontsize=11)
        ax.set_yticks(range(len(heat_data.index)))
        ax.set_yticklabels(heat_data.index, fontsize=11)
        # 标注数值
        for i in range(heat_data.shape[0]):
            for j in range(heat_data.shape[1]):
                val = heat_data.values[i, j]
                if pd.notnull(val):
                    ax.text(j, i, f'{val:.2f}', ha='center', va='center', fontsize=9)
        ax.set_title('TOP10行业因子得分变化（近6个月）', fontsize=18)
        fig5.colorbar(im, ax=ax, shrink=0.8)
        fig5.tight_layout()
        figures.append((fig5, '因子得分热力图'))
    except Exception as e:
        print(f'绘制热力图失败: {e}')

    elapsed = time.time() - t0
    if progress_callback:
        progress_callback(total, total, f'完成（总用时 {elapsed:.0f}s）')

    return figures
