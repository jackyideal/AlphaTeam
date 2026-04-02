"""
ind_20 - 优质股票筛选策略
原始文件: 技术模型/理想选股模型/roe，pb，ps，pb此刻的优质股票.ipynb
基于ROE+PB+PE+PS多因子筛选当前低估优质股票
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from datetime import datetime
from .shared_utils import pro
from .db_utils import get_data_by_sql, get_pivot_data
from ..config import DB_ROOT

INDICATOR_META = {
    'id': 'ind_20_quality_stocks',
    'name': '优质股票筛选',
    'group': '策略模型',
    'description': '基于ROE盈利质量、PB/PE/PS多维估值因子的量化选股模型，筛选兼具高盈利能力与低估值特征的优质标的，输出当期推荐股票排名与估值散点分析',
    'input_type': 'none',
    'default_code': '',
    'requires_db': True,
    'slow': True,
    'chart_count': 4,
    'chart_descriptions': [
        '筛选结果概览统计，展示符合条件的股票总数与各因子阈值设置',
        '推荐股票TOP30排名表，按综合得分排序的最优标的及关键财务指标',
        '推荐股票第31-60名排名表，次优标的池',
        'PE vs PB散点图，所有候选股票的估值分布，识别估值洼地',
    ],
}


def _render_stock_table(df, title, figsize=(20, 12)):
    """将股票推荐列表渲染为matplotlib表格"""
    fig, ax = plt.subplots(figsize=figsize, facecolor='white')
    ax.axis('off')
    ax.set_title(title, fontsize=16, pad=15)

    display_df = df.copy()
    for col in display_df.columns:
        if display_df[col].dtype in [np.float64, np.float32]:
            display_df[col] = display_df[col].apply(lambda x: f'{x:.2f}' if pd.notnull(x) else '')

    table = ax.table(
        cellText=display_df.values,
        colLabels=display_df.columns,
        cellLoc='center',
        loc='center',
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.4)

    for (row, col), cell in table.get_celld().items():
        if row == 0:
            cell.set_facecolor('#4472C4')
            cell.set_text_props(color='white', fontweight='bold', fontsize=10)
        elif row % 2 == 0:
            cell.set_facecolor('#D6E4F0')

    fig.tight_layout()
    return fig


def generate(start_date='20200101', progress_callback=None, **kwargs):
    figures = []
    total = 4
    file_path = f'sqlite:////{DB_ROOT}'
    t0 = time.time()

    def _progress(step, msg):
        elapsed = time.time() - t0
        if progress_callback:
            progress_callback(step, total, f'{msg} (已用时 {elapsed:.0f}s)')

    _progress(0, '获取全市场股票列表...')

    # ── 获取股票列表 ──
    codess = pro.query('stock_basic', exchange='SSE', list_status='L',
                       fields='ts_code,symbol,name,area,industry,list_date')
    codess1 = pro.query('stock_basic', exchange='SZSE', list_status='L',
                        fields='ts_code,symbol,name,area,industry,list_date')
    stock_info = pd.concat([codess, codess1]).reset_index(drop=True)
    codes = list(set(stock_info['ts_code']))

    _progress(0, '加载数据库: dailybasic...')

    # ── 加载dailybasic数据 ──
    df_dailybasic = get_data_by_sql(file_path, 'dailybasic', 'dailybasic', codes, '*')

    _progress(1, '加载数据库: fina_indicator...')

    # ── 加载财务指标数据 ──
    df_fina = get_data_by_sql(file_path, 'fina_indicator', 'fina_indicator', codes, '*')

    _progress(2, '筛选低估值优质股票...')

    # ── 获取最新日期数据 ──
    latest_date = df_dailybasic['trade_date'].max()
    latest_basic = df_dailybasic[df_dailybasic['trade_date'] == latest_date].copy()
    latest_basic = latest_basic.drop_duplicates(subset=['ts_code'], keep='last')

    # ── 计算分位数阈值 ──
    # PE: 取正值中的低分位（便宜）
    pe_valid = latest_basic[latest_basic['pe_ttm'] > 0]['pe_ttm']
    pe_threshold = pe_valid.quantile(0.10)

    # PB: 低分位
    pb_valid = latest_basic[latest_basic['pb'] > 0]['pb']
    pb_threshold = pb_valid.quantile(0.10)

    # PS: 低分位
    if 'ps_ttm' in latest_basic.columns:
        ps_valid = latest_basic[latest_basic['ps_ttm'] > 0]['ps_ttm']
        ps_threshold = ps_valid.quantile(0.10)
        ps_col = 'ps_ttm'
    elif 'ps' in latest_basic.columns:
        ps_valid = latest_basic[latest_basic['ps'] > 0]['ps']
        ps_threshold = ps_valid.quantile(0.10)
        ps_col = 'ps'
    else:
        ps_threshold = None
        ps_col = None

    # ── 筛选低估值股票 ──
    mask = (
        (latest_basic['pe_ttm'] > 0) & (latest_basic['pe_ttm'] <= pe_threshold) &
        (latest_basic['pb'] > 0) & (latest_basic['pb'] <= pb_threshold)
    )
    if ps_col and ps_threshold:
        mask = mask & (latest_basic[ps_col] > 0) & (latest_basic[ps_col] <= ps_threshold)

    value_stocks = latest_basic[mask][['ts_code', 'pe_ttm', 'pb', 'close', 'total_mv']].copy()
    if ps_col:
        value_stocks[ps_col] = latest_basic.loc[value_stocks.index, ps_col]

    # ── 获取最新季度ROE ──
    if 'end_date' in df_fina.columns:
        df_fina_sorted = df_fina.sort_values(['ts_code', 'end_date']).drop_duplicates(
            subset=['ts_code', 'end_date'], keep='last')
        # 取每只股票最新一期
        latest_fina = df_fina_sorted.drop_duplicates(subset=['ts_code'], keep='last')
    else:
        latest_fina = df_fina.drop_duplicates(subset=['ts_code'], keep='last')

    roe_cols = ['ts_code']
    if 'roe' in latest_fina.columns:
        roe_cols.append('roe')
    if 'roe_waa' in latest_fina.columns:
        roe_cols.append('roe_waa')
    roe_data = latest_fina[roe_cols].copy()

    # ROE > 0 筛选
    roe_col = 'roe' if 'roe' in roe_data.columns else 'roe_waa'
    quality_stocks = roe_data[roe_data[roe_col] > 0]

    # ── 合并：低估值 + 正ROE ──
    result = pd.merge(value_stocks, quality_stocks, on='ts_code')

    # 加入股票名称和行业
    result = pd.merge(result, stock_info[['ts_code', 'name', 'industry']], on='ts_code', how='left')

    # 按ROE降序排列
    result = result.sort_values(roe_col, ascending=False).reset_index(drop=True)

    _progress(3, '生成图表...')

    # ── 图1: 筛选结果概览 ──
    fig1, axes = plt.subplots(2, 2, figsize=(16, 10), facecolor='white')
    fig1.suptitle(f'优质低估值股票筛选结果概览（{latest_date.strftime("%Y-%m-%d")}）', fontsize=18)

    # PE分布
    axes[0, 0].hist(result['pe_ttm'], bins=20, color='#2196F3', alpha=0.8, edgecolor='white')
    axes[0, 0].axvline(pe_threshold, color='red', linestyle='--', label=f'阈值={pe_threshold:.1f}')
    axes[0, 0].set_title(f'入选股票PE分布 (共{len(result)}只)', fontsize=13)
    axes[0, 0].legend()

    # PB分布
    axes[0, 1].hist(result['pb'], bins=20, color='#4CAF50', alpha=0.8, edgecolor='white')
    axes[0, 1].axvline(pb_threshold, color='red', linestyle='--', label=f'阈值={pb_threshold:.2f}')
    axes[0, 1].set_title('入选股票PB分布', fontsize=13)
    axes[0, 1].legend()

    # ROE分布
    axes[1, 0].hist(result[roe_col], bins=20, color='#FF9800', alpha=0.8, edgecolor='white')
    axes[1, 0].set_title('入选股票ROE分布', fontsize=13)

    # 行业分布 (TOP10)
    industry_count = result['industry'].value_counts().head(10)
    axes[1, 1].barh(range(len(industry_count)), industry_count.values, color='#9C27B0', alpha=0.8)
    axes[1, 1].set_yticks(range(len(industry_count)))
    axes[1, 1].set_yticklabels(industry_count.index, fontsize=10)
    axes[1, 1].set_title('入选股票行业分布 TOP10', fontsize=13)

    for ax in axes.flat:
        ax.grid(alpha=0.3)
    fig1.tight_layout()
    figures.append((fig1, '筛选结果概览'))

    # ── 图2: 推荐股票列表（TOP 30）──
    display_cols = ['ts_code', 'name', 'industry', 'pe_ttm', 'pb', roe_col, 'close', 'total_mv']
    if ps_col and ps_col in result.columns:
        display_cols.insert(5, ps_col)

    top30 = result[display_cols].head(30).copy()
    top30.columns = [
        c.replace('pe_ttm', 'PE').replace('pb', 'PB').replace('total_mv', '总市值(万)')
        .replace('close', '收盘价').replace('roe', 'ROE').replace('roe_waa', 'ROE')
        .replace('ps_ttm', 'PS').replace('ps', 'PS').replace('ts_code', '代码')
        .replace('name', '名称').replace('industry', '行业')
        for c in top30.columns
    ]
    top30.index = range(1, len(top30) + 1)

    fig2 = _render_stock_table(
        top30,
        f'推荐优质低估值股票 TOP 30（{latest_date.strftime("%Y-%m-%d")}，共筛出{len(result)}只）',
        figsize=(22, 14)
    )
    figures.append((fig2, '推荐股票TOP30'))

    # ── 图3: 推荐股票列表（第31-60名）──
    if len(result) > 30:
        next30 = result[display_cols].iloc[30:60].copy()
        next30.columns = top30.columns
        next30.index = range(31, 31 + len(next30))

        fig3 = _render_stock_table(
            next30,
            f'推荐优质低估值股票 第31-{min(60, len(result))}名',
            figsize=(22, 14)
        )
        figures.append((fig3, '推荐股票31-60'))

    # ── 图4: PE vs PB 散点图 ──
    try:
        fig4, ax = plt.subplots(figsize=(14, 8), facecolor='white')
        scatter = ax.scatter(
            result['pb'], result['pe_ttm'],
            c=result[roe_col], cmap='RdYlGn', s=60, alpha=0.7, edgecolors='grey'
        )
        ax.set_xlabel('PB (市净率)', fontsize=14)
        ax.set_ylabel('PE (市盈率)', fontsize=14)
        ax.set_title(f'入选股票 PE vs PB（颜色=ROE）', fontsize=18)
        ax.grid(alpha=0.3)
        fig4.colorbar(scatter, ax=ax, label='ROE (%)')

        # 标注ROE最高的几只
        top_roe = result.nlargest(5, roe_col)
        for _, row in top_roe.iterrows():
            ax.annotate(row['name'], (row['pb'], row['pe_ttm']),
                        fontsize=9, fontweight='bold',
                        xytext=(5, 5), textcoords='offset points')

        fig4.tight_layout()
        figures.append((fig4, 'PE vs PB散点图'))
    except Exception as e:
        print(f'绘制散点图失败: {e}')

    elapsed = time.time() - t0
    if progress_callback:
        progress_callback(total, total, f'完成（总用时 {elapsed:.0f}s，共筛出{len(result)}只股票）')

    return figures
