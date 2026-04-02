# -*- coding: utf-8 -*-
"""
ind_23 - 大师选股策略
原始文件: 技术模型/基于大师选股策略/选股逻辑.ipynb
集成9+位投资大师的经典选股策略，基于最新市场数据筛选当前符合条件的股票

支持两种使用方式：
1. 通过 indicator 通用页面（generate() 一次性运行）
2. 通过专属页面（load_data() + run_strategy() 两阶段交互）
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from datetime import datetime
from .shared_utils import pro
from .db_utils import get_data_by_sql, get_pivot_data
from ..config import DB_ROOT

# ── 全局数据缓存（避免重复加载） ──
_cache = {
    'data': None,
    'stock_info': None,
    'loaded_at': None,
}
_CACHE_TTL = 3600  # 缓存1小时

INDICATOR_META = {
    'id': 'ind_23_master_strategies',
    'name': '大师选股策略',
    'group': '策略模型',
    'description': '集成三元价值、Greenblatt神奇公式、David Dodd、Graham估值、Benjamin Graham、'
                   'Burton Malkiel、Richard & Martin、Okumus集中投资、Davis加速增长等9+位投资大师的'
                   '经典选股策略，基于最新财务数据与估值数据筛选当前符合条件的优质标的',
    'input_type': 'none',
    'default_code': '',
    'requires_db': True,
    'slow': True,
    'chart_count': 10,
    'chart_descriptions': [
        '三元价值策略 — PE<30%分位 + PB<10%分位 + 股息率>90%分位',
        'Greenblatt神奇公式 — ROIC持续优秀 + EP+ROIC综合排名',
        'David Dodd投资理念 — 大市值+低杠杆+高ROE+正现金流+利润增长+高ROIC',
        'Graham估值公式 — 内在价值 = 净利润 × (8.5 + 2×增长率)',
        'Benjamin Graham选股 — PE<均值+PB<3+流动比率>1.1+5年分红+利润增长TOP',
        'Burton Malkiel理念 — 大市值+PE<20%分位+5年利润增长TOP10%',
        'Richard & Martin价值法则 — 高股息+低负债+利润增长+低PE+高现金流',
        'Okumus集中投资 — 营收/利润/现金流/净资产增长+高ROE+低估值',
        'Davis加速增长策略 — 季度利润加速增长+营收持续增长',
        '策略筛选结果综合对比',
    ],
}

# 策略名称映射
STRATEGY_NAMES = {
    'sanyuan': '三元价值策略',
    'greenblatt': 'Greenblatt神奇公式',
    'dodd': 'David Dodd投资理念',
    'graham_formula': 'Graham估值公式',
    'graham_select': 'Benjamin Graham选股',
    'malkiel': 'Burton Malkiel理念',
    'richard_martin': 'Richard & Martin价值法则',
    'okumus': 'Okumus集中投资',
    'davis': 'Davis加速增长策略',
}

STRATEGY_ORDER = list(STRATEGY_NAMES.keys())

STRATEGY_DESCRIPTIONS = {
    'sanyuan': 'PE<30%分位 + PB<10%分位 + 股息率>90%分位，按股息率排名取Top20',
    'greenblatt': '排除银行，ROIC持续3年>70%分位，按EP+ROIC综合排名取Top20',
    'dodd': '大市值 + 低杠杆 + 高ROE + 正现金流 + 3年利润增长 + 2年ROIC均值优秀',
    'graham_formula': '内在价值 = 年报净利润 × (8.5 + 2×增长率)，取内在价值最高的20只',
    'graham_select': 'PE<均值 + PB<3 + 流动比率>1.1 + 5年分红 + 5年利润增长TOP10%',
    'malkiel': '大市值 + PE<20%分位 + 5年利润增长TOP10%',
    'richard_martin': '市值>10%分位 + 股息率>1.5倍均值 + 低负债 + 利润增长 + 低PE + 高现金流',
    'okumus': '营收/利润/现金流/净资产增长均>80%市场均值 + ROE>均值 + PB<4 + PE<30',
    'davis': '季度净利润>300万 + 利润同比增速加速 + 营收持续增长，按营收加速排名Top25',
}


def load_data(progress_callback=None):
    """
    第一阶段：加载全市场数据（耗时操作）。
    结果缓存在内存中，1小时内复用。

    Returns:
        (data_dict, stock_info_df)
    """
    now = time.time()

    # 检查缓存
    if (_cache['data'] is not None and _cache['loaded_at'] is not None
            and now - _cache['loaded_at'] < _CACHE_TTL):
        if progress_callback:
            progress_callback(6, 6, '数据已缓存，直接使用')
        return _cache['data'], _cache['stock_info']

    t0 = time.time()
    file_path = f'sqlite:////{DB_ROOT}/'

    def _p(step, msg):
        if progress_callback:
            elapsed = time.time() - t0
            progress_callback(step, 6, f'{msg} (已用时 {elapsed:.0f}s)')

    _p(0, '获取全市场股票列表...')
    dd1 = pro.query('stock_basic', exchange='SSE', list_status='L',
                    fields='ts_code,symbol,name,area,industry,list_date')
    dd2 = pro.query('stock_basic', exchange='SZSE', list_status='L',
                    fields='ts_code,symbol,name,area,industry,list_date')
    stock_info = pd.concat([dd1, dd2]).reset_index(drop=True)
    codes = list(set(stock_info['ts_code']))

    _p(1, '加载 dailybasic 数据...')
    data = _load_all_data(file_path, codes, progress_callback, t0)

    # 存入缓存
    _cache['data'] = data
    _cache['stock_info'] = stock_info
    _cache['loaded_at'] = time.time()

    elapsed = time.time() - t0
    if progress_callback:
        progress_callback(6, 6, f'数据加载完成（总用时 {elapsed:.0f}s）')

    return data, stock_info


def run_strategy(strategy_id, data=None, stock_info=None):
    """
    第二阶段：运行单个策略（秒级完成），返回 JSON 可序列化的结果。

    Returns:
        dict: {'columns': [...], 'data': [...], 'count': N, 'strategy_name': '...'}
    """
    if data is None or stock_info is None:
        data, stock_info = _cache['data'], _cache['stock_info']

    if data is None:
        return {'columns': [], 'data': [], 'count': 0, 'error': '数据未加载'}

    func = STRATEGY_FUNCS.get(strategy_id)
    if not func:
        return {'columns': [], 'data': [], 'count': 0, 'error': f'未知策略: {strategy_id}'}

    try:
        result_df = func(data, stock_info)
    except Exception as e:
        return {'columns': [], 'data': [], 'count': 0, 'error': str(e)}

    if result_df is None or result_df.empty:
        return {
            'columns': [],
            'data': [],
            'count': 0,
            'strategy_name': STRATEGY_NAMES.get(strategy_id, strategy_id),
        }

    result_df.index = range(1, len(result_df) + 1)

    # 数值列四舍五入
    for col in result_df.columns:
        if result_df[col].dtype in [np.float64, np.float32]:
            result_df[col] = result_df[col].apply(
                lambda x: round(float(x), 2) if pd.notna(x) else None)

    # NaN → None
    result_df = result_df.where(pd.notna(result_df), None)

    return {
        'columns': list(result_df.columns),
        'data': result_df.to_dict(orient='records'),
        'count': len(result_df),
        'strategy_name': STRATEGY_NAMES.get(strategy_id, strategy_id),
    }


def get_strategies_info():
    """返回所有策略的元信息（供前端展示）"""
    result = []
    for sid in STRATEGY_ORDER:
        result.append({
            'id': sid,
            'name': STRATEGY_NAMES[sid],
            'description': STRATEGY_DESCRIPTIONS.get(sid, ''),
        })
    return result


def _render_stock_table(df, title, figsize=(22, 14)):
    """将股票推荐列表渲染为matplotlib表格"""
    fig, ax = plt.subplots(figsize=figsize, facecolor='white')
    ax.axis('off')
    ax.set_title(title, fontsize=16, pad=15, fontweight='bold')

    if df.empty:
        ax.text(0.5, 0.5, '当前无符合条件的股票', ha='center', va='center',
                fontsize=18, color='#999', transform=ax.transAxes)
        fig.tight_layout()
        return fig

    display_df = df.copy()
    for col in display_df.columns:
        if display_df[col].dtype in [np.float64, np.float32]:
            display_df[col] = display_df[col].apply(
                lambda x: f'{x:.2f}' if pd.notnull(x) else '')

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


def _load_all_data(file_path, codes, progress_callback=None, t0=0):
    """一次性加载所有策略所需的数据"""
    data = {}

    def _p(step, msg):
        if progress_callback:
            elapsed = time.time() - t0
            progress_callback(step, 6, f'{msg} (已用时 {elapsed:.0f}s)')

    # ── dailybasic ──
    _p(1, '加载 dailybasic 数据（估值指标）...')
    df_basic = get_data_by_sql(
        file_path, 'dailybasic', 'dailybasic', codes,
        'ts_code,trade_date,total_mv,pe_ttm,pb,dv_ttm'
    )
    df_basic = df_basic.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
    df_basic = df_basic.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')
    data['df_basic'] = df_basic

    # 取最新日期的横截面数据
    latest_date = df_basic['trade_date'].max()
    latest_basic = df_basic[df_basic['trade_date'] == latest_date].copy()
    latest_basic = latest_basic.drop_duplicates(subset=['ts_code'], keep='last')
    latest_basic = latest_basic.set_index('ts_code')
    data['latest_basic'] = latest_basic
    data['latest_date'] = latest_date

    # ── fina_indicator ──
    _p(2, '加载 fina_indicator 数据（财务指标）...')
    df_fina = get_data_by_sql(
        file_path, 'fina_indicator', 'fina_indicator', codes, '*'
    )
    df_fina = df_fina.sort_values(['ts_code', 'end_date']).reset_index(drop=True)
    df_fina = df_fina.drop_duplicates(subset=['ts_code', 'end_date'], keep='last')
    data['df_fina'] = df_fina

    # 取每只股票最新一期财务数据
    latest_fina = df_fina.drop_duplicates(subset=['ts_code'], keep='last').set_index('ts_code')
    data['latest_fina'] = latest_fina

    # 取每只股票最近N期的历史数据（用于滚动计算）
    # 按end_date降序，每只股取最近20期
    fina_recent = df_fina.sort_values(['ts_code', 'end_date'], ascending=[True, False])
    fina_recent = fina_recent.groupby('ts_code').head(20)
    data['fina_recent'] = fina_recent

    # ── 资产负债表 ──
    _p(3, '加载资产负债表数据...')
    try:
        df_balance = get_data_by_sql(
            file_path, 'financial_data', '资产负债表', codes,
            'ts_code,f_ann_date,end_date,report_type,'
            'total_liab,lt_borr,total_cur_assets,total_cur_liab,'
            'total_hldr_eqy_inc_min_int'
        )
        df_balance = df_balance.sort_values(['ts_code', 'end_date']).reset_index(drop=True)
        df_balance = df_balance.drop_duplicates(subset=['ts_code', 'end_date'], keep='last')
        latest_balance = df_balance.drop_duplicates(subset=['ts_code'], keep='last').set_index('ts_code')
        data['latest_balance'] = latest_balance
        data['df_balance'] = df_balance
    except Exception as e:
        print(f'加载资产负债表失败: {e}')
        data['latest_balance'] = pd.DataFrame()
        data['df_balance'] = pd.DataFrame()

    # ── 利润表（单季度，report_type=2）──
    _p(4, '加载利润表数据...')
    try:
        df_income = get_data_by_sql(
            file_path, 'financial_data', '利润表', codes, '*'
        )
        # 单季度数据
        df_income_q = df_income[df_income['report_type'] == '2'].copy()
        df_income_q = df_income_q.sort_values(['ts_code', 'end_date']).reset_index(drop=True)
        df_income_q = df_income_q.drop_duplicates(subset=['ts_code', 'end_date'], keep='last')
        data['df_income_q'] = df_income_q

        # 年报数据（report_type=1，end_date以1231结尾）
        df_income_y = df_income[df_income['report_type'] == '1'].copy()
        df_income_y = df_income_y[df_income_y['end_date'].astype(str).str.endswith('1231') |
                                  df_income_y['end_date'].apply(
                                      lambda x: str(x)[-4:] == '1231' if pd.notna(x) else False)]
        df_income_y = df_income_y.sort_values(['ts_code', 'end_date']).reset_index(drop=True)
        df_income_y = df_income_y.drop_duplicates(subset=['ts_code', 'end_date'], keep='last')
        data['df_income_y'] = df_income_y
    except Exception as e:
        print(f'加载利润表失败: {e}')
        data['df_income_q'] = pd.DataFrame()
        data['df_income_y'] = pd.DataFrame()

    # ── 分红数据 ──
    _p(5, '加载分红数据...')
    try:
        df_div = get_data_by_sql(
            file_path, 'dividend', 'dividend', codes, '*'
        )
        df_div = df_div.sort_values(['ts_code', 'end_date']).reset_index(drop=True)
        df_div = df_div.drop_duplicates(subset=['ts_code', 'end_date'], keep='last')
        data['df_dividend'] = df_div
    except Exception as e:
        print(f'加载分红数据失败: {e}')
        data['df_dividend'] = pd.DataFrame()

    return data


# ════════════════════════════════════════════════════════
#  策略 1: 三元价值策略
# ════════════════════════════════════════════════════════
def strategy_sanyuan(data, stock_info):
    """
    三一投资管理公司价值选股策略
    PE<30%分位 + PB<10%分位 + 股息率>90%分位，按股息率降序取Top20
    """
    lb = data['latest_basic'].copy()
    lb = lb[lb['pe_ttm'].notna() & lb['pb'].notna() & lb['dv_ttm'].notna()]

    pe_pct = lb['pe_ttm'].rank(pct=True)
    pb_pct = lb['pb'].rank(pct=True)
    dv_pct = lb['dv_ttm'].rank(pct=True)

    mask = (lb['pe_ttm'] > 0) & (pe_pct < 0.3) & (pb_pct < 0.1) & (dv_pct > 0.9)
    result = lb[mask].copy()

    # 按股息率降序排列
    result = result.sort_values('dv_ttm', ascending=False).head(20)
    result = result.reset_index()

    # 合并股票名称
    result = pd.merge(result, stock_info[['ts_code', 'name', 'industry']], on='ts_code', how='left')

    cols = ['ts_code', 'name', 'industry', 'pe_ttm', 'pb', 'dv_ttm', 'total_mv']
    result = result[[c for c in cols if c in result.columns]]
    result.columns = [c.replace('pe_ttm', 'PE').replace('pb', 'PB')
                      .replace('dv_ttm', '股息率').replace('total_mv', '总市值(万)')
                      .replace('ts_code', '代码').replace('name', '名称')
                      .replace('industry', '行业')
                      for c in result.columns]
    return result


# ════════════════════════════════════════════════════════
#  策略 2: Greenblatt 神奇公式
# ════════════════════════════════════════════════════════
def strategy_greenblatt(data, stock_info):
    """
    乔尔·格林布拉特 神奇公式
    排除银行股，ROIC 3年滚动最低值>70%分位，按EP+ROIC综合排名取Top20
    """
    lb = data['latest_basic'].copy()
    lf = data['latest_fina'].copy()
    fina_recent = data['fina_recent'].copy()

    # 排除银行股（行业名含"银行"）
    bank_codes = set(stock_info[stock_info['industry'].str.contains('银行', na=False)]['ts_code'])

    # 计算每只股票近3年（12期季报）ROIC的最低值
    roic_min = {}
    for code, grp in fina_recent.groupby('ts_code'):
        if code in bank_codes:
            continue
        roic_vals = grp['roic'].dropna()
        if len(roic_vals) >= 12:
            roic_min[code] = roic_vals.tail(12).min()
        elif len(roic_vals) >= 4:
            roic_min[code] = roic_vals.min()

    roic_min_s = pd.Series(roic_min)
    roic_min_pct = roic_min_s.rank(pct=True)

    # ROIC 3年滚动最低值 > 70%分位
    qualified = roic_min_pct[roic_min_pct > 0.7].index.tolist()
    qualified = [c for c in qualified if c in lb.index and c in lf.index]

    if not qualified:
        return pd.DataFrame(columns=['代码', '名称', '行业', 'ROIC最低', 'EP', 'ROIC', '综合排名'])

    # EP = 1/PE
    ep = (1 / lb.loc[qualified, 'pe_ttm']).replace([np.inf, -np.inf], np.nan).dropna()
    roic_latest = lf.loc[lf.index.isin(qualified), 'roic'].dropna()
    common = list(set(ep.index) & set(roic_latest.index))

    if not common:
        return pd.DataFrame(columns=['代码', '名称', '行业', 'ROIC最低', 'EP', 'ROIC', '综合排名'])

    ep = ep[common]
    roic_latest = roic_latest[common]
    combined_rank = ep.rank() + roic_latest.rank()
    top20 = combined_rank.nlargest(20).index.tolist()

    result = pd.DataFrame({
        'ts_code': top20,
        'ROIC最低': [roic_min.get(c, np.nan) for c in top20],
        'EP': [ep.get(c, np.nan) for c in top20],
        'ROIC': [roic_latest.get(c, np.nan) for c in top20],
        '综合排名': [combined_rank.get(c, np.nan) for c in top20],
    })
    result = pd.merge(result, stock_info[['ts_code', 'name', 'industry']], on='ts_code', how='left')
    result = result.sort_values('综合排名', ascending=False).reset_index(drop=True)

    cols = ['ts_code', 'name', 'industry', 'ROIC', 'ROIC最低', 'EP', '综合排名']
    result = result[[c for c in cols if c in result.columns]]
    result.columns = [c.replace('ts_code', '代码').replace('name', '名称')
                      .replace('industry', '行业') for c in result.columns]
    return result


# ════════════════════════════════════════════════════════
#  策略 3: David Dodd 投资理念
# ════════════════════════════════════════════════════════
def strategy_dodd(data, stock_info):
    """
    戴维·波伦投资理念
    大市值+低杠杆+高ROE+正现金流+3年利润增长+2年ROIC均值高
    """
    lb = data['latest_basic'].copy()
    lf = data['latest_fina'].copy()
    lb_balance = data['latest_balance']
    fina_recent = data['fina_recent'].copy()

    common = list(set(lb.index) & set(lf.index))
    if lb_balance is not None and not lb_balance.empty:
        common = list(set(common) & set(lb_balance.index))

    if not common:
        return pd.DataFrame()

    lb = lb.loc[lb.index.isin(common)]
    lf = lf.loc[lf.index.isin(common)]

    # con1: 市值 > 市场均值
    mv_mean = lb['total_mv'].mean()
    con1 = lb['total_mv'] > mv_mean

    # con2: 产权比率 < 市场均值（total_liab / total_hldr_eqy_inc_min_int）
    con2 = pd.Series(False, index=common)
    if not lb_balance.empty:
        bal = lb_balance.loc[lb_balance.index.isin(common)]
        equity_ratio = bal['total_liab'] / bal['total_hldr_eqy_inc_min_int'].replace(0, np.nan)
        equity_ratio = equity_ratio.dropna()
        eq_mean = equity_ratio.mean()
        con2.loc[equity_ratio.index] = equity_ratio < eq_mean

    # con3: ROE > 市场均值
    roe = lf['roe'].dropna()
    roe_mean = roe.mean()
    con3 = pd.Series(False, index=common)
    con3.loc[roe.index] = roe > roe_mean

    # con4: CFPS > 0 且 > 2/3 × 市场均值
    cfps = lf['cfps'].dropna()
    cfps_mean = cfps.mean()
    con4 = pd.Series(False, index=common)
    con4.loc[cfps.index] = (cfps > 0) & (cfps > 2/3 * cfps_mean)

    # con5: 近3年扣非净利润增长率均值 > 市场均值
    profit_growth_3yr = {}
    for code, grp in fina_recent.groupby('ts_code'):
        if code not in common:
            continue
        vals = grp['dt_netprofit_yoy'].dropna()
        if len(vals) >= 12:
            profit_growth_3yr[code] = vals.tail(12).mean()
        elif len(vals) >= 4:
            profit_growth_3yr[code] = vals.mean()
    pg3 = pd.Series(profit_growth_3yr)
    pg3_mean = pg3.mean()
    con5 = pd.Series(False, index=common)
    con5.loc[pg3.index] = pg3 > pg3_mean

    # con6: 近2年ROIC均值 > 市场均值
    roic_2yr = {}
    for code, grp in fina_recent.groupby('ts_code'):
        if code not in common:
            continue
        vals = grp['roic'].dropna()
        if len(vals) >= 8:
            roic_2yr[code] = vals.tail(8).mean()
        elif len(vals) >= 4:
            roic_2yr[code] = vals.mean()
    r2 = pd.Series(roic_2yr)
    r2_mean = r2.mean()
    con6 = pd.Series(False, index=common)
    con6.loc[r2.index] = r2 > r2_mean

    mask = con1 & con2 & con3 & con4 & con5 & con6
    selected = mask[mask].index.tolist()

    if not selected:
        return pd.DataFrame(columns=['代码', '名称', '行业', 'ROE', 'ROIC_2Y', 'CFPS', '利润增长_3Y', '总市值(万)'])

    result = pd.DataFrame({
        'ts_code': selected,
        'ROE': [lf.loc[c, 'roe'] if c in lf.index else np.nan for c in selected],
        'ROIC_2Y': [roic_2yr.get(c, np.nan) for c in selected],
        'CFPS': [lf.loc[c, 'cfps'] if c in lf.index else np.nan for c in selected],
        '利润增长_3Y': [profit_growth_3yr.get(c, np.nan) for c in selected],
        '总市值(万)': [lb.loc[c, 'total_mv'] if c in lb.index else np.nan for c in selected],
    })
    result = pd.merge(result, stock_info[['ts_code', 'name', 'industry']], on='ts_code', how='left')
    result = result.sort_values('ROE', ascending=False).head(30).reset_index(drop=True)

    cols = ['ts_code', 'name', 'industry', 'ROE', 'ROIC_2Y', 'CFPS', '利润增长_3Y', '总市值(万)']
    result = result[[c for c in cols if c in result.columns]]
    result.columns = [c.replace('ts_code', '代码').replace('name', '名称')
                      .replace('industry', '行业') for c in result.columns]
    return result


# ════════════════════════════════════════════════════════
#  策略 4: Graham 估值公式
# ════════════════════════════════════════════════════════
def strategy_graham_formula(data, stock_info):
    """
    格雷厄姆估值公式
    内在价值 = 年报净利润 × (8.5 + 2×增长率)
    取内在价值最低的20只（最被低估的）
    """
    df_income_y = data.get('df_income_y', pd.DataFrame())
    if df_income_y.empty:
        return pd.DataFrame(columns=['代码', '名称', '行业', '年报净利润', '增长率斜率', '内在价值'])

    # 需要 n_income_attr_p 列
    income_col = 'n_income_attr_p' if 'n_income_attr_p' in df_income_y.columns else 'n_income'
    if income_col not in df_income_y.columns:
        return pd.DataFrame(columns=['代码', '名称', '行业', '年报净利润', '增长率斜率', '内在价值'])

    results = []
    for code, grp in df_income_y.groupby('ts_code'):
        grp = grp.sort_values('end_date').drop_duplicates(subset=['end_date'], keep='last')
        ni = grp[income_col].values
        if len(ni) < 5:
            continue

        # 取最近5年
        ni = ni[-5:]
        ni_valid = ni[ni != 0]
        if len(ni_valid) < 3:
            continue

        # 计算增长率
        growth_rates = []
        for i in range(1, len(ni)):
            if ni[i-1] != 0 and abs(ni[i-1]) > 1e-6:
                growth_rates.append((ni[i] - ni[i-1]) / abs(ni[i-1]))
        if len(growth_rates) < 2:
            continue

        # 增长率斜率 (EGRO简化)
        egro = np.mean(growth_rates[-3:]) if len(growth_rates) >= 3 else np.mean(growth_rates)

        # 内在价值 = 最新净利润 × (8.5 + 2 × 增长率)
        latest_ni = ni[-1]
        intrinsic_value = latest_ni * (8.5 + 2 * egro)

        results.append({
            'ts_code': code,
            '年报净利润': latest_ni / 1e4,  # 转换为万元
            '增长率': egro,
            '内在价值': intrinsic_value / 1e4,
        })

    if not results:
        return pd.DataFrame(columns=['代码', '名称', '行业', '年报净利润(万)', '增长率', '内在价值(万)'])

    result = pd.DataFrame(results)
    # 内在价值 > 0，取最高的20只（最被低估）
    result = result[result['内在价值'] > 0]
    result = result.sort_values('内在价值', ascending=False).head(20).reset_index(drop=True)

    result = pd.merge(result, stock_info[['ts_code', 'name', 'industry']], on='ts_code', how='left')
    cols = ['ts_code', 'name', 'industry', '年报净利润', '增长率', '内在价值']
    result = result[[c for c in cols if c in result.columns]]
    result.columns = [c.replace('ts_code', '代码').replace('name', '名称')
                      .replace('industry', '行业')
                      .replace('年报净利润', '年报净利润(万)')
                      .replace('内在价值', '内在价值(万)')
                      for c in result.columns]
    return result


# ════════════════════════════════════════════════════════
#  策略 5: Benjamin Graham 选股策略
# ════════════════════════════════════════════════════════
def strategy_graham_select(data, stock_info):
    """
    格雷厄姆选股策略
    PE<市场均值 + PB<3 + 长期负债/营运资金<5 + 流动比率>1.1 + 5年有分红 + 5年利润增长TOP10%
    """
    lb = data['latest_basic'].copy()
    lf = data['latest_fina'].copy()
    lb_balance = data['latest_balance']
    fina_recent = data['fina_recent'].copy()
    df_dividend = data.get('df_dividend', pd.DataFrame())

    common = list(set(lb.index) & set(lf.index))
    if not common:
        return pd.DataFrame()

    lb = lb.loc[lb.index.isin(common)]
    lf = lf.loc[lf.index.isin(common)]

    # con1: PE < 市场均值（PE>0）
    pe_valid = lb['pe_ttm'][lb['pe_ttm'] > 0]
    pe_mean = pe_valid.mean()
    con1 = (lb['pe_ttm'] > 0) & (lb['pe_ttm'] < pe_mean)

    # con2: PB < 3
    con2 = lb['pb'] < 3

    # con3: 长期负债/营运资金 < 5
    con3 = pd.Series(True, index=common)  # 默认通过
    if not lb_balance.empty:
        bal = lb_balance.loc[lb_balance.index.isin(common)]
        working_capital = bal['total_cur_assets'] - bal['total_cur_liab']
        lt_wc_ratio = bal['lt_borr'] / working_capital.replace(0, np.nan)
        lt_wc_ratio = lt_wc_ratio.dropna()
        valid_ratio = lt_wc_ratio[lt_wc_ratio.notna()]
        con3_mask = valid_ratio < 5
        con3 = pd.Series(False, index=common)
        con3.loc[con3_mask[con3_mask].index] = True
        # 无数据的也通过
        no_data = [c for c in common if c not in valid_ratio.index]
        con3.loc[no_data] = True

    # con4: 流动比率 > 1.1
    cr = lf['current_ratio'].dropna()
    con4 = pd.Series(False, index=common)
    con4.loc[cr.index] = cr > 1.1

    # con5: 5年内有分红
    con5 = pd.Series(False, index=common)
    if not df_dividend.empty:
        div_codes = df_dividend.groupby('ts_code')['cash_div'].sum()
        has_div = div_codes[div_codes > 0].index.tolist()
        con5.loc[con5.index.isin(has_div)] = True

    # con6: 5年扣非净利润增长率均值 >= 90%分位
    profit_growth_5yr = {}
    for code, grp in fina_recent.groupby('ts_code'):
        if code not in common:
            continue
        vals = grp['dt_netprofit_yoy'].dropna()
        if len(vals) >= 4:
            profit_growth_5yr[code] = vals.mean()
    pg5 = pd.Series(profit_growth_5yr)
    pg5_pct = pg5.rank(pct=True)
    con6 = pd.Series(False, index=common)
    con6.loc[pg5_pct[pg5_pct >= 0.9].index] = True

    mask = con1 & con2 & con3 & con4 & con5 & con6
    selected = mask[mask].index.tolist()

    if not selected:
        return pd.DataFrame(columns=['代码', '名称', '行业', 'PE', 'PB', '流动比率', '利润增长均值'])

    result = pd.DataFrame({
        'ts_code': selected,
        'PE': [lb.loc[c, 'pe_ttm'] if c in lb.index else np.nan for c in selected],
        'PB': [lb.loc[c, 'pb'] if c in lb.index else np.nan for c in selected],
        '流动比率': [lf.loc[c, 'current_ratio'] if c in lf.index else np.nan for c in selected],
        '利润增长均值': [profit_growth_5yr.get(c, np.nan) for c in selected],
    })
    result = pd.merge(result, stock_info[['ts_code', 'name', 'industry']], on='ts_code', how='left')
    result = result.sort_values('利润增长均值', ascending=False).head(30).reset_index(drop=True)

    cols = ['ts_code', 'name', 'industry', 'PE', 'PB', '流动比率', '利润增长均值']
    result = result[[c for c in cols if c in result.columns]]
    result.columns = [c.replace('ts_code', '代码').replace('name', '名称')
                      .replace('industry', '行业') for c in result.columns]
    return result


# ════════════════════════════════════════════════════════
#  策略 6: Burton Malkiel 投资理念
# ════════════════════════════════════════════════════════
def strategy_malkiel(data, stock_info):
    """
    柏顿·墨基尔投资理念
    大市值 + PE<20%分位 + 5年扣非净利润增长率均值>=90%分位
    """
    lb = data['latest_basic'].copy()
    fina_recent = data['fina_recent'].copy()

    common = list(lb.index)

    # con1: 市值 > 市场均值
    mv_mean = lb['total_mv'].mean()
    con1 = lb['total_mv'] > mv_mean

    # con2: PE 在20%分位以下
    pe_pct = lb['pe_ttm'].rank(pct=True)
    con2 = pe_pct < 0.2

    # con3: 5年扣非净利润增长率均值 >= 90%分位
    profit_growth_5yr = {}
    for code, grp in fina_recent.groupby('ts_code'):
        if code not in common:
            continue
        vals = grp['dt_netprofit_yoy'].dropna()
        if len(vals) >= 4:
            profit_growth_5yr[code] = vals.mean()
    pg5 = pd.Series(profit_growth_5yr)
    pg5_pct = pg5.rank(pct=True)
    con3 = pd.Series(False, index=common)
    con3.loc[pg5_pct[pg5_pct >= 0.9].index] = True

    mask = con1 & con2 & con3
    selected = mask[mask].index.tolist()

    if not selected:
        return pd.DataFrame(columns=['代码', '名称', '行业', 'PE', '总市值(万)', '利润增长均值'])

    result = pd.DataFrame({
        'ts_code': selected,
        'PE': [lb.loc[c, 'pe_ttm'] if c in lb.index else np.nan for c in selected],
        '总市值(万)': [lb.loc[c, 'total_mv'] if c in lb.index else np.nan for c in selected],
        '利润增长均值': [profit_growth_5yr.get(c, np.nan) for c in selected],
    })
    result = pd.merge(result, stock_info[['ts_code', 'name', 'industry']], on='ts_code', how='left')
    result = result.sort_values('利润增长均值', ascending=False).head(30).reset_index(drop=True)

    cols = ['ts_code', 'name', 'industry', 'PE', '总市值(万)', '利润增长均值']
    result = result[[c for c in cols if c in result.columns]]
    result.columns = [c.replace('ts_code', '代码').replace('name', '名称')
                      .replace('industry', '行业') for c in result.columns]
    return result


# ════════════════════════════════════════════════════════
#  策略 7: Richard & Martin 价值法则
# ════════════════════════════════════════════════════════
def strategy_richard_martin(data, stock_info):
    """
    理查与马文价值导向选股法则
    市值>10%分位 + 股息率>1.5倍均值 + 负债率<均值 + 5年利润增长>均值 + PE<均值 + CFPS>均值
    """
    lb = data['latest_basic'].copy()
    lf = data['latest_fina'].copy()
    fina_recent = data['fina_recent'].copy()

    common = list(set(lb.index) & set(lf.index))
    if not common:
        return pd.DataFrame()

    lb = lb.loc[lb.index.isin(common)]
    lf = lf.loc[lf.index.isin(common)]

    # con1: 市值 > 10%分位
    mv_pct = lb['total_mv'].rank(pct=True)
    con1 = mv_pct > 0.1

    # con2: 股息率 > 1.5 × 市场均值
    dv = lb['dv_ttm'].dropna()
    dv_mean = dv.mean()
    con2 = pd.Series(False, index=common)
    con2.loc[dv.index] = dv > 1.5 * dv_mean

    # con3: 负债率 < 市场均值
    da = lf['debt_to_assets'].dropna()
    da_mean = da.mean()
    con3 = pd.Series(False, index=common)
    con3.loc[da.index] = da < da_mean

    # con4: 5年扣非净利润增长率均值 > 市场均值
    profit_growth_5yr = {}
    for code, grp in fina_recent.groupby('ts_code'):
        if code not in common:
            continue
        vals = grp['dt_netprofit_yoy'].dropna()
        if len(vals) >= 4:
            profit_growth_5yr[code] = vals.mean()
    pg5 = pd.Series(profit_growth_5yr)
    pg5_mean = pg5.mean()
    con4 = pd.Series(False, index=common)
    con4.loc[pg5[pg5 > pg5_mean].index] = True

    # con5: PE < 市场均值
    pe_valid = lb['pe_ttm'][lb['pe_ttm'] > 0]
    pe_mean = pe_valid.mean()
    con5 = (lb['pe_ttm'] > 0) & (lb['pe_ttm'] < pe_mean)

    # con6: CFPS > 市场均值
    cfps = lf['cfps'].dropna()
    cfps_mean = cfps.mean()
    con6 = pd.Series(False, index=common)
    con6.loc[cfps.index] = cfps > cfps_mean

    mask = con1 & con2 & con3 & con4 & con5 & con6
    selected = mask[mask].index.tolist()

    if not selected:
        return pd.DataFrame(columns=['代码', '名称', '行业', 'PE', '股息率', '负债率', 'CFPS', '利润增长'])

    result = pd.DataFrame({
        'ts_code': selected,
        'PE': [lb.loc[c, 'pe_ttm'] if c in lb.index else np.nan for c in selected],
        '股息率': [lb.loc[c, 'dv_ttm'] if c in lb.index else np.nan for c in selected],
        '负债率': [lf.loc[c, 'debt_to_assets'] if c in lf.index else np.nan for c in selected],
        'CFPS': [lf.loc[c, 'cfps'] if c in lf.index else np.nan for c in selected],
        '利润增长': [profit_growth_5yr.get(c, np.nan) for c in selected],
    })
    result = pd.merge(result, stock_info[['ts_code', 'name', 'industry']], on='ts_code', how='left')
    result = result.sort_values('股息率', ascending=False).head(30).reset_index(drop=True)

    cols = ['ts_code', 'name', 'industry', 'PE', '股息率', '负债率', 'CFPS', '利润增长']
    result = result[[c for c in cols if c in result.columns]]
    result.columns = [c.replace('ts_code', '代码').replace('name', '名称')
                      .replace('industry', '行业') for c in result.columns]
    return result


# ════════════════════════════════════════════════════════
#  策略 8: Okumus 集中投资理念
# ════════════════════════════════════════════════════════
def strategy_okumus(data, stock_info):
    """
    阿梅特·欧卡莫斯集中投资理念
    5年营收增长>80%均值 + 5年利润增长>80%均值 + 现金流增长>80%均值
    + 净资产增长>80%均值 + ROE>均值 + PB<4 + PE<30
    """
    lb = data['latest_basic'].copy()
    lf = data['latest_fina'].copy()
    lb_balance = data['latest_balance']
    fina_recent = data['fina_recent'].copy()

    common = list(set(lb.index) & set(lf.index))
    if not common:
        return pd.DataFrame()

    lb = lb.loc[lb.index.isin(common)]
    lf = lf.loc[lf.index.isin(common)]

    # con1: 5年营收增长率均值 > 80% × 市场均值
    or_yoy_avg = {}
    for code, grp in fina_recent.groupby('ts_code'):
        if code not in common:
            continue
        vals = grp['or_yoy'].dropna()
        if len(vals) >= 4:
            or_yoy_avg[code] = vals.mean()
    or_s = pd.Series(or_yoy_avg)
    or_mean = or_s.mean()
    con1 = pd.Series(False, index=common)
    con1.loc[or_s[or_s > 0.8 * or_mean].index] = True

    # con2: 5年扣非利润增长率均值 > 80% × 市场均值
    pg_avg = {}
    for code, grp in fina_recent.groupby('ts_code'):
        if code not in common:
            continue
        vals = grp['dt_netprofit_yoy'].dropna()
        if len(vals) >= 4:
            pg_avg[code] = vals.mean()
    pg_s = pd.Series(pg_avg)
    pg_mean = pg_s.mean()
    con2 = pd.Series(False, index=common)
    con2.loc[pg_s[pg_s > 0.8 * pg_mean].index] = True

    # con3: 现金流增长率 > 80% × 市场均值（用CFPS近2期变化率）
    cfps_growth = {}
    for code, grp in fina_recent.groupby('ts_code'):
        if code not in common:
            continue
        vals = grp.sort_values('end_date')['cfps'].dropna()
        if len(vals) >= 5:
            prev = vals.iloc[-5]
            curr = vals.iloc[-1]
            if abs(prev) > 1e-6:
                cfps_growth[code] = (curr - prev) / abs(prev)
    cg_s = pd.Series(cfps_growth)
    cg_mean = cg_s.mean() if len(cg_s) > 0 else 0
    con3 = pd.Series(False, index=common)
    if len(cg_s) > 0:
        con3.loc[cg_s[cg_s > 0.8 * cg_mean].index] = True

    # con4: 净资产增长率 > 80% × 市场均值
    equity_growth = {}
    if not lb_balance.empty:
        df_bal = data['df_balance']
        for code, grp in df_bal.groupby('ts_code'):
            if code not in common:
                continue
            grp = grp.sort_values('end_date')
            vals = grp['total_hldr_eqy_inc_min_int'].dropna()
            if len(vals) >= 5:
                prev = vals.iloc[-5]
                curr = vals.iloc[-1]
                if abs(prev) > 1e-6:
                    equity_growth[code] = (curr - prev) / abs(prev)
    eg_s = pd.Series(equity_growth)
    eg_mean = eg_s.mean() if len(eg_s) > 0 else 0
    con4 = pd.Series(False, index=common)
    if len(eg_s) > 0:
        con4.loc[eg_s[eg_s > 0.8 * eg_mean].index] = True

    # con5: ROE > 市场均值
    roe = lf['roe'].dropna()
    roe_mean = roe.mean()
    con5 = pd.Series(False, index=common)
    con5.loc[roe[roe > roe_mean].index] = True

    # con6: PB < 4
    con6 = lb['pb'] < 4

    # con7: PE < 30
    con7 = (lb['pe_ttm'] > 0) & (lb['pe_ttm'] < 30)

    mask = con1 & con2 & con3 & con4 & con5 & con6 & con7
    selected = mask[mask].index.tolist()

    if not selected:
        return pd.DataFrame(columns=['代码', '名称', '行业', 'PE', 'PB', 'ROE', '营收增长', '利润增长'])

    result = pd.DataFrame({
        'ts_code': selected,
        'PE': [lb.loc[c, 'pe_ttm'] if c in lb.index else np.nan for c in selected],
        'PB': [lb.loc[c, 'pb'] if c in lb.index else np.nan for c in selected],
        'ROE': [lf.loc[c, 'roe'] if c in lf.index else np.nan for c in selected],
        '营收增长': [or_yoy_avg.get(c, np.nan) for c in selected],
        '利润增长': [pg_avg.get(c, np.nan) for c in selected],
    })
    result = pd.merge(result, stock_info[['ts_code', 'name', 'industry']], on='ts_code', how='left')
    result = result.sort_values('ROE', ascending=False).head(30).reset_index(drop=True)

    cols = ['ts_code', 'name', 'industry', 'PE', 'PB', 'ROE', '营收增长', '利润增长']
    result = result[[c for c in cols if c in result.columns]]
    result.columns = [c.replace('ts_code', '代码').replace('name', '名称')
                      .replace('industry', '行业') for c in result.columns]
    return result


# ════════════════════════════════════════════════════════
#  策略 9: Davis 加速增长策略
# ════════════════════════════════════════════════════════
def strategy_davis(data, stock_info):
    """
    戴维斯策略
    季度净利润>300万 + 利润同比增速>0（连续2期）+ 利润增速加速
    + 营收同比增速>0（连续2期），按营收增速加速排名取Top25
    """
    df_income_q = data.get('df_income_q', pd.DataFrame())
    if df_income_q.empty:
        return pd.DataFrame(columns=['代码', '名称', '行业', '季度净利润(万)', '利润同比增速',
                                      '利润增速加速', '营收同比增速', '营收增速加速'])

    income_col = 'n_income' if 'n_income' in df_income_q.columns else None
    revenue_col = 'revenue' if 'revenue' in df_income_q.columns else None
    if not income_col or not revenue_col:
        return pd.DataFrame()

    results = []
    for code, grp in df_income_q.groupby('ts_code'):
        grp = grp.sort_values('end_date').drop_duplicates(subset=['end_date'], keep='last')
        ni = grp[income_col].values
        rev = grp[revenue_col].values

        if len(ni) < 6:
            continue

        # 取最近几期
        ni = ni.astype(float)
        rev = rev.astype(float)

        # 同比增速（当期 vs 4期前，对应同一季度）
        if len(ni) < 5:
            continue

        ni_yoy = (ni[-1] - ni[-5]) / abs(ni[-5]) if abs(ni[-5]) > 1e-6 else 0
        ni_yoy_prev = (ni[-2] - ni[-6]) / abs(ni[-6]) if len(ni) >= 6 and abs(ni[-6]) > 1e-6 else 0
        ni_accel = (ni_yoy - ni_yoy_prev) / abs(ni_yoy_prev) if abs(ni_yoy_prev) > 1e-6 else 0

        rev_yoy = (rev[-1] - rev[-5]) / abs(rev[-5]) if abs(rev[-5]) > 1e-6 else 0
        rev_yoy_prev = (rev[-2] - rev[-6]) / abs(rev[-6]) if len(rev) >= 6 and abs(rev[-6]) > 1e-6 else 0
        rev_accel = (rev_yoy - rev_yoy_prev) / abs(rev_yoy_prev) if abs(rev_yoy_prev) > 1e-6 else 0

        # 筛选条件
        # con1: 季度净利润 > 300万
        if ni[-1] <= 3000000:
            continue
        # con2: 利润同比增速 > 0
        if ni_yoy <= 0:
            continue
        # con3: 上期利润同比增速 > 0
        if ni_yoy_prev <= 0:
            continue
        # con4: 利润增速加速 > 0
        if ni_accel <= 0:
            continue
        # con5: 营收同比增速 > 0
        if rev_yoy <= 0:
            continue
        # con6: 上期营收同比增速 > 0
        if rev_yoy_prev <= 0:
            continue

        results.append({
            'ts_code': code,
            '季度净利润(万)': round(ni[-1] / 1e4, 2),
            '利润同比增速': round(ni_yoy * 100, 2),
            '利润增速加速': round(ni_accel * 100, 2),
            '营收同比增速': round(rev_yoy * 100, 2),
            '营收增速加速': round(rev_accel * 100, 2),
        })

    if not results:
        return pd.DataFrame(columns=['代码', '名称', '行业', '季度净利润(万)', '利润同比增速(%)',
                                      '利润增速加速(%)', '营收同比增速(%)', '营收增速加速(%)'])

    result = pd.DataFrame(results)
    # 按营收增速加速排序，取Top25
    result = result.sort_values('营收增速加速', ascending=False).head(25).reset_index(drop=True)

    result = pd.merge(result, stock_info[['ts_code', 'name', 'industry']], on='ts_code', how='left')
    cols = ['ts_code', 'name', 'industry', '季度净利润(万)', '利润同比增速',
            '利润增速加速', '营收同比增速', '营收增速加速']
    result = result[[c for c in cols if c in result.columns]]
    result.columns = [c.replace('ts_code', '代码').replace('name', '名称')
                      .replace('industry', '行业')
                      .replace('利润同比增速', '利润同比增速(%)')
                      .replace('利润增速加速', '利润增速加速(%)')
                      .replace('营收同比增速', '营收同比增速(%)')
                      .replace('营收增速加速', '营收增速加速(%)')
                      for c in result.columns]
    return result


# ════════════════════════════════════════════════════════
#  策略函数映射
# ════════════════════════════════════════════════════════
STRATEGY_FUNCS = {
    'sanyuan': strategy_sanyuan,
    'greenblatt': strategy_greenblatt,
    'dodd': strategy_dodd,
    'graham_formula': strategy_graham_formula,
    'graham_select': strategy_graham_select,
    'malkiel': strategy_malkiel,
    'richard_martin': strategy_richard_martin,
    'okumus': strategy_okumus,
    'davis': strategy_davis,
}


# ════════════════════════════════════════════════════════
#  历史回测
# ════════════════════════════════════════════════════════

# 回测结果缓存
_backtest_cache = {
    'result': None,
    'computed_at': None,
}


def _make_snapshot(data, stock_info, as_of_date):
    """
    构造截至 as_of_date 的数据快照，用于在历史某一天"回放"策略。
    as_of_date: pandas Timestamp
    """
    snapshot = {}

    # ── dailybasic: 取 trade_date <= as_of_date 的最新一天 ──
    df_basic = data['df_basic']
    mask_basic = df_basic['trade_date'] <= as_of_date
    df_basic_sub = df_basic[mask_basic]
    if df_basic_sub.empty:
        return None
    latest_td = df_basic_sub['trade_date'].max()
    latest_basic = df_basic_sub[df_basic_sub['trade_date'] == latest_td].copy()
    latest_basic = latest_basic.drop_duplicates(subset=['ts_code'], keep='last')
    latest_basic = latest_basic.set_index('ts_code')
    snapshot['df_basic'] = df_basic_sub
    snapshot['latest_basic'] = latest_basic
    snapshot['latest_date'] = latest_td

    # ── fina_indicator: end_date <= as_of_date ──
    df_fina = data['df_fina']
    # end_date 列可能是字符串或datetime，统一比较
    if 'end_date' in df_fina.columns:
        as_of_str = as_of_date.strftime('%Y%m%d') if hasattr(as_of_date, 'strftime') else str(as_of_date)
        mask_fina = df_fina['end_date'].astype(str).str[:8] <= as_of_str[:8]
        df_fina_sub = df_fina[mask_fina].copy()
    else:
        df_fina_sub = df_fina.copy()
    snapshot['df_fina'] = df_fina_sub
    latest_fina = df_fina_sub.drop_duplicates(subset=['ts_code'], keep='last').set_index('ts_code')
    snapshot['latest_fina'] = latest_fina
    fina_recent = df_fina_sub.sort_values(['ts_code', 'end_date'], ascending=[True, False])
    fina_recent = fina_recent.groupby('ts_code').head(20)
    snapshot['fina_recent'] = fina_recent

    # ── 资产负债表 ──
    df_balance = data.get('df_balance', pd.DataFrame())
    if not df_balance.empty and 'end_date' in df_balance.columns:
        mask_bal = df_balance['end_date'].astype(str).str[:8] <= as_of_str[:8]
        df_bal_sub = df_balance[mask_bal].copy()
        snapshot['df_balance'] = df_bal_sub
        snapshot['latest_balance'] = df_bal_sub.drop_duplicates(
            subset=['ts_code'], keep='last').set_index('ts_code')
    else:
        snapshot['df_balance'] = pd.DataFrame()
        snapshot['latest_balance'] = pd.DataFrame()

    # ── 利润表 ──
    for key in ['df_income_q', 'df_income_y']:
        df_inc = data.get(key, pd.DataFrame())
        if not df_inc.empty and 'end_date' in df_inc.columns:
            mask_inc = df_inc['end_date'].astype(str).str[:8] <= as_of_str[:8]
            snapshot[key] = df_inc[mask_inc].copy()
        else:
            snapshot[key] = pd.DataFrame()

    # ── 分红数据 ──
    df_div = data.get('df_dividend', pd.DataFrame())
    if not df_div.empty and 'end_date' in df_div.columns:
        mask_div = df_div['end_date'].astype(str).str[:8] <= as_of_str[:8]
        snapshot['df_dividend'] = df_div[mask_div].copy()
    else:
        snapshot['df_dividend'] = pd.DataFrame()

    return snapshot


def _get_rebalance_dates(df_basic, years=3):
    """获取过去N年每季度末的调仓日（使用实际交易日）"""
    all_dates = df_basic['trade_date'].drop_duplicates().sort_values()
    end_date = all_dates.max()
    start_date = end_date - pd.DateOffset(years=years)
    all_dates = all_dates[all_dates >= start_date]

    # 按季度分组，取每季度最后一个交易日
    date_df = pd.DataFrame({'date': all_dates})
    date_df['quarter'] = date_df['date'].dt.to_period('Q')
    rebal_dates = date_df.groupby('quarter')['date'].max().values
    rebal_dates = pd.to_datetime(rebal_dates)
    return sorted(rebal_dates)


def backtest_all_strategies(progress_callback=None):
    """
    对9大策略进行历史回测（3年，每季度调仓），返回累计净值曲线。

    Returns:
        dict: {
            'dates': ['2023-03-31', ...],
            'strategies': {
                'sanyuan': {'name': '三元价值策略', 'values': [1.0, ...]},
                ...
            },
            'benchmark': {'name': '沪深300', 'values': [1.0, ...]}
        }
    """
    data = _cache.get('data')
    stock_info = _cache.get('stock_info')
    if data is None:
        return {'error': '数据未加载，请先加载全市场数据'}

    t0 = time.time()
    total_steps = 5  # 大致：加载价格(1) + 准备调仓日(1) + 回测循环(1) + 计算净值(1) + 完成(1)

    def _p(step, msg):
        if progress_callback:
            elapsed = time.time() - t0
            progress_callback(step, total_steps, f'{msg} (已用时 {elapsed:.0f}s)')

    # ── 1. 加载日收益率数据 ──
    _p(1, '加载日线数据计算收益率...')
    file_path = f'sqlite:////{DB_ROOT}/'

    # 3年前的日期
    rebal_dates = _get_rebalance_dates(data['df_basic'], years=3)
    if len(rebal_dates) < 3:
        return {'error': '历史数据不足，无法回测'}

    backtest_start = rebal_dates[0]
    start_date_str = (backtest_start - pd.DateOffset(days=10)).strftime('%Y%m%d')

    codes = list(set(stock_info['ts_code']))

    # 加载日线K线和复权因子
    df_kline = get_data_by_sql(
        file_path, 'daily_kline', 'daily_kline', codes,
        'ts_code,trade_date,close', start_date=start_date_str
    )
    df_adj = get_data_by_sql(
        file_path, 'daily_adj', 'daily_adj', codes,
        'ts_code,trade_date,adj_factor', start_date=start_date_str
    )

    # 整理为 pivot 表
    df_kline = df_kline.sort_values(['ts_code', 'trade_date']).drop_duplicates(
        subset=['ts_code', 'trade_date'], keep='last')
    df_adj = df_adj.sort_values(['ts_code', 'trade_date']).drop_duplicates(
        subset=['ts_code', 'trade_date'], keep='last')

    df_close = get_pivot_data(df_kline, 'close')
    df_adj_piv = get_pivot_data(df_adj, 'adj_factor')

    # 复权收盘价
    common_cols = df_close.columns.intersection(df_adj_piv.columns)
    df_close = df_close[common_cols]
    df_adj_piv = df_adj_piv[common_cols]
    df_close_adj = (df_close * df_adj_piv / df_adj_piv.iloc[-1]).round(4)

    # 日收益率
    df_daily_ret = df_close_adj.pct_change()

    # ── 2. 获取基准（沪深300） ──
    _p(2, '加载沪深300基准数据...')
    try:
        df_bench = pro.index_daily(
            ts_code='000300.SH', start_date=start_date_str,
            fields='trade_date,close'
        )
        df_bench['trade_date'] = pd.to_datetime(df_bench['trade_date'])
        df_bench = df_bench.sort_values('trade_date').set_index('trade_date')
        bench_ret = df_bench['close'].pct_change()
    except Exception:
        bench_ret = pd.Series(dtype=float)

    # ── 3. 回测主循环 ──
    _p(3, '运行策略回测...')
    # 初始化各策略的每日收益序列
    strategy_daily_returns = {sid: [] for sid in STRATEGY_ORDER}
    all_trade_dates = df_daily_ret.index.sort_values()

    for i, rebal_date in enumerate(rebal_dates[:-1]):
        next_rebal = rebal_dates[i + 1]

        # 构造该调仓日的数据快照
        snapshot = _make_snapshot(data, stock_info, rebal_date)
        if snapshot is None:
            continue

        # 该区间的交易日
        period_mask = (all_trade_dates > rebal_date) & (all_trade_dates <= next_rebal)
        period_dates = all_trade_dates[period_mask]
        if len(period_dates) == 0:
            continue

        # 运行每个策略
        for sid in STRATEGY_ORDER:
            func = STRATEGY_FUNCS[sid]
            try:
                result_df = func(snapshot, stock_info)
            except Exception:
                result_df = None

            if result_df is not None and not result_df.empty and '代码' in result_df.columns:
                selected_codes = result_df['代码'].tolist()
                # 只取在收益率矩阵中存在的股票
                valid_codes = [c for c in selected_codes if c in df_daily_ret.columns]
                if valid_codes:
                    # 等权持有：每日收益 = 选中股票日收益的均值
                    period_ret = df_daily_ret.loc[period_dates, valid_codes].mean(axis=1)
                    strategy_daily_returns[sid].append(period_ret)
                else:
                    strategy_daily_returns[sid].append(pd.Series(0.0, index=period_dates))
            else:
                strategy_daily_returns[sid].append(pd.Series(0.0, index=period_dates))

        if progress_callback:
            pct = 3 + (i + 1) / len(rebal_dates)
            progress_callback(int(pct), total_steps,
                              f'回测进度 {i + 1}/{len(rebal_dates) - 1} (已用时 {time.time() - t0:.0f}s)')

    # ── 4. 合并日收益，计算累计净值 ──
    _p(4, '计算累计净值...')

    # 所有交易日（回测区间内）
    bt_start = rebal_dates[0]
    bt_end = rebal_dates[-1]
    bt_mask = (all_trade_dates > bt_start) & (all_trade_dates <= bt_end)
    bt_dates = all_trade_dates[bt_mask]

    result = {
        'dates': [d.strftime('%Y-%m-%d') for d in bt_dates],
        'strategies': {},
        'benchmark': {'name': '沪深300', 'values': []},
    }

    for sid in STRATEGY_ORDER:
        if strategy_daily_returns[sid]:
            combined = pd.concat(strategy_daily_returns[sid]).fillna(0)
            # 去重（如有交叉日期）
            combined = combined[~combined.index.duplicated(keep='first')]
            combined = combined.reindex(bt_dates, fill_value=0)
            nav = (1 + combined).cumprod()
            result['strategies'][sid] = {
                'name': STRATEGY_NAMES[sid],
                'values': [round(float(v), 4) for v in nav.values],
            }
        else:
            result['strategies'][sid] = {
                'name': STRATEGY_NAMES[sid],
                'values': [1.0] * len(bt_dates),
            }

    # 基准净值
    if not bench_ret.empty:
        bench_ret_aligned = bench_ret.reindex(bt_dates, fill_value=0)
        bench_nav = (1 + bench_ret_aligned).cumprod()
        result['benchmark']['values'] = [round(float(v), 4) for v in bench_nav.values]
    else:
        result['benchmark']['values'] = [1.0] * len(bt_dates)

    # 缓存结果
    _backtest_cache['result'] = result
    _backtest_cache['computed_at'] = time.time()

    elapsed = time.time() - t0
    _p(5, f'回测完成（共耗时 {elapsed:.0f}s）')

    return result


def get_backtest_result():
    """获取缓存的回测结果"""
    return _backtest_cache.get('result')


# ════════════════════════════════════════════════════════
#  主入口
# ════════════════════════════════════════════════════════
def generate(query='all', progress_callback=None, **kwargs):
    """
    大师选股策略主函数。

    Args:
        query: 策略选择，'all' 运行全部，或单个策略ID如 'sanyuan'、'greenblatt' 等
        progress_callback: 进度回调函数
    """
    figures = []
    t0 = time.time()
    file_path = f'sqlite:////{DB_ROOT}/'

    def _progress(step, total, msg):
        elapsed = time.time() - t0
        if progress_callback:
            progress_callback(step, total, f'{msg} (已用时 {elapsed:.0f}s)')

    # 确定运行哪些策略
    if query == 'all' or not query:
        strategies_to_run = STRATEGY_ORDER
    else:
        q = query.strip().lower()
        if q in STRATEGY_FUNCS:
            strategies_to_run = [q]
        else:
            strategies_to_run = STRATEGY_ORDER

    total_steps = len(strategies_to_run) + 3  # 数据加载 + 各策略 + 综合对比

    _progress(0, total_steps, '获取全市场股票列表...')

    # ── 获取股票列表 ──
    dd1 = pro.query('stock_basic', exchange='SSE', list_status='L',
                    fields='ts_code,symbol,name,area,industry,list_date')
    dd2 = pro.query('stock_basic', exchange='SZSE', list_status='L',
                    fields='ts_code,symbol,name,area,industry,list_date')
    stock_info = pd.concat([dd1, dd2]).reset_index(drop=True)
    codes = list(set(stock_info['ts_code']))

    _progress(1, total_steps, '加载数据库（dailybasic、fina_indicator、资产负债表、利润表、分红）...')

    # ── 一次性加载所有数据 ──
    data = _load_all_data(file_path, codes, progress_callback, t0)

    _progress(2, total_steps, '数据加载完成，开始运行策略...')

    # ── 运行各策略 ──
    summary_rows = []

    for i, sid in enumerate(strategies_to_run):
        step = 3 + i
        sname = STRATEGY_NAMES.get(sid, sid)
        _progress(step, total_steps, f'正在运行: {sname}...')

        try:
            func = STRATEGY_FUNCS[sid]
            result_df = func(data, stock_info)

            n_selected = len(result_df) if result_df is not None else 0
            summary_rows.append({
                '策略': sname,
                '筛选数量': n_selected,
                '状态': '完成',
            })

            # 渲染表格
            latest_date = data.get('latest_date', datetime.now())
            date_str = latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date)[:10]
            title = f'{sname}（{date_str}，筛出{n_selected}只）'

            if result_df is not None and not result_df.empty:
                result_df.index = range(1, len(result_df) + 1)

            fig = _render_stock_table(result_df if result_df is not None else pd.DataFrame(), title)
            figures.append((fig, sname))

        except Exception as e:
            print(f'策略 {sname} 执行失败: {e}')
            import traceback
            traceback.print_exc()
            summary_rows.append({
                '策略': sname,
                '筛选数量': 0,
                '状态': f'失败: {str(e)[:50]}',
            })
            fig = _render_stock_table(
                pd.DataFrame(), f'{sname}（执行失败: {str(e)[:80]}）')
            figures.append((fig, sname))

    # ── 综合对比图 ──
    if len(strategies_to_run) > 1:
        _progress(total_steps - 1, total_steps, '生成综合对比...')
        summary_df = pd.DataFrame(summary_rows)

        fig_summary, ax = plt.subplots(figsize=(14, 8), facecolor='white')
        ax.axis('off')
        latest_date = data.get('latest_date', datetime.now())
        date_str = latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date)[:10]
        ax.set_title(f'大师选股策略综合对比（{date_str}）', fontsize=18, fontweight='bold', pad=15)

        if not summary_df.empty:
            table = ax.table(
                cellText=summary_df.values,
                colLabels=summary_df.columns,
                cellLoc='center',
                loc='center',
            )
            table.auto_set_font_size(False)
            table.set_fontsize(12)
            table.scale(1, 2.0)

            for (row, col), cell in table.get_celld().items():
                if row == 0:
                    cell.set_facecolor('#2E86C1')
                    cell.set_text_props(color='white', fontweight='bold', fontsize=14)
                elif row % 2 == 0:
                    cell.set_facecolor('#D6EAF8')

        fig_summary.tight_layout()
        figures.append((fig_summary, '策略综合对比'))

    elapsed = time.time() - t0
    total_selected = sum(r['筛选数量'] for r in summary_rows)
    if progress_callback:
        progress_callback(total_steps, total_steps,
                          f'完成（总用时 {elapsed:.0f}s，共运行{len(strategies_to_run)}个策略，'
                          f'合计筛出{total_selected}只股票）')

    return figures
