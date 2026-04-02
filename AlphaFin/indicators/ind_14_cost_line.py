"""
ind_14 - 个股加权成本线
原始文件: 各种指标2/个股加权成本线/成本价均线.ipynb
说明：改为批量获取 cyq_perf + 本地缓存回退，避免逐日调用导致长时间无结果
"""
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from matplotlib import font_manager
from AlphaFin.services.stock_service import get_cyq_perf, get_daily_data

INDICATOR_META = {
    'id': 'ind_14_cost_line',
    'name': '加权持仓成本压力带',
    'group': '资金面指标',
    'description': '基于筹码分布理论构建的股民胜率因子，通过加权成本线反映市场整体持仓盈亏状态，胜率因子突破阈值可作为趋势反转的辅助确认信号',
    'input_type': 'stock',
    'default_code': '600425.SH',
    'requires_db': False,
    'slow': True,
    'chart_count': 1,
    'chart_descriptions': [
        '股民胜率因子与股价走势叠加，胜率因子反映市场整体持仓盈亏比例，突破阈值可作为趋势反转的辅助确认',
    ],
}

_FONT_READY = False


def _ensure_cn_font():
    """
    独立设置中文字体，避免依赖 shared_utils（其会引入额外依赖）。
    """
    global _FONT_READY
    if _FONT_READY:
        return
    try:
        candidates = [
            'PingFang SC', 'Heiti SC', 'Hiragino Sans GB', 'Songti SC',
            'Arial Unicode MS', 'SimHei', 'Microsoft YaHei',
            'Noto Sans CJK SC', 'WenQuanYi Zen Hei'
        ]
        available = {f.name for f in font_manager.fontManager.ttflist}
        selected = None
        for name in candidates:
            if name in available:
                selected = name
                break
        if selected:
            matplotlib.rcParams['font.sans-serif'] = [selected, 'DejaVu Sans']
        else:
            # 保底：至少不报错；若无中文字体则中文可能显示方块
            matplotlib.rcParams['font.sans-serif'] = ['DejaVu Sans']
        matplotlib.rcParams['axes.unicode_minus'] = False
    except Exception:
        pass
    _FONT_READY = True


def generate(ts_code='600425.SH', start_date='20201208', progress_callback=None, **kwargs):
    _ensure_cn_font()
    figures = []
    total = 3
    ts_code = str(ts_code or '600425.SH').strip().upper()
    start_date = str(start_date or '20201208').replace('-', '')
    if len(start_date) != 8 or not start_date.isdigit():
        start_date = '20201208'

    if progress_callback:
        progress_callback(0, total, '获取筹码分布数据...')

    cyq = get_cyq_perf(ts_code=ts_code, start_date=start_date)
    dates = cyq.get('dates') or []
    winner_rate = cyq.get('winner_rate') or []
    if not dates or not winner_rate:
        if progress_callback:
            progress_callback(total, total, '无可用筹码分布数据（请检查代码或数据源）')
        return figures

    wr = pd.DataFrame({
        'trade_date': pd.to_datetime(dates),
        'winner_rate': pd.to_numeric(winner_rate, errors='coerce')
    }).dropna(subset=['trade_date']).drop_duplicates(subset=['trade_date'], keep='last')
    wr = wr.sort_values('trade_date').set_index('trade_date')

    if progress_callback:
        progress_callback(1, total, '获取股价数据...')

    daily = get_daily_data(ts_code=ts_code, start_date=start_date)
    price_dates = daily.get('dates') or []
    ohlc = daily.get('ohlc') or []
    closes = []
    for row in ohlc:
        try:
            closes.append(float(row[3]))
        except Exception:
            closes.append(None)
    price_df = pd.DataFrame({
        'trade_date': pd.to_datetime(price_dates),
        'close': closes
    }).dropna(subset=['trade_date', 'close']).drop_duplicates(subset=['trade_date'], keep='last')
    price_df = price_df.sort_values('trade_date').set_index('trade_date')

    if price_df.empty:
        if progress_callback:
            progress_callback(total, total, '无可用股价数据（请检查代码或数据源）')
        return figures

    if progress_callback:
        progress_callback(2, total, '生成图表...')

    # ── 图1: 股民胜率因子 vs 股价 ──
    aligned = price_df.join(wr[['winner_rate']], how='inner')
    if aligned.empty:
        if progress_callback:
            progress_callback(total, total, '股价与筹码数据日期未对齐，无法绘图')
        return figures

    window = 20 if len(aligned) >= 20 else max(5, min(20, len(aligned)))
    aligned['winner_rate_smooth'] = aligned['winner_rate'].rolling(window=window, min_periods=max(3, window // 3)).mean()

    smooth = aligned['winner_rate_smooth'].dropna()
    if smooth.empty:
        smooth = aligned['winner_rate'].dropna()
    if smooth.empty:
        if progress_callback:
            progress_callback(total, total, '胜率数据为空，无法绘图')
        return figures

    wr_mean = float(smooth.mean())
    wr_std = float(smooth.std()) if len(smooth) > 1 else 0.0
    upper = wr_mean + wr_std
    lower = wr_mean - wr_std + 2.5

    fig, ax1 = plt.subplots(figsize=(20, 8), facecolor='white')
    ax1.plot(aligned.index, aligned['close'], label=ts_code, color='IndianRed', linewidth=3.2)
    ax1.legend(loc='upper right', fontsize=15)
    ax1.set_ylabel(ts_code, fontsize=20)
    ax1.tick_params(labelsize=15)

    ax2 = ax1.twinx()
    ax2.plot(aligned.index, aligned['winner_rate_smooth'], label='胜率(%d日均)' % window, color='orange', linewidth=2)
    ax2.plot(aligned.index, [upper] * len(aligned), color='green', linestyle='--', linewidth=1.8, label='均值+1σ')
    ax2.plot(aligned.index, [wr_mean] * len(aligned), color='red', linestyle='--', linewidth=1.2, label='均值')
    ax2.plot(aligned.index, [lower] * len(aligned), color='green', linestyle='--', linewidth=1.8, label='均值-1σ+2.5')
    ax2.legend(loc='upper left', fontsize=15)
    ax2.tick_params(labelsize=15)

    cyq_src = cyq.get('data_source', 'unknown')
    px_src = daily.get('data_source', 'unknown')
    plt.title('股民胜率因子 与 股价关系图（筹码:%s / 股价:%s）' % (cyq_src, px_src), fontsize=22)
    ax1.grid(alpha=0.3)
    fig.tight_layout()
    figures.append((fig, f'{ts_code}股民胜率因子'))

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
