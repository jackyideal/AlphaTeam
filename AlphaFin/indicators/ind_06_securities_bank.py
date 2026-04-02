"""
ind_06 - 证券与银行剪刀差
原始文件: 各种指标/证券 与 银行 差值/证券与银行剪刀差….ipynb
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from .shared_utils import pro

INDICATOR_META = {
    'id': 'ind_06_securities_bank',
    'name': '券商-银行风格剪刀差',
    'group': '市场结构指标',
    'description': '追踪证券、银行、半导体等行业板块的累计收益轨迹与剪刀差走势，叠加科技/顺周期/逆周期三大行业簇的轮动节奏，辅助判断市场风格切换',
    'input_type': 'none',
    'default_code': '',
    'requires_db': False,
    'slow': True,
    'chart_count': 7,
    'chart_descriptions': [
        '银行、证券板块与上证指数的累计收益走势对比，三者分化反映资金风格偏好',
        '银行-证券累计收益剪刀差走势，差值扩大预示银行风格占优，反之证券更强',
        '证券与半导体板块的累计收益差异，成长与周期风格的切换指标',
        '证券板块与引力指标的叠加分析，判断证券板块偏离均值的程度',
        '行业大类（金融/消费/科技/周期）累计收益对比，宏观轮动视角',
        '科技/逆周期/顺周期三大簇累计收益对比，中观行业簇的轮动节奏',
        '科技-顺周期累计收益差值，正值表示科技风格占优，拐点预示风格切换',
    ],
}


def generate(start_date='20200101', progress_callback=None, **kwargs):
    figures = []
    total = 7

    if progress_callback:
        progress_callback(0, total, '获取行业指数数据...')

    # 获取申万行业分类
    df_sw = pro.index_classify(level='L2', src='SW2014')
    sw_dict = dict(zip(df_sw['industry_name'], df_sw['index_code']))

    # 获取证券、银行、半导体
    def fetch_sw(code, name):
        df = pro.sw_daily(ts_code=code)
        df = df.sort_values(by=['trade_date']).reset_index(drop=True)
        df.index = pd.to_datetime(df['trade_date'])
        df.rename(columns={'close': name}, inplace=True)
        return df[[name]]

    证券 = fetch_sw('801193.SI', '证券')
    银行 = fetch_sw('801780.SI', '银行')
    半导体 = fetch_sw('801081.SI', '半导体')

    # 上证指数
    df2 = pro.index_daily(ts_code='000001.SH', start_date='20010101')
    df2 = df2.sort_values(by=['trade_date']).reset_index(drop=True)
    df2.index = pd.to_datetime(df2['trade_date'])
    df2 = df2[['close']].rename(columns={'close': '上证指数'})

    # 合并
    merged_df = pd.merge(银行, 证券, on='trade_date', how='inner')
    merged_df = pd.merge(merged_df, 半导体, on='trade_date', how='inner')
    merged_df = pd.merge(merged_df, df2, on='trade_date', how='inner')
    merged_df = merged_df[merged_df.index >= start_date[:4] + '-01-01']

    基准价格 = merged_df.iloc[0]
    累计收益 = (merged_df - 基准价格) / 基准价格 * 100
    累计收益 = 累计收益.round(2)
    累计收益['银行-证券'] = 累计收益['银行'] - 累计收益['证券']
    累计收益['证券-半导体'] = 累计收益['证券'] - 累计收益['半导体']
    累计收益['证券-上证指数'] = 累计收益['证券'] - 累计收益['上证指数']
    累计收益['银行-上证指数'] = 累计收益['银行'] - 累计收益['上证指数']

    if progress_callback:
        progress_callback(1, total, '图1: 累计收益...')

    # ── 图1: 银行、证券、上证指数累计收益 ──
    fig1, ax = plt.subplots(figsize=(20, 8), facecolor='white')
    ax.plot(累计收益.index, 累计收益['银行'], label='银行累计收益', linewidth=2)
    ax.plot(累计收益.index, 累计收益['证券'], label='证券累计收益', linewidth=2)
    ax.plot(累计收益.index, 累计收益['上证指数'], label='上证指数累计收益', linewidth=2)
    ax.set_title('银行、证券和上证指数累计收益', fontsize=16)
    ax.set_xlabel('交易日期', fontsize=14)
    ax.set_ylabel('累计收益 (%)', fontsize=14)
    ax.legend(fontsize=12)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=14)
    fig1.tight_layout()
    figures.append((fig1, '银行、证券和上证指数累计收益'))

    if progress_callback:
        progress_callback(2, total, '图2: 剪刀差...')

    # ── 图2: 银行-证券 和 证券-上证指数 差异 ──
    fig2, ax = plt.subplots(figsize=(20, 8), facecolor='white')
    ax.plot(累计收益.index, 累计收益['银行-证券'], label='银行-证券', linewidth=2)
    ax.plot(累计收益.index, 累计收益['证券-上证指数'], label='证券-上证指数', linewidth=2)
    ax.set_title('银行-证券 和 证券-上证指数 累计收益差异', fontsize=16)
    ax.set_xlabel('交易日期', fontsize=14)
    ax.set_ylabel('累计收益差异 (%)', fontsize=14)
    ax.legend(fontsize=12)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=14)
    fig2.tight_layout()
    figures.append((fig2, '银行-证券 剪刀差'))

    if progress_callback:
        progress_callback(3, total, '图3: 半导体差异...')

    # ── 图3: 证券-半导体 差异 ──
    fig3, ax = plt.subplots(figsize=(20, 8), facecolor='white')
    ax.plot(累计收益.index, 累计收益['证券-半导体'], label='证券-半导体', linewidth=2)
    ax.plot(累计收益.index, 累计收益['证券-上证指数'], label='证券-上证指数', linewidth=2)
    ax.set_title('证券-半导体 和 证券-上证指数 累计收益差异', fontsize=16)
    ax.set_xlabel('交易日期', fontsize=14)
    ax.set_ylabel('累计收益差异 (%)', fontsize=14)
    ax.legend(fontsize=12)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=14)
    fig3.tight_layout()
    figures.append((fig3, '证券-半导体 累计收益差异'))

    if progress_callback:
        progress_callback(4, total, '图4: 证券均值回归...')

    # ── 图4: 证券与引力指标（均值回归） ──
    证券df = merged_df[['证券']].copy()
    SMA = 20
    证券df['SMA'] = 证券df['证券'].rolling(SMA).mean()
    证券df['均值回归'] = 证券df['证券'] - 证券df['SMA']
    证券df['mean'] = 证券df['均值回归'].mean()
    证券df['均值回归+1std'] = 证券df['均值回归'].mean() + 证券df['均值回归'].std()
    证券df['均值回归-1std'] = 证券df['均值回归'].mean() - 1.2 * 证券df['均值回归'].std()

    fig4, ax1 = plt.subplots(figsize=(20, 8), facecolor='white')
    ax1.plot(证券df.index, 证券df['均值回归'], color='orange', label='引力指标')
    ax1.plot(证券df.index, 证券df['mean'], linestyle='--', label='均值')
    ax1.plot(证券df.index, 证券df['均值回归+1std'], linestyle='--', label='+1std')
    ax1.plot(证券df.index, 证券df['均值回归-1std'], linestyle='--', label='-1std')
    ax1.set_ylabel('引力指标', fontsize=20)
    ax1.legend(loc='upper left', fontsize=14)
    ax1.tick_params(labelsize=14)

    ax2 = ax1.twinx()
    ax2.plot(证券df.index, 证券df['证券'], color='IndianRed', linewidth=2, label='证券')
    ax2.set_ylabel('证券', fontsize=20)
    ax2.legend(loc='upper right', fontsize=14)
    ax2.tick_params(labelsize=14)

    ax1.set_title('证券与引力指标', fontsize=22)
    ax1.grid(alpha=0.3)
    fig4.tight_layout()
    figures.append((fig4, '证券与引力指标'))

    if progress_callback:
        progress_callback(5, total, '图5-7: 行业大类轮动...')

    # ── 图5-7: 行业大类轮动分析 ──
    try:
        df_sw_l1 = pro.index_classify(level='L1', src='SW2014')
        sw_dict_l1 = dict(zip(df_sw_l1['industry_name'], df_sw_l1['index_code']))

        category_mapping = {
            '科技': ['电子', '计算机', '通信', '传媒', '国防军工'],
            '金融': ['银行', '非银金融', '综合'],
            '消费': ['家用电器', '纺织服装', '食品饮料', '休闲服务', '商业贸易'],
            '顺周期': ['采掘', '化工', '钢铁', '有色金属', '建筑材料', '电气设备',
                      '机械设备', '汽车', '轻工制造', '建筑装饰', '农林牧渔'],
            '逆周期': ['医药生物', '公用事业', '交通运输', '房地产'],
        }

        all_data = {}
        for industry, code in sw_dict_l1.items():
            try:
                df = pro.sw_daily(ts_code=code, start_date=start_date)
                df = df.sort_values('trade_date').set_index('trade_date')
                df.index = pd.to_datetime(df.index)
                cum_ret = (df['close'] / df['close'].iloc[0]).rename(industry)
                all_data[industry] = cum_ret
                time.sleep(0.5)
            except Exception:
                pass

        cumulative_df = pd.DataFrame(all_data)

        category_returns = {}
        for category, industries in category_mapping.items():
            valid_cols = [ind for ind in industries if ind in cumulative_df.columns]
            if valid_cols:
                category_returns[category] = cumulative_df[valid_cols].mean(axis=1)

        # 上证指数基准
        df_sh = pro.index_daily(ts_code='000001.SH', start_date=start_date)
        df_sh = df_sh.sort_values('trade_date').set_index('trade_date')
        df_sh.index = pd.to_datetime(df_sh.index)
        sh_cum = (df_sh['close'] / df_sh['close'].iloc[0]).rename('上证指数')

        final_df = pd.DataFrame(category_returns).join(sh_cum).dropna()

        # ── 图5: 行业大类累计收益 ──
        fig5, ax = plt.subplots(figsize=(20, 10), facecolor='white')
        for col in final_df.columns:
            lw = 3 if col == '上证指数' else 2
            ax.plot(final_df.index, final_df[col], label=col, linewidth=lw)
        ax.set_title('行业大类指数 vs 上证指数 累计收益对比', fontsize=18)
        ax.set_xlabel('日期', fontsize=14)
        ax.set_ylabel('累计收益倍数', fontsize=14)
        ax.legend(fontsize=14)
        ax.grid(alpha=0.3)
        ax.tick_params(labelsize=14)
        fig5.tight_layout()
        figures.append((fig5, '行业大类累计收益对比'))

        if progress_callback:
            progress_callback(6, total, '图6: 科技vs顺周期...')

        # ── 图6: 科技/逆周期/顺周期 ──
        if '科技' in final_df.columns and '顺周期' in final_df.columns:
            fig6, ax = plt.subplots(figsize=(16, 8), facecolor='white')
            ax.plot(final_df.index, final_df['科技'], label='科技', linewidth=2)
            if '逆周期' in final_df.columns:
                ax.plot(final_df.index, final_df['逆周期'], label='逆周期', linewidth=2)
            ax.plot(final_df.index, final_df['顺周期'], label='顺周期', linewidth=2)
            ax.set_title('科技/逆周期/顺周期 累计收益', fontsize=14)
            ax.legend(fontsize=12)
            ax.grid(alpha=0.3)
            ax.tick_params(labelsize=14)
            fig6.tight_layout()
            figures.append((fig6, '科技/逆周期/顺周期累计收益'))

            # ── 图7: 科技-顺周期差值 ──
            diff = final_df['科技'] - final_df['顺周期']
            fig7, ax = plt.subplots(figsize=(16, 8), facecolor='white')
            ax.plot(diff.index, diff, label='科技-顺周期差', linewidth=2, color='#d62728')
            ax.set_title('科技 - 顺周期 累计收益差值', fontsize=14)
            ax.legend(fontsize=12)
            ax.grid(alpha=0.3)
            ax.tick_params(labelsize=14)
            fig7.tight_layout()
            figures.append((fig7, '科技-顺周期累计收益差值'))

    except Exception as e:
        print(f'行业轮动分析失败: {e}')

    if progress_callback:
        progress_callback(total, total, '完成')

    return figures
