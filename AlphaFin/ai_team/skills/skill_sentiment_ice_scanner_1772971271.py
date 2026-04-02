"""
自动生成的技能: sentiment_ice_scanner
描述: 情绪冰点扫描器：基于价格位置(较20日高点跌>10%)、筹码结构(胜率<40%)、量能特征(成交量/20日均量<0.35)三维度识别情绪冰点信号
创建者: quant
"""

import pandas as pd
import numpy as np

# input_data 应包含: df (DataFrame with columns: close, vol, ma20_close, ma20_vol, chip_win_rate, chip_cost)
df = input_data.get('df', pd.DataFrame())

if df.empty:
    result = {'status': 'error', 'message': 'No data provided'}
else:
    # 计算三维度情绪冰点指标
    # 1. 价格位置: 较20日高点跌幅
    df['price_drop'] = (df['ma20_close'] - df['close']) / df['ma20_close']
    
    # 2. 量能特征: 成交量/20日均量
    df['vol_ratio'] = df['vol'] / df['ma20_vol']
    
    # 3. 筹码胜率 (直接从输入获取)
    df['chip_condition'] = df['chip_win_rate'] < 0.40
    
    # 冰点信号判定
    df['ice_signal'] = (
        (df['price_drop'] > 0.10) &  # 价格跌>10%
        (df['chip_condition']) &      # 胜率<40%
        (df['vol_ratio'] < 0.35)      # 缩量>65%
    )
    
    # 输出结果
    latest = df.iloc[-1] if len(df) > 0 else None
    result = {
        'status': 'success',
        'latest_date': df.index[-1] if len(df) > 0 else None,
        'price_drop_pct': round(latest['price_drop'] * 100, 2) if latest is not None else None,
        'vol_ratio': round(latest['vol_ratio'], 3) if latest is not None else None,
        'chip_win_rate': round(latest['chip_win_rate'] * 100, 2) if latest is not None else None,
        'ice_signal': bool(latest['ice_signal']) if latest is not None else False,
        'total_signals': int(df['ice_signal'].sum())
    }