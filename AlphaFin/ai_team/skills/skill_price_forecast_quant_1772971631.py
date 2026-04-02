"""
自动生成的技能: price_forecast_quant
描述: 基于多因子模型的个股上半年价格区间预测技能，综合技术面、估值面、基本面、资金面因子
创建者: quant
"""

import pandas as pd
import numpy as np
import math
from datetime import datetime

# input_data 包含：kline_data, valuation_data, financial_data, market_indicators

def calculate_technical_score(kline_data):
    """技术面评分 (0-10)"""
    if kline_data is None or len(kline_data) < 20:
        return 5.0
    
    df = pd.DataFrame(kline_data)
    current_price = df['close'].iloc[0]
    
    # 20日区间位置
    high_20 = df['high'].head(20).max()
    low_20 = df['low'].head(20).min()
    price_position = (current_price - low_20) / (high_20 - low_20) if high_20 > low_20 else 0.5
    
    # 均线系统
    ma5 = df['close'].head(5).mean()
    ma20 = df['close'].head(20).mean()
    ma_trend = 1 if current_price > ma20 else 0
    
    # 量能
    vol_avg = df['vol'].head(20).mean()
    current_vol = df['vol'].iloc[0]
    vol_ratio = current_vol / vol_avg if vol_avg > 0 else 1
    
    # 评分
    score = 5.0
    score += price_position * 2  # 区间位置贡献0-2分
    score += ma_trend * 1.5  # 均线趋势贡献0-1.5分
    score += min(vol_ratio, 1.5) * 1  # 量能贡献0-1.5分
    score += 0.5 if current_price > ma5 else 0  # 短期强势加分
    
    return min(max(score, 0), 10)

def calculate_valuation_score(valuation_data):
    """估值面评分 (0-10)"""
    if valuation_data is None:
        return 5.0
    
    pe_ttm = valuation_data.get('pe_ttm', 25)
    pb = valuation_data.get('pb', 1.5)
    
    # PE评分 (周期股PE 15-25倍为合理区间)
    if pe_ttm < 15:
        pe_score = 8
    elif pe_ttm < 25:
        pe_score = 6
    elif pe_ttm < 35:
        pe_score = 4
    else:
        pe_score = 2
    
    # PB评分 (1-1.5倍为合理)
    if pb < 1.0:
        pb_score = 8
    elif pb < 1.5:
        pb_score = 6
    elif pb < 2.0:
        pb_score = 4
    else:
        pb_score = 2
    
    return (pe_score + pb_score) / 2

def calculate_fundamental_score(financial_data):
    """基本面评分 (0-10)"""
    if financial_data is None or len(financial_data) == 0:
        return 5.0
    
    latest = financial_data[0] if isinstance(financial_data, list) else financial_data
    
    roe = latest.get('roe', 5)
    revenue_yoy = latest.get('operating_revenue_yoy', 0)
    net_margin = latest.get('net_profit_margin', 5)
    
    # ROE评分
    if roe > 15:
        roe_score = 9
    elif roe > 10:
        roe_score = 7
    elif roe > 5:
        roe_score = 5
    elif roe > 0:
        roe_score = 3
    else:
        roe_score = 1
    
    # 营收增速评分
    if revenue_yoy > 20:
        rev_score = 9
    elif revenue_yoy > 10:
        rev_score = 7
    elif revenue_yoy > 0:
        rev_score = 5
    elif revenue_yoy > -10:
        rev_score = 3
    else:
        rev_score = 1
    
    # 净利率评分
    if net_margin > 15:
        margin_score = 9
    elif net_margin > 10:
        margin_score = 7
    elif net_margin > 5:
        margin_score = 5
    elif net_margin > 0:
        margin_score = 3
    else:
        margin_score = 1
    
    return (roe_score + rev_score + margin_score) / 3

def calculate_capital_flow_score(market_indicators):
    """资金面评分 (0-10)"""
    if market_indicators is None:
        return 5.0
    
    # 融资余额变化
    financing_growth = market_indicators.get('financing_growth', 0)
    turnover_rate = market_indicators.get('turnover_rate', 2)
    
    # 融资评分
    if financing_growth > 50:
        fin_score = 3  # 过度杠杆风险
    elif financing_growth > 20:
        fin_score = 5
    elif financing_growth > 0:
        fin_score = 7
    else:
        fin_score = 6
    
    # 换手率评分 (1-3%为健康)
    if 1 <= turnover_rate <= 3:
        turn_score = 8
    elif 0.5 <= turnover_rate < 1 or 3 < turnover_rate <= 5:
        turn_score = 6
    else:
        turn_score = 4
    
    return (fin_score + turn_score) / 2

def calculate_price_forecast(current_price, composite_score, market_heat):
    """计算价格区间预测"""
    
    # 基础波动率假设 (半年)
    base_volatility = 0.25  # 25%半年波动
    
    # 根据综合评分调整
    if composite_score > 7:
        upside_factor = 1.20
        downside_factor = 0.85
    elif composite_score > 6:
        upside_factor = 1.15
        downside_factor = 0.88
    elif composite_score > 5:
        upside_factor = 1.10
        downside_factor = 0.90
    elif composite_score > 4:
        upside_factor = 1.05
        downside_factor = 0.92
    else:
        upside_factor = 1.00
        downside_factor = 0.95
    
    # 根据市场热度调整
    if market_heat > 80:
        upside_factor *= 0.9  # 过热时上行空间受限
        downside_factor *= 1.05  # 下行风险加大
    elif market_heat < 20:
        upside_factor *= 1.1  # 过冷时上行空间大
        downside_factor *= 0.95
    
    upside_price = current_price * upside_factor
    downside_price = current_price * downside_factor
    
    return {
        'current_price': current_price,
        'upside_target': round(upside_price, 2),
        'downside_support': round(downside_price, 2),
        'upside_potential': round((upside_factor - 1) * 100, 1),
        'downside_risk': round((1 - downside_factor) * 100, 1),
        'risk_reward_ratio': round((upside_factor - 1) / (1 - downside_factor), 2)
    }

# 主执行逻辑
kline_data = input_data.get('kline_data', [])
valuation_data = input_data.get('valuation_data', {})
financial_data = input_data.get('financial_data', [])
market_indicators = input_data.get('market_indicators', {})
market_heat = input_data.get('market_heat', 50)

# 计算各维度评分
tech_score = calculate_technical_score(kline_data)
val_score = calculate_valuation_score(valuation_data)
fund_score = calculate_fundamental_score(financial_data)
cap_score = calculate_capital_flow_score(market_indicators)

# 综合评分 (权重: 技术25%, 估值20%, 基本面35%, 资金面20%)
composite_score = (
    tech_score * 0.25 + 
    val_score * 0.20 + 
    fund_score * 0.35 + 
    cap_score * 0.20
)

# 获取当前价格
current_price = valuation_data.get('close', 4.62) if valuation_data else 4.62

# 计算价格预测
forecast = calculate_price_forecast(current_price, composite_score, market_heat)

# 生成结果
result = {
    'composite_score': round(composite_score, 2),
    'dimension_scores': {
        'technical': round(tech_score, 2),
        'valuation': round(val_score, 2),
        'fundamental': round(fund_score, 2),
        'capital_flow': round(cap_score, 2)
    },
    'price_forecast': forecast,
    'rating': '强买' if composite_score >= 8 else '买入' if composite_score >= 7 else '中性偏多' if composite_score >= 6 else '中性' if composite_score >= 5 else '中性偏空' if composite_score >= 4 else '卖出',
    'confidence': 0.75 if len(kline_data) >= 60 else 0.65
}