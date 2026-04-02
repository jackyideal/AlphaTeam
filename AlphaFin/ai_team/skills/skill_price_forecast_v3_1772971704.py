"""
自动生成的技能: price_forecast_v3
描述: 极简版个股价格区间预测技能，无外部依赖
创建者: quant
"""

# input_data 包含：kline_data, valuation_data, financial_data, market_indicators, market_heat

def calculate_technical_score(kline_data):
    if not kline_data or len(kline_data) < 5:
        return 5.0
    
    current_price = kline_data[0]['close']
    highs = [d['high'] for d in kline_data[:20]]
    lows = [d['low'] for d in kline_data[:20]]
    high_20 = max(highs)
    low_20 = min(lows)
    price_position = (current_price - low_20) / (high_20 - low_20) if high_20 > low_20 else 0.5
    
    closes = [d['close'] for d in kline_data[:20]]
    ma20 = sum(closes) / len(closes)
    ma_trend = 1 if current_price > ma20 else 0
    
    vols = [d['vol'] for d in kline_data[:20]]
    vol_avg = sum(vols) / len(vols)
    current_vol = kline_data[0]['vol']
    vol_ratio = current_vol / vol_avg if vol_avg > 0 else 1
    
    score = 5.0
    score += price_position * 2
    score += ma_trend * 1.5
    score += min(vol_ratio, 1.5) * 1
    closes5 = [d['close'] for d in kline_data[:5]]
    ma5 = sum(closes5) / len(closes5)
    score += 0.5 if current_price > ma5 else 0
    
    return min(max(score, 0), 10)

def calculate_valuation_score(valuation_data):
    if not valuation_data:
        return 5.0
    
    pe_ttm = valuation_data.get('pe_ttm', 25)
    pb = valuation_data.get('pb', 1.5)
    
    pe_score = 8 if pe_ttm < 15 else 6 if pe_ttm < 25 else 4 if pe_ttm < 35 else 2
    pb_score = 8 if pb < 1.0 else 6 if pb < 1.5 else 4 if pb < 2.0 else 2
    
    return (pe_score + pb_score) / 2

def calculate_fundamental_score(financial_data):
    if not financial_data or len(financial_data) == 0:
        return 5.0
    
    latest = financial_data[0]
    roe = latest.get('roe', 5)
    revenue_yoy = latest.get('operating_revenue_yoy', 0)
    net_margin = latest.get('net_profit_margin', 5)
    
    roe_score = 9 if roe > 15 else 7 if roe > 10 else 5 if roe > 5 else 3 if roe > 0 else 1
    rev_score = 9 if revenue_yoy > 20 else 7 if revenue_yoy > 10 else 5 if revenue_yoy > 0 else 3 if revenue_yoy > -10 else 1
    margin_score = 9 if net_margin > 15 else 7 if net_margin > 10 else 5 if net_margin > 5 else 3 if net_margin > 0 else 1
    
    return (roe_score + rev_score + margin_score) / 3

def calculate_capital_flow_score(market_indicators):
    if not market_indicators:
        return 5.0
    
    financing_growth = market_indicators.get('financing_growth', 0)
    turnover_rate = market_indicators.get('turnover_rate', 2)
    
    fin_score = 3 if financing_growth > 50 else 5 if financing_growth > 20 else 7 if financing_growth > 0 else 6
    turn_score = 8 if 1 <= turnover_rate <= 3 else 6 if (0.5 <= turnover_rate < 1 or 3 < turnover_rate <= 5) else 4
    
    return (fin_score + turn_score) / 2

def calculate_price_forecast(current_price, composite_score, market_heat):
    if composite_score > 7:
        upside_factor, downside_factor = 1.20, 0.85
    elif composite_score > 6:
        upside_factor, downside_factor = 1.15, 0.88
    elif composite_score > 5:
        upside_factor, downside_factor = 1.10, 0.90
    elif composite_score > 4:
        upside_factor, downside_factor = 1.05, 0.92
    else:
        upside_factor, downside_factor = 1.00, 0.95
    
    if market_heat > 80:
        upside_factor *= 0.9
        downside_factor *= 1.05
    elif market_heat < 20:
        upside_factor *= 1.1
        downside_factor *= 0.95
    
    upside_price = current_price * upside_factor
    downside_price = current_price * downside_factor
    rr = (upside_factor - 1) / (1 - downside_factor) if downside_factor < 1 else 999
    
    return {
        'current_price': current_price,
        'upside_target': round(upside_price, 2),
        'downside_support': round(downside_price, 2),
        'upside_potential': round((upside_factor - 1) * 100, 1),
        'downside_risk': round((1 - downside_factor) * 100, 1),
        'risk_reward_ratio': round(rr, 2)
    }

kline_data = input_data.get('kline_data', [])
valuation_data = input_data.get('valuation_data', {})
financial_data = input_data.get('financial_data', [])
market_indicators = input_data.get('market_indicators', {})
market_heat = input_data.get('market_heat', 50)

tech_score = calculate_technical_score(kline_data)
val_score = calculate_valuation_score(valuation_data)
fund_score = calculate_fundamental_score(financial_data)
cap_score = calculate_capital_flow_score(market_indicators)

composite_score = tech_score * 0.25 + val_score * 0.20 + fund_score * 0.35 + cap_score * 0.20

current_price = valuation_data.get('close', 4.62) if valuation_data else 4.62
forecast = calculate_price_forecast(current_price, composite_score, market_heat)

rating = '强买' if composite_score >= 8 else '买入' if composite_score >= 7 else '中性偏多' if composite_score >= 6 else '中性' if composite_score >= 5 else '中性偏空' if composite_score >= 4 else '卖出'

result = {
    'composite_score': round(composite_score, 2),
    'dimension_scores': {
        'technical': round(tech_score, 2),
        'valuation': round(val_score, 2),
        'fundamental': round(fund_score, 2),
        'capital_flow': round(cap_score, 2)
    },
    'price_forecast': forecast,
    'rating': rating,
    'confidence': 0.75
}