"""
自动生成的技能: asset_injection_score_simple
描述: 资产注入预期量化评估模型（简化版）：综合基本面、技术面、传闻可信度、市场环境，计算风险调整后期望收益和交易建议
创建者: quant
"""

# 资产注入预期量化评估模型（简化版，不使用外部库）

def calculate_score(input_data):
    """
    资产注入预期量化评估模型
    返回: 综合评分、风险收益比、交易建议
    """
    
    # 提取输入数据
    current_price = input_data.get('current_price', 4.23)
    price_20d_high = input_data.get('price_20d_high', 4.70)
    price_20d_low = input_data.get('price_20d_low', 4.01)
    pe_ttm = input_data.get('pe_ttm', 19.19)
    pb = input_data.get('pb', 1.04)
    roe = input_data.get('roe', 4.30)
    revenue_growth = input_data.get('revenue_growth', -10.28)
    turnover_rate = input_data.get('turnover_rate', 1.49)
    total_mv = input_data.get('total_mv', 68)
    
    market_heat_percentile = input_data.get('market_heat_percentile', 75)
    small_vs_large = input_data.get('small_vs_large', -0.49)
    
    rumor_confidence = input_data.get('rumor_confidence', 0.5)
    historical_success_rate = input_data.get('historical_success_rate', 0.35)
    shareholder_change = input_data.get('shareholder_change', 1)
    official_denial = input_data.get('official_denial', 0)
    
    # ===== 1. 基本面评分 (0-100) =====
    roe_score = max(0, min(100, (roe / 8) * 100))
    revenue_score = max(0, min(100, 50 + revenue_growth * 3))
    pb_score = max(0, min(100, 100 - (pb - 1) * 50))
    fundamental_score = roe_score * 0.3 + revenue_score * 0.3 + pb_score * 0.4
    
    # ===== 2. 技术面评分 (0-100) =====
    price_range = price_20d_high - price_20d_low
    if price_range > 0:
        price_position = (current_price - price_20d_low) / price_range
    else:
        price_position = 0.5
    price_score = 100 - price_position * 100
    turnover_score = max(0, min(100, 100 - turnover_rate * 20))
    technical_score = price_score * 0.6 + turnover_score * 0.4
    
    # ===== 3. 资产注入预期评分 (0-100) =====
    base_success_prob = historical_success_rate
    if shareholder_change == 1:
        base_success_prob += 0.15
    if official_denial == 1:
        base_success_prob -= 0.30
    if roe < 5:
        base_success_prob += 0.10
    
    adjusted_success_prob = base_success_prob * rumor_confidence
    adjusted_success_prob = max(0, min(0.8, adjusted_success_prob))
    injection_score = adjusted_success_prob * 100
    
    # ===== 4. 风险收益测算 =====
    if total_mv < 100:
        upside_potential = 0.30
    else:
        upside_potential = 0.20
    
    downside_risk = 0.15
    expected_return = adjusted_success_prob * upside_potential - (1 - adjusted_success_prob) * downside_risk
    
    if downside_risk > 0 and (1 - adjusted_success_prob) > 0:
        risk_reward_ratio = (adjusted_success_prob * upside_potential) / ((1 - adjusted_success_prob) * downside_risk)
    else:
        risk_reward_ratio = 999
    
    # ===== 5. 市场环境调整 =====
    if market_heat_percentile > 80:
        market_adjustment = 0.7
    elif market_heat_percentile < 30:
        market_adjustment = 1.3
    else:
        market_adjustment = 1.0
    
    if small_vs_large < -0.3:
        style_adjustment = 0.8
    elif small_vs_large > 0.3:
        style_adjustment = 1.2
    else:
        style_adjustment = 1.0
    
    # ===== 6. 综合评分与建议 =====
    composite_score = (
        fundamental_score * 0.25 +
        technical_score * 0.25 +
        injection_score * 0.35 +
        100 * expected_return * 0.15
    ) * market_adjustment * style_adjustment
    
    if composite_score >= 70 and expected_return > 0.05:
        recommendation = "BUY"
        position_suggestion = min(0.3, 0.1 + (composite_score - 70) / 100)
    elif composite_score >= 50 and expected_return > 0:
        recommendation = "HOLD"
        position_suggestion = 0.0
    else:
        recommendation = "AVOID"
        position_suggestion = 0.0
    
    result = {
        'composite_score': round(composite_score, 2),
        'fundamental_score': round(fundamental_score, 2),
        'technical_score': round(technical_score, 2),
        'injection_score': round(injection_score, 2),
        'success_probability': round(adjusted_success_prob * 100, 2),
        'expected_return': round(expected_return * 100, 2),
        'risk_reward_ratio': round(risk_reward_ratio, 2),
        'recommendation': recommendation,
        'position_suggestion': round(position_suggestion, 3),
        'market_adjustment': round(market_adjustment, 2),
        'style_adjustment': round(style_adjustment, 2)
    }
    
    return result

result = calculate_score(input_data)