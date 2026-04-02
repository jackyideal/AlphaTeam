"""
自动生成的技能: restructuring_risk_v2
描述: 重组预期风险量化分析工具（简化版）
创建者: risk
"""

# input_data 应包含重组分析所需参数
current_price = input_data.get('current_price', 4.54)
current_pe = input_data.get('current_pe', 20.6)
current_pb = input_data.get('current_pb', 1.12)
market_cap = input_data.get('market_cap', 73.0)

industry_avg_pe = input_data.get('industry_avg_pe', 15.0)
industry_avg_pb = input_data.get('industry_avg_pb', 0.85)

target_price_low = input_data.get('target_price_low', 8.60)
target_price_high = input_data.get('target_price_high', 9.20)

margin_balance = input_data.get('margin_balance', 3.99)
circ_mv = input_data.get('circ_mv', 73.66)

pure_business_pb = input_data.get('pure_business_pb', 0.85)

# 计算每股净资产
bvps = current_price / current_pb

# 1. 重组溢价比例（PB法）
pure_business_value = bvps * pure_business_pb
restructuring_premium_pct = (current_price - pure_business_value) / pure_business_value * 100

# PE法计算重组溢价
pure_business_pe_value = current_price * (industry_avg_pe / current_pe)
restructuring_premium_pe_pct = (current_price - pure_business_pe_value) / pure_business_pe_value * 100

# 2. 下行风险
downside_to_pure_pb = (current_price - pure_business_value) / current_price * 100
extreme_downside_pb = 0.6
extreme_downside_price = bvps * extreme_downside_pb
extreme_downside_pct = (current_price - extreme_downside_price) / current_price * 100

upside_low = (target_price_low - current_price) / current_price * 100
upside_high = (target_price_high - current_price) / current_price * 100

# 3. 融资盘分析
margin_ratio = margin_balance / circ_mv * 100
pressure_test_10pct = margin_balance * 0.3
pressure_test_20pct = margin_balance * 0.6

# 4. 仓位建议
restructuring_certainty_score = 40 + 30 + 20 - 15 - 10
base_position = 5
position_ceiling = base_position + (restructuring_certainty_score / 100) * 15
position_ceiling = min(position_ceiling, 20)

stop_loss_price = pure_business_value * 0.9

result = {
    'restructuring_premium_pb_pct': round(restructuring_premium_pct, 1),
    'restructuring_premium_pe_pct': round(restructuring_premium_pe_pct, 1),
    'pure_business_value': round(pure_business_value, 2),
    'downside_to_pure_pb_pct': round(downside_to_pure_pb, 1),
    'extreme_downside_pct': round(extreme_downside_pct, 1),
    'extreme_downside_price': round(extreme_downside_price, 2),
    'upside_to_target_low_pct': round(upside_low, 1),
    'upside_to_target_high_pct': round(upside_high, 1),
    'margin_ratio_pct': round(margin_ratio, 2),
    'pressure_test_10pct': round(pressure_test_10pct, 2),
    'pressure_test_20pct': round(pressure_test_20pct, 2),
    'position_ceiling_pct': round(position_ceiling, 1),
    'stop_loss_price': round(stop_loss_price, 2),
    'certainty_score': restructuring_certainty_score
}