"""
自动生成的技能: restructuring_risk_analyzer
描述: 重组预期风险量化分析工具：计算重组溢价比例、下行风险空间、融资盘敏感度
创建者: risk
"""

import pandas as pd
import numpy as np

# input_data 应包含:
# current_price, current_pe, current_pb, market_cap
# industry_avg_pe, industry_avg_pb
# target_price_low, target_price_high
# margin_balance, circ_mv
# pure_business_pb (纯业务估值PB)

current_price = input_data.get('current_price', 4.54)
current_pe = input_data.get('current_pe', 20.6)
current_pb = input_data.get('current_pb', 1.12)
market_cap = input_data.get('market_cap', 73.0)  # 亿元

industry_avg_pe = input_data.get('industry_avg_pe', 15.0)  # 传统水泥PE
industry_avg_pb = input_data.get('industry_avg_pb', 0.85)  # 传统水泥PB

target_price_low = input_data.get('target_price_low', 8.60)
target_price_high = input_data.get('target_price_high', 9.20)

margin_balance = input_data.get('margin_balance', 3.99)  # 亿元
circ_mv = input_data.get('circ_mv', 73.66)  # 亿元

pure_business_pb = input_data.get('pure_business_pb', 0.85)
bvps = current_price / current_pb  # 每股净资产

# 1. 重组溢价比例计算
pure_business_value = bvps * pure_business_pb
restructuring_premium_pct = (current_price - pure_business_value) / pure_business_value * 100

# PE法计算重组溢价
pure_business_pe_value = current_price * (industry_avg_pe / current_pe)
restructuring_premium_pe_pct = (current_price - pure_business_pe_value) / pure_business_pe_value * 100

# 2. 重组落空下行风险
downside_to_pure_pb = (current_price - pure_business_value) / current_price * 100
extreme_downside_pb = 0.6  # 极端情况PB
extreme_downside_price = bvps * extreme_downside_pb
extreme_downside_pct = (current_price - extreme_downside_price) / current_price * 100

# 3. 融资盘敏感度分析
margin_ratio = margin_balance / circ_mv * 100  # 占流通市值比例
# 假设融资盘平均成本为近20日均价（估算为当前价的98%）
avg_cost = current_price * 0.98
maintenance_ratio = 1.3  # 维持担保比例假设
liquidation_price = avg_cost * (1 / maintenance_ratio) * 1.5  # 简化估算
liquidation_downside = (current_price - liquidation_price) / current_price * 100 if liquidation_price < current_price else 0

# 融资盘压力测试
pressure_test_10pct = margin_balance * 0.3  # 下跌10%可能平仓30%
pressure_test_20pct = margin_balance * 0.6  # 下跌20%可能平仓60%

# 4. 仓位建议
# 基于重组确定性评分（0-100）
# 已完成控股变更: +40分
# 资产注入进行中: +30分
# 政策支持: +20分
# 时间不确定性: -15分
# 融资盘拥挤: -10分
restructuring_certainty_score = 40 + 30 + 20 - 15 - 10  # 65分

# 仓位上限 = 基础仓位(5%) + 确定性评分调整
base_position = 5
position_ceiling = base_position + (restructuring_certainty_score / 100) * 15  # 最高20%
position_ceiling = min(position_ceiling, 20)  # 上限20%

result = {
    'restructuring_premium_analysis': {
        'pb_method_premium_pct': round(restructuring_premium_pct, 1),
        'pe_method_premium_pct': round(restructuring_premium_pe_pct, 1),
        'pure_business_value': round(pure_business_value, 2),
        'implied_premium_range': f"{round(restructuring_premium_pct, 0)}%-{round(restructuring_premium_pe_pct, 0)}%"
    },
    'downside_risk_analysis': {
        'to_pure_business_pb': f"{round(downside_to_pure_pb, 1)}%",
        'to_pure_business_price': round(pure_business_value, 2),
        'extreme_downside_pb_0.6': f"{round(extreme_downside_pct, 1)}%",
        'extreme_downside_price': round(extreme_downside_price, 2),
        'upside_to_target': f"{round((target_price_low - current_price) / current_price * 100, 1)}% - {round((target_price_high - current_price) / current_price * 100, 1)}%"
    },
    'margin_sensitivity': {
        'margin_balance_billion': margin_balance,
        'margin_ratio_pct': round(margin_ratio, 2),
        'margin_change_ytd': '+37%',
        'liquidation_risk_level': '中等' if margin_ratio > 5 else '低',
        'pressure_test_10pct_drop': f"{round(pressure_test_10pct, 2)}亿元可能平仓",
        'pressure_test_20pct_drop': f"{round(pressure_test_20pct, 2)}亿元可能平仓",
        'sensitivity_rating': '高（融资盘+37%显示对重组消息高度敏感）'
    },
    'position_recommendation': {
        'restructuring_certainty_score': restructuring_certainty_score,
        'position_ceiling_pct': round(position_ceiling, 1),
        'base_position_pct': base_position,
        'rationale': '重组已实质性落地（控股变更完成）但资产注入尚未完成，建议中等仓位',
        'stop_loss_price': round(pure_business_value * 0.9, 2),
        'stop_loss_reason': '跌破纯业务估值90%表明重组预期基本落空'
    },
    'risk_summary': {
        'key_risks': [
            '资产注入进度不及预期（审批流程风险）',
            '融资盘拥挤可能加剧波动',
            '水泥主业仍承压（2025年均价同比-3.2%）',
            '地缘风险影响中吉乌铁路协同'
        ],
        'risk_level': '中等偏高',
        'recommendation': '可持有但需严格止损，仓位不超过15%'
    }
}