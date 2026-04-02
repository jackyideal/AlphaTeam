"""
自动生成的技能: risk_assessment_qingsong
描述: 青松建化(600425.SH)专项风险评估模型，计算下行风险位、仓位建议、止损触发条件
创建者: risk
"""

import pandas as pd
import numpy as np

# 输入数据包含：价格数据、财务指标、风险因素
input_data = input_data if 'input_data' in dir() else {}

# 提取关键参数
current_price = input_data.get('current_price', 4.62)
support_20d = input_data.get('support_20d', 4.43)
chip_cost = input_data.get('chip_cost', 4.51)
pe_ratio = input_data.get('pe_ratio', 20.96)
pb_ratio = input_data.get('pb_ratio', 1.13)
turnover_rate = input_data.get('turnover_rate', 2.0)
revenue_yoy = input_data.get('revenue_yoy', -11.44)
chip_win_rate = input_data.get('chip_win_rate', 35.4)

# 1. 计算波动率风险（基于20日价格区间）
high_20d = input_data.get('high_20d', 4.67)
low_20d = input_data.get('low_20d', 4.43)
price_range_pct = (high_20d - low_20d) / low_20d * 100  # 约5.4%

# 2. 计算下行风险位（多情景分析）
# 情景1：技术支撑位（20日低点）
support_tech = low_20d
downside_tech = (current_price - support_tech) / current_price * 100

# 情景2：筹码成本位（多数筹码亏损位）
support_chip = chip_cost * 0.95  # 筹码成本下方5%
downside_chip = (current_price - support_chip) / current_price * 100

# 情景3：极端情景（2025年低点假设）
support_extreme = 4.20  # 假设年线支撑
downside_extreme = (current_price - support_extreme) / current_price * 100

# 3. 风险评分（0-100，越高越危险）
risk_score = 0

# 估值风险（PE>20为高风险）
if pe_ratio > 25:
    risk_score += 25
elif pe_ratio > 20:
    risk_score += 15
elif pe_ratio > 15:
    risk_score += 10

# 基本面风险（营收负增长）
if revenue_yoy < -10:
    risk_score += 25
elif revenue_yoy < -5:
    risk_score += 15
elif revenue_yoy < 0:
    risk_score += 10

# 筹码风险（胜率低）
if chip_win_rate < 40:
    risk_score += 20
elif chip_win_rate < 50:
    risk_score += 10

# 技术面风险（接近低点）
dist_to_low = (current_price - low_20d) / current_price * 100
if dist_to_low < 3:
    risk_score += 15
elif dist_to_low < 5:
    risk_score += 10

# 流动性风险（换手率）
if turnover_rate < 1:
    risk_score += 10
elif turnover_rate > 5:
    risk_score += 5

# 4. 仓位建议（基于风险评分）
if risk_score >= 70:
    position_limit = 5  # 极高风险，不超过5%
elif risk_score >= 55:
    position_limit = 10  # 高风险，不超过10%
elif risk_score >= 40:
    position_limit = 20  # 中高风险，不超过20%
elif risk_score >= 25:
    position_limit = 30  # 中等风险，不超过30%
else:
    position_limit = 50  # 低风险，不超过50%

# 5. 止损位建议
# 硬止损：跌破20日低点3%
stop_loss_hard = low_20d * 0.97
# 软止损：跌破筹码成本5%
stop_loss_soft = chip_cost * 0.95

# 6. 最大回撤概率估算（基于历史波动）
# 假设日波动率约2%，20日波动率约9%
daily_vol = price_range_pct / 20  # 约0.27%
monthly_vol = daily_vol * np.sqrt(20)  # 约1.2%

# 95%置信度下的最大回撤
max_drawdown_95 = monthly_vol * 1.96  # 约2.4%

result = {
    'risk_score': risk_score,
    'risk_level': '高风险' if risk_score >= 55 else ('中高风险' if risk_score >= 40 else '中等风险'),
    'downside_scenarios': {
        'technical_support': f'{downside_tech:.1f}% (至{support_tech:.2f}元)',
        'chip_cost_support': f'{downside_chip:.1f}% (至{support_chip:.2f}元)',
        'extreme_scenario': f'{downside_extreme:.1f}% (至{support_extreme:.2f}元)'
    },
    'position_limit_pct': position_limit,
    'stop_loss': {
        'hard_stop': f'{stop_loss_hard:.2f}元 (跌破20日低点3%)',
        'soft_stop': f'{stop_loss_soft:.2f}元 (跌破筹码成本5%)'
    },
    'volatility_metrics': {
        '20d_range_pct': round(price_range_pct, 2),
        'estimated_monthly_vol': round(monthly_vol, 2),
        'max_drawdown_95_conf': round(max_drawdown_95, 2)
    },
    'key_risk_factors': [
        f'PE {pe_ratio}倍（水泥行业偏高）',
        f'营收增速{revenue_yoy}%（负增长）',
        f'筹码胜率{chip_win_rate}%（多数浮亏）',
        '年报披露风险（3月27日）',
        'CBAM碳关税政策风险',
        '哈萨克斯坦关税不确定性'
    ]
}