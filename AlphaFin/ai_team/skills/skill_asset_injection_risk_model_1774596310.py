"""
自动生成的技能: asset_injection_risk_model
描述: 资产注入预期量化风险评估模型：基于传闻可信度、历史成功率、基本面和估值计算风险调整后期望收益和仓位建议
创建者: quant
"""

"""
资产注入预期量化风险评估模型
输入数据要求:
- current_price: 当前股价
- pe_ratio: 市盈率
- revenue_growth: 营收增速
- roe: ROE
- rumor_confidence: 传闻可信度(0-1)
- historical_success_rate: 历史类似案例成功率
- downside_risk: 预期落空时的下行风险比例

输出:
- risk_adjusted_expectation: 风险调整后期望收益
- position_recommendation: 仓位建议(0-0.3)
- confidence_level: 置信度
"""
import numpy as np

current_price = input_data.get('current_price', 4.23)
pe_ratio = input_data.get('pe_ratio', 19.19)
revenue_growth = input_data.get('revenue_growth', -0.1028)
roe = input_data.get('roe', 0.043)
rumor_confidence = input_data.get('rumor_confidence', 0.5)  # 待情报确认
historical_success_rate = input_data.get('historical_success_rate', 0.35)  # 历史统计
downside_risk = input_data.get('downside_risk', 0.15)  # 预期落空回撤

# 基本面得分 (0-1)
fundamental_score = max(0, min(1, (roe * 5 + (1 + revenue_growth)) / 2))

# 估值得分 (PE越低得分越高，基准PE=15)
valuation_score = max(0, min(1, 1 - (pe_ratio - 10) / 30))

# 事件驱动期望收益计算
upside_potential = 0.25  # 资产注入成功潜在涨幅假设
success_prob = rumor_confidence * historical_success_rate

# 风险调整后期望收益
expected_return = success_prob * upside_potential - (1 - success_prob) * downside_risk
risk_adjusted_return = expected_return * fundamental_score * valuation_score

# 仓位建议 (基于期望收益和置信度)
if risk_adjusted_return > 0.05:
    position = min(0.3, 0.1 + risk_adjusted_return * 2)
    confidence = 0.6 + risk_adjusted_return * 3
elif risk_adjusted_return > 0:
    position = min(0.15, 0.05 + risk_adjusted_return * 3)
    confidence = 0.4 + risk_adjusted_return * 5
else:
    position = 0
    confidence = 0.3

result = {
    'risk_adjusted_expectation': round(risk_adjusted_return, 4),
    'position_recommendation': round(position, 3),
    'confidence_level': round(min(0.95, confidence), 3),
    'success_probability': round(success_prob, 3),
    'fundamental_score': round(fundamental_score, 3),
    'valuation_score': round(valuation_score, 3),
    'recommendation': 'HOLD' if position < 0.05 else ('LIGHT_BUY' if position < 0.15 else 'BUY'),
    'key_risk_factors': [
        '传闻可信度待验证' if rumor_confidence < 0.7 else '传闻较可靠',
        '基本面疲软' if fundamental_score < 0.5 else '基本面尚可',
        '估值偏高' if valuation_score < 0.5 else '估值合理',
        f'历史成功率仅{round(historical_success_rate*100)}%'
    ]
}