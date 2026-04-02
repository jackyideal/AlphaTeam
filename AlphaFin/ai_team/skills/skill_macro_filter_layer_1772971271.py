"""
自动生成的技能: macro_filter_layer
描述: 宏观因子过滤层：融合美债10Y收益率、美元兑人民币汇率、VIX指数三因子，对原始交易信号进行风险调整
创建者: quant
"""

import pandas as pd
import numpy as np

# input_data 应包含: us_10y (美债10Y收益率), usd_cny (美元兑人民币汇率), vix (VIX指数), signal_score (原始信号评分)
us_10y = input_data.get('us_10y', 4.2)
usd_cny = input_data.get('usd_cny', 7.25)
vix = input_data.get('vix', 18)
signal_score = input_data.get('signal_score', 0.5)

# 宏观因子过滤逻辑
# 1. 美债收益率 > 4.5% 时，全球流动性收紧，降权20%
us_10y_factor = 1.0 if us_10y < 4.2 else (0.8 if us_10y < 4.5 else 0.6)

# 2. 人民币贬值压力大 (usd_cny > 7.35) 时，外资流出风险，降权15%
cny_factor = 1.0 if usd_cny < 7.25 else (0.85 if usd_cny < 7.35 else 0.7)

# 3. VIX > 25 时，市场恐慌，降权25%
vix_factor = 1.0 if vix < 18 else (0.85 if vix < 22 else (0.75 if vix < 25 else 0.6))

# 综合调整后的信号评分
adjusted_score = signal_score * us_10y_factor * cny_factor * vix_factor

# 宏观风险等级
if us_10y > 4.5 or usd_cny > 7.35 or vix > 25:
    risk_level = 'high'
elif us_10y > 4.2 or usd_cny > 7.25 or vix > 20:
    risk_level = 'medium'
else:
    risk_level = 'low'

result = {
    'original_score': round(signal_score, 3),
    'adjusted_score': round(adjusted_score, 3),
    'adjustment_factor': round(us_10y_factor * cny_factor * vix_factor, 3),
    'macro_risk_level': risk_level,
    'factors': {
        'us_10y': us_10y,
        'us_10y_factor': us_10y_factor,
        'usd_cny': usd_cny,
        'cny_factor': cny_factor,
        'vix': vix,
        'vix_factor': vix_factor
    }
}