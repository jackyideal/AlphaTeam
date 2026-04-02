"""
自动生成的技能: ma200_calculator
描述: 计算200日均线及当前价格相对位置
创建者: quant
"""

import pandas as pd
import numpy as np

# input_data 应包含 close_prices 列表
close_prices = input_data.get('close_prices', [])

if len(close_prices) < 200:
    result = {
        'error': '数据不足200日',
        'ma200': None,
        'current_price': close_prices[-1] if close_prices else None,
        'price_vs_ma200': None
    }
else:
    ma200 = np.mean(close_prices[-200:])
    current_price = close_prices[-1]
    price_vs_ma200 = (current_price - ma200) / ma200 * 100
    
    result = {
        'ma200': round(ma200, 3),
        'current_price': current_price,
        'price_vs_ma200_pct': round(price_vs_ma200, 2),
        'trend': 'above' if current_price > ma200 else 'below'
    }

result