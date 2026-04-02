"""
自动生成的技能: valuation_percentile
描述: 计算估值历史分位（PE/PB百分位）
创建者: quant
"""

import pandas as pd
import numpy as np

# input_data 应包含 'pe_series' 和 'pb_series' 两个列表
pe_series = input_data.get('pe_series', [])
pb_series = input_data.get('pb_series', [])
current_pe = input_data.get('current_pe', pe_series[-1] if pe_series else 0)
current_pb = input_data.get('current_pb', pb_series[-1] if pb_series else 0)

# 计算百分位
def calc_percentile(series, current):
    if not series:
        return 0
    series_sorted = sorted(series)
    rank = sum(1 for x in series_sorted if x < current)
    return round(rank / len(series_sorted) * 100, 2)

pe_percentile = calc_percentile(pe_series, current_pe)
pb_percentile = calc_percentile(pb_series, current_pb)

# 统计描述
pe_mean = np.mean(pe_series) if pe_series else 0
pe_std = np.std(pe_series) if pe_series else 0
pb_mean = np.mean(pb_series) if pb_series else 0
pb_std = np.std(pb_series) if pb_series else 0

result = {
    'current_pe': current_pe,
    'pe_percentile': pe_percentile,
    'pe_mean': round(pe_mean, 2),
    'pe_std': round(pe_std, 2),
    'current_pb': current_pb,
    'pb_percentile': pb_percentile,
    'pb_mean': round(pb_mean, 2),
    'pb_std': round(pb_std, 2),
    'valuation_assessment': '高估' if pe_percentile > 70 else ('低估' if pe_percentile < 30 else '合理')
}