"""
自动生成的技能: valuation_percentile_simple
描述: 计算估值历史分位（PE/PB百分位）- 简化版
创建者: quant
"""

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

# 计算均值和标准差
def calc_stats(series):
    if not series:
        return 0, 0
    n = len(series)
    mean = sum(series) / n
    variance = sum((x - mean) ** 2 for x in series) / n
    std = variance ** 0.5
    return round(mean, 2), round(std, 2)

pe_percentile = calc_percentile(pe_series, current_pe)
pb_percentile = calc_percentile(pb_series, current_pb)
pe_mean, pe_std = calc_stats(pe_series)
pb_mean, pb_std = calc_stats(pb_series)

# 判断估值状态
if pe_percentile > 70:
    assessment = '高估'
elif pe_percentile < 30:
    assessment = '低估'
else:
    assessment = '合理'

result = {
    'current_pe': current_pe,
    'pe_percentile': pe_percentile,
    'pe_mean': pe_mean,
    'pe_std': pe_std,
    'current_pb': current_pb,
    'pb_percentile': pb_percentile,
    'pb_mean': pb_mean,
    'pb_std': pb_std,
    'valuation_assessment': assessment
}