"""
自动生成的技能: annual_report_analysis
描述: 分析年报披露后5/10/20日股价表现规律
创建者: analyst
"""

import pandas as pd
import numpy as np

# input_data 应包含: kline_data (DataFrame with trade_date, close, vol), report_dates (list of report disclosure dates)

kline_data = input_data.get('kline_data', pd.DataFrame())
report_dates = input_data.get('report_dates', [])

if kline_data.empty or not report_dates:
    result = {'error': 'No data provided'}
else:
    kline_data['trade_date'] = pd.to_datetime(kline_data['trade_date'], format='%Y%m%d')
    kline_data = kline_data.sort_values('trade_date')
    
    analysis_results = []
    
    for report_date in report_dates:
        report_dt = pd.to_datetime(report_date, format='%Y%m%d')
        
        # 找到披露日收盘价
        report_row = kline_data[kline_data['trade_date'] == report_dt]
        if report_row.empty:
            # 找最近交易日
            prev_rows = kline_data[kline_data['trade_date'] <= report_dt]
            if not prev_rows.empty:
                report_row = prev_rows.iloc[-1:]
                report_dt = report_row['trade_date'].values[0]
            else:
                continue
        
        report_close = report_row['close'].values[0]
        
        # 计算披露后5/10/20日的涨跌幅
        future_5 = kline_data[(kline_data['trade_date'] > report_dt) & (kline_data['trade_date'] <= report_dt + pd.Timedelta(days=10))]
        future_10 = kline_data[(kline_data['trade_date'] > report_dt) & (kline_data['trade_date'] <= report_dt + pd.Timedelta(days=15))]
        future_20 = kline_data[(kline_data['trade_date'] > report_dt) & (kline_data['trade_date'] <= report_dt + pd.Timedelta(days=30))]
        
        ret_5 = (future_5['close'].iloc[-1] / report_close - 1) * 100 if not future_5.empty else None
        ret_10 = (future_10['close'].iloc[-1] / report_close - 1) * 100 if not future_10.empty else None
        ret_20 = (future_20['close'].iloc[-1] / report_close - 1) * 100 if not future_20.empty else None
        
        analysis_results.append({
            'report_date': report_date,
            'close': report_close,
            'ret_5d': round(ret_5, 2) if ret_5 else None,
            'ret_10d': round(ret_10, 2) if ret_10 else None,
            'ret_20d': round(ret_20, 2) if ret_20 else None
        })
    
    result = {'analysis': analysis_results, 'summary': f'Analyzed {len(analysis_results)} report dates'}