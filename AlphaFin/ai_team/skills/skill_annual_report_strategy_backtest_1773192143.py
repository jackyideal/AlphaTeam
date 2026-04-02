"""
自动生成的技能: annual_report_strategy_backtest
描述: 回测年报前建仓vs年报后建仓策略的胜率和盈亏比
创建者: quant
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 输入数据：input_data 包含 kline_data (DataFrame) 和 report_dates (列表)
kline_data = input_data.get('kline_data', pd.DataFrame())
report_dates = input_data.get('report_dates', [])

if kline_data.empty or not report_dates:
    result = {'error': '数据不足，无法回测'}
else:
    # 策略1：年报前20日建仓，持有至年报后20日
    # 策略2：年报后1日建仓，持有20日
    
    pre_report_returns = []
    post_report_returns = []
    
    kline_data['trade_date'] = pd.to_datetime(kline_data['trade_date'], format='%Y%m%d')
    kline_data = kline_data.sort_values('trade_date')
    
    for report_date in report_dates:
        report_dt = pd.to_datetime(report_date, format='%Y%m%d')
        
        # 找到年报前20个交易日
        pre_dates = kline_data[kline_data['trade_date'] < report_dt].tail(20)
        # 找到年报后20个交易日
        post_dates = kline_data[kline_data['trade_date'] > report_dt].head(20)
        
        if len(pre_dates) >= 20 and len(post_dates) >= 20:
            # 策略1：前20日买入，后20日卖出
            buy_price_pre = pre_dates.iloc[0]['close']
            sell_price_pre = post_dates.iloc[-1]['close']
            ret_pre = (sell_price_pre - buy_price_pre) / buy_price_pre
            
            # 策略2：后1日买入，持有20日
            buy_price_post = post_dates.iloc[0]['close']
            sell_price_post = post_dates.iloc[-1]['close']
            ret_post = (sell_price_post - buy_price_post) / buy_price_post
            
            pre_report_returns.append(ret_pre)
            post_report_returns.append(ret_post)
    
    if pre_report_returns and post_report_returns:
        pre_arr = np.array(pre_report_returns)
        post_arr = np.array(post_report_returns)
        
        result = {
            'pre_report': {
                'win_rate': np.mean(pre_arr > 0),
                'avg_return': np.mean(pre_arr),
                'max_return': np.max(pre_arr),
                'min_return': np.min(pre_arr),
                'std': np.std(pre_arr),
                'samples': len(pre_arr)
            },
            'post_report': {
                'win_rate': np.mean(post_arr > 0),
                'avg_return': np.mean(post_arr),
                'max_return': np.max(post_arr),
                'min_return': np.min(post_arr),
                'std': np.std(post_arr),
                'samples': len(post_arr)
            },
            'comparison': {
                'pre_vs_post_avg': np.mean(pre_arr) - np.mean(post_arr),
                'pre_vs_post_win': np.mean(pre_arr > 0) - np.mean(post_arr > 0)
            }
        }
    else:
        result = {'error': '有效样本不足'}