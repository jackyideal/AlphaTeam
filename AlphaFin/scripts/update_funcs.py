# -*- coding: utf-8 -*-
"""
数据库更新函数集合。

说明：
- 本文件用于替代本地私有目录中的 update_funcs.py。
- 所有密钥均从环境变量读取（通过 AlphaFin.config.TUSHARE_TOKEN）。
"""

import time

import pandas as pd
import tushare as ts

from AlphaFin.config import TUSHARE_TOKEN

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    def tqdm(x):
        return x


def _get_pro():
    token = str(TUSHARE_TOKEN or '').strip()
    if not token:
        raise RuntimeError('未设置 TUSHARE_TOKEN，无法更新数据库。')
    return ts.pro_api(token)


def _load_existing_dates(engine, table_name, field_name):
    try:
        exist = pd.read_sql(f"select distinct {field_name} from {table_name}", engine)
        vals = set(str(x) for x in exist[field_name].dropna().astype(str).tolist())
        return vals
    except Exception:
        return set()


def _drop_table_if_exists(engine, table_name):
    from sqlalchemy import text
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))


def update_fina_indicator_data(table_name, stocks, engine):
    """
    财务指标为全量重建模式（按股票拉取）。
    """
    pro = _get_pro()
    _drop_table_if_exists(engine, table_name)

    for stock in tqdm(stocks):
        try:
            df = pro.fina_indicator(ts_code=stock)
            if df is None or df.empty:
                continue
            if 'ann_date' in df.columns:
                df['trade_date'] = df['ann_date']
            df.to_sql(table_name, engine, index=True, if_exists='append')
            time.sleep(0.1)
        except Exception:
            continue


def update_dailybasic_data(table_name, cal_dates, engine):
    pro = _get_pro()
    exist_dates = _load_existing_dates(engine, table_name, 'trade_date')
    todo_dates = sorted(set(str(x) for x in cal_dates) - exist_dates)

    for cal_date in tqdm(todo_dates):
        try:
            df = pro.daily_basic(trade_date=cal_date)
            if df is None or df.empty:
                continue
            df.to_sql(table_name, engine, index=True, if_exists='append')
        except Exception:
            continue


def update_dailykline_data(table_name, cal_dates, engine):
    pro = _get_pro()
    exist_dates = _load_existing_dates(engine, table_name, 'trade_date')
    todo_dates = sorted(set(str(x) for x in cal_dates) - exist_dates)

    for cal_date in tqdm(todo_dates):
        try:
            df = pro.daily(trade_date=cal_date)
            if df is None or df.empty:
                continue
            df.to_sql(table_name, engine, index=True, if_exists='append')
        except Exception:
            continue


def update_dailyadj_data(table_name, cal_dates, engine):
    pro = _get_pro()
    exist_dates = _load_existing_dates(engine, table_name, 'trade_date')
    todo_dates = sorted(set(str(x) for x in cal_dates) - exist_dates)

    for cal_date in tqdm(todo_dates):
        try:
            df = pro.adj_factor(trade_date=cal_date)
            if df is None or df.empty:
                continue
            df.to_sql(table_name, engine, index=True, if_exists='append')
        except Exception:
            continue


def update_financial_data(table_name, end_dates, engine):
    """
    更新三张财务报表（利润表/现金流量表/资产负债表）。
    """
    import sqlalchemy

    pro = _get_pro()

    try:
        exist_dates = _load_existing_dates(engine, table_name, 'end_date')

        md = sqlalchemy.MetaData()
        table = sqlalchemy.Table(table_name, md, autoload_with=engine)
        fields = [c.name for c in table.c if c.name != 'level_0']
        first_update = False
    except Exception:
        exist_dates = set()
        fields = []
        first_update = True

    todo_dates = sorted(set(str(x) for x in end_dates) - exist_dates)
    for end_date in tqdm(todo_dates):
        try:
            if table_name == '利润表':
                dd1 = pro.income_vip(period=end_date, report_type='1')
                dd2 = pro.income_vip(period=end_date, report_type='2')
                dd = pd.concat([dd1, dd2], ignore_index=True)
            elif table_name == '现金流量表':
                dd1 = pro.cashflow_vip(period=end_date, report_type='1')
                dd2 = pro.cashflow_vip(period=end_date, report_type='2')
                dd = pd.concat([dd1, dd2], ignore_index=True)
            elif table_name == '资产负债表':
                dd = pro.balancesheet_vip(period=end_date)
            else:
                continue

            if dd is None or dd.empty:
                continue

            dd = dd.reset_index(drop=True)
            if (not first_update) and fields:
                keep_cols = [c for c in fields if c in dd.columns]
                dd = dd[keep_cols]
            dd.to_sql(table_name, engine, index=True, if_exists='append')
        except Exception:
            continue


def update_hkhold_data(table_name, cal_dates, engine):
    pro = _get_pro()
    cal_dates = [str(x) for x in cal_dates if str(x) >= '20160629']

    exist_dates = _load_existing_dates(engine, table_name, 'trade_date')
    todo_dates = sorted(set(cal_dates) - exist_dates)

    for cal_date in tqdm(todo_dates):
        try:
            df = pro.hk_hold(trade_date=cal_date)
            if df is None or df.empty:
                continue
            df.to_sql(table_name, engine, index=True, if_exists='append')
            time.sleep(0.15)
        except Exception:
            continue
