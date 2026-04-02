"""
数据库更新服务 - 调用 AlphaFin/scripts/update_funcs.py 的更新函数
"""
import os
from datetime import datetime, timedelta
from AlphaFin.config import DB_ROOT
from AlphaFin.services.progress_service import update_progress
from AlphaFin.scripts import update_funcs


def _get_cal_dates(start_date='20170101'):
    """获取从start_date到昨天的交易日列表"""
    from AlphaFin.indicators.shared_utils import pro

    end_date = datetime.strftime(datetime.now() - timedelta(days=1), '%Y%m%d')
    df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
    df = df[df['is_open'] == 1].sort_values('cal_date').reset_index(drop=True)
    return list(df['cal_date'])


def _get_stocks():
    """获取全市场股票代码列表"""
    from AlphaFin.indicators.shared_utils import pro

    codess = pro.query('stock_basic', exchange='SSE', list_status='L',
                       fields='ts_code')
    codess1 = pro.query('stock_basic', exchange='SZSE', list_status='L',
                        fields='ts_code')
    return list(set(codess['ts_code'])) + list(set(codess1['ts_code']))


def run_update(task_id, include_fina=False, start_date='20170101'):
    """运行数据库增量更新

    Args:
        task_id: 任务ID
        include_fina: 是否更新财务指标（全量更新，耗时较长）
        start_date: 增量更新起始日期（YYYYMMDD）
    """
    try:
        try:
            from sqlalchemy import create_engine
        except Exception as e:
            raise RuntimeError('缺少依赖 SQLAlchemy，请先安装 requirements.txt') from e

        os.makedirs(DB_ROOT, exist_ok=True)

        file_path = f'sqlite:////{DB_ROOT}/'

        update_progress(task_id, 0, 1, '获取交易日历...')
        cal_dates = _get_cal_dates(start_date=start_date)

        steps = [
            {
                'msg': '更新日K线数据...',
                'fn': update_funcs.update_dailykline_data,
                'args': ('daily_kline', cal_dates,
                         create_engine(file_path + 'daily_kline.db')),
            },
            {
                'msg': '更新每日基础数据...',
                'fn': update_funcs.update_dailybasic_data,
                'args': ('dailybasic', cal_dates,
                         create_engine(file_path + 'dailybasic.db')),
            },
            {
                'msg': '更新复权因子...',
                'fn': update_funcs.update_dailyadj_data,
                'args': ('daily_adj', cal_dates,
                         create_engine(file_path + 'daily_adj.db')),
            },
        ]

        if include_fina:
            stocks = _get_stocks()
            steps.append({
                'msg': '更新财务指标（全量，耗时较长）...',
                'fn': update_funcs.update_fina_indicator_data,
                'args': ('fina_indicator', stocks,
                         create_engine(file_path + 'fina_indicator.db')),
            })

        steps.extend([
            {
                'msg': '更新财务报表（利润表/现金流/资产负债表）...',
                'fn': _update_financial_tables,
                'args': (update_funcs, file_path, cal_dates),
            },
            {
                'msg': '更新港股通持仓...',
                'fn': update_funcs.update_hkhold_data,
                'args': ('hkhold', cal_dates,
                         create_engine(file_path + 'hkhold.db')),
            },
        ])

        total = len(steps)
        for i, step in enumerate(steps):
            update_progress(task_id, i, total, step['msg'])
            try:
                step['fn'](*step['args'])
            except Exception as e:
                update_progress(task_id, i, total, f"{step['msg']} 出错: {e}")

        update_progress(task_id, total, total, '数据库更新完成', done=True)

    except Exception as e:
        update_progress(task_id, 0, 1, f'更新失败: {str(e)}', done=True)


def _update_financial_tables(update_funcs, file_path, cal_dates):
    """更新三张财务报表"""
    from sqlalchemy import create_engine
    engine = create_engine(file_path + 'financial_data.db')
    # 生成财报日期列表 (每季度末)
    end_dates = _get_financial_end_dates()
    for table_name in ['利润表', '现金流量表', '资产负债表']:
        try:
            update_funcs.update_financial_data(table_name, end_dates, engine)
        except Exception as e:
            print(f'更新{table_name}失败: {e}')


def _get_financial_end_dates():
    """生成从2017年到当前的财报季度末日期列表"""
    dates = []
    current = datetime.now()
    for year in range(2017, current.year + 1):
        for month in ['0331', '0630', '0930', '1231']:
            d = f'{year}{month}'
            if d <= current.strftime('%Y%m%d'):
                dates.append(d)
    return dates
