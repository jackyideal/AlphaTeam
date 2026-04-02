# -*- coding: utf-8 -*-
"""
数据库构建脚本（快速优先）。

用法：
    python3 -m AlphaFin.scripts.build_db --mode quick
    python3 -m AlphaFin.scripts.build_db --mode full --include-fina
"""

import argparse
import datetime as dt
import uuid

from AlphaFin.config import DB_ROOT


def _quick_start_date(days=365):
    d = dt.datetime.now() - dt.timedelta(days=max(1, int(days)))
    return d.strftime('%Y%m%d')


def main():
    parser = argparse.ArgumentParser(description='Build AlphaFin local databases.')
    parser.add_argument(
        '--mode',
        choices=['quick', 'full'],
        default='quick',
        help='quick=最近N天快速构建；full=全量构建'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=365,
        help='quick 模式下回溯天数，默认 365'
    )
    parser.add_argument(
        '--include-fina',
        action='store_true',
        help='是否构建 fina_indicator（耗时较长）'
    )
    args = parser.parse_args()
    from AlphaFin.services.progress_service import get_progress
    from AlphaFin.services.update_service import run_update

    if args.mode == 'quick':
        start_date = _quick_start_date(args.days)
    else:
        start_date = '20170101'

    task_id = 'cli_update_' + uuid.uuid4().hex[:8]
    print('[AlphaFin] 开始构建数据库')
    print(f'[AlphaFin] DB_ROOT={DB_ROOT}')
    print(f'[AlphaFin] mode={args.mode}, start_date={start_date}, include_fina={bool(args.include_fina)}')

    run_update(task_id=task_id, include_fina=bool(args.include_fina), start_date=start_date)
    state = get_progress(task_id)
    message = str(state.get('message') or '')
    done = bool(state.get('done'))

    print(f'[AlphaFin] done={done}, message={message}')
    if not done:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
