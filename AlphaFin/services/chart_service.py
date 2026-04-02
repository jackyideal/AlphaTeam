"""
图表生成服务 - 调用指标模块生成图表并保存为PNG
"""
import os
import uuid
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from AlphaFin.config import CHART_DIR
from AlphaFin.indicators.indicator_registry import REGISTRY, get_indicator_module
from AlphaFin.services.progress_service import update_progress


def generate_charts(indicator_id, params, task_id):
    """
    在后台线程中运行指标计算，保存PNG图表。
    params: {'ts_code': '600425.SH', 'start_date': '20200101'}
    """
    os.makedirs(CHART_DIR, exist_ok=True)
    meta = REGISTRY.get(indicator_id)
    if not meta:
        update_progress(task_id, 0, 1, f'未找到指标: {indicator_id}', done=True)
        return

    try:
        mod = get_indicator_module(indicator_id)
    except Exception as e:
        update_progress(task_id, 0, 1, f'加载指标模块失败: {str(e)}', done=True)
        return
    if not mod:
        update_progress(task_id, 0, 1, f'指标模块不存在: {indicator_id}', done=True)
        return

    def progress_cb(step, total, msg):
        update_progress(task_id, step, total, msg)

    update_progress(task_id, 0, meta.get('chart_count', 1), '初始化...')

    try:
        # 构建调用参数
        kwargs = {'progress_callback': progress_cb}
        if meta['input_type'] == 'query':
            kwargs['query'] = params.get('query', meta.get('default_query', ''))
        elif meta['input_type'] in ('stock', 'index'):
            kwargs['ts_code'] = params.get('ts_code', meta.get('default_code', '600425.SH'))
        if 'start_date' in params:
            kwargs['start_date'] = params['start_date']

        # 调用 generate 函数
        figures = mod.generate(**kwargs)

        # 保存图表为PNG
        chart_paths = []
        chart_titles = []
        save_dpi = 170 if indicator_id == 'ind_27_logicfin' else 120
        for i, (fig, title) in enumerate(figures):
            fname = f"{indicator_id}_{uuid.uuid4().hex[:8]}.png"
            fpath = os.path.join(CHART_DIR, fname)
            fig.savefig(fpath, dpi=save_dpi, bbox_inches='tight', facecolor='white')
            plt.close(fig)
            chart_paths.append(f'/static/charts/{fname}')
            chart_titles.append(title)

        update_progress(task_id, len(figures), len(figures), '完成',
                        done=True, chart_paths=chart_paths, chart_titles=chart_titles)

    except Exception as e:
        import traceback
        traceback.print_exc()
        update_progress(task_id, 0, 1, f'错误: {str(e)}', done=True)
