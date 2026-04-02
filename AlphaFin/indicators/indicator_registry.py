"""
指标注册表 - 自动发现所有 ind_*.py 模块并构建注册表

性能优化：
- 启动时仅解析 INDICATOR_META（AST），不导入指标模块
- 运行指标时再按需导入对应模块（lazy load）
"""
import ast
import glob
import importlib
import os
import threading

REGISTRY = {}  # {indicator_id: {meta + _module_name}}
_MODULE_CACHE = {}  # {_module_name: module}
_MODULE_LOCK = threading.Lock()
_EXCLUDED_INDICATOR_IDS = {
    'ind_22_fin_evolver',
    'ind_27_logicfin',
}


def _extract_meta_from_file(fpath):
    """从源文件静态提取 INDICATOR_META，避免导入重依赖。"""
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            source = f.read()
        tree = ast.parse(source, filename=fpath)
    except Exception:
        return None

    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue

        for target in targets:
            if isinstance(target, ast.Name) and target.id == 'INDICATOR_META':
                try:
                    meta = ast.literal_eval(node.value)
                except Exception:
                    return None
                if isinstance(meta, dict):
                    return meta
                return None
    return None


def _discover():
    """扫描 indicators 目录下所有 ind_*.py 文件，构建轻量注册表。"""
    pattern = os.path.join(os.path.dirname(__file__), 'ind_*.py')
    for fpath in sorted(glob.glob(pattern)):
        mod_name = os.path.basename(fpath)[:-3]  # e.g. ind_01_volume_strategy
        meta = _extract_meta_from_file(fpath)

        # 回退：静态解析失败时，尝试导入模块获取元信息
        if not meta:
            try:
                mod = importlib.import_module(f'.{mod_name}', package='AlphaFin.indicators')
                if hasattr(mod, 'INDICATOR_META'):
                    meta = dict(mod.INDICATOR_META)
                    _MODULE_CACHE[mod_name] = mod
            except Exception as e:
                print(f'[WARNING] 读取指标元信息失败 {mod_name}: {e}')
                continue

        indicator_id = meta.get('id')
        if not indicator_id:
            print(f'[WARNING] 指标 {mod_name} 缺少 id，已跳过')
            continue
        if indicator_id in _EXCLUDED_INDICATOR_IDS:
            continue

        item = dict(meta)
        item['_module_name'] = mod_name
        REGISTRY[indicator_id] = item


def get_indicator_module(indicator_id):
    """按需加载指标模块。"""
    meta = REGISTRY.get(indicator_id)
    if not meta:
        return None

    mod_name = meta.get('_module_name')
    if not mod_name:
        return None

    with _MODULE_LOCK:
        cached = _MODULE_CACHE.get(mod_name)
        if cached is not None:
            return cached

        mod = importlib.import_module(f'.{mod_name}', package='AlphaFin.indicators')
        if not hasattr(mod, 'generate'):
            raise RuntimeError('指标模块缺少 generate 函数: %s' % mod_name)
        _MODULE_CACHE[mod_name] = mod
        return mod


def get_grouped():
    """按 group 分组返回指标列表。"""
    groups = {}
    for _, meta in REGISTRY.items():
        g = meta.get('group', '未分组')
        groups.setdefault(g, []).append(meta)
    return groups


_discover()
