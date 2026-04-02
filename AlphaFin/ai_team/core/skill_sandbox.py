"""
技能安全沙箱 - AST白名单验证 + 安全执行
智能体可创建新的分析函数，沙箱确保安全性
"""
import ast
import json
import multiprocessing as mp
import os
import time
import threading
from queue import Empty

from AlphaFin.ai_team.config import (
    ALLOWED_SKILL_IMPORTS, SKILL_EXECUTION_TIMEOUT,
    AUTO_APPROVE_CATEGORIES, MANUAL_REVIEW_CATEGORIES, SKILLS_DIR
)

# 禁止的名称（函数/模块）
FORBIDDEN_NAMES = {
    'open', 'exec', 'eval', '__import__', 'compile', 'globals', 'locals',
    'getattr', 'setattr', 'delattr', 'os', 'sys', 'subprocess', 'requests',
    'urllib', 'socket', 'shutil', 'pathlib', 'importlib', 'ctypes',
    'pickle', 'marshal', 'shelve', 'tempfile', 'signal',
}


class SecurityError(Exception):
    """安全违规异常"""
    pass


def _stringify_skill_output(output):
    """将技能输出转换为可序列化文本。"""
    if isinstance(output, (dict, list)):
        return json.dumps(output, ensure_ascii=False, default=str)[:3000]
    return str(output)[:3000]


def _build_safe_globals(input_data=None):
    """构建技能执行的受限全局变量。"""
    safe_globals = {
        '__builtins__': {
            'print': print, 'len': len, 'range': range, 'enumerate': enumerate,
            'zip': zip, 'map': map, 'filter': filter, 'sorted': sorted,
            'min': min, 'max': max, 'sum': sum, 'abs': abs, 'round': round,
            'int': int, 'float': float, 'str': str, 'bool': bool,
            'list': list, 'dict': dict, 'tuple': tuple, 'set': set,
            'isinstance': isinstance, 'type': type,
            'True': True, 'False': False, 'None': None,
        }
    }
    try:
        import pandas as pd
        import numpy as np
        safe_globals['pd'] = pd
        safe_globals['pandas'] = pd
        safe_globals['np'] = np
        safe_globals['numpy'] = np
    except ImportError:
        pass

    import math
    import json as _json
    import datetime
    safe_globals['math'] = math
    safe_globals['json'] = _json
    safe_globals['datetime'] = datetime
    safe_globals['input_data'] = input_data or {}
    return safe_globals


def _skill_exec_process(code_string, input_data, result_queue):
    """
    子进程执行技能代码。
    说明：结果统一转成文本，避免跨进程传递复杂对象失败。
    """
    try:
        safe_globals = _build_safe_globals(input_data)
        exec(code_string, safe_globals)
        output = safe_globals.get('result', '技能执行完成（无返回值）')
        payload = _stringify_skill_output(output)
        result_queue.put({'ok': True, 'output': payload})
    except Exception as e:
        result_queue.put({'ok': False, 'error': str(e)})


class SkillSandbox:
    """技能安全沙箱"""

    def __init__(self):
        self._lock = threading.Lock()
        self._manifest_path = os.path.join(SKILLS_DIR, 'skills_manifest.json')

    def validate_code(self, code_string):
        """
        解析 AST 并检查安全性。

        Returns:
            True 如果通过验证

        Raises:
            SecurityError 如果发现违规
        """
        try:
            tree = ast.parse(code_string)
        except SyntaxError as e:
            raise SecurityError('语法错误: %s' % str(e))

        for node in ast.walk(tree):
            # 检查 import 语句
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root_module = alias.name.split('.')[0]
                    if root_module not in ALLOWED_SKILL_IMPORTS:
                        raise SecurityError('禁止导入: %s (允许: %s)' % (
                            alias.name, ', '.join(ALLOWED_SKILL_IMPORTS)))

            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    root_module = node.module.split('.')[0]
                    if root_module not in ALLOWED_SKILL_IMPORTS:
                        raise SecurityError('禁止导入: from %s (允许: %s)' % (
                            node.module, ', '.join(ALLOWED_SKILL_IMPORTS)))

            # 检查禁止的名称
            elif isinstance(node, ast.Name):
                if node.id in FORBIDDEN_NAMES:
                    raise SecurityError('禁止使用: %s' % node.id)

            # 检查属性访问中的危险模式
            elif isinstance(node, ast.Attribute):
                if node.attr in ('system', 'popen', 'exec', 'eval', 'remove', 'rmdir', 'unlink'):
                    raise SecurityError('禁止调用: .%s()' % node.attr)
                # 禁止 __dunder__ 访问
                if node.attr.startswith('__') and node.attr.endswith('__'):
                    if node.attr not in ('__init__', '__str__', '__repr__', '__len__'):
                        raise SecurityError('禁止访问: %s' % node.attr)

        return True

    def create_skill(self, name, code_string, description, category='data_analysis', creator='unknown'):
        """
        创建新技能。

        Args:
            name: 技能名称（英文，用作函数名）
            code_string: Python 代码
            description: 技能描述
            category: 分类（data_analysis/visualization/statistics/trading_strategy/risk_rule/portfolio）
            creator: 创建者智能体ID

        Returns:
            dict: {'success': bool, 'message': str, 'skill_id': str, 'approved': bool}
        """
        # 验证代码安全性
        try:
            self.validate_code(code_string)
        except SecurityError as e:
            return {'success': False, 'message': '安全验证失败: %s' % str(e)}

        # 生成技能ID
        skill_id = 'skill_%s_%d' % (name, int(time.time()))

        # 确定是否自动批准
        auto_approve = category in AUTO_APPROVE_CATEGORIES

        # 保存技能代码
        os.makedirs(SKILLS_DIR, exist_ok=True)
        skill_file = os.path.join(SKILLS_DIR, skill_id + '.py')
        with open(skill_file, 'w', encoding='utf-8') as f:
            f.write('"""\n自动生成的技能: %s\n描述: %s\n创建者: %s\n"""\n\n' % (name, description, creator))
            f.write(code_string)

        # 更新清单
        skill_entry = {
            'id': skill_id,
            'name': name,
            'description': description,
            'category': category,
            'creator': creator,
            'approved': auto_approve,
            'file': skill_id + '.py',
            'created_at': time.time(),
        }

        with self._lock:
            manifest = self._load_manifest()
            manifest['skills'].append(skill_entry)
            self._save_manifest(manifest)

        status = '已自动部署' if auto_approve else '等待人工审核'
        return {
            'success': True,
            'message': '技能创建成功 (%s)' % status,
            'skill_id': skill_id,
            'approved': auto_approve,
        }

    def execute_skill(self, skill_id, input_data=None):
        """
        在沙箱中执行技能。

        Args:
            skill_id: 技能ID
            input_data: 传入的数据（dict）

        Returns:
            str: 执行结果的文本描述
        """
        # 检查是否已批准
        manifest = self._load_manifest()
        skill_entry = None
        for s in manifest.get('skills', []):
            if s['id'] == skill_id:
                skill_entry = s
                break

        if not skill_entry:
            return '技能不存在: %s' % skill_id
        if not skill_entry.get('approved', False):
            return '技能尚未批准: %s' % skill_id

        # 读取代码
        skill_file = os.path.join(SKILLS_DIR, skill_entry['file'])
        if not os.path.exists(skill_file):
            return '技能文件不存在'

        with open(skill_file, 'r', encoding='utf-8') as f:
            code_string = f.read()

        # 使用独立子进程执行，超时可真正终止，避免残留线程持续占用资源
        ctx = mp.get_context('spawn')
        result_queue = ctx.Queue(maxsize=1)
        proc = ctx.Process(
            target=_skill_exec_process,
            args=(code_string, input_data or {}, result_queue),
            daemon=True
        )
        try:
            proc.start()
        except Exception as e:
            return '技能执行错误: 子进程启动失败: %s' % str(e)

        proc.join(timeout=SKILL_EXECUTION_TIMEOUT)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=1.0)
            if proc.is_alive() and hasattr(proc, 'kill'):
                proc.kill()
                proc.join(timeout=1.0)
            try:
                result_queue.close()
                result_queue.join_thread()
            except Exception:
                pass
            return '技能执行超时（%d秒限制，已强制终止）' % SKILL_EXECUTION_TIMEOUT

        try:
            msg = result_queue.get(timeout=0.2)
        except Empty:
            msg = None
        except Exception:
            msg = None
        finally:
            try:
                result_queue.close()
                result_queue.join_thread()
            except Exception:
                pass

        if isinstance(msg, dict) and msg.get('ok'):
            return str(msg.get('output') or '')

        if isinstance(msg, dict) and msg.get('error'):
            return '技能执行错误: %s' % str(msg.get('error') or '')

        if proc.exitcode not in (0, None):
            return '技能执行错误: 子进程异常退出(exit=%s)' % str(proc.exitcode)

        return '技能执行错误: 未获取到执行结果'

    def list_skills(self, approved_only=False):
        """列出所有技能"""
        manifest = self._load_manifest()
        skills = manifest.get('skills', [])
        if approved_only:
            skills = [s for s in skills if s.get('approved', False)]
        return skills

    def _load_manifest(self):
        if os.path.exists(self._manifest_path):
            with open(self._manifest_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {'skills': [], 'version': '1.0'}

    def _save_manifest(self, manifest):
        os.makedirs(SKILLS_DIR, exist_ok=True)
        with open(self._manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)


# 全局单例
sandbox = SkillSandbox()
