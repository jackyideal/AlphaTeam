"""
统一上下文上传服务（官方文件解析模式）：
- 使用 DashScope OpenAI 兼容 Files 接口上传（purpose=file-extract）
- 保存 context_id -> file_id 映射
- 对外提供 fileid:// 引用与文本兜底摘要
"""
import json
import os
import re
import shutil
import time
import uuid

import requests

from AlphaFin.config import BASE_DIR, QWEN_API_KEY, QWEN_BASE_URL

CONTEXT_ROOT = os.path.join(BASE_DIR, 'AlphaFin', 'data', 'context_uploads')
MAX_FILES_PER_UPLOAD = 8
MAX_FILE_BYTES = 20 * 1024 * 1024
MAX_TOTAL_CONTEXT_CHARS = 12000
CONTEXT_TTL_SECONDS = 7 * 24 * 3600
FILE_PARSE_MODEL = os.getenv('ALPHAFIN_FILE_PARSE_MODEL', 'qwen-long')

TEXT_EXTS = {
    'txt', 'md', 'markdown', 'csv', 'tsv', 'json', 'log', 'ini', 'cfg',
    'yaml', 'yml', 'py', 'sql', 'xml', 'html', 'htm'
}
DOC_EXTS = {'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'epub', 'mobi'}
IMAGE_EXTS = {'png', 'jpg', 'jpeg', 'webp', 'bmp', 'gif'}
ALLOWED_EXTS = TEXT_EXTS | DOC_EXTS | IMAGE_EXTS


def _safe_filename(name):
    base = os.path.basename(str(name or 'file'))
    base = re.sub(r'[^A-Za-z0-9._-]+', '_', base)
    if not base:
        base = 'file'
    return base[:120]


def _safe_ext(name):
    low = str(name or '').lower().strip()
    if '.' not in low:
        return ''
    return low.rsplit('.', 1)[-1]


def _safe_context_id(raw):
    cid = str(raw or '').strip()
    if re.fullmatch(r'[a-f0-9]{12}', cid):
        return cid
    return ''


def _extract_content_text(message):
    if isinstance(message, str):
        return message.strip()
    if isinstance(message, list):
        chunks = []
        for item in message:
            if isinstance(item, dict):
                txt = item.get('text')
                if txt:
                    chunks.append(str(txt))
            elif isinstance(item, str):
                chunks.append(item)
        return '\n'.join(chunks).strip()
    return str(message or '').strip()


def _clip_text(text, limit=2600):
    s = str(text or '').strip()
    if len(s) <= limit:
        return s
    return s[:limit] + '\n...(已截断)'


def _cleanup_expired_contexts():
    now_ts = time.time()
    if not os.path.isdir(CONTEXT_ROOT):
        return
    for name in os.listdir(CONTEXT_ROOT):
        cid = _safe_context_id(name)
        if not cid:
            continue
        dpath = os.path.join(CONTEXT_ROOT, cid)
        try:
            mtime = os.path.getmtime(dpath)
        except Exception:
            continue
        if now_ts - float(mtime) > CONTEXT_TTL_SECONDS:
            try:
                shutil.rmtree(dpath, ignore_errors=True)
            except Exception:
                pass


def _upload_file_extract(name, raw, timeout=120):
    """
    官方方法：
    POST {compatible-mode}/files
    form: file + purpose=file-extract
    """
    if not QWEN_API_KEY:
        return {'ok': False, 'error': '未配置 QWEN_API_KEY'}

    url = QWEN_BASE_URL.rstrip('/') + '/files'
    headers = {'Authorization': 'Bearer ' + QWEN_API_KEY}
    files = {'file': (name, raw)}
    data = {'purpose': 'file-extract'}
    try:
        resp = requests.post(url, headers=headers, files=files, data=data, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json() or {}
        file_id = payload.get('id') or ''
        status = payload.get('status') or ''
        if not file_id:
            return {'ok': False, 'error': '上传成功但未返回 file_id'}
        return {'ok': True, 'file_id': file_id, 'status': status, 'raw': payload}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def _normalize_context_ids(context_ids):
    if context_ids is None:
        return []
    if isinstance(context_ids, str):
        parts = [s.strip() for s in context_ids.split(',')]
        return [x for x in parts if _safe_context_id(x)]
    if isinstance(context_ids, (list, tuple)):
        out = []
        for c in context_ids:
            cid = _safe_context_id(c)
            if cid:
                out.append(cid)
        return out
    return []


def _load_context_meta(context_id):
    cid = _safe_context_id(context_id)
    if not cid:
        return None
    meta_path = os.path.join(CONTEXT_ROOT, cid, 'meta.json')
    if not os.path.isfile(meta_path):
        return None
    try:
        with open(meta_path, 'r', encoding='utf-8') as fr:
            return json.load(fr)
    except Exception:
        return None


def upload_context_files(files, module='general'):
    os.makedirs(CONTEXT_ROOT, exist_ok=True)
    _cleanup_expired_contexts()

    file_list = list(files or [])
    if not file_list:
        return {'ok': False, 'error': '未检测到上传文件'}
    if len(file_list) > MAX_FILES_PER_UPLOAD:
        file_list = file_list[:MAX_FILES_PER_UPLOAD]

    context_id = uuid.uuid4().hex[:12]
    ctx_dir = os.path.join(CONTEXT_ROOT, context_id)
    os.makedirs(ctx_dir, exist_ok=True)

    warnings = []
    parsed_items = []

    for idx, fobj in enumerate(file_list):
        name = _safe_filename(getattr(fobj, 'filename', '') or ('file_%d' % idx))
        ext = _safe_ext(name)
        if ext and ext not in ALLOWED_EXTS:
            warnings.append('%s: 暂不支持此类型(%s)' % (name, ext))
            continue

        try:
            raw = fobj.read() or b''
        except Exception:
            warnings.append('%s: 读取失败' % name)
            continue

        if not raw:
            warnings.append('%s: 空文件，已跳过' % name)
            continue
        if len(raw) > MAX_FILE_BYTES:
            warnings.append('%s: 文件过大(>%dMB)，已跳过' % (name, int(MAX_FILE_BYTES / 1024 / 1024)))
            continue

        local_name = '%02d_%s' % (idx + 1, name)
        fpath = os.path.join(ctx_dir, local_name)
        try:
            with open(fpath, 'wb') as fw:
                fw.write(raw)
        except Exception:
            warnings.append('%s: 本地保存失败' % name)
            continue

        upload_result = _upload_file_extract(name, raw)
        if not upload_result.get('ok'):
            warnings.append('%s: 官方上传失败(%s)' % (name, upload_result.get('error', 'unknown')))
            # 记录失败条目，便于前端可见
            parsed_items.append({
                'name': name,
                'ext': ext or '',
                'size': len(raw),
                'type': 'image' if ext in IMAGE_EXTS else 'document',
                'parse_source': 'dashscope_file_extract',
                'status': 'upload_failed',
                'file_id': '',
                'summary': '官方文件解析上传失败，暂不可被模型直接读取。',
            })
            continue

        file_id = str(upload_result.get('file_id') or '')
        status = str(upload_result.get('status') or '')
        parsed_items.append({
            'name': name,
            'ext': ext or '',
            'size': len(raw),
            'type': 'image' if ext in IMAGE_EXTS else 'document',
            'parse_source': 'dashscope_file_extract',
            'status': status or 'processed',
            'file_id': file_id,
            'summary': '官方 file_id: %s（状态: %s）' % (file_id, status or 'processed'),
        })

    if not parsed_items:
        try:
            shutil.rmtree(ctx_dir, ignore_errors=True)
        except Exception:
            pass
        return {
            'ok': False,
            'error': '没有可用文件（可能类型不支持或读取失败）',
            'warnings': warnings,
        }

    payload = {
        'context_id': context_id,
        'module': str(module or 'general'),
        'created_at': int(time.time()),
        'files': parsed_items,
    }
    try:
        with open(os.path.join(ctx_dir, 'meta.json'), 'w', encoding='utf-8') as fw:
            json.dump(payload, fw, ensure_ascii=False, indent=2)
    except Exception as e:
        return {'ok': False, 'error': '保存上下文失败: %s' % str(e)}

    return {
        'ok': True,
        'context_id': context_id,
        'files': [
            {
                'name': item['name'],
                'type': item['type'],
                'size': item['size'],
                'file_id': item.get('file_id', ''),
                'status': item.get('status', ''),
                'preview': _clip_text(item.get('summary', ''), 180),
            }
            for item in parsed_items
        ],
        'warnings': warnings,
    }


def build_context_file_refs(context_ids):
    _cleanup_expired_contexts()
    ids = _normalize_context_ids(context_ids)
    if not ids:
        return {'file_ids': [], 'fileid_system_content': '', 'used_ids': [], 'file_count': 0}

    file_ids = []
    used_ids = []
    for cid in ids:
        meta = _load_context_meta(cid)
        if not meta:
            continue
        files = meta.get('files') or []
        if not isinstance(files, list):
            continue
        used_ids.append(cid)
        for item in files:
            fid = str(item.get('file_id', '')).strip()
            if fid and fid not in file_ids:
                file_ids.append(fid)

    if not file_ids:
        return {'file_ids': [], 'fileid_system_content': '', 'used_ids': used_ids, 'file_count': 0}

    refs = ['fileid://%s' % fid for fid in file_ids]
    return {
        'file_ids': file_ids,
        'fileid_system_content': ','.join(refs),
        'used_ids': used_ids,
        'file_count': len(file_ids),
    }


def build_context_text(context_ids, max_total_chars=MAX_TOTAL_CONTEXT_CHARS):
    """
    文本兜底（用于日志展示/不支持 fileid 场景）：
    不再做本地内容解析，仅展示文件元信息与官方 file_id。
    """
    refs = build_context_file_refs(context_ids)
    file_ids = refs.get('file_ids') or []
    if not file_ids:
        return {'context_text': '', 'used_ids': refs.get('used_ids') or [], 'file_count': 0}

    lines = [
        '【用户上传上下文材料】',
        '材料已按官方 file-extract 上传，可通过 fileid:// 引用。',
        'file_id 列表:',
    ]
    for fid in file_ids:
        lines.append('- %s' % fid)
    text = '\n'.join(lines)
    if len(text) > max_total_chars:
        text = text[:max_total_chars] + '\n...(已截断)'
    return {
        'context_text': text,
        'used_ids': refs.get('used_ids') or [],
        'file_count': len(file_ids),
    }


def summarize_context_with_qwen_long(context_ids, user_question=''):
    """
    使用官方 file_id + qwen-long 生成简要摘要，供不支持 fileid 的链路使用（如多智能体问答入口）。
    """
    refs = build_context_file_refs(context_ids)
    system_ref = refs.get('fileid_system_content') or ''
    if not system_ref:
        return {'ok': True, 'summary': '', 'file_count': 0, 'file_ids': []}
    if not QWEN_API_KEY:
        return {'ok': False, 'error': '未配置 QWEN_API_KEY', 'summary': '', 'file_count': 0, 'file_ids': []}

    prompt = (
        '请基于所引用文档给出结构化摘要：核心事实、关键数字、时间与来源、风险提示。'
        '输出尽量精炼，控制在 12 条以内。'
    )
    if str(user_question or '').strip():
        prompt += '\n用户问题: ' + str(user_question).strip()

    url = QWEN_BASE_URL.rstrip('/') + '/chat/completions'
    headers = {
        'Authorization': 'Bearer ' + QWEN_API_KEY,
        'Content-Type': 'application/json',
    }
    payload = {
        'model': FILE_PARSE_MODEL,
        'messages': [
            {'role': 'system', 'content': '你是文档分析助手，请仅基于引用文档作答，不编造。'},
            {'role': 'system', 'content': system_ref},
            {'role': 'user', 'content': prompt},
        ],
        'temperature': 0.1,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json() or {}
        choices = data.get('choices') or []
        if not choices:
            return {'ok': False, 'error': 'qwen-long 返回空 choices', 'summary': '', 'file_count': 0, 'file_ids': []}
        msg = choices[0].get('message') or {}
        summary = _clip_text(_extract_content_text(msg.get('content')), 5000)
        return {
            'ok': True,
            'summary': summary,
            'file_count': int(refs.get('file_count') or 0),
            'file_ids': refs.get('file_ids') or [],
            'fileid_system_content': system_ref,
            'model': FILE_PARSE_MODEL,
        }
    except Exception as e:
        return {
            'ok': False,
            'error': str(e),
            'summary': '',
            'file_count': int(refs.get('file_count') or 0),
            'file_ids': refs.get('file_ids') or [],
            'fileid_system_content': system_ref,
            'model': FILE_PARSE_MODEL,
        }
