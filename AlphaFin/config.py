"""
AlphaFin 系统配置
"""
import os

# 项目根目录（repository root）
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 本地数据库目录（不入仓库）；默认放在仓库内 data/db
DB_ROOT = os.getenv('ALPHAFIN_DB_ROOT', os.path.join(BASE_DIR, 'data', 'db'))
CHART_DIR = os.path.join(BASE_DIR, 'AlphaFin', 'static', 'charts')
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN', '')

# Qwen API 配置（通义千问，OpenAI兼容模式）
QWEN_API_KEY = os.getenv('QWEN_API_KEY', '')
QWEN_BASE_URL = os.getenv('QWEN_BASE_URL', 'https://dashscope.aliyuncs.com/compatible-mode/v1')
QWEN_MODEL = os.getenv('QWEN_MODEL', 'qwen-vl-plus')  # 需要视觉模型才能分析图表

# Kimi / Moonshot API 配置（用于统一联网搜索）
# 仅从环境变量读取，避免密钥写入仓库。
MOONSHOT_API_KEY = os.getenv('MOONSHOT_API_KEY', '')
MOONSHOT_BASE_URL = os.getenv('MOONSHOT_BASE_URL', 'https://api.moonshot.cn/v1')
MOONSHOT_MODEL = os.getenv('MOONSHOT_MODEL', 'kimi-k2.5')

# Flask 配置
SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-change-me')
DEBUG = str(os.getenv('FLASK_DEBUG', '1')).strip().lower() in ('1', 'true', 'yes', 'on')
