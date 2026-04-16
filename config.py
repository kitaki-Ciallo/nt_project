import os

# ================= 核心配置区域 =================
# 数据库连接配置 (优先读取环境变量)
DB_URL = os.getenv('DB_URL', "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db")

# 推送服务 Token (PushPlus)
PUSHPLUS_TOKEN = os.getenv('PUSHPLUS_TOKEN', "your_pushplus_token_here")

# Tushare Token (仅 etl_ingest_tushare.py 使用)
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN', "your_tushare_token_here")

# ================= 业务参数配置 =================
# 国家队/机构识别关键词
SSF_KEYWORDS = ["社保", "梧桐树投资", "证金", "中央汇金", "全国社保", "基本养老", "中国证券金融", "社保基金", "汇金资管", "国新投资", "国家集成电路"]

# 🚀 并发配置
SHAREHOLDER_WORKERS = 8  # 股东数据抓取：极速
SENSITIVE_WORKERS = 2     # K线/基本面抓取：慢速 (防封)

# 💰 成本估算策略
COST_DISCOUNT = 0.95      # 估算成交价相对于 VWAP 的折扣
