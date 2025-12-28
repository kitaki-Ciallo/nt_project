#!/bin/bash

# ==========================================
# 🇨🇳 国家队数据更新自动化脚本
# 功能：清空旧数据 -> 运行ETL -> 运行分析引擎
# ==========================================

# 1. 配置路径变量 (根据你的 tree 结构)
PROJECT_DIR="/root/nt_project"
CONDA_BASE="/root/miniconda3"
ENV_NAME="nt_env"

# 2. 记录开始时间
echo "============================================"
echo "🚀 [$(date '+%Y-%m-%d %H:%M:%S')] 任务开始: 数据全量更新"
echo "============================================"

# 3. 进入项目目录
cd $PROJECT_DIR || { echo "❌ 错误: 找不到项目目录 $PROJECT_DIR"; exit 1; }

# 4. 激活 Conda 环境
# 注意：Shell 脚本中 source 激活是最稳妥的方式
source "$CONDA_BASE/bin/activate" $ENV_NAME

# 5. [核心步骤] 清空数据库 (只删数据，不删表)
# 使用 docker-compose exec 调用容器内的 psql 工具
# TRUNCATE 是最快的清空方式，CASCADE 会级联清除关联数据，RESTART IDENTITY 重置 ID 计数
echo "🧹 [1/3] 正在清空数据库表..."

# 请确保这里的表名是你数据库里实际存在的表名
# 如果你有更多表，请在命令中添加
docker-compose exec -T db psql -U quant_user -d national_team_db -c "
TRUNCATE TABLE 
    nt_shareholders, 
    nt_market_data, 
    nt_positions_analysis,
    stock_basic
RESTART IDENTITY CASCADE;
"

if [ $? -eq 0 ]; then
    echo "✅ 数据库已清空"
else
    echo "❌ 数据库清空失败，请检查 Docker 容器状态"
    exit 1
fi

# 6. 运行 ETL (数据采集与入库)
echo "--------------------------------------------"
echo "📥 [2/3] 正在运行 ETL (etl_ingest.py)..."
python etl_ingest.py

if [ $? -eq 0 ]; then
    echo "✅ ETL 数据采集完成"
else
    echo "❌ ETL 运行失败，脚本终止"
    exit 1
fi

# 7. 运行分析引擎 (计算盈亏)
echo "--------------------------------------------"
echo "🧮 [3/3] 正在运行分析引擎 (analysis_engine.py)..."
python analysis_engine.py

if [ $? -eq 0 ]; then
    echo "✅ 分析计算完成"
else
    echo "❌ 分析引擎运行失败"
    exit 1
fi

# 8. 结束
echo "============================================"
echo "🎉 [$(date '+%Y-%m-%d %H:%M:%S')] 所有任务执行完毕！"
echo "============================================"
