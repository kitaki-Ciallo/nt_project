#!/bin/bash

# ==========================================
# 🟢 环境变量配置
# ==========================================
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/root/bin:$PATH

# ==========================================
# 🇨🇳 国家队数据更新自动化脚本 (v2.1 稳定流水线版)
# 执行策略：双重分析 (Analysis -> Fix -> Analysis)
# ==========================================

# 1. 配置路径
PROJECT_DIR="/root/nt_project"
PYTHON_EXEC="/root/miniconda3/bin/python"

# 2. 记录开始
echo "============================================"
echo "🚀 [$(date '+%Y-%m-%d %H:%M:%S')] 任务开始: 数据全量更新"
echo "============================================"

# 3. 进入目录
cd $PROJECT_DIR || { echo "❌ 错误: 找不到项目目录 $PROJECT_DIR"; exit 1; }

# 4. [步骤 1] 清空原始数据表 (保留 nt_history_cost)
echo "🧹 [1/6] 正在重置原始数据表..."
docker-compose exec -T db psql -U quant_user -d national_team_db -c "
TRUNCATE TABLE 
    nt_shareholders, 
    nt_market_data, 
    nt_positions_analysis, 
    stock_basic
RESTART IDENTITY CASCADE;
"
if [ $? -ne 0 ]; then echo "❌ 数据库操作失败"; exit 1; fi

# 5. [步骤 2] 运行 ETL (采集)
echo "--------------------------------------------"
echo "📥 [2/6] 正在采集最新数据 (etl_ingest.py)..."
$PYTHON_EXEC etl_ingest.py
if [ $? -ne 0 ]; then echo "❌ ETL 运行失败"; exit 1; fi

# 6. [步骤 3] 运行全量考古 (计算)
echo "--------------------------------------------"
echo "⏳ [3/6] 正在进行全量历史考古 (batch_history_trace.py)..."
$PYTHON_EXEC batch_history_trace.py

# 7. [步骤 4] 第一次运行分析 (为补漏脚本提供侦测目标)
echo "--------------------------------------------"
echo "🧮 [4/6] 正在生成初步分析报告 (analysis_engine.py)..."
$PYTHON_EXEC analysis_engine.py
if [ $? -ne 0 ]; then echo "❌ 初步分析运行失败"; exit 1; fi

# 8. [步骤 5] 运行自动巡检修复 (补漏)
# 这一步会读取上一步生成的 nt_positions_analysis 表，找出“近期估算”的股票进行修复
echo "--------------------------------------------"
echo "🔧 [5/6] 正在执行自动巡检与修复 (fix_stock.py)..."
$PYTHON_EXEC fix_stock.py

# 9. [步骤 6] 第二次运行分析 (刷新最终结果)
echo "--------------------------------------------"
echo "🏁 [6/6] 正在刷新最终报表 (analysis_engine.py)..."
$PYTHON_EXEC analysis_engine.py
if [ $? -ne 0 ]; then echo "❌ 最终分析运行失败"; exit 1; fi

# 10. 结束
echo "============================================"
echo "🎉 [$(date '+%Y-%m-%d %H:%M:%S')] 所有任务执行完毕！系统已更新至最新状态。"
echo "============================================"
