#!/bin/bash
# 文件名: update_data.sh

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/root/bin:$PATH
PROJECT_DIR="/root/nt_project"
PYTHON_EXEC="/root/miniconda3/bin/python"
PUSHPLUS_TOKEN="your_pushplus_token_here"  # 请替换为您的 PushPlus Token

# --- 函数定义 ---
send_pushplus() {
    local title="$1"
    local content="$2"
    local url="http://www.pushplus.plus/send"
    
    # 简单的 JSON 转义
    local safe_content=$(echo "$content" | sed 's/"/\\"/g' | sed ':a;N;$!ba;s/\n/\\n/g')
    
    curl -s -X POST "$url" \
        -H "Content-Type: application/json" \
        -d "{\"token\": \"$PUSHPLUS_TOKEN\", \"title\": \"【NT_Project】$title\", \"content\": \"$safe_content\"}" > /dev/null
}

handle_error() {
    local line_no=$1
    local command="$BASH_COMMAND"
    echo "❌ [错误] 命令 '$command' 在第 $line_no 行执行失败。"
    send_pushplus "任务崩溃 (Shell)" "脚本 update_data.sh 在第 $line_no 行执行 '$command' 时失败，已停止运行。"
    exit 1
}

# 🛡️ 开启错误捕获
trap 'handle_error $LINENO' ERR

echo "============================================"
echo "🚀 [$(date '+%Y-%m-%d %H:%M:%S')] 任务开始: 增量智能更新"
echo "============================================"

cd $PROJECT_DIR

# 🟢 [核心修改] 删除了清库操作，直接开始增量采集
echo "--------------------------------------------"
echo "📥 [1/5] 正在执行增量采集 (etl_ingest_tushare.py)..."
$PYTHON_EXEC etl_ingest_tushare.py

# [步骤 2] 运行全量考古
echo "--------------------------------------------"
echo "⏳ [2/5] 正在进行历史考古 (batch_history_trace.py)..."
$PYTHON_EXEC batch_history_trace.py

# [步骤 3] 初步分析
echo "--------------------------------------------"
echo "🧮 [3/5] 生成初步分析..."
$PYTHON_EXEC analysis_engine.py

# [步骤 4] 自动修复
echo "--------------------------------------------"
echo "🔧 [4/5] 执行自动修复..."
$PYTHON_EXEC fix_stock.py

# [步骤 5] 最终分析
echo "--------------------------------------------"
echo "🏁 [5/5] 刷新最终报表..."
$PYTHON_EXEC analysis_engine.py

echo "============================================"
echo "🎉 更新完毕！"
echo "============================================"

# 发送成功通知
send_pushplus "任务完成" "update_data.sh 所有步骤已成功执行完毕。"
