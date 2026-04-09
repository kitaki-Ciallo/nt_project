#!/bin/bash
set -e

# 等待数据库 就绪
echo "等待数据库就绪..."
until psql $DB_URL -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

echo "Postgres is up - checking schema"

# 检查表是否存在，不存在则执行初始化
if ! psql $DB_URL -c "SELECT 1 FROM nt_shareholders LIMIT 1;" > /dev/null 2>&1; then
  echo "初始化数据库结构..."
  psql $DB_URL -f database_onlyTables.sql
fi

echo "启动 Streamlit 服务..."
streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
