#!/bin/bash
set -e

# 解析出供 psql 使用的标准连接串 (去掉 +psycopg2)
PSQL_URL="${DB_URL/+psycopg2/}"

# 等待数据库 就绪
echo "等待数据库就绪..."
until psql "$PSQL_URL" -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

echo "Postgres is up - checking schema"

# 检查表是否存在，不存在则执行初始化
if ! psql "$PSQL_URL" -c "SELECT 1 FROM nt_shareholders LIMIT 1;" > /dev/null 2>&1; then
  echo "初始化数据库结构..."
  psql "$PSQL_URL" -f database_onlyTables.sql
fi

echo "启动 Streamlit 服务..."
streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
