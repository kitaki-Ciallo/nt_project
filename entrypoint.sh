#!/bin/bash
set -e

echo "启动 Streamlit 服务..."
exec streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
