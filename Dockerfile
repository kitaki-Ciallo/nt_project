FROM python:3.10-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# 设置环境变量默认值
ENV DB_URL=postgresql+psycopg2://quant_user:quant_password_123@db:5432/national_team_db
ENV PYTHONUNBUFFERED=1
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# 暴露端口
EXPOSE 8501

# 设置启动脚本权限
RUN chmod +x entrypoint.sh update_data.sh

# 启动命令
ENTRYPOINT ["./entrypoint.sh"]
