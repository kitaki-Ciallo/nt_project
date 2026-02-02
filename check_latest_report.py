# -*- coding: utf-8 -*-
"""
财报期监控脚本
功能：扫描数据库中的最新财报期，如果最新财报期（end_date）或最新公告日（ann_date）是今天，则发送 PushPlus 推送。
"""
import requests
import datetime
from sqlalchemy import create_engine, text
import os

# ================= 配置区域 =================
# 数据库连接
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"

# PushPlus Token (与 etl_ingest_tushare.py 保持一致)
PUSHPLUS_TOKEN = "your_pushplus_token"
# ===========================================

def send_pushplus(title, content):
    """发送 PushPlus 通知"""
    if not PUSHPLUS_TOKEN: 
        print("⚠️ 未配置 PushPlus Token，跳过推送。")
        return
    
    url = "http://www.pushplus.plus/send"
    data = {
        "token": PUSHPLUS_TOKEN, 
        "title": f"【NT_Monitor】{title}", 
        "content": content
    }
    try: 
        res = requests.post(url, json=data, timeout=5)
        if res.status_code == 200:
            print("✅ 推送成功")
        else:
            print(f"⚠️ 推送失败: {res.text}")
    except Exception as e: 
        print(f"❌ 推送异常: {e}")

def check_latest_report():
    print(f"🔍 正在扫描数据库最新财报期... (当前时间: {datetime.datetime.now()})")
    
    try:
        engine = create_engine(DB_URL)
        today = datetime.date.today()
        
        with engine.connect() as conn:
            # 1. 获取库中最新的财报期 (end_date)
            latest_end_date = conn.execute(text("SELECT MAX(end_date) FROM nt_shareholders")).scalar()
            
            # 2. 获取库中最新的公告日期 (ann_date)
            latest_ann_date = conn.execute(text("SELECT MAX(ann_date) FROM nt_shareholders")).scalar()
            
            print(f"📊 库中最新财报期 (end_date): {latest_end_date}")
            print(f"📢 库中最新公告日 (ann_date): {latest_ann_date}")
            
            messages = []
            
            def get_stock_details(date_field, date_val):
                # 关联 stock_basic 获取股票名称
                sql = text(f"""
                    SELECT DISTINCT t.ts_code, b.name 
                    FROM nt_shareholders t
                    LEFT JOIN stock_basic b ON t.ts_code = b.ts_code
                    WHERE t.{date_field} = :d
                """)
                return conn.execute(sql, {"d": date_val}).fetchall()

            # 检查逻辑 1: 最新财报期是否就是今天
            if latest_end_date == today:
                rows = get_stock_details("end_date", today)
                if rows:
                    # 格式化输出：代码 名称
                    details = "\n".join([f"- {r[0]} {r[1] if r[1] else ''}" for r in rows[:50]])
                    if len(rows) > 50: details += f"\n... (共 {len(rows)} 只，仅显示前 50 只)"
                    
                    msg = f"📅 发现最新财报期 (end_date) 为今天 ({today})！\n涉及股票:\n{details}"
                    messages.append(msg)
                    print(f"✅ 触发 end_date 推送，共 {len(rows)} 条")
            
            # 检查逻辑 2: 最新公告日是否就是今天
            if latest_ann_date == today:
                rows = get_stock_details("ann_date", today)
                if rows:
                    details = "\n".join([f"- {r[0]} {r[1] if r[1] else ''}" for r in rows[:50]])
                    if len(rows) > 50: details += f"\n... (共 {len(rows)} 只，仅显示前 50 只)"
                    
                    msg = f"📝 发现最新公告日 (ann_date) 为今天 ({today})！\n涉及股票:\n{details}\n\n⚠️ 请注意国家队动向"
                    messages.append(msg)
                    print(f"✅ 触发 ann_date 推送，共 {len(rows)} 条")
                
            # 如果有触发条件，发送推送
            if messages:
                full_content = "\n\n".join(messages)
                send_pushplus(f"发现最新财报数据 ({today})", full_content)
            else:
                print("💤 今天不是最新财报期，也无最新公告，无推送。")

    except Exception as e:
        print(f"❌ 检查失败: {e}")
        send_pushplus("监控脚本报错", str(e))

if __name__ == "__main__":
    check_latest_report()
