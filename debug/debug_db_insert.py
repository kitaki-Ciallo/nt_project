# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine, text
import requests
import time

# 配置 (与 docker-compose 保持一致，使用 localhost 因为你在宿主机运行脚本)
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
SSF_KEYWORDS = ["社保", "养老", "证金", "中央汇金", "全国社保", "基本养老", "中国证券金融", "社保基金", "汇金资管"]

# 🎯 目标：工商银行 (数据量巨大，最容易撑爆数据库字段)
CODE = "601398"
SECUCODE = "601398.SH"

def debug_insert():
    print(f"🚀 开始诊断数据库写入：目标 {CODE} (工商银行)...")
    
    # 1. 抓取数据
    url = "https://datacenter.eastmoney.com/securities/api/data/get"
    params = {
        "type": "RPT_F10_EH_HOLDERS",
        "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_NUM_CHANGE",
        "filter": f'(SECUCODE="{SECUCODE}")',
        "p": "1", "ps": "20",
        "source": "SELECT_SECU_DATA", "client": "WEB",
        "_": str(int(time.time() * 1000))
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://data.eastmoney.com/"
    }
    
    print("📡 发送请求中...")
    res = requests.get(url, params=params, headers=headers)
    data = res.json()
    
    if not (data.get('result') and data['result'].get('data')):
        print("❌ API 未返回数据，无法测试！")
        return

    raw_df = pd.DataFrame(data['result']['data'])
    print(f"✅ 抓取成功，原始数据 {len(raw_df)} 条")

    # 2. 清洗数据
    mask = raw_df['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
    target_df = raw_df[mask].copy()
    print(f"✅ 筛选国家队，命中 {len(target_df)} 条")
    
    # 构造入库数据
    clean_df = pd.DataFrame()
    clean_df['ts_code'] = [CODE] * len(target_df)
    clean_df['ann_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
    clean_df['end_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
    clean_df['holder_name'] = target_df['HOLDER_NAME']
    # ⚠️ 注意：这里除以了 10000，变成了“万股”
    clean_df['hold_amount'] = target_df['HOLD_NUM'].astype(float) / 10000
    clean_df['chg_amount'] = 0 # 简化测试

    print("🧪 准备入库的数据样本 (注意看 hold_amount 的大小):")
    print(clean_df[['holder_name', 'hold_amount']].head(2))

    # 3. 暴力入库 (无 try-except保护)
    print("\n⚡️ 正在尝试写入数据库 (如有报错将直接显示)...")
    engine = create_engine(DB_URL)
    
    data_list = clean_df.to_dict(orient='records')
    cols = list(data_list[0].keys())
    values_str = ", ".join([f":{c}" for c in cols])
    
    sql = text(f"INSERT INTO nt_shareholders ({','.join(cols)}) VALUES ({values_str}) ON CONFLICT (ts_code, holder_name, end_date) DO NOTHING")
    
    with engine.connect() as conn:
        conn.execute(sql, data_list)
        conn.commit()
    
    print("\n🎉🎉🎉 入库成功！")
    print("如果看到这句话，说明数据库字段够大，问题可能出在别的字段。")

if __name__ == "__main__":
    try:
        debug_insert()
    except Exception as e:
        print("\n💣💣💣 数据库写入崩溃 (CRITICAL ERROR) 💣💣💣")
        print(f"❌ 错误类型: {type(e)}")
        print(f"❌ 错误详情: {e}")
        print("\n💡 分析建议：")
        if "numeric field overflow" in str(e):
            print("👉 实锤了！是因为持股数量太大，超过了数据库字段的上限。")
            print("👉 比如 DECIMAL(10,2) 最多存 9999万，而汇金持有工行 1240亿。")
        elif "value too long" in str(e):
            print("👉 字段长度不够。可能是 holder_name 定义得太短。")
