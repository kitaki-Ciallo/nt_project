# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine, text
import requests
import time
import json

# 配置 (直连本地 Docker 映射端口)
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
SSF_KEYWORDS = ["社保", "养老", "证金", "中央汇金", "全国社保", "基本养老", "中国证券金融", "社保基金", "汇金资管"]

CODE = "600031"
SECUCODE = "600031.SH"
NAME = "三一重工"

def test_insert():
    print(f"🚀 开始测试: {NAME} ({CODE})...")
    
    # 1. 抓取
    url = "https://datacenter.eastmoney.com/securities/api/data/get"
    params = {
        "type": "RPT_F10_EH_HOLDERS",
        "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_NUM_CHANGE", # 注意：没要 HOLD_RATIO
        "filter": f'(SECUCODE="{SECUCODE}")',
        "p": "1", "ps": "20",
        "source": "SELECT_SECU_DATA", "client": "WEB",
        "_": str(int(time.time() * 1000))
    }
    
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, params=params, headers=headers)
    data = res.json()
    
    if not (data.get('result') and data['result'].get('data')):
        print("❌ 抓取失败：API 返回空")
        return

    raw_df = pd.DataFrame(data['result']['data'])
    print(f"✅ 抓取到 {len(raw_df)} 条股东数据")

    # 2. 清洗 (复刻 v9.4)
    mask = raw_df['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
    target_df = raw_df[mask].copy().reset_index(drop=True) # 关键修复点
    
    print(f"✅ 命中 {len(target_df)} 条国家队数据")
    if target_df.empty: return

    clean_df = pd.DataFrame()
    clean_df['ts_code'] = [CODE] * len(target_df)
    
    # 这里的逻辑其实是错的，ann_date 应该是公告日，但为了不报错先这么填
    clean_df['ann_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
    clean_df['end_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
    
    clean_df['holder_name'] = target_df['HOLDER_NAME']
    clean_df['hold_amount'] = target_df['HOLD_NUM'].astype(float) / 10000
    
    def parse_chg(x):
        try: return float(x) / 10000
        except: return 0
    clean_df['chg_amount'] = target_df['HOLD_NUM_CHANGE'].apply(parse_chg)
    
    # 3. 入库
    print("⚡️ 正在尝试写入数据库...")
    engine = create_engine(DB_URL)
    
    data_list = clean_df.to_dict(orient='records')
    cols = list(data_list[0].keys())
    values_str = ", ".join([f":{c}" for c in cols])
    
    sql = text(f"INSERT INTO nt_shareholders ({','.join(cols)}) VALUES ({values_str}) ON CONFLICT (ts_code, holder_name, end_date) DO NOTHING")
    
    try:
        with engine.connect() as conn:
            conn.execute(sql, data_list)
            conn.commit()
        print("🎉🎉🎉 入库成功！")
        
        # 验证一下
        with engine.connect() as conn:
            res = conn.execute(text(f"SELECT * FROM nt_shareholders WHERE ts_code='{CODE}'"))
            rows = res.fetchall()
            print(f"🔎 查库确认: 数据库里现在有 {len(rows)} 条三一重工记录")
            
    except Exception as e:
        print("\n💣💣💣 入库崩溃！")
        print(f"错误信息: {e}")

if __name__ == "__main__":
    test_insert()
