# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine, text
import requests
import time

DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
SSF_KEYWORDS = ["社保", "养老", "证金", "中央汇金", "全国社保", "基本养老", "中国证券金融", "社保基金", "汇金资管"]

CODE = "600031"
SECUCODE = "600031.SH"
NAME = "三一重工"

def final_test():
    print(f"🚀 终极测试: {NAME} ({CODE}) 入库验证...")
    
    # 1. 抓取 (关键：带上排序参数 st, sr)
    url = "https://datacenter.eastmoney.com/securities/api/data/get"
    params = {
        "type": "RPT_F10_EH_HOLDERS",
        "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_NUM_CHANGE",
        "filter": f'(SECUCODE="{SECUCODE}")',
        "p": "1", "ps": "20",
        "st": "END_DATE", "sr": "-1", # 🟢 必须加这个，否则抓到2003年的数据
        "source": "SELECT_SECU_DATA", "client": "WEB",
        "_": str(int(time.time() * 1000))
    }
    
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, params=params, headers=headers)
    raw_df = pd.DataFrame(res.json()['result']['data'])
    print(f"✅ 抓取成功，最新一期日期: {raw_df.iloc[0]['END_DATE']}")

    # 2. 清洗 (关键：reset_index 防止数据库报错)
    mask = raw_df['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
    
    # 🟢 核心修复：reset_index(drop=True)
    # 如果不加这个，Pandas索引会对不上，导致插入 NaN，引发数据库崩溃
    target_df = raw_df[mask].copy().reset_index(drop=True)
    
    print(f"✅ 命中 {len(target_df)} 条国家队数据")

    clean_df = pd.DataFrame()
    clean_df['ts_code'] = [CODE] * len(target_df)
    clean_df['ann_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
    clean_df['end_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
    clean_df['holder_name'] = target_df['HOLDER_NAME']
    clean_df['hold_amount'] = target_df['HOLD_NUM'].astype(float) / 10000
    clean_df['chg_amount'] = 0 
    
    # 3. 入库
    print("⚡️ 正在写入数据库...")
    engine = create_engine(DB_URL)
    data_list = clean_df.to_dict(orient='records')
    cols = list(data_list[0].keys())
    values_str = ", ".join([f":{c}" for c in cols])
    
    sql = text(f"INSERT INTO nt_shareholders ({','.join(cols)}) VALUES ({values_str}) ON CONFLICT (ts_code, holder_name, end_date) DO NOTHING")
    
    with engine.connect() as conn:
        conn.execute(sql, data_list)
        conn.commit()
    
    # 4. 查库验证
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT holder_name, hold_amount, end_date FROM nt_shareholders WHERE ts_code='{CODE}'"))
        rows = res.fetchall()
        print(f"\n🎉🎉🎉 验证成功！数据库中现有 {len(rows)} 条记录：")
        for r in rows:
            print(f"   - {r[0]} | {r[1]}万股 | {r[2]}")

if __name__ == "__main__":
    final_test()
