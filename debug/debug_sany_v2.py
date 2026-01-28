# -*- coding: utf-8 -*-
import requests
import pandas as pd
import time

# 目标：三一重工
CODE = "600031"
SECUCODE = "600031.SH"

# 你的关键词列表
SSF_KEYWORDS = ["社保", "养老", "证金", "中央汇金", "全国社保", "基本养老", "中国证券金融", "社保基金", "汇金资管"]

def inspect_raw_data():
    print(f"🚀 深度诊断: 三一重工 ({CODE})...")
    
    # 我们同时抓取【十大股东】和【十大流通股东】，看看国家队到底藏在哪里
    report_types = {
        "十大股东 (HOLDERS)": "RPT_F10_EH_HOLDERS",
        "十大流通股东 (FREEHOLDERS)": "RPT_F10_EH_FREEHOLDERS"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://data.eastmoney.com/"
    }

    for name, rpt_type in report_types.items():
        print(f"\n📡 正在抓取: {name} ...")
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        params = {
            "type": rpt_type,
            "sty": "END_DATE,HOLDER_NAME,HOLD_NUM",
            "filter": f'(SECUCODE="{SECUCODE}")',
            "p": "1", "ps": "10", # 只看最新的前10个
            "st": "END_DATE", "sr": "-1", # 按日期倒序
            "source": "SELECT_SECU_DATA", "client": "WEB",
            "_": str(int(time.time() * 1000))
        }
        
        try:
            res = requests.get(url, params=params, headers=headers, timeout=10)
            data = res.json()
            
            if not (data.get('result') and data['result'].get('data')):
                print(f"❌ {name} 返回空数据！")
                continue
                
            raw_df = pd.DataFrame(data['result']['data'])
            print(f"✅ 成功获取 {len(raw_df)} 条记录。")
            
            # 🔍 核心：打印原始数据，看看到底是啥
            print(f"👀 原始数据预览 (前 10 条):")
            print(f"{'日期':<12} | {'股东名称'}")
            print("-" * 50)
            
            for index, row in raw_df.iterrows():
                h_date = str(row.get('END_DATE', 'N/A'))[:10]
                h_name = str(row.get('HOLDER_NAME', 'N/A'))
                
                # 实时检查匹配情况
                is_match = any(k in h_name for k in SSF_KEYWORDS)
                mark = "✅" if is_match else "  "
                
                print(f"{h_date} | {mark} {h_name}")
                
        except Exception as e:
            print(f"❌ 请求发生异常: {e}")

if __name__ == "__main__":
    inspect_raw_data()
