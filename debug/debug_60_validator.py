# -*- coding: utf-8 -*-
import pandas as pd
import requests
import time
import random
import json
import os

DEBUG_COUNT = 60 
OUTPUT_FILE = "debug_result.json"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
    "Connection": "keep-alive"
}

def get_secid(code):
    # 逻辑：沪市(6)是 1.xxx，其他(深/北)都是 0.xxx
    if str(code).startswith('6'): return f"1.{code}"
    else: return f"0.{code}"

def get_secucode(code):
    c = str(code)
    if c.startswith('6'): return f"{c}.SH"
    # 🟢 关键修复：加入 '9' 开头支持北交所
    elif c.startswith('8') or c.startswith('4') or c.startswith('9'): return f"{c}.BJ"
    else: return f"{c}.SZ"

def fetch_shareholders(code):
    url = "https://datacenter.eastmoney.com/securities/api/data/get"
    params = {
        "type": "RPT_F10_EH_HOLDERS",
        "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_NUM_CHANGE", 
        "filter": f'(SECUCODE="{get_secucode(code)}")',
        "p": "1", "ps": "10", 
        "st": "END_DATE", "sr": "-1",
        "source": "SELECT_SECU_DATA", "client": "WEB",
        "_": str(int(time.time() * 1000))
    }
    try:
        time.sleep(random.uniform(0.3, 0.6))
        res = requests.get(url, params=params, headers=headers, timeout=5)
        if res.status_code == 200:
            data = res.json()
            if data.get('result') and data['result'].get('data'):
                return data['result']['data'][:3]
    except: pass
    return []

def fetch_fundamentals(code):
    url = "http://push2.eastmoney.com/api/qt/stock/get"
    # 🟢 关键修复：加上 cb 参数，模拟旧版行为
    params = {
        "invt": "2", "fltt": "2",
        "fields": "f43,f116,f162,f164,f167,f173,f186,f184,f185", 
        "secid": get_secid(code),
        "ut": "fa5fd1943c7b386f172d68934880c8d6",
        "cb": "jQuery123",  # 👈 加上这个
        "_": str(int(time.time() * 1000))
    }
    
    try:
        time.sleep(random.uniform(0.2, 0.4))
        res = requests.get(url, params=params, headers=headers, timeout=5)
        text = res.text
        
        # 兼容处理：有括号切括号，没括号解JSON
        data = None
        if "(" in text:
            json_str = text.split("(", 1)[1].rsplit(")", 1)[0]
            d = json.loads(json_str).get('data')
        else:
            try: d = res.json().get('data')
            except: d = None
            
        if d:
            def f(v, unit=""): return f"{v}{unit}" if v != "-" else "无"
            return {
                "当前股价": f(d.get('f43')),
                "总市值": f(d.get('f116')/100000000, "亿") if d.get('f116') != "-" else "-",
                "市盈率(动)": f(d.get('f162')),
                "市净率(PB)": f(d.get('f167')),
                "ROE": f(d.get('f173'), "%"),
                "净利增长": f(d.get('f185'), "%")
            }
    except Exception as e:
        print(f"❌ {code} 基本面异常: {e}")
    return None

def main():
    print(f"🚀 启动 60 只股票体检 (已修复基本面抓取)...")
    if not os.path.exists("stock_list_cache.csv"): return
    df = pd.read_csv("stock_list_cache.csv", dtype={'ts_code': str})
    targets = df.head(DEBUG_COUNT).to_dict('records')
    results = []
    
    for i, row in enumerate(targets):
        code = row['ts_code']
        name = row['name']
        print(f"[{i+1}/{DEBUG_COUNT}] 检查: {name} ({code})...")
        holders = fetch_shareholders(code)
        funds = fetch_fundamentals(code)
        
        status_h = "✅ 有数据" if holders else "❌ 股东空"
        status_f = "✅ 有数据" if funds else "❌ 基本面空"
        
        results.append({
            "股票": f"{name} ({code})",
            "状态": f"股东[{status_h}] | 基本面[{status_f}]",
            "基本面": funds,
            "股东(前3)": holders
        })
        
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    print(f"🎉 完成！请查看 {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
