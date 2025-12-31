# -*- coding: utf-8 -*-
"""
Debug v2: 全面扫描增长率字段
目标：一次性查清 f183-f187 到底对应什么指标
"""
import requests
import json
import time

def scan_fields(ts_code, name):
    secid = f"1.{ts_code}" if str(ts_code).startswith('6') else f"0.{ts_code}"
    url = "http://push2.eastmoney.com/api/qt/stock/get"
    
    # 扩大扫描范围：f183 ~ f187
    params = {
        "invt": "2", "fltt": "2",
        "fields": "f58,f183,f184,f185,f186,f187", 
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d68934880c8d6", "cb": "jQuery123", "_": str(int(time.time() * 1000))
    }
    
    print(f"\n🔍 正在扫描: {name} ({ts_code}) ...")
    try:
        res = requests.get(url, params=params, timeout=5)
        text = res.text
        if "(" in text:
            json_str = text.split("(", 1)[1].rsplit(")", 1)[0]
            data = json.loads(json_str)['data']
            
            # 辅助函数：尝试转亿
            def fmt_yi(val):
                try: 
                    v = float(val)
                    if v > 100000000: return f"{v/100000000:.2f} 亿"
                    return str(v)
                except: return str(val)

            print(f"   ----------------------------------------")
            print(f"   [f183] (猜测:总营收):   {fmt_yi(data.get('f183'))}")
            print(f"   [f184] (猜测:营收增长): {data.get('f184')}%")
            print(f"   [f185] (猜测:净利润):   {fmt_yi(data.get('f185'))}")
            print(f"   [f186] (猜测:净利增长): {data.get('f186')}%")
            print(f"   [f187] (备用字段):      {data.get('f187')}")
            print(f"   ----------------------------------------")
    except Exception as e:
        print(f"❌ 失败: {e}")

if __name__ == "__main__":
    scan_fields("600519", "贵州茅台")
    scan_fields("000957", "中通客车")
