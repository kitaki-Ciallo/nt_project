# -*- coding: utf-8 -*-
"""
Debug: 验证营收增长与利润增长字段
目标：确认 f183 和 f184 分别对应什么指标
"""
import requests
import json
import time

def check_metrics(ts_code, name):
    # 6开头用1.xxx，其他用0.xxx
    secid = f"1.{ts_code}" if str(ts_code).startswith('6') else f"0.{ts_code}"
    
    url = "http://push2.eastmoney.com/api/qt/stock/get"
    # f58:名称, f43:现价, f183:疑似营收增长, f184:疑似利润增长
    params = {
        "invt": "2", "fltt": "2",
        "fields": "f58,f43,f183,f184", 
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d68934880c8d6", 
        "cb": "jQuery123", 
        "_": str(int(time.time() * 1000))
    }
    
    print(f"\n🔍 正在查询: {name} ({ts_code}) ...")
    try:
        res = requests.get(url, params=params, timeout=5)
        text = res.text
        
        # 解析 jQuery123({...})
        if "(" in text and ")" in text:
            json_str = text.split("(", 1)[1].rsplit(")", 1)[0]
            data = json.loads(json_str)['data']
            
            val_183 = data.get('f183', '-')
            val_184 = data.get('f184', '-')
            
            print(f"   📊 字段 f183 (待验证:营收增长): {val_183}%")
            print(f"   💰 字段 f184 (待验证:利润增长): {val_184}%")
            print("-" * 30)
            print("   👉 请打开东方财富APP/网页，核对F10资料：")
            print(f"      看看【营收同比】是不是 {val_183}%？")
            print(f"      看看【净利同比】是不是 {val_184}%？")
    except Exception as e:
        print(f"❌ 请求失败: {e}")

if __name__ == "__main__":
    # 测试两只典型股票，方便比对
    check_metrics("600519", "贵州茅台")
    check_metrics("000957", "中通客车")
