# -*- coding: utf-8 -*-
import requests
import time
import json

# 选取几只高分红代表股 + 你的持仓
# 601398: 工商银行 (分红大户，肯定有数据)
# 600519: 贵州茅台 (高价股代表)
# 002028: 思源电气 (你关注的)
# 601127: 赛力斯 (可能不分红，作为对照)
TEST_CODES = ['601398', '600519', '002028', '601127']

def get_secid(code):
    if str(code).startswith('6'): return f"1.{code}"
    return f"0.{code}"

def check_static_dividend(ts_code):
    print(f"\n>>> 🔍 正在探测: {ts_code} ...")
    
    url = "http://push2.eastmoney.com/api/qt/stock/get"
    
    # 我们重点请求这几个字段：
    # f58: 名称
    # f43: 最新价
    # f170: 股息率 (TTM) - 官方给的
    # f115: 每股股利 (我们想用来算静态的) <--- 重点关注它！
    # f184: 净利增长率
    params = {
        "invt": "2", "fltt": "2",
        "fields": "f58,f43,f170,f115,f184", 
        "secid": get_secid(ts_code),
        "ut": "fa5fd1943c7b386f172d68934880c8d6",
        "cb": "jQuery123",
        "_": str(int(time.time() * 1000))
    }
    
    try:
        res = requests.get(url, params=params, timeout=3)
        text = res.text
        
        # 简单的 JSONP 解析
        if "(" in text and ")" in text:
            json_str = text.split("(", 1)[1].rsplit(")", 1)[0]
            data = json.loads(json_str)
            
            if data and data.get('data'):
                d = data['data']
                name = d.get('f58')
                price = d.get('f43')
                ttm_div = d.get('f170')
                dps = d.get('f115') # 每股股利 (Dividend Per Share)
                
                print(f"   🏠 股票名称: {name}")
                print(f"   💰 最新股价 (f43): {price}")
                print(f"   🌊 股息率TTM (f170): {ttm_div}%")
                print(f"   💵 每股股利 (f115): {dps}  <--- 核心关注！")
                
                # 尝试计算静态股息率
                static_rate = 0
                if dps != "-" and price != "-" and float(price) > 0:
                    static_rate = (float(dps) / float(price)) * 100
                    print(f"   🧮 你的计算结果 (f115/f43): {static_rate:.2f}%")
                else:
                    print(f"   ❌ 无法计算 (缺少每股股利或股价)")
            else:
                print("   ❌ 数据为空")
        else:
            print("   ❌ 格式错误")
            
    except Exception as e:
        print(f"   ❌ 请求报错: {e}")

if __name__ == "__main__":
    print("================ 🚀 静态股息率字段探测 ================")
    for code in TEST_CODES:
        check_static_dividend(code)
    print("\n======================================================")
