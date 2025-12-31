# -*- coding: utf-8 -*-
import requests
import pandas as pd
import time

# 测试目标
TEST_CODES = ['002028', '600519', '601398', '601318']

def get_secid(code):
    """根据代码判断市场 ID (东方财富格式: 1.600xxx, 0.000xxx)"""
    if str(code).startswith('6'):
        return f"1.{code}"
    elif str(code).startswith(('8', '4')): 
        return f"0.{code}" # 北交所通常也是 0
    else:
        return f"0.{code}" # 深市

def test_fetch_realtime_indicators(ts_code):
    print(f"\n>>> 正在测试代码: {ts_code} (API 直连) ...")
    start_time = time.time()
    
    # 东方财富实时行情接口 (网页版 F10 头部数据源)
    url = "http://push2.eastmoney.com/api/qt/stock/get"
    
    # f162: PE(动), f164: PE(TTM), f167: PB, f170: 股息率, f55: EPS (可能不准，通常用计算), f57: 代码
    # f58: 名称, f43: 最新价, f163: PE(静)
    params = {
        "invt": "2",
        "fltt": "2",
        "fields": "f43,f57,f58,f162,f164,f167,f170,f163", 
        "secid": get_secid(ts_code),
        "ut": "fa5fd1943c7b386f172d68934880c8d6", # 公共 Token
        "cb": "jQuery123", # JSONP 回调 (虽然我们不用 JSONP，但加上模拟浏览器)
        "_": str(int(time.time() * 1000))
    }
    
    data = {
        "ts_code": ts_code,
        "name": None, "price": None,
        "pe_ttm": None, "pe_dyn": None, "pe_static": None,
        "pb": None, "div_rate": None
    }
    
    try:
        res = requests.get(url, params=params, timeout=3)
        # 接口返回的是 jQuery123({...}); 格式，需要清洗
        text = res.text
        # 提取 {...} JSON 部分
        if "(" in text and ")" in text:
            json_str = text.split("(", 1)[1].rsplit(")", 1)[0]
            import json
            resp_json = json.loads(json_str)
            
            if resp_json and resp_json.get('data'):
                d = resp_json['data']
                
                # 辅助函数：处理 "-" 为 None
                def parse_val(val):
                    if val == "-": return None
                    try: return float(val)
                    except: return val

                data['name'] = d.get('f58')
                data['price'] = parse_val(d.get('f43'))
                data['pe_dyn'] = parse_val(d.get('f162'))
                data['pe_static'] = parse_val(d.get('f163'))
                data['pe_ttm'] = parse_val(d.get('f164'))
                data['pb'] = parse_val(d.get('f167'))
                data['div_rate'] = parse_val(d.get('f170'))
                
                print(f"   ✅ 抓取成功: {data['name']} (¥{data['price']})")
                print(f"      PE(TTM): {data['pe_ttm']}, 股息率: {data['div_rate']}%, PE(动): {data['pe_dyn']}")
            else:
                print("   ❌ 接口返回 data 为空 (可能代码错误或停牌)")
        else:
            print("   ❌ 接口返回格式异常")
            
    except Exception as e:
        print(f"   ❌ 请求发生异常: {e}")

    elapsed = time.time() - start_time
    print(f"   ⏱️ 耗时: {elapsed:.2f}秒")
    return data

if __name__ == "__main__":
    print("================ 🚀 开始 DEBUG 测试 (v6 - API直连) ================")
    results = []
    for code in TEST_CODES:
        res = test_fetch_realtime_indicators(code)
        results.append(res)
    
    print("\n================ 📊 测试结果汇总 ================")
    df_res = pd.DataFrame(results)
    print(df_res)
    
    print("\n-------------------------------------------------")
    valid_ttm = df_res['pe_ttm'].count()
    valid_div = df_res['div_rate'].count()
    
    if valid_ttm == len(TEST_CODES):
        print(f"✅ TTM 完美获取 ({valid_ttm}/{len(TEST_CODES)})")
    else:
        print(f"⚠️ TTM 缺失 ({valid_ttm}/{len(TEST_CODES)})")
        
    if valid_div > 0:
        print(f"✅ 股息率获取成功 ({valid_div}/{len(TEST_CODES)})")
    else:
        print("⚠️ 股息率依然有问题...")
