import requests
import pandas as pd
import time
import json

print("🕵️‍♂️ 正在诊断贵州茅台 (600519) 的股东数据接口...")

# 这是 ETL 脚本里实际用的 URL (datacenter 域名，跟 push2 不一样！)
url = "https://datacenter.eastmoney.com/securities/api/data/get"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
}

# 查茅台的前十大股东
params = {
    "type": "RPT_F10_EH_HOLDERS",
    "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_NUM_CHANGE",
    "filter": '(SECUCODE="600519.SH")',
    "p": "1", "ps": "50", "st": "END_DATE", "sr": "-1",
    "source": "SELECT_SECU_DATA", "client": "WEB",
    "_": str(int(time.time() * 1000))
}

try:
    print(f"📡 请求 URL: {url}")
    res = requests.get(url, params=params, headers=headers, timeout=10)
    
    print(f"⬇️ HTTP 状态码: {res.status_code}")
    
    if res.status_code != 200:
        print("❌ 接口报错，可能被封了！")
        exit()
        
    data = res.json()
    # 打印原始数据的简略版
    print(f"📦 返回数据结构 keys: {list(data.keys())}")
    
    if data.get('result') and data['result'].get('data'):
        df = pd.DataFrame(data['result']['data'])
        print(f"✅ 成功获取到 {len(df)} 条股东记录！")
        print("--- 前 5 条数据 ---")
        print(df[['END_DATE', 'HOLDER_NAME']].head())
        
        # 测试国家队关键词匹配
        keywords = ["社保", "养老", "证金", "中央汇金", "中国证券金融"]
        mask = df['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in keywords))
        matches = df[mask]
        
        print(f"\n🔍 匹配到国家队: {len(matches)} 条")
        if not matches.empty:
            print(matches[['END_DATE', 'HOLDER_NAME']])
        else:
            print("⚠️ 数据里居然没找到国家队？可能是关键词列表需要更新。")
            
    else:
        print("❌ 警告：HTTP 200 OK，但返回了空数据 (result=None 或 data=None)！")
        print("💡 结论：这就是典型的【静默限流】。服务器不想理你，但为了不报错，给了个空壳。")
        print(f"原始响应: {res.text[:200]}...")

except Exception as e:
    print(f"❌ 发生异常: {e}")
