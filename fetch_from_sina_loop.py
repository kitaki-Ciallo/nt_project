import requests
import pandas as pd
import time
import sys

print("🏎️ 启动新浪财经极速翻页模式...")

url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"}

all_stocks = []
page = 1

# 新浪只要没数据了就会返回空列表，所以死循环读到空为止
while True:
    params = {
        "page": page,
        "num": 100,  # 顺从它的限制
        "sort": "symbol",
        "asc": 1,
        "node": "hs_a", 
        "symbol": "",
        "_s_r_a": "page"
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=5)
        data = r.json()
        
        if not data:
            print(f"\n🏁 第 {page} 页为空，抓取结束！")
            break
            
        # 提取数据
        df_temp = pd.DataFrame(data)
        if 'code' in df_temp.columns and 'name' in df_temp.columns:
            # 清洗一下，只要 code 和 name
            batch = df_temp[['code', 'name']].rename(columns={'code': 'ts_code'}).to_dict('records')
            all_stocks.extend(batch)
        
        # 打印进度 (覆盖同一行)
        sys.stdout.write(f"\r🚀 正在冲刺第 {page} 页 | 已获取: {len(all_stocks)} 只")
        sys.stdout.flush()
        
        page += 1
        
        # 极短的休息，新浪一般不管
        time.sleep(0.1)
        
        # 安全阈值，防止死循环
        if page > 100: 
            print("\n⚠️ 达到页数上限，强制停止")
            break

    except Exception as e:
        print(f"\n❌ 第 {page} 页发生错误: {e}")
        # 遇到错误不要停，尝试跳过或停止
        break

print("\n")

if len(all_stocks) > 1000:
    print("💾 正在保存缓存...")
    df = pd.DataFrame(all_stocks)
    # 再次确保列名正确
    df = df[['ts_code', 'name']]
    df.to_csv("stock_list_cache.csv", index=False)
    print(f"🎉 胜利会师！已生成 stock_list_cache.csv，共 {len(df)} 只股票。")
    print("👉 请立即运行 ./update_data.sh")
else:
    print(f"⚠️ 只抓到了 {len(all_stocks)} 只，数量太少，可能有问题。")
    # 哪怕少，也先存下来给个保底
    if len(all_stocks) > 0:
        pd.DataFrame(all_stocks).to_csv("stock_list_cache.csv", index=False)
