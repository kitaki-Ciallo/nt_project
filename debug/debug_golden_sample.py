# -*- coding: utf-8 -*-
import requests
import pandas as pd
import time
import json

# 🎯 黄金样本：这些股票的前十大股东里，【绝对】有“汇金”或“证金”
# 601398 工商银行, 601288 农业银行, 601988 中国银行, 600036 招商银行, 600519 贵州茅台
GOLDEN_SAMPLES = ["601398", "601288", "601988", "600036", "600519"]

# 你的关键字
SSF_KEYWORDS = ["社保", "养老", "证金", "中央汇金", "全国社保", "基本养老", "中国证券金融", "社保基金", "汇金资管"]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
    "Connection": "keep-alive"
}

def check_one_stock(code, name):
    print(f"\n======== 正在诊断: {name} ({code}) ========")
    
    # 1. 构造请求
    secucode = f"{code}.SH" # 这些都是沪市大蓝筹
    url = "https://datacenter.eastmoney.com/securities/api/data/get"
    params = {
        "type": "RPT_F10_EH_HOLDERS",
        "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_NUM_CHANGE",
        "filter": f'(SECUCODE="{secucode}")',
        "p": "1", "ps": "20",
        "st": "END_DATE", "sr": "-1",
        "source": "SELECT_SECU_DATA", "client": "WEB",
        "_": str(int(time.time() * 1000))
    }
    
    try:
        # 2. 发送请求
        print(f"📡 [Step 1] 发送请求...")
        res = requests.get(url, params=params, headers=headers, timeout=10)
        if res.status_code != 200:
            print(f"❌ HTTP 失败: {res.status_code}")
            return

        data = res.json()
        if not (data.get('result') and data['result'].get('data')):
            print(f"❌ API 返回空数据 (Result is Null)")
            print(f"   原始响应: {str(data)[:200]}")
            return
            
        raw_rows = data['result']['data']
        print(f"✅ [Step 2] 成功获取 {len(raw_rows)} 条股东数据")
        
        # 3. 模拟筛选逻辑
        print(f"🔍 [Step 3] 开始关键词匹配 (关键词库: {len(SSF_KEYWORDS)}个)...")
        matched_count = 0
        
        for i, row in enumerate(raw_rows):
            holder = str(row['HOLDER_NAME'])
            is_match = any(k in holder for k in SSF_KEYWORDS)
            
            # 打印前5个看看长啥样
            if i < 5:
                mark = "✅ 命中!" if is_match else "❌ 未中"
                print(f"   - 股东: {holder} -> {mark}")
            
            if is_match:
                matched_count += 1
                
        print(f"📊 [Step 4] 筛选结果: 共命中 {matched_count} 条国家队记录")
        
        if matched_count == 0:
            print("⚠️ 警告: 居然没筛出来？请检查关键词列表是否涵盖了该股票的股东名。")
            
        # 4. 模拟数据清洗 (检测是否会在类型转换时报错)
        print(f"🧪 [Step 5] 模拟入库清洗 (检测崩溃风险)...")
        try:
            df = pd.DataFrame(raw_rows)
            # 模拟你脚本里的逻辑
            mask = df['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
            target_df = df[mask].copy()
            
            if not target_df.empty:
                # 重点检测这里！
                print("   正在执行 float 转换...")
                hold_nums = target_df['HOLD_NUM'].astype(float)
                print(f"   ✅ float 转换成功，示例: {hold_nums.iloc[0]}")
                
                print("   正在清洗 change 字段...")
                def parse_chg(x):
                    try: return float(x) / 10000
                    except: return 0
                target_df['HOLD_NUM_CHANGE'].apply(parse_chg)
                print("   ✅ change 清洗成功")
            else:
                print("   (无命中数据，跳过清洗测试)")
                
        except Exception as e:
            print(f"❌ [CRITICAL FAIL] 清洗步骤崩溃: {e}")
            print("💡 这就是为什么主程序跑完了却没数据的原因！")

    except Exception as e:
        print(f"❌ 发生未预期的异常: {e}")

if __name__ == "__main__":
    check_one_stock("601398", "工商银行")
    check_one_stock("600036", "招商银行")
    check_one_stock("600519", "贵州茅台")
