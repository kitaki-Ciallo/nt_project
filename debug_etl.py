import requests
import pandas as pd
import time
import json

TEST_CODE = "601328" # 交通银行

print(f"--- 正在尝试【直连东财API】获取 {TEST_CODE} 的股东数据 ---")

def get_shareholders_custom(stock_code):
    """
    自定义函数：直连东方财富 API 获取十大流通股东
    """
    # 转换代码格式：601328 -> 601328.SH, 000001 -> 000001.SZ
    # 简单判断：6开头是SH，其他是SZ (北交所暂略)
    if stock_code.startswith('6'):
        secucode = f"{stock_code}.SH"
    else:
        secucode = f"{stock_code}.SZ"

    # 东财官方接口 (RPT_F10_EH_FREEHOLDERS = 十大流通股东)
    url = "https://datacenter.eastmoney.com/securities/api/data/get"
    params = {
        "type": "RPT_F10_EH_FREEHOLDERS", 
        "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_RATIO,HOLD_NUM_CHANGE,CHANGE_RATIO,HOLDER_TYPE",
        "filter": f'(SECUCODE="{secucode}")',
        "p": "1",
        "ps": "50", # 获取最近50条记录(即最近5个季度)
        "st": "END_DATE",
        "sr": "-1",
        "source": "SELECT_SECU_DATA",
        "client": "WEB",
    }
    
    try:
        res = requests.get(url, params=params, timeout=5)
        data = res.json()
        
        if data['result'] and data['result']['data']:
            df = pd.DataFrame(data['result']['data'])
            # 重命名列以符合习惯
            df = df.rename(columns={
                'END_DATE': '报告期',
                'HOLDER_NAME': '股东名称',
                'HOLD_NUM': '持股数',
                'HOLD_RATIO': '持股比例',
                'HOLD_NUM_CHANGE': '增减'
            })
            return df
        return None
    except Exception as e:
        print(f"API请求失败: {e}")
        return None

try:
    start_time = time.time()
    df = get_shareholders_custom(TEST_CODE)
    end_time = time.time()
    
    print(f"接口响应耗时: {end_time - start_time:.4f} 秒")

    if df is None or df.empty:
        print("❌ 接口返回空数据")
    else:
        print("✅ 成功获取数据！(直连模式)")
        print(f"数据行数: {len(df)}")
        print("--- 前5行预览 ---")
        print(df[['报告期', '股东名称', '持股数', '增减']].head())
        
        # 测试筛选
        KEYWORDS = ["社保", "养老", "汇金", "证金"]
        mask = df['股东名称'].apply(lambda x: any(k in str(x) for k in KEYWORDS))
        found = df[mask]
        print(f"\n--- 国家队筛选测试 ---")
        print(f"找到 {len(found)} 条记录")

except Exception as e:
    print(f"❌ 程序崩溃: {e}")
