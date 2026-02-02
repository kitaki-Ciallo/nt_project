# -*- coding: utf-8 -*-
import requests
import time
import json

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
    "Connection": "keep-alive"
}

# 🟢 核心逻辑：正确识别 920 为 .BJ
def get_secucode_v2(code):
    c = str(code)
    if c.startswith('6'):
        return f"{c}.SH"
    elif c.startswith('8') or c.startswith('4') or c.startswith('9'): 
        return f"{c}.BJ"
    else:
        return f"{c}.SZ"

def test_fetch(code, name, expect_success=True):
    secucode = get_secucode_v2(code)
    print(f"\n🧪 正在测试: {name} ({code})")
    print(f"   👉 请求代码: [{secucode}]")
    
    url = "https://datacenter.eastmoney.com/securities/api/data/get"
    params = {
        "type": "RPT_F10_EH_HOLDERS",
        "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_NUM_CHANGE",
        "filter": f'(SECUCODE="{secucode}")',
        "p": "1", "ps": "5",
        "st": "END_DATE", "sr": "-1",
        "source": "SELECT_SECU_DATA", "client": "WEB",
        "_": str(int(time.time() * 1000))
    }
    
    try:
        time.sleep(0.5)
        res = requests.get(url, params=params, headers=headers, timeout=10)
        
        if res.status_code == 200:
            data = res.json()
            if data.get('result') and data['result'].get('data'):
                rows = data['result']['data']
                print(f"   ✅ 成功！抓取到 {len(rows)} 条股东数据")
                print(f"   👀 最新一期: {rows[0]['END_DATE']} - 第一大股东: {rows[0]['HOLDER_NAME']}")
                if not expect_success:
                    print("   ⚠️ 警告：预期失败但成功了？旧代码居然还能用？")
            else:
                if expect_success:
                    print(f"   ❌ 失败：返回空数据。")
                else:
                    print(f"   ✅ 验证通过：旧代码已失效，返回空数据符合预期。")
        else:
            print(f"   ❌ HTTP 报错: {res.status_code}")
            
    except Exception as e:
        print(f"   ❌ 发生异常: {e}")

if __name__ == "__main__":
    print("🚀 启动交易所代码大迁徙验证...")
    
    # 1. 沪市基准
    test_fetch("600519", "贵州茅台", expect_success=True)
    
    # 2. 北交所：科力股份 (920088)
    test_fetch("920088", "科力股份", expect_success=True)
    
    # 3. 北交所：驱动力 (920275) - 刚才失败的 838275 的新身
    test_fetch("920275", "驱动力(新)", expect_success=True)

    # 4. 反向验证：贝特瑞 (旧代码 835185)
    # 如果新闻属实，这个旧代码应该已经废了
    test_fetch("835185", "贝特瑞(旧代码)", expect_success=False)

    print("\n🏁 结论：如果前三个都 ✅，说明你的系统已经完美适配新时代！")
