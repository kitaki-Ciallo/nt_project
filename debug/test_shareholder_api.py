import requests
import pandas as pd
import time

def get_secucode(code):
    c = str(code)
    if c.startswith('6'): return f"{c}.SH"
    elif c.startswith('8') or c.startswith('4') or c.startswith('9'): return f"{c}.BJ"
    else: return f"{c}.SZ"

def test_api(code):
    url = "https://datacenter.eastmoney.com/securities/api/data/get"
    secucode = get_secucode(code)
    params = {
        "type": "RPT_F10_EH_HOLDERS",
        "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_NUM_CHANGE",
        "filter": f'(SECUCODE="{secucode}")',
        "p": "1", "ps": "50", "st": "END_DATE", "sr": "-1",
        "source": "SELECT_SECU_DATA", "client": "WEB",
        "_": str(int(time.time() * 1000))
    }
    
    print(f"Testing API for {code} ({secucode})...")
    try:
        res = requests.get(url, params=params, timeout=10)
        print(f"Status Code: {res.status_code}")
        if res.status_code == 200:
            data = res.json()
            if data.get('result') and data['result'].get('data'):
                df = pd.DataFrame(data['result']['data'])
                print(f"✅ Success! Got {len(df)} rows.")
                print(df[['END_DATE', 'HOLDER_NAME']].head())
            else:
                print("⚠️ Response 200 but no data found (empty result).")
                print(f"Response body: {res.text[:200]}...")
        else:
            print(f"❌ HTTP Error: {res.text}")
    except Exception as e:
        print(f"❌ Exception: {e}")

if __name__ == "__main__":
    test_api("600036") # 招商银行
    test_api("000001") # 平安银行
