import requests
import time

def get_secucode(code):
    c = str(code)
    if c.startswith('6'): return f"{c}.SH"
    elif c.startswith('8') or c.startswith('4') or c.startswith('9'): return f"{c}.BJ"
    else: return f"{c}.SZ"

def test_type(code, rpt_type):
    url = "https://datacenter.eastmoney.com/securities/api/data/get"
    secucode = get_secucode(code)
    # Try WITH HOLD_RATIO
    params = {
        "type": rpt_type,
        "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_RATIO,HOLD_NUM_CHANGE",
        "filter": f'(SECUCODE="{secucode}")',
        "p": "1", "ps": "50", "st": "END_DATE", "sr": "-1",
        "source": "SELECT_SECU_DATA", "client": "WEB",
        "_": str(int(time.time() * 1000))
    }
    
    print(f"Testing {rpt_type} for {code} WITH HOLD_RATIO...")
    try:
        res = requests.get(url, params=params, timeout=10)
        if res.status_code == 200:
            data = res.json()
            if data.get('success') is False:
                print(f"❌ Failed: {data.get('message')}")
            else:
                print(f"✅ Success! Data found: {bool(data.get('result'))}")
        else:
            print(f"❌ HTTP Error: {res.status_code}")
    except Exception as e:
        print(f"❌ Exception: {e}")
    print("-" * 30)

if __name__ == "__main__":
    # Test both types for a sample stock
    test_type("600036", "RPT_F10_EH_HOLDERS")      # 十大股东
    test_type("600036", "RPT_F10_EH_FREEHOLDERS")  # 十大流通股东
