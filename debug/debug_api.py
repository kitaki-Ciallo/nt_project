import urllib.request
import json
import time

def get_history_holders(secucode):
    url = "https://datacenter.eastmoney.com/securities/api/data/get"
    # params need to be encoded
    params = [
        ("type", "RPT_F10_EH_HOLDERS"),
        ("sty", "END_DATE,HOLDER_NAME,HOLD_NUM"),
        ("filter", f'(SECUCODE="{secucode}")'),
        ("p", "1"),
        ("ps", "5000"),
        ("st", "END_DATE"),
        ("sr", "1"),
        ("source", "SELECT_SECU_DATA"),
        ("client", "WEB")
    ]
    query_string = urllib.parse.urlencode(params)
    full_url = f"{url}?{query_string}"
    
    print(f"Requesting: {full_url}")
    try:
        with urllib.request.urlopen(full_url, timeout=10) as response:
            if response.status == 200:
                data = json.loads(response.read().decode('utf-8'))
                if data['result'] and data['result']['data']:
                    return data['result']['data']
    except Exception as e:
        print(f"Error: {e}")
    return []

if __name__ == "__main__":
    code = "600036.SH"
    print(f"Fetching holders for {code}...")
    holders = get_history_holders(code)
    if holders:
        print(f"Found {len(holders)} records.")
        names = set(h['HOLDER_NAME'] for h in holders)
        print("Sample Holder Names:")
        for name in list(names)[:20]:
            print(f" - {name}")
    else:
        print("No data found.")
