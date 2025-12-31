# -*- coding: utf-8 -*-
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 关键词配置
SSF_KEYWORDS = ["汇金", "证金", "社保", "投资有限责任公司", "中央汇金", "全国社保", "养老"]

class Debugger:
    def __init__(self):
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def get_history_holders(self, secucode):
        print(f"📡 正在全方位扫描 (十大股东 + 流通股东): {secucode}...")
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        dfs = []
        for rpt_type in ["RPT_F10_EH_HOLDERS", "RPT_F10_EH_FREEHOLDERS"]:
            params = {
                "type": rpt_type,
                "sty": "END_DATE,HOLDER_NAME,HOLD_NUM",
                "filter": f'(SECUCODE="{secucode}")',
                "p": "1", "ps": "500", "st": "END_DATE", "sr": "1",
                "source": "SELECT_SECU_DATA", "client": "WEB",
            }
            try:
                res = self.session.get(url, params=params, timeout=10)
                data = res.json()
                if data['result'] and data['result']['data']:
                    dfs.append(pd.DataFrame(data['result']['data']))
            except: pass
        
        if dfs:
            full_df = pd.concat(dfs).drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
            return full_df
        return pd.DataFrame()

    def run(self, ts_code="000957"):
        secucode = f"{ts_code}.SH" if ts_code.startswith('6') else f"{ts_code}.SZ"
        df = self.get_history_holders(secucode)
        
        if df.empty:
            print("❌ 未抓取到任何数据")
            return

        # 过滤关键词
        mask = df['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
        nt_df = df[mask].copy().sort_values('END_DATE')

        if nt_df.empty:
            print("❌ 历史上无国家队持仓")
            return

        print(f"\n✅ 发现 {len(nt_df)} 条国家队历史记录！")
        print("------------------------------------------------")
        print(f"🕵️ 最早一条记录时间: {nt_df.iloc[0]['END_DATE']}")
        print(f"🕵️ 最早持仓者: {nt_df.iloc[0]['HOLDER_NAME']}")
        print("------------------------------------------------")

        # 模拟计算逻辑（检查是否断档）
        group_df = nt_df.groupby('END_DATE')['HOLD_NUM'].sum().reset_index()
        group_df['END_DATE'] = pd.to_datetime(group_df['END_DATE'])
        
        first_buy_date = None
        prev_date = None
        
        print("\n🧮 模拟逻辑回溯中...")
        for _, row in group_df.iterrows():
            curr_date = row['END_DATE']
            
            # 检查断档：如果两次持仓记录间隔超过 180 天（约半年），说明中间肯定清仓退出了
            if prev_date:
                days_diff = (curr_date - prev_date).days
                if days_diff > 180:
                    print(f"✂️ [断档发现] {prev_date.date()} -> {curr_date.date()} (间隔 {days_diff} 天)")
                    print(f"   => 之前的 {first_buy_date.date()} 建仓已失效，重置起点！")
                    first_buy_date = curr_date # 重置
                else:
                    pass # 连续持仓
            else:
                first_buy_date = curr_date
                print(f"🏁 初始建仓: {first_buy_date.date()}")
            
            prev_date = curr_date
            
        print("------------------------------------------------")
        print(f"🛑 最终计算出的建仓日: {first_buy_date.date()}")

if __name__ == "__main__":
    Debugger().run()
