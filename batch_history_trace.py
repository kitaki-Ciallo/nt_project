# -*- coding: utf-8 -*-
"""
后台批处理任务：国家队持仓考古 v4.0 (精细化单账户版)
升级内容：
1. [粒度] 从“股票级”聚合成本 -> 拆分为“股东级”独立成本。
2. [逻辑] 社保101和社保102现在会拥有完全不同的建仓成本和时间。
"""

import requests
import pandas as pd
import datetime
import os
from sqlalchemy import create_engine, text
from tqdm import tqdm
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
SSF_KEYWORDS = ["社保", "养老", "证金", "中央汇金", "全国社保", "基本养老", "中国证券金融", "社保基金", "汇金资管"]


LOG_DIR = "storage"
if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
logging.basicConfig(filename=os.path.join(LOG_DIR, 'history_trace.log'), level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class HistoryTracer:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def get_target_stocks(self):
        df = pd.read_sql("SELECT DISTINCT ts_code FROM nt_shareholders", self.engine)
        return df['ts_code'].tolist()

    def get_secid(self, code):
        return f"1.{code}" if str(code).startswith('6') else f"0.{code}"

    def get_kline_vwap_api(self, secid, start_date, end_date):
        s_str = start_date.replace("-", "")
        e_str = end_date.replace("-", "")
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {"secid": secid, "klt": "101", "fqt": "1", "lmt": "1000", "beg": s_str, "end": e_str, "fields1": "f1", "fields2": "f51,f56,f57"}
        try:
            res = self.session.get(url, params=params, timeout=5)
            data = res.json()
            if data and data.get('data') and data['data'].get('klines'):
                total_amt = 0.0
                total_vol = 0.0
                for k in data['data']['klines']:
                    parts = k.split(',')
                    if len(parts) >= 3:
                        total_vol += float(parts[1])
                        total_amt += float(parts[2])
                if total_vol > 0: return total_amt / (total_vol * 100)
        except: pass
        return 0

    def get_history_holders(self, secucode):
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        dfs = []
        for rpt_type in ["RPT_F10_EH_HOLDERS", "RPT_F10_EH_FREEHOLDERS"]:
            params = {"type": rpt_type, "sty": "END_DATE,HOLDER_NAME,HOLD_NUM", "filter": f'(SECUCODE="{secucode}")', "p": "1", "ps": "5000", "st": "END_DATE", "sr": "1", "source": "SELECT_SECU_DATA", "client": "WEB"}
            try:
                res = self.session.get(url, params=params, timeout=10)
                data = res.json()
                if data['result'] and data['result']['data']:
                    dfs.append(pd.DataFrame(data['result']['data']))
            except: pass
        if dfs: return pd.concat(dfs).drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
        return pd.DataFrame()

    def calculate_single_holder(self, holder_df, secid):
        """计算单个股东的独立成本与建仓时间"""
        holder_df = holder_df.sort_values('END_DATE', ascending=True)
        
        total_shares = 0
        total_cost_amt = 0.0
        first_buy_date = None
        last_hold_date = None 
        last_shares = 0
        
        for _, row in holder_df.iterrows():
            date = row['END_DATE']
            curr_shares = row['HOLD_NUM']
            
            # 断档检测 (180天)
            if last_hold_date:
                if (date - last_hold_date).days > 180:
                    first_buy_date = None
                    total_shares = 0
                    total_cost_amt = 0.0
                    last_shares = 0
            
            last_hold_date = date
            diff = curr_shares - last_shares
            
            if diff > 0: # 加仓
                if first_buy_date is None: first_buy_date = date
                q_end = date.strftime("%Y-%m-%d")
                q_start = (date - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
                vwap = self.get_kline_vwap_api(secid, q_start, q_end)
                total_shares += diff
                total_cost_amt += diff * vwap
            elif diff < 0: # 减仓
                if total_shares > 0:
                    avg_cost = total_cost_amt / total_shares
                    total_cost_amt += diff * avg_cost 
                    total_shares += diff
            last_shares = curr_shares
            
        final_cost = total_cost_amt / total_shares if total_shares > 0 else 0
        return final_cost, first_buy_date, total_shares, total_cost_amt

    def run(self):
        stocks = self.get_target_stocks()
        print(f"🚀 开始全量考古 (精细化版 v4.0)，目标股票: {len(stocks)} 只")
        
        count = 0
        with tqdm(total=len(stocks)) as pbar:
            for code in stocks:
                pbar.set_description(f"Proc {code}")
                try:
                    secucode = f"{code}.SH" if code.startswith('6') else f"{code}.SZ"
                    secid = self.get_secid(code)
                    
                    df_all = self.get_history_holders(secucode)
                    if not df_all.empty:
                        # 🟢 核心改变：按股东名字分组！
                        mask = df_all['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
                        nt_df = df_all[mask].copy()
                        
                        for holder_name, group in nt_df.groupby('HOLDER_NAME'):
                            cost, f_date, t_shares, t_invest = self.calculate_single_holder(group, secid)
                            
                            if f_date:
                                sql = text("""
                                    INSERT INTO nt_history_cost (ts_code, holder_name, hist_cost, total_invest, total_shares, first_buy_date, calc_date)
                                    VALUES (:ts_code, :holder, :cost, :inv, :sh, :fdate, NOW())
                                    ON CONFLICT (ts_code, holder_name) DO UPDATE 
                                    SET hist_cost = EXCLUDED.hist_cost,
                                        total_invest = EXCLUDED.total_invest,
                                        total_shares = EXCLUDED.total_shares,
                                        first_buy_date = EXCLUDED.first_buy_date,
                                        calc_date = NOW();
                                """)
                                with self.engine.connect() as conn:
                                    conn.execute(sql, {"ts_code": code, "holder": holder_name, "cost": cost, "inv": t_invest, "sh": t_shares, "fdate": f_date})
                                    conn.commit()
                                count += 1
                except Exception as e:
                    logging.error(f"Error {code}: {e}")
                pbar.update(1)
        print(f"✅ 考古完成！生成了 {count} 条精细化档案。")

if __name__ == "__main__":
    HistoryTracer().run()
