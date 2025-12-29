# -*- coding: utf-8 -*-
"""
后台批处理任务：国家队持仓考古 (Position Archaeology) v2.2
修复：
1. [日志管理] 日志输出到 storage/ 目录，保持根目录整洁。
2. [环境] 自动创建 storage 目录。
"""

import requests
import pandas as pd
import datetime
import time
import random
import os  # 🟢 新增
from sqlalchemy import create_engine, text
from tqdm import tqdm
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================= 配置 =================
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
SSF_KEYWORDS = ["汇金", "证金", "社保", "投资有限责任公司", "中央汇金", "全国社保", "养老"]

# 🟢 新增：配置日志目录
LOG_DIR = "storage"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# 🟢 修改：日志文件路径指向 storage/history_trace.log
logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'history_trace.log'), 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class HistoryTracer:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://quote.eastmoney.com/"
        })

    def get_target_stocks(self):
        sql = "SELECT DISTINCT ts_code FROM nt_shareholders"
        df = pd.read_sql(sql, self.engine)
        return df['ts_code'].tolist()

    def get_secid(self, code):
        if str(code).startswith('6'): return f"1.{code}"
        return f"0.{code}"

    def get_kline_vwap_api(self, secid, start_date, end_date):
        """获取指定时间段的前复权VWAP"""
        s_str = start_date.replace("-", "")
        e_str = end_date.replace("-", "")
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": secid, "klt": "101", "fqt": "1", "lmt": "1000",
            "beg": s_str, "end": e_str,
            "fields1": "f1", 
            "fields2": "f51,f56,f57" 
        }
        try:
            res = self.session.get(url, params=params, timeout=5)
            data = res.json()
            if not data or not data.get('data') or not data['data'].get('klines'):
                return None
            
            klines = data['data']['klines']
            total_amt = 0.0
            total_vol = 0.0
            
            for k in klines:
                parts = k.split(',')
                if len(parts) >= 3:
                    vol = float(parts[1]) 
                    amt = float(parts[2]) 
                    total_vol += vol
                    total_amt += amt
            
            if total_vol > 0:
                return total_amt / (total_vol * 100)
                
        except Exception as e:
            pass
        return None

    def get_history_holders(self, secucode):
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        params = {
            "type": "RPT_F10_EH_HOLDERS",
            "sty": "END_DATE,HOLDER_NAME,HOLD_NUM",
            "filter": f'(SECUCODE="{secucode}")',
            "p": "1", "ps": "500", "st": "END_DATE", "sr": "1",
            "source": "SELECT_SECU_DATA", "client": "WEB",
        }
        try:
            res = self.session.get(url, params=params, timeout=5)
            data = res.json()
            if data['result'] and data['result']['data']:
                return pd.DataFrame(data['result']['data'])
        except: pass
        return pd.DataFrame()

    def process_single_stock(self, ts_code):
        secucode = f"{ts_code}.SH" if ts_code.startswith('6') else f"{ts_code}.SZ"
        secid = self.get_secid(ts_code)
        
        df_holders = self.get_history_holders(secucode)
        if df_holders.empty: return None

        mask = df_holders['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
        nt_df = df_holders[mask].copy()
        if nt_df.empty: return None

        group_df = nt_df.groupby('END_DATE')['HOLD_NUM'].sum().reset_index()
        group_df['END_DATE'] = pd.to_datetime(group_df['END_DATE'])
        group_df = group_df.sort_values('END_DATE', ascending=True)

        total_shares = 0
        total_cost_amt = 0.0
        first_buy_date = None

        last_shares = 0
        for _, row in group_df.iterrows():
            date = row['END_DATE']
            curr_shares = row['HOLD_NUM']
            diff = curr_shares - last_shares
            
            if diff > 0: # 加仓
                if first_buy_date is None: first_buy_date = date
                
                q_end = date.strftime("%Y-%m-%d")
                q_start = (date - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
                vwap = self.get_kline_vwap_api(secid, q_start, q_end)
                
                if not vwap: vwap = 0 
                
                total_shares += diff
                total_cost_amt += diff * vwap
                
            elif diff < 0: # 减仓
                if total_shares > 0:
                    avg_cost = total_cost_amt / total_shares
                    total_cost_amt += diff * avg_cost 
                    total_shares += diff
            
            last_shares = curr_shares

        if total_shares > 0:
            final_cost = total_cost_amt / total_shares
            return {
                "ts_code": ts_code,
                "hist_cost": round(final_cost, 4),
                "total_invest": round(total_cost_amt, 2),
                "total_shares": total_shares,
                "first_buy_date": first_buy_date
            }
        return None

    def run(self):
        stocks = self.get_target_stocks()
        print(f"🚀 开始全量考古 (日志存储至 {LOG_DIR}/)，目标股票: {len(stocks)} 只")
        
        success_count = 0
        valid_cost_count = 0
        
        with tqdm(total=len(stocks)) as pbar:
            for code in stocks:
                pbar.set_description(f"Processing {code}")
                try:
                    res = self.process_single_stock(code)
                    if res:
                        if res['hist_cost'] > 0:
                            valid_cost_count += 1
                        
                        sql = text("""
                            INSERT INTO nt_history_cost (ts_code, hist_cost, total_invest, total_shares, first_buy_date, calc_date)
                            VALUES (:ts_code, :hist_cost, :total_invest, :total_shares, :first_buy_date, NOW())
                            ON CONFLICT (ts_code) DO UPDATE 
                            SET hist_cost = EXCLUDED.hist_cost,
                                total_invest = EXCLUDED.total_invest,
                                total_shares = EXCLUDED.total_shares,
                                calc_date = NOW();
                        """)
                        with self.engine.connect() as conn:
                            conn.execute(sql, res)
                            conn.commit()
                        success_count += 1
                except Exception as e:
                    logging.error(f"Error processing {code}: {e}")
                
                pbar.update(1)
                time.sleep(random.uniform(0.1, 0.3))
        
        print(f"✅ 考古完成！")
        print(f"   - 入库记录: {success_count} 条")
        print(f"   - 有效成本(>0): {valid_cost_count} 条")

if __name__ == "__main__":
    tracer = HistoryTracer()
    tracer.run()
