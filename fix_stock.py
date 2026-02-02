# -*- coding: utf-8 -*-
"""
自动化巡检修复机器人 v1.3 (北交所修复版)
修复内容：
1. [交易所适配] 增加对 9/8/4 开头代码的识别，正确映射为 .BJ 后缀。
"""
import requests
import pandas as pd
import datetime
import logging
import os
import time
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
SSF_KEYWORDS = ["社保", "养老", "证金", "中央汇金", "全国社保", "基本养老", "中国证券金融", "社保基金", "汇金资管"]

# 🔴 请替换为您的 PushPlus Token
PUSHPLUS_TOKEN = "your_pushplus_token_here"

def send_pushplus(title, content):
    """发送 PushPlus 通知"""
    if not PUSHPLUS_TOKEN or "YOUR_" in PUSHPLUS_TOKEN: return
    url = "http://www.pushplus.plus/send"
    data = {"token": PUSHPLUS_TOKEN, "title": f"【NT_Fix】{title}", "content": content }
    try: requests.post(url, json=data, timeout=3)
    except: pass

LOG_DIR = "storage"
if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(os.path.join(LOG_DIR, "auto_fix.log"), mode='w', encoding='utf-8'), logging.StreamHandler()])
logger = logging.getLogger("AutoFixer")

class AutoFixer:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://data.eastmoney.com/"
        })

    def get_secid(self, code):
        return f"1.{code}" if str(code).startswith('6') else f"0.{code}"
    
    # 🟢 [关键修复] 正确的 SECUCODE 获取逻辑
    def get_secucode(self, code):
        c = str(code)
        if "." in c: return c
        if c.startswith('6'): return f"{c}.SH"
        elif c.startswith('8') or c.startswith('4') or c.startswith('9'): return f"{c}.BJ"
        else: return f"{c}.SZ"

    def detect_problems(self):
        sql_missing = "SELECT DISTINCT ts_code FROM nt_positions_analysis WHERE cost_source LIKE '%%近期估算%%'"
        sql_zero = "SELECT DISTINCT ts_code FROM nt_history_cost WHERE hist_cost = 0"
        try:
            codes = list(set(pd.read_sql(sql_missing, self.engine)['ts_code'].tolist() + pd.read_sql(sql_zero, self.engine)['ts_code'].tolist()))
            return codes
        except: return []

    def get_kline_vwap_api(self, secid, start_date, end_date):
        s_str = start_date.replace("-", "")
        e_str = end_date.replace("-", "")
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {"secid": secid, "klt": "101", "fqt": "1", "lmt": "1000", "beg": s_str, "end": e_str, "fields1": "f1", "fields2": "f51,f56,f57"}
        try:
            res = self.session.get(url, params=params, timeout=5)
            data = res.json()
            if data and data.get('data') and data['data'].get('klines'):
                total_amt = 0.0; total_vol = 0.0
                for k in data['data']['klines']:
                    parts = k.split(',')
                    if len(parts) >= 3:
                        total_vol += float(parts[1]); total_amt += float(parts[2])
                if total_vol > 0: return total_amt / (total_vol * 100)
        except: pass
        return 0

    def calculate_single_holder(self, holder_df, secid):
        holder_df = holder_df.sort_values('END_DATE', ascending=True)
        total_shares = 0; total_cost_amt = 0.0
        first_buy_date = None; last_hold_date = None; last_shares = 0
        
        for _, row in holder_df.iterrows():
            date = pd.to_datetime(row['END_DATE'])
            curr_shares = float(row['HOLD_NUM'])
            
            if last_hold_date and (date - last_hold_date).days > 180:
                first_buy_date = None; total_shares = 0; total_cost_amt = 0.0; last_shares = 0
            
            last_hold_date = date
            diff = curr_shares - last_shares
            
            if diff > 0: 
                if first_buy_date is None: first_buy_date = date
                q_end = date.strftime("%Y-%m-%d")
                q_start = (date - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
                vwap = self.get_kline_vwap_api(secid, q_start, q_end)
                total_shares += diff
                total_cost_amt += diff * vwap
            elif diff < 0 and total_shares > 0:
                total_cost_amt += diff * (total_cost_amt / total_shares)
                total_shares += diff
            last_shares = curr_shares
            
        final_cost = total_cost_amt / total_shares if total_shares > 0 else 0
        return final_cost, first_buy_date, total_shares, total_cost_amt

    def fix_one_stock(self, ts_code):
        secid = self.get_secid(ts_code)
        # 🟢 使用修复后的逻辑
        secucode = self.get_secucode(ts_code)
        
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        dfs = []
        for rpt in ["RPT_F10_EH_HOLDERS", "RPT_F10_EH_FREEHOLDERS"]:
            try:
                time.sleep(0.2)
                res = self.session.get(url, params={"type": rpt, "sty": "END_DATE,HOLDER_NAME,HOLD_NUM", "filter": f'(SECUCODE="{secucode}")', "p":1, "ps":5000, "st":"END_DATE", "sr":1}, timeout=10)
                if res.json()['result']: dfs.append(pd.DataFrame(res.json()['result']['data']))
            except: pass
        
        if not dfs: return 0
        
        df_all = pd.concat(dfs).drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
        mask = df_all['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
        nt_df = df_all[mask].copy()
        
        fixed_count = 0
        for holder_name, group in nt_df.groupby('HOLDER_NAME'):
            cost, f_date, t_shares, t_invest = self.calculate_single_holder(group, secid)
            if f_date and cost > 0:
                try:
                    # 🟢 [预防] 显式转换类型
                    cost = float(cost)
                    t_invest = float(t_invest)
                    t_shares = int(t_shares)

                    sql = text("""
                        INSERT INTO nt_history_cost (ts_code, holder_name, hist_cost, total_invest, total_shares, first_buy_date, calc_date)
                        VALUES (:c, :h, :cost, :inv, :sh, :fd, NOW())
                        ON CONFLICT (ts_code, holder_name) DO UPDATE 
                        SET hist_cost = EXCLUDED.hist_cost, first_buy_date = EXCLUDED.first_buy_date, calc_date = NOW();
                    """)
                    with self.engine.connect() as conn:
                        conn.execute(sql, {"c": ts_code, "h": holder_name, "cost": cost, "inv": t_invest, "sh": t_shares, "fd": f_date})
                        conn.commit()
                    fixed_count += 1
                except: pass
        return fixed_count

    def run(self):
        targets = self.detect_problems()
        if not targets:
            logger.info("✅ 无需修复。")
            return
            
        logger.info(f"🔧 增量修复: 目标 {len(targets)} 只")
        for code in tqdm(targets):
            try:
                self.fix_one_stock(code)
            except: pass

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    try:
        AutoFixer().run()
        
        duration = datetime.datetime.now() - start_time
        send_pushplus("修复任务完成", f"自动化修复任务已成功执行完毕。\n耗时: {duration}")
        
    except Exception as e:
        import traceback
        import sys
        err_msg = traceback.format_exc()
        logger.error(err_msg)
        print(err_msg)
        send_pushplus("修复任务崩溃", f"脚本发生严重错误，已停止。\n\n{str(e)}")
        sys.exit(1)
