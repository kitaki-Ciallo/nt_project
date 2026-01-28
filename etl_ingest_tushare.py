# -*- coding: utf-8 -*-
"""
数据采集引擎 v10.2 (报警修复版)
修复内容：
1. [关键修复] 修复了遇到 'Empty reply' (curl 52) 等底层连接错误时，脚本静默跳过的问题。
   现在遇到连接中断会直接打印红色报错并报警。
2. [配置保持] 延续 PushPlus 和混合变速策略。
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
import requests
import time
import random
import os
import json
import re
import traceback
import psycopg2.extras
import tushare as ts

# 初始化 Tushare Pro 接口
pro = ts.pro_api('your_tushare_token_here')

# ================= 配置区域 =================
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
SSF_KEYWORDS = ["社保", "养老", "证金", "中央汇金", "全国社保", "基本养老", "中国证券金融", "社保基金", "汇金资管"]

# 🔴 请替换为您的 PushPlus Token
PUSHPLUS_TOKEN = "your_pushplus_token_here"

# 🚀 混合并发配置
SHAREHOLDER_WORKERS = 8  # 股东：极速
SENSITIVE_WORKERS = 2     # K线/基本面：慢速

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def send_pushplus(title, content):
    """发送 PushPlus 通知"""
    if not PUSHPLUS_TOKEN or "YOUR_" in PUSHPLUS_TOKEN: return
    url = "http://www.pushplus.plus/send"
    data = {"token": PUSHPLUS_TOKEN, "title": f"【NT_Project】{title}", "content": content }
    try: requests.post(url, json=data, timeout=3)
    except: pass

class DataEngine:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://data.eastmoney.com/",
            "Connection": "keep-alive"
        })
        self.today = datetime.datetime.now().strftime("%Y%m%d")
        self.alert_cache = {} 
        
        # 🔒 速率限制控制
        self.lock = Lock()
        self.daily_count = 0
        self.daily_resume_time = 0
        self.fund_count = 0
        self.fund_resume_time = 0 

    def check_alert(self, error_type):
        """防止同一分钟内发送大量重复报警"""
        now = time.time()
        last_time = self.alert_cache.get(error_type, 0)
        if now - last_time > 60: # 1分钟冷却
            self.alert_cache[error_type] = now
            return True
        return False

    def get_secid(self, code):
        if str(code).startswith('6'): return f"1.{code}"
        else: return f"0.{code}"
    
    def get_secucode(self, code):
        c = str(code)
        if c.startswith('6'): return f"{c}.SH"
        elif c.startswith('8') or c.startswith('4') or c.startswith('9'): return f"{c}.BJ"
        else: return f"{c}.SZ"

    def get_stock_list(self):
        if os.path.exists("stock_list_cache.csv"):
            print("📦 使用本地缓存: stock_list_cache.csv")
            df = pd.read_csv("stock_list_cache.csv", dtype={'ts_code': str})
            try:
                save_df = df[['ts_code', 'name']].copy()
                data_list = save_df.to_dict(orient='records')
                sql = text("INSERT INTO stock_basic (ts_code, name) VALUES (:ts_code, :name) ON CONFLICT (ts_code) DO UPDATE SET name = EXCLUDED.name")
                with self.engine.connect() as conn:
                    conn.execute(sql, data_list)
                    conn.commit()
            except: pass
            return df['ts_code'].tolist()
        return []

    # --- 模块1: 股东数据 (极速) ---
    def fetch_eastmoney_api_safe(self, code, report_type):
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        secucode = self.get_secucode(code)
        params = {
            "type": report_type,
            "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_RATIO,HOLD_NUM_CHANGE",
            "filter": f'(SECUCODE="{secucode}")',
            "p": "1", "ps": "50", "st": "END_DATE", "sr": "-1",
            "source": "SELECT_SECU_DATA", "client": "WEB",
            "_": str(int(time.time() * 1000))
        }
        
        for attempt in range(3):
            if attempt > 0: time.sleep(0.5)
            try:
                res = self.session.get(url, params=params, timeout=10)
                if res.status_code == 200:
                    data = res.json()
                    if data.get('result') and data['result'].get('data'):
                        return pd.DataFrame(data['result']['data'])
                    return pd.DataFrame()
                else:
                    msg = f"HTTP {res.status_code} | Code: {code}"
                    print(f"⚠️ [股东接口] {msg}")
            except Exception as e:
                err_msg = str(e)
                if attempt == 2: 
                    print(f"❌ [网络错误] 股东抓取失败 {code}: {err_msg}")
                    # 🚨 严重错误报警并停止
                    if "RemoteDisconnected" in err_msg or "Connection aborted" in err_msg:
                        send_pushplus("股东接口连接中断", f"检测到底层连接被断开 (curl 52)。\nCode: {code}\n详情: {err_msg}")
                        print("🛑 检测到严重连接错误，正在终止程序...")
                        os._exit(1)
                time.sleep(0.5)
        return pd.DataFrame()

    def fetch_and_save_shareholders(self, ts_code):
        df1 = self.fetch_eastmoney_api_safe(ts_code, "RPT_F10_EH_FREEHOLDERS")
        df2 = self.fetch_eastmoney_api_safe(ts_code, "RPT_F10_EH_HOLDERS")
        df = pd.concat([df1, df2])
        if df.empty: return 0
        
        df = df.drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
        mask = df['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
        target_df = df[mask].copy().reset_index(drop=True)
        if target_df.empty: return 0

        clean_df = pd.DataFrame()
        clean_df['ts_code'] = [ts_code] * len(target_df)
        clean_df['ann_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
        clean_df['end_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
        clean_df['holder_name'] = target_df['HOLDER_NAME']
        clean_df['hold_amount'] = target_df['HOLD_NUM'].astype(float) / 10000
        clean_df['hold_ratio'] = target_df.get('HOLD_RATIO', None).astype(float) if 'HOLD_RATIO' in target_df else None
        
        def parse_chg(x):
            try: return float(x) / 10000
            except: return 0
        clean_df['chg_amount'] = target_df['HOLD_NUM_CHANGE'].apply(parse_chg)

        try:
            data_list = clean_df.to_dict(orient='records')
            cols = list(data_list[0].keys())
            values_str = ", ".join([f":{c}" for c in cols])
            sql = text(f"INSERT INTO nt_shareholders ({','.join(cols)}) VALUES ({values_str}) ON CONFLICT (ts_code, holder_name, end_date) DO NOTHING")
            with self.engine.connect() as conn:
                conn.execute(sql, data_list)
                conn.commit()
            return len(clean_df)
        except: return 0

    # --- 模块2: 日线数据 (Tushare 高速版) ---
    def fetch_and_save_daily_data(self, ts_code):
        try:
            #  [增量更新] 智能判断起始日期
            start_date = "20060101"
            try:
                # 查询该股票在数据库中的最新日期
                max_date_sql = text("SELECT max(trade_date) FROM nt_market_data WHERE ts_code = :code")
                with self.engine.connect() as conn:
                    result = conn.execute(max_date_sql, {"code": ts_code}).scalar()
                
                if result:
                    # 如果有数据，从最新日期的下一天开始抓
                    next_day = result + datetime.timedelta(days=1)
                    start_date = next_day.strftime("%Y%m%d")
            except: pass

            # 如果计算出的起始日期超过了今天，说明已经是最新，直接返回
            if start_date > self.today:
                return

            end_date = self.today
            
            # 使用 Tushare 获取数据
            # 注意：Tushare 需要带后缀的代码 (e.g. 600000.SH)
            tushare_code = self.get_secucode(ts_code)
            
            try:
                df = pro.daily(**{
                    "ts_code": tushare_code,
                    "start_date": start_date,
                    "end_date": end_date
                }, fields=[
                    "ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"
                ])
            except Exception as e:
                print(f"⚠️ [Tushare报错] {ts_code}: {e}")
                time.sleep(1) # 稍微休息一下避免触发 Tushare 频率限制（如果有的话）
                return

            if df is None or df.empty:
                return

            # 数据清洗与转换
            # 1. Tushare 返回的 ts_code 是带后缀的，我们需要改回纯数字以匹配数据库
            df['ts_code'] = ts_code
            
            # 2. Tushare 的 amount 单位是千元，数据库通常存元 (根据之前 Eastmoney 的逻辑)
            #    Eastmoney 的 amount 通常是元。
            #    Tushare amount: 成交额 （千元）
            #    所以需要 * 1000
            df['amount'] = df['amount'] * 1000
            
            # 3. Tushare 的 vol 单位是手 (100股)
            #    Eastmoney 的 vol 通常也是手 (f5)
            #    batch_history_trace.py 中计算 VWAP 是 total_amt / (total_vol * 100)
            #    说明 vol 应该是手。所以 Tushare 的 vol 不需要转换。
            
            # 准备写入
            rows = df.to_dict(orient='records')
            
            if rows:
                # 🚀 [优化] 使用 execute_values 进行批量插入
                cols = ["ts_code", "trade_date", "open", "close", "high", "low", "vol", "amount"]
                values = [[row[c] for c in cols] for row in rows]
                
                insert_sql = """
                    INSERT INTO nt_market_data (ts_code, trade_date, open, close, high, low, vol, amount)
                    VALUES %s
                    ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                        open = EXCLUDED.open,
                        close = EXCLUDED.close,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        vol = EXCLUDED.vol,
                        amount = EXCLUDED.amount
                """
                
                with self.engine.connect() as conn:
                    cursor = conn.connection.cursor()
                    psycopg2.extras.execute_values(cursor, insert_sql, values)
                    conn.connection.commit()
                    
        except Exception as e:
            print(f"❌ [同步错误] {ts_code}: {e}")

    # --- 模块3: 基本面 (慢速+报警) ---
    def fetch_combined_data(self, ts_code):
        # --- 🛑 速率限制逻辑 (基本面) ---
        sleep_needed = 0
        with self.lock:
            if time.time() < self.fund_resume_time:
                sleep_needed = self.fund_resume_time - time.time()
            else:
                self.fund_count += 1
                if self.fund_count % 130 == 0:
                    pause_duration = random.randint(5, 15)
                    self.fund_resume_time = time.time() + pause_duration
                    sleep_needed = pause_duration
                    print(f"😴 [基本面] 已抓取 {self.fund_count} 次，触发反爬保护，暂停 {pause_duration} 秒...")
        
        if sleep_needed > 0:
            time.sleep(sleep_needed)
        # ---------------------------

        url = "http://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "invt": "2", "fltt": "2",
            "fields": "f43,f57,f58,f162,f164,f167,f170,f163,f116,f173,f183,f184,f185,f186,f187", 
            "secid": self.get_secid(ts_code),
            "ut": "fa5fd1943c7b386f172d68934880c8d6", 
            "cb": "jQuery123",
            "_": str(int(time.time() * 1000))
        }
        
        data = {"ts_code": ts_code}

        try:
            time.sleep(random.uniform(0.3, 0.8))
            res = requests.get(url, params=params, headers=self.session.headers, timeout=5)
            
            if res.status_code != 200:
                msg = f"HTTP {res.status_code} | Code: {ts_code}"
                print(f"🚨 [基本面接口] {msg}")
                if self.check_alert("fundamental_block"):
                    send_pushplus("基本面接口被封", f"详情: {msg}")
                return data

            resp_text = res.text
            if "(" in resp_text:
                json_str = resp_text.split("(", 1)[1].rsplit(")", 1)[0]
                d = json.loads(json_str).get('data')
                
                if d:
                    def parse_val(val): return float(val) if val != "-" else None
                    data['curr_price'] = parse_val(d.get('f43'))
                    data['total_mv'] = parse_val(d.get('f116'))
                    data['pe_dyn'] = parse_val(d.get('f162'))
                    data['pe_ttm'] = parse_val(d.get('f164'))
                    data['pe_static'] = parse_val(d.get('f163'))
                    data['pb'] = parse_val(d.get('f167'))
                    data['roe'] = parse_val(d.get('f173'))
                    data['revenue'] = parse_val(d.get('f183'))
                    data['revenue_growth'] = parse_val(d.get('f184'))
                    data['net_profit_growth'] = parse_val(d.get('f185'))
                    data['gross_margin'] = parse_val(d.get('f186'))
                    data['net_margin'] = parse_val(d.get('f187'))
                    
                    raw_div = parse_val(d.get('f170'))
                    if raw_div: data['div_rate'] = raw_div
                    if data['curr_price'] and data['pe_ttm']: 
                        data['eps'] = round(data['curr_price'] / data['pe_ttm'], 2)
        except Exception as e:
            # 🟢 [修复] 同样改为打印
            print(f"❌ [连接中断] 基本面同步出错 {ts_code}: {e}")
            if self.check_alert("fundamental_conn_err"):
                 send_pushplus("基本面连接中断", f"详情: {str(e)}")
                 print("🛑 检测到严重连接错误，正在终止程序...")
                 os._exit(1)
        return data

    # --- 执行入口 ---
    def run_shareholder_sync(self):
        print(f">>> 🚀 [1/3] 扫描股东数据 (极速: {SHAREHOLDER_WORKERS}线程)...")
        stock_list = self.get_stock_list()
        count = 0
        with ThreadPoolExecutor(max_workers=SHAREHOLDER_WORKERS) as executor:
            future_to_code = {executor.submit(self.fetch_and_save_shareholders, code): code for code in stock_list}
            for future in tqdm(as_completed(future_to_code), total=len(stock_list)):
                try:
                    found = future.result()
                    if found > 0: count += 1
                except: pass
        print(f"✅ 股东扫描结束，捕获 {count} 只。")

    def run_market_data_sync(self):
        print(f">>> 🛡️ [2/3] 同步日线数据 (安全: {SENSITIVE_WORKERS}线程)...")
        try:
            target_stocks = pd.read_sql("SELECT DISTINCT ts_code FROM nt_shareholders", self.engine)
            stock_list = target_stocks['ts_code'].tolist()
            
            # 🚀 [优化] 检查哪些股票今天已经抓取过了，直接跳过
            existing_df = pd.read_sql(
                text("SELECT DISTINCT ts_code FROM nt_market_data WHERE trade_date = :today"), 
                self.engine, 
                params={"today": self.today}
            )
            existing_codes = set(existing_df['ts_code'].tolist())
            
            # 过滤掉已存在的
            original_len = len(stock_list)
            stock_list = [c for c in stock_list if c not in existing_codes]
            skipped_count = original_len - len(stock_list)
            
            if skipped_count > 0:
                print(f"⏩ 已跳过 {skipped_count} 只今日已更新的股票，剩余 {len(stock_list)} 只待处理。")
            
            if not stock_list:
                print("✅ 所有目标股票今日数据均已存在，无需更新。")
                return

        except: return
        
        with ThreadPoolExecutor(max_workers=SENSITIVE_WORKERS) as executor:
            futures = {executor.submit(self.fetch_and_save_daily_data, code): code for code in stock_list}
            for _ in tqdm(as_completed(futures), total=len(stock_list)): pass

    def run_fundamentals_sync(self):
        print(f">>> 🛡️ [3/3] 同步基本面数据 (安全: {SENSITIVE_WORKERS}线程)...")
        try:
            target_stocks = pd.read_sql("SELECT DISTINCT ts_code FROM nt_shareholders", self.engine)
            stock_list = target_stocks['ts_code'].tolist()
            
            # 🚀 [优化] 检查哪些股票今天已经更新过基本面，直接跳过
            try:
                existing_df = pd.read_sql(
                    text("SELECT ts_code FROM nt_stock_fundamentals WHERE update_date = :today"), 
                    self.engine, 
                    params={"today": self.today}
                )
                existing_codes = set(existing_df['ts_code'].tolist())
                
                original_len = len(stock_list)
                stock_list = [c for c in stock_list if c not in existing_codes]
                skipped_count = original_len - len(stock_list)
                
                if skipped_count > 0:
                    print(f"⏩ [基本面] 已跳过 {skipped_count} 只今日已更新的股票，剩余 {len(stock_list)} 只待处理。")
                
                if not stock_list:
                    print("✅ 所有目标股票今日基本面均已更新。")
                    return
            except Exception as e:
                print(f"⚠️ 无法检查基本面增量状态 (可能是缺少 update_date 字段): {e}")

        except: return

        final_data_list = []
        with ThreadPoolExecutor(max_workers=SENSITIVE_WORKERS) as executor:
            future_to_code = {executor.submit(self.fetch_combined_data, code): code for code in stock_list}
            for future in tqdm(as_completed(future_to_code), total=len(stock_list)):
                res = future.result()
                if 'curr_price' in res and res['curr_price']: 
                    res['update_date'] = self.today  # ✅ 增加更新日期
                    final_data_list.append(res)
        
        if final_data_list:
            df = pd.DataFrame(final_data_list)
            cols = list(final_data_list[0].keys())
            values_str = ", ".join([f":{c}" for c in cols])
            update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != 'ts_code'])
            sql = text(f"INSERT INTO nt_stock_fundamentals ({','.join(cols)}) VALUES ({values_str}) ON CONFLICT (ts_code) DO UPDATE SET {update_set}")
            with self.engine.connect() as conn:
                conn.execute(sql, df.to_dict(orient='records'))
                conn.commit()
            print(f"🎉 基本面更新完成，共 {len(df)} 条。")

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    try:
        engine = DataEngine()
        
        # 🟢 如果股东数据已经跑完，您可以注释掉下面这行跳过它
        engine.run_shareholder_sync()
        
        # 2. 日线 & 基本面
        engine.run_market_data_sync()
        engine.run_fundamentals_sync()
        
        duration = datetime.datetime.now() - start_time
        send_pushplus("任务完成", f"ETL 任务已成功执行完毕。\n耗时: {duration}")
        
    except Exception as e:
        err_msg = traceback.format_exc()
        print(err_msg)
        send_pushplus("任务崩溃", f"脚本发生严重错误，已停止。\n\n{str(e)}")
        os._exit(1)
