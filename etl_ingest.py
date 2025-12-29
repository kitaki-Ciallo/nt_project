# -*- coding: utf-8 -*-
"""
数据采集引擎 (ETL Ingest Engine) - 静态股息计算版 v5.0
核心功能：
1. [快轨] API 直连获取行情、PE(TTM)、PB、ROE。
2. [慢轨] 针对持仓股，逐个拉取历史分红，计算精准的“静态股息率”。
3. [计算] 静态股息率 = (上一年度累计每股分红 / 当前股价) * 100%
"""

import akshare as ak
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import requests
import time
import re

# ================= 配置区域 =================
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
SSF_KEYWORDS = ["社保", "养老",  "证金", "中央汇金", "全国社保", "基本养老"]
MAX_WORKERS = 8  # 计算密集型，适当降低并发

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class DataEngine:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.today = datetime.datetime.now().strftime("%Y%m%d")
        # 自动计算“去年”是哪一年 (例如现在是2025，去年就是2024)
        self.last_year = datetime.datetime.now().year - 1

    def get_secid(self, code):
        if str(code).startswith('6'): return f"1.{code}"
        else: return f"0.{code}"

    # --- 模块1: 股东数据 ---
    def get_stock_list(self):
        try:
            df = ak.stock_zh_a_spot_em()
            df = df[['代码', '名称']].copy()
            df.columns = ['ts_code', 'name']
            df.to_sql('stock_basic', self.engine, if_exists='replace', index=False, dtype={})
            return df['ts_code'].tolist()
        except: return []

    def fetch_eastmoney_api(self, secucode, report_type):
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        params = {
            "type": report_type,
            "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_RATIO,HOLD_NUM_CHANGE",
            "filter": f'(SECUCODE="{secucode}")',
            "p": "1", "ps": "50", "st": "END_DATE", "sr": "-1",
            "source": "SELECT_SECU_DATA", "client": "WEB",
        }
        try:
            res = requests.get(url, params=params, timeout=5)
            data = res.json()
            if data['result'] and data['result']['data']:
                return pd.DataFrame(data['result']['data'])
            return pd.DataFrame()
        except: return pd.DataFrame()

    def fetch_and_save_shareholders(self, ts_code):
        try:
            if str(ts_code).startswith('6'): secucode = f"{ts_code}.SH"
            elif str(ts_code).startswith(('8', '4')): secucode = f"{ts_code}.BJ"
            else: secucode = f"{ts_code}.SZ"

            df_free = self.fetch_eastmoney_api(secucode, "RPT_F10_EH_FREEHOLDERS")
            df_top10 = self.fetch_eastmoney_api(secucode, "RPT_F10_EH_HOLDERS")
            df = pd.concat([df_free, df_top10]).drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
            if df.empty: return 0

            mask = df['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
            target_df = df[mask].copy()
            if target_df.empty: return 0

            clean_df = pd.DataFrame()
            clean_df['ts_code'] = [ts_code] * len(target_df)
            clean_df['ann_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
            clean_df['end_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
            clean_df['holder_name'] = target_df['HOLDER_NAME']
            clean_df['hold_amount'] = target_df['HOLD_NUM'].astype(float) / 10000
            
            def parse_chg(x):
                try: return float(x) / 10000
                except: return 0
            clean_df['chg_amount'] = target_df['HOLD_NUM_CHANGE'].apply(parse_chg)

            try:
                clean_df.to_sql('nt_shareholders', self.engine, if_exists='append', index=False)
                return len(clean_df)
            except: return 0
        except: return 0

    def run_shareholder_sync(self):
        print(">>> 🚀 开始扫描股东数据...")
        stock_list = self.get_stock_list()
        total_saved = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self.fetch_and_save_shareholders, code): code for code in stock_list}
            for future in tqdm(as_completed(futures), total=len(stock_list), desc="Shareholder ETL"):
                total_saved += future.result()
        print(f"✅ 股东同步完成，累计入库 {total_saved} 条。")

    # --- 模块2: 日线行情 ---
    def fetch_and_save_daily_data(self, ts_code, start_date="20230101"):
        try:
            end_date = self.today
            df = ak.stock_zh_a_hist(symbol=ts_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            if df.empty: return
            save_df = pd.DataFrame()
            save_df['ts_code'] = df['股票代码']
            save_df['trade_date'] = pd.to_datetime(df['日期']).dt.date
            save_df['open'] = df['开盘']; save_df['high'] = df['最高']
            save_df['low'] = df['最低']; save_df['close'] = df['收盘']
            save_df['vol'] = df['成交量']; save_df['amount'] = df['成交额']
            try: save_df.to_sql('nt_market_data', self.engine, if_exists='append', index=False)
            except: pass
        except: pass

    def run_market_data_sync(self):
        print(">>> 🚀 开始同步日线数据...")
        try:
            target_stocks = pd.read_sql("SELECT DISTINCT ts_code FROM nt_shareholders", self.engine)
            stock_list = target_stocks['ts_code'].tolist()
        except: return
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self.fetch_and_save_daily_data, code, "20230101"): code for code in stock_list}
            for future in tqdm(as_completed(futures), total=len(stock_list), desc="MarketData ETL"):
                future.result()

    # ================= ✨ 模块3: 基本面 + 静态股息计算 =================
    
    def calculate_static_dividend(self, ts_code, curr_price):
        """
        ⚡️ 慢轨：查询历史分红，计算上一年度累计分红
        """
        if not curr_price or curr_price <= 0: return None
        
        try:
            # 获取分红历史 (东方财富接口)
            df = ak.stock_fhps_detail_em(symbol=ts_code)
            if df.empty: return None
            
            # 筛选上一年度的财报 (例如 2024-12-31, 2024-06-30 等)
            # 这里的‘截止日期’通常是财报期
            last_year_str = str(self.last_year)
            target_rows = df[df['截止日期'].astype(str).str.startswith(last_year_str)]
            
            if target_rows.empty: return 0.0 # 去年没分红
            
            total_dps = 0.0 # 每股累计股利
            
            for _, row in target_rows.iterrows():
                # 解析 "10派X元" 或 "每10股派X元"
                # 字段名通常是 "现金分红" (内容如: 10派3.064元)
                scheme = str(row.get('现金分红', ''))
                
                # 正则提取数字
                match = re.search(r'派([\d\.]+)元', scheme)
                if match:
                    cash_per_10 = float(match.group(1))
                    total_dps += (cash_per_10 / 10.0) # 转为每股
            
            # 静态股息率 = (去年分红总和 / 现价) * 100
            static_rate = (total_dps / curr_price) * 100
            return round(static_rate, 2)
            
        except Exception:
            return None

    def fetch_combined_data(self, ts_code):
        # 1. 快轨: 获取 API 实时数据
        url = "http://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "invt": "2", "fltt": "2",
            "fields": "f43,f57,f58,f162,f164,f167,f170,f163,f116,f173,f184", 
            "secid": self.get_secid(ts_code),
            "ut": "fa5fd1943c7b386f172d68934880c8d6", "cb": "jQuery123", "_": str(int(time.time() * 1000))
        }
        
        data = {
            "ts_code": ts_code,
            "pe_ttm": None, "pe_dyn": None, "pe_static": None,
            "pb": None, "div_rate": None, "div_rate_static": None,
            "total_mv": None, "curr_price": None,
            "eps": None, "roe": None, "net_profit_growth": None, "net_margin": None
        }

        try:
            res = requests.get(url, params=params, timeout=3)
            text = res.text
            if "(" in text and ")" in text:
                import json
                json_str = text.split("(", 1)[1].rsplit(")", 1)[0]
                resp_json = json.loads(json_str)
                
                if resp_json and resp_json.get('data'):
                    d = resp_json['data']
                    def parse_val(val):
                        if val == "-": return None
                        try: return float(val)
                        except: return val

                    data['curr_price'] = parse_val(d.get('f43'))
                    data['total_mv'] = parse_val(d.get('f116'))
                    data['pe_dyn'] = parse_val(d.get('f162'))
                    data['pe_ttm'] = parse_val(d.get('f164'))
                    data['pe_static'] = parse_val(d.get('f163'))
                    data['pb'] = parse_val(d.get('f167'))
                    data['roe'] = parse_val(d.get('f173'))
                    data['net_profit_growth'] = parse_val(d.get('f184'))

                    # TTM 股息率 (官方)
                    raw_div = parse_val(d.get('f170'))
                    if raw_div is not None and raw_div > 0:
                        data['div_rate'] = raw_div
                        
                    # 估算 EPS
                    if data['curr_price'] and data['pe_ttm'] and data['pe_ttm'] > 0:
                        data['eps'] = round(data['curr_price'] / data['pe_ttm'], 2)

                    # 2. 慢轨: 计算静态股息率 (只有当拿到现价时才算)
                    if data['curr_price']:
                        static_val = self.calculate_static_dividend(ts_code, data['curr_price'])
                        if static_val is not None:
                            data['div_rate_static'] = static_val

        except Exception: pass
        return data

    def run_fundamentals_sync(self):
        print(f">>> 🚀 开始同步基本面数据 (基准年: {self.last_year})...")
        try:
            target_stocks = pd.read_sql("SELECT DISTINCT ts_code FROM nt_shareholders", self.engine)
            stock_list = target_stocks['ts_code'].tolist()
            print(f">>> 目标更新股票数: {len(stock_list)}")
        except: return

        final_data_list = []
        # 注意：这里因为加了 calculate_static_dividend，速度会变慢，所以用多线程
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_code = {executor.submit(self.fetch_combined_data, code): code for code in stock_list}
            for future in tqdm(as_completed(future_to_code), total=len(stock_list), desc="Fetching Data"):
                res = future.result()
                if res['curr_price'] is not None:
                    final_data_list.append(res)
        
        df_final = pd.DataFrame(final_data_list)
        
        if not df_final.empty:
            print(">>> 正在更新数据库 (Schema Update)...")
            
            # 补全列
            expected_cols = ['ts_code', 'total_mv', 'pe_dyn', 'pe_ttm', 'pe_static', 'pb', 'curr_price', 'eps', 'roe', 'div_rate', 'div_rate_static', 'net_profit_growth', 'net_margin']
            for col in expected_cols:
                if col not in df_final.columns:
                    df_final[col] = None
            
            # 使用 replace 确保 div_rate_static 字段被创建
            df_final[expected_cols].to_sql('nt_stock_fundamentals', self.engine, if_exists='replace', index=False)
            print(f"🎉 基本面数据更新完成！共 {len(df_final)} 条。")
        else:
            print("⚠️ 未获取到有效数据。")

if __name__ == "__main__":
    engine = DataEngine()
    # 第一次运行建议全部解开
    engine.run_shareholder_sync()
    engine.run_market_data_sync() 
    engine.run_fundamentals_sync()
