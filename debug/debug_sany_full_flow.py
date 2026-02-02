# -*- coding: utf-8 -*-
"""
三一重工 (600031) 全流程单股验证脚本 v2
修复：
1. [Bug] 修复变量名冲突导致的 'str object is not callable' 错误。
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import datetime
import requests
import time
import random
import json

# ================= 配置区域 =================
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
SSF_KEYWORDS = ["社保", "养老", "证金", "中央汇金", "全国社保", "基本养老", "中国证券金融", "社保基金", "汇金资管"]
TARGET_CODE = "600031"
TARGET_NAME = "三一重工"

class DataEngine:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://data.eastmoney.com/",
            "Connection": "keep-alive"
        }
        self.today = datetime.datetime.now().strftime("%Y%m%d")

    def get_secid(self, code):
        if str(code).startswith('6'): return f"1.{code}"
        else: return f"0.{code}"

    def get_secucode(self, code):
        c = str(code)
        if c.startswith('6'): return f"{c}.SH"
        elif c.startswith('8') or c.startswith('4') or c.startswith('9'): return f"{c}.BJ"
        else: return f"{c}.SZ"

    def fetch_eastmoney_api_safe(self, code, report_type):
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        secucode = self.get_secucode(code)
        
        params = {
            "type": report_type,
            "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_RATIO,HOLD_NUM_CHANGE",
            "filter": f'(SECUCODE="{secucode}")',
            "p": "1", "ps": "50", 
            "st": "END_DATE", "sr": "-1",
            "source": "SELECT_SECU_DATA", "client": "WEB",
            "_": str(int(time.time() * 1000))
        }
        
        print(f"📡 [API] 请求 {report_type}...")
        try:
            res = requests.get(url, params=params, headers=self.headers, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if data.get('result') and data['result'].get('data'):
                    df = pd.DataFrame(data['result']['data'])
                    print(f"   ✅ 获取到 {len(df)} 条数据 (最新日期: {df.iloc[0]['END_DATE']})")
                    return df
                else:
                    print("   ⚠️ 获取数据为空")
            else:
                print(f"   ❌ HTTP {res.status_code}")
        except Exception as e:
            print(f"   ❌ 请求异常: {e}")
        return pd.DataFrame()

    def sync_stock_info(self, ts_code):
        print(f"\n>>> 🚀 [0/3] 同步股票基础信息 ({ts_code})...")
        try:
            sql = text("INSERT INTO stock_basic (ts_code, name) VALUES (:ts_code, :name) ON CONFLICT (ts_code) DO UPDATE SET name = EXCLUDED.name")
            with self.engine.connect() as conn:
                conn.execute(sql, {"ts_code": ts_code, "name": TARGET_NAME})
                conn.commit()
            print("✅ 股票名称入库成功")
        except Exception as e:
            print(f"❌ 基础信息同步失败: {e}")

    def run_shareholder_flow(self, ts_code):
        print(f"\n>>> 🚀 [1/3] 测试股东数据抓取与入库 ({ts_code})...")
        df1 = self.fetch_eastmoney_api_safe(ts_code, "RPT_F10_EH_FREEHOLDERS")
        df2 = self.fetch_eastmoney_api_safe(ts_code, "RPT_F10_EH_HOLDERS")
        df = pd.concat([df1, df2])
        
        if df.empty:
            print("❌ 未抓取到任何股东数据")
            return

        df = df.drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
        mask = df['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
        
        target_df = df[mask].copy().reset_index(drop=True)
        print(f"✅ 筛选出 {len(target_df)} 条国家队持仓")
        
        if target_df.empty: return

        clean_df = pd.DataFrame()
        clean_df['ts_code'] = [ts_code] * len(target_df)
        clean_df['ann_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
        clean_df['end_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
        clean_df['holder_name'] = target_df['HOLDER_NAME']
        clean_df['hold_amount'] = target_df['HOLD_NUM'].astype(float) / 10000
        
        if 'HOLD_RATIO' in target_df.columns:
            clean_df['hold_ratio'] = target_df['HOLD_RATIO'].astype(float)
        else:
            clean_df['hold_ratio'] = None
            
        def parse_chg(x):
            try: return float(x) / 10000
            except: return 0
        clean_df['chg_amount'] = target_df['HOLD_NUM_CHANGE'].apply(parse_chg)

        print("⚡️ 正在写入数据库 (nt_shareholders)...")
        try:
            data_list = clean_df.to_dict(orient='records')
            cols = list(data_list[0].keys())
            values_str = ", ".join([f":{c}" for c in cols])
            sql = text(f"INSERT INTO nt_shareholders ({','.join(cols)}) VALUES ({values_str}) ON CONFLICT (ts_code, holder_name, end_date) DO NOTHING")
            with self.engine.connect() as conn:
                conn.execute(sql, data_list)
                conn.commit()
            print(f"🎉 入库成功！共 {len(clean_df)} 条记录")
        except Exception as e:
            print(f"❌ 入库失败: {e}")

    def run_market_data_flow(self, ts_code):
        print(f"\n>>> 🚀 [2/3] 测试日线数据同步 ({ts_code})...")
        try:
            start_date = "20230101"
            end_date = self.today
            url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
            secid = self.get_secid(ts_code)
            params = {
                "secid": secid, "klt": "101", "fqt": "1", "lmt": "2000", 
                "beg": start_date, "end": end_date, 
                "fields1": "f1", "fields2": "f51,f52,f53,f54,f55,f56,f57"
            }
            res = requests.get(url, params=params, headers=self.headers, timeout=5)
            data = res.json()
            
            if not (data.get('data') and data['data'].get('klines')):
                print("⚠️ 未获取到K线数据")
                return

            rows = []
            for k in data['data']['klines']:
                parts = k.split(',')
                rows.append({
                    "ts_code": ts_code,
                    "trade_date": parts[0],
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "high": float(parts[3]),
                    "low": float(parts[4]),
                    "vol": float(parts[5]),
                    "amount": float(parts[6])
                })
            
            print(f"✅ 获取到 {len(rows)} 条日线数据")
            
            if rows:
                df = pd.DataFrame(rows)
                data_list = df.to_dict(orient='records')
                cols = list(data_list[0].keys())
                values_str = ", ".join([f":{c}" for c in cols])
                update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c not in ['ts_code', 'trade_date']])
                sql = text(f"INSERT INTO nt_market_data ({','.join(cols)}) VALUES ({values_str}) ON CONFLICT (ts_code, trade_date) DO UPDATE SET {update_set}")
                with self.engine.connect() as conn:
                    conn.execute(sql, data_list)
                    conn.commit()
                print("🎉 日线数据入库成功！")
        except Exception as e:
            print(f"❌ 日线同步失败: {e}")

    def run_fundamentals_flow(self, ts_code):
        print(f"\n>>> 🚀 [3/3] 测试基本面数据同步 ({ts_code})...")
        url = "http://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "invt": "2", "fltt": "2",
            "fields": "f43,f57,f58,f162,f164,f167,f170,f163,f116,f173,f183,f184,f185,f186,f187", 
            "secid": self.get_secid(ts_code),
            "ut": "fa5fd1943c7b386f172d68934880c8d6", 
            "cb": "jQuery123",
            "_": str(int(time.time() * 1000))
        }
        
        data = {
            "ts_code": ts_code, "pe_ttm": None, "pe_dyn": None, "pe_static": None, "pb": None,
            "div_rate": None, "div_rate_static": None, "total_mv": None, "curr_price": None,
            "eps": None, "roe": None, "revenue": None, "revenue_growth": None, 
            "net_profit_growth": None, "gross_margin": None, "net_margin": None
        }

        try:
            res = requests.get(url, params=params, headers=self.headers, timeout=5)
            # 🟢 [修复] 变量名改为 resp_text，避免覆盖 sqlalchemy.text
            resp_text = res.text 
            if "(" in resp_text:
                json_str = resp_text.split("(", 1)[1].rsplit(")", 1)[0]
                d = json.loads(json_str).get('data')
                
                if d:
                    print(f"✅ 获取到基本面数据 (现价: {d.get('f43')})")
                    def parse_val(val): return float(val) if val != "-" else None
                    data['curr_price'] = parse_val(d.get('f43'))
                    data['total_mv'] = parse_val(d.get('f116'))
                    data['pe_ttm'] = parse_val(d.get('f164'))
                    # ... 其他字段省略，主要验证入库逻辑
                    
                    # 入库
                    # 🟢 [修复] 这里调用 text() 就不会报错了
                    sql = text("INSERT INTO nt_stock_fundamentals (ts_code, curr_price, total_mv, pe_ttm) VALUES (:ts_code, :curr_price, :total_mv, :pe_ttm) ON CONFLICT (ts_code) DO UPDATE SET curr_price = EXCLUDED.curr_price")
                    with self.engine.connect() as conn:
                        conn.execute(sql, data)
                        conn.commit()
                    print("🎉 基本面入库成功！")
                else:
                    print("⚠️ 基本面数据为空")
        except Exception as e:
            print(f"❌ 基本面同步失败: {e}")
            import traceback
            traceback.print_exc()

    def verify_db(self, ts_code):
        print(f"\n>>> 🔍 [4/4] 最终数据库核验 ({ts_code})...")
        with self.engine.connect() as conn:
            # 查股东
            res = conn.execute(text(f"SELECT count(*) FROM nt_shareholders WHERE ts_code='{ts_code}'"))
            count_h = res.scalar()
            
            # 查日线
            res = conn.execute(text(f"SELECT count(*) FROM nt_market_data WHERE ts_code='{ts_code}'"))
            count_m = res.scalar()
            
            # 查基本面
            res = conn.execute(text(f"SELECT curr_price, total_mv FROM nt_stock_fundamentals WHERE ts_code='{ts_code}'"))
            fund = res.fetchone()
            
            print(f"📊 核验结果:")
            print(f"   - 股东记录数: {count_h} (预期: >0)")
            print(f"   - 日线记录数: {count_m} (预期: >0)")
            if fund:
                print(f"   - 基本面: 现价={fund[0]}, 市值={fund[1]}")
            else:
                print(f"   - 基本面: ❌ 未找到")

if __name__ == "__main__":
    engine = DataEngine()
    print(f"🔥 开始 v9.5 全流程单股调试: {TARGET_NAME} (变量名修复版) 🔥")
    engine.sync_stock_info(TARGET_CODE)
    engine.run_shareholder_flow(TARGET_CODE)
    engine.run_market_data_flow(TARGET_CODE)
    engine.run_fundamentals_flow(TARGET_CODE)
    engine.verify_db(TARGET_CODE)
