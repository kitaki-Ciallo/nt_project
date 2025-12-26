# -*- coding: utf-8 -*-
"""
数据采集引擎 (ETL Ingest Engine) - 最终直连API版
功能：
1. 使用 EastMoney 原生 API 直连抓取股东数据 (极速、稳定)
2. 同时抓取 [十大流通股东] 和 [十大股东]，防止漏掉汇金/社保
3. 抓取日线行情用于后续计算
4. 存入 PostgreSQL 数据库
"""

import akshare as ak
import pandas as pd
from sqlalchemy import create_engine
import datetime
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import random
import requests
import json

# ================= 配置区域 =================
# 数据库连接
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"

# 国家队关键词 (扩充了关键词库)
SSF_KEYWORDS = [
    "社保", "养老", "汇金", "证金", "Social Security", "Investment", 
    "中央汇金", "全国社保", "基本养老"
]

# 并发数 (API 响应很快，8线程非常稳)
MAX_WORKERS = 8

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class DataEngine:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.today = datetime.datetime.now().strftime("%Y%m%d")

    def get_stock_list(self):
        """获取全市场股票列表"""
        print(">>> 正在更新股票基础信息...")
        try:
            df = ak.stock_zh_a_spot_em()
            df = df[['代码', '名称']].copy()
            df.columns = ['ts_code', 'name']
            df['symbol'] = df['ts_code']
            df['list_date'] = '1990-01-01'
            df.to_sql('stock_basic', self.engine, if_exists='replace', index=False, dtype={})
            print(f"✅ 基础信息更新完毕，共 {len(df)} 只股票")
            return df['ts_code'].tolist()
        except Exception as e:
            logging.error(f"基础信息更新失败: {e}")
            return []

    def fetch_eastmoney_api(self, secucode, report_type):
        """
        封装通用的东财 API 请求
        report_type: 'RPT_F10_EH_FREEHOLDERS' (流通) 或 'RPT_F10_EH_HOLDERS' (十大)
        """
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        params = {
            "type": report_type,
            "sty": "END_DATE,HOLDER_NAME,HOLD_NUM,HOLD_RATIO,HOLD_NUM_CHANGE",
            "filter": f'(SECUCODE="{secucode}")',
            "p": "1",
            "ps": "50", # 拿最近 50 条 (约5年)
            "st": "END_DATE",
            "sr": "-1",
            "source": "SELECT_SECU_DATA",
            "client": "WEB",
        }
        try:
            res = requests.get(url, params=params, timeout=5)
            data = res.json()
            if data['result'] and data['result']['data']:
                return pd.DataFrame(data['result']['data'])
            return pd.DataFrame()
        except:
            return pd.DataFrame()

    def fetch_and_save_shareholders(self, ts_code):
        """
        [双重保障] 获取股东并入库
        """
        try:
            # 随机休眠
            time.sleep(random.uniform(0.05, 0.1))
            
            # 构造 secucode
            if str(ts_code).startswith('6'):
                secucode = f"{ts_code}.SH"
            elif str(ts_code).startswith('8') or str(ts_code).startswith('4'):
                secucode = f"{ts_code}.BJ"
            else:
                secucode = f"{ts_code}.SZ"

            # 1. 并行获取 [十大流通] 和 [十大股东]
            # 有些国家队锁定期没过，只出现在十大股东里；有些在流通股里
            df_free = self.fetch_eastmoney_api(secucode, "RPT_F10_EH_FREEHOLDERS")
            df_top10 = self.fetch_eastmoney_api(secucode, "RPT_F10_EH_HOLDERS")
            
            # 合并两个 DataFrame
            df = pd.concat([df_free, df_top10]).drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
            
            if df.empty:
                return 0

            # 2. 筛选国家队
            if 'HOLDER_NAME' not in df.columns:
                return 0
                
            mask = df['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
            target_df = df[mask].copy()
            
            if target_df.empty:
                return 0

            # 3. 清洗
            clean_df = pd.DataFrame()
            clean_df['ts_code'] = [ts_code] * len(target_df)
            
            clean_df['ann_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
            clean_df['end_date'] = pd.to_datetime(target_df['END_DATE']).dt.date
            clean_df['holder_name'] = target_df['HOLDER_NAME']
            
            # 持股数
            clean_df['hold_amount'] = target_df['HOLD_NUM'].astype(float) / 10000
            
            # 比例
            clean_df['hold_ratio'] = target_df['HOLD_RATIO'].astype(float)
            
            # 变动
            def parse_chg(x):
                if str(x) == '新进' or x is None:
                    return 0 # 暂填0，后续分析脚本会算出具体新进量
                try:
                    return float(x) / 10000
                except:
                    return 0
            clean_df['chg_amount'] = target_df['HOLD_NUM_CHANGE'].apply(parse_chg)

            # 4. 入库 (忽略重复)
            try:
                clean_df.to_sql('nt_shareholders', self.engine, if_exists='append', index=False)
                return len(clean_df)
            except:
                # 出现重复主键时，转为逐行插入，跳过已存在的
                count = 0
                for _, row in clean_df.iterrows():
                    try:
                        pd.DataFrame([row]).to_sql('nt_shareholders', self.engine, if_exists='append', index=False)
                        count += 1
                    except:
                        continue
                return count

        except Exception as e:
            return 0

    def run_shareholder_sync(self):
        print(">>> 🚀 开始扫描全市场股东数据 (直连API双通道版)...")
        stock_list = self.get_stock_list()
        
        # stock_list = stock_list[:100] # 调试用，正式跑请注释掉

        total_saved = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self.fetch_and_save_shareholders, code): code for code in stock_list}
            
            for future in tqdm(as_completed(futures), total=len(stock_list), desc="Shareholder ETL"):
                total_saved += future.result()
        
        print(f"✅ 股东同步完成，累计入库 {total_saved} 条记录。")

    def fetch_and_save_daily_data(self, ts_code, start_date="20230101"):
        """同步日线数据"""
        try:
            time.sleep(random.uniform(0.1, 0.3))
            end_date = self.today
            df = ak.stock_zh_a_hist(symbol=ts_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            if df.empty: return

            save_df = pd.DataFrame()
            save_df['ts_code'] = df['股票代码']
            save_df['trade_date'] = pd.to_datetime(df['日期']).dt.date
            save_df['open'] = df['开盘']
            save_df['high'] = df['最高']
            save_df['low'] = df['最低']
            save_df['close'] = df['收盘']
            save_df['vol'] = df['成交量']
            save_df['amount'] = df['成交额']

            try:
                save_df.to_sql('nt_market_data', self.engine, if_exists='append', index=False)
            except:
                pass
        except Exception:
            pass

    def run_market_data_sync(self):
        print(">>> 🚀 开始同步日线数据 (仅针对国家队持仓股)...")
        try:
            target_stocks = pd.read_sql("SELECT DISTINCT ts_code FROM nt_shareholders", self.engine)
            stock_list = target_stocks['ts_code'].tolist()
        except:
            stock_list = []
            print("⚠️ 数据库中没有股东数据，跳过日线同步。")

        print(f"需要同步行情的股票数量: {len(stock_list)}")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            start_dt = "20230101" 
            futures = {executor.submit(self.fetch_and_save_daily_data, code, start_dt): code for code in stock_list}
            for future in tqdm(as_completed(futures), total=len(stock_list), desc="MarketData ETL"):
                future.result()
        print("✅ 日线行情同步完成。")

if __name__ == "__main__":
    engine = DataEngine()
    
    # 1. 抓股东 (国家队在哪？)
    engine.run_shareholder_sync()
    
    # 2. 抓行情 (为了算成本)
    engine.run_market_data_sync()
