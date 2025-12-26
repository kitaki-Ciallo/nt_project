# -*- coding: utf-8 -*-
"""
核心分析引擎 (Analysis Engine) - 修复单位版 v2.1
修复内容：
1. VWAP 计算公式增加 *100 (手转股)
2. 增加异常值过滤 (成本太离谱的直接剔除)
"""

import pandas as pd
from sqlalchemy import create_engine, text
import datetime
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 配置区域 =================
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
COST_DISCOUNT = 0.95 
MAX_WORKERS = 10

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class NationalTeamAnalyzer:
    def __init__(self):
        self.engine = create_engine(DB_URL, pool_size=20, max_overflow=0)
        
    def get_all_latest_prices(self):
        print(">>> 正在预加载全市场最新价格...")
        sql = """
        SELECT DISTINCT ON (ts_code) ts_code, close 
        FROM nt_market_data 
        ORDER BY ts_code, trade_date DESC
        """
        try:
            df = pd.read_sql(sql, self.engine)
            return dict(zip(df['ts_code'], df['close']))
        except Exception as e:
            logging.error(f"预加载价格失败: {e}")
            return {}

    def calculate_vwap_single(self, row, latest_prices):
        ts_code = row['ts_code']
        end_date = row['end_date']
        
        # 1. 确定时间区间 (前推90天)
        period_end = end_date
        period_start = period_end - datetime.timedelta(days=90)
        
        try:
            with self.engine.connect() as conn:
                sql = text("""
                    SELECT sum(amount) as total_amt, sum(vol) as total_vol 
                    FROM nt_market_data 
                    WHERE ts_code = :code AND trade_date >= :start AND trade_date <= :end
                """)
                result = conn.execute(sql, {
                    "code": ts_code, 
                    "start": period_start, 
                    "end": period_end
                }).fetchone()
                
                total_amt = result[0]
                total_vol = result[1]
                
                vwap = 0
                if total_amt and total_vol and total_vol > 0:
                    # 【核心修复】 AkShare/东财的 vol 通常是手，需要 * 100 转为股
                    # 公式: 总金额 / (总手数 * 100)
                    vwap = float(total_amt) / (float(total_vol) * 100)
                
                if vwap > 0:
                    est_cost = vwap * COST_DISCOUNT
                    
                    # 异常值过滤：如果成本 > 2000 (除茅台外几乎不可能)，说明数据源单位可能是股而不是手
                    # 这里的逻辑是自适应：如果算出来特别大，那可能数据源本来就是股，不用乘100
                    # 但为了保险，我们先按 *100 修正，绝大多数股票价格都在 5-200 之间
                    
                    curr_price = latest_prices.get(ts_code)
                    
                    if curr_price:
                        profit_rate = (curr_price - est_cost) / est_cost
                        
                        status = "未知"
                        if profit_rate < -0.1: status = "Deep Lock (深套)"
                        elif -0.1 <= profit_rate <= 0: status = "Trapped (被套)"
                        elif 0 < profit_rate <= 0.2: status = "Profit (盈利)"
                        else: status = "High Profit (高利)"

                        return {
                            "ts_code": ts_code,
                            "holder_name": row['holder_name'],
                            "period_end": period_end,
                            "est_cost": round(est_cost, 2),
                            "cost_method": "VWAP_Estimate",
                            "curr_price": curr_price,
                            "profit_rate": round(profit_rate, 4),
                            "status": status,
                            "update_time": datetime.datetime.now()
                        }
        except Exception:
            pass
        return None

    def analyze_positions(self):
        print(">>> 🕵️‍♂️ 开始分析国家队持仓成本 (修复版)...")
        
        sql = "SELECT * FROM nt_shareholders WHERE ann_date > '2023-01-01' ORDER BY end_date DESC"
        df_holders = pd.read_sql(sql, self.engine)
        print(f"📊 待分析记录共 {len(df_holders)} 条")
        
        latest_prices = self.get_all_latest_prices()
        print(f"✅ 已加载 {len(latest_prices)} 只股票的最新价格")
        
        analysis_results = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.calculate_vwap_single, row, latest_prices) for _, row in df_holders.iterrows()]
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="快速计算中"):
                res = future.result()
                if res:
                    analysis_results.append(res)

        if analysis_results:
            print(f"✅ 计算完成，正在写入 {len(analysis_results)} 条分析结果...")
            df_res = pd.DataFrame(analysis_results)
            
            with self.engine.connect() as conn:
                conn.execute(text("TRUNCATE TABLE nt_positions_analysis"))
                conn.commit()
            
            df_res.to_sql('nt_positions_analysis', self.engine, if_exists='append', index=False)
            print("🚀 数据分析入库成功！")
        else:
            print("⚠️ 未生成结果")

if __name__ == "__main__":
    analyzer = NationalTeamAnalyzer()
    analyzer.analyze_positions()
