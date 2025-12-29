# -*- coding: utf-8 -*-
"""
核心分析引擎 (Analysis Engine) - 深度透视版 v4.2 (文案微调版)
修改内容：
1. [文案] "持仓 (原有)" 统一改为 "持仓未动"。
"""

import pandas as pd
from sqlalchemy import create_engine, text
import datetime
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 配置区域 =================
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
COST_DISCOUNT = 0.95  # 机构吸筹折价系数
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

    def get_history_info(self):
        """预加载考古信息：历史成本 + 建仓日期"""
        print(">>> 正在加载历史考古档案...")
        try:
            sql = "SELECT ts_code, hist_cost, first_buy_date FROM nt_history_cost"
            df = pd.read_sql(sql, self.engine)
            cost_map = dict(zip(df['ts_code'], df['hist_cost']))
            date_map = {}
            for _, row in df.iterrows():
                d = row['first_buy_date']
                if d:
                    date_map[row['ts_code']] = pd.to_datetime(d)
            return cost_map, date_map
        except:
            return {}, {}

    def get_quarter_vwap(self, ts_code, end_date):
        """计算特定季度(90天)的 VWAP"""
        start_date = end_date - datetime.timedelta(days=90)
        try:
            with self.engine.connect() as conn:
                sql = text("""
                    SELECT sum(amount) as total_amt, sum(vol) as total_vol 
                    FROM nt_market_data 
                    WHERE ts_code = :code AND trade_date >= :start AND trade_date <= :end
                """)
                res = conn.execute(sql, {"code": ts_code, "start": start_date, "end": end_date}).fetchone()
                if res and res[0] and res[1] and res[1] > 0:
                    return float(res[0]) / (float(res[1]) * 100)
        except: pass
        return 0.0

    def generate_change_analysis(self, row, prev_row, hist_first_cost, curr_price, first_buy_date):
        """生成硬核的变动分析文案"""
        current_date = pd.to_datetime(row['end_date'])
        
        # --- 情况1: 列表中的第一条记录 (prev_row is None) ---
        if prev_row is None:
            is_new = True
            # 如果有历史建仓时间，且历史时间比当前报告期早超过 180 天，说明是老股
            if first_buy_date and pd.notnull(first_buy_date):
                if (current_date - first_buy_date).days > 180:
                    is_new = False
            
            if is_new:
                return "🆕 新进建仓"
            else:
                # 🟢 修改点：改为“持仓未动”
                return "🔹 持仓未动"
        
        # --- 情况2: 有上一期记录 ---
        hold_now = row['hold_amount']
        hold_prev = prev_row['hold_amount']
        
        if hold_now == hold_prev:
            return "🔹 持仓未动"
        
        diff = hold_now - hold_prev
        pct_change = (diff / hold_prev * 100) if hold_prev > 0 else 0
        
        period_vwap = self.get_quarter_vwap(row['ts_code'], row['end_date'])
        op_cost = period_vwap * COST_DISCOUNT if period_vwap > 0 else 0
        
        vs_first_pct_str = "N/A"
        if hist_first_cost and hist_first_cost > 0 and op_cost > 0:
            val = (op_cost - hist_first_cost) / hist_first_cost * 100
            vs_first_pct_str = f"{val:+.1f}%"
            
        vs_curr_pct_str = "N/A"
        if curr_price and curr_price > 0 and op_cost > 0:
            val = (curr_price - op_cost) / op_cost * 100
            vs_curr_pct_str = f"{val:+.1f}%"

        op_cost_str = f"{op_cost:.2f}" if op_cost > 0 else "未知"

        if diff > 0:
            return f"🔺 加仓{pct_change:.1f}% | 成本≈{op_cost_str} (较建仓{vs_first_pct_str}, 较现价{vs_curr_pct_str})"
        else:
            return f"🔻 减仓{abs(pct_change):.1f}% | 均价≈{op_cost_str} (较建仓{vs_first_pct_str}, 较现价{vs_curr_pct_str})"

    def process_group(self, group_df, latest_prices, hist_costs, hist_dates):
        results = []
        group_df = group_df.sort_values('end_date', ascending=True)
        
        prev_row = None
        total_records = len(group_df)
        
        for idx, row in enumerate(group_df.to_dict('records')):
            ts_code = row['ts_code']
            
            est_cost = 0.0
            cost_method = "未知"
            h_cost = hist_costs.get(ts_code, 0)
            
            if h_cost > 0:
                est_cost = float(h_cost)
                cost_method = "⏳ 历史回溯"
            else:
                vwap = self.get_quarter_vwap(ts_code, row['end_date'])
                if vwap > 0:
                    est_cost = vwap * COST_DISCOUNT
                    cost_method = "⚡️ 近期估算"
            
            curr_price = latest_prices.get(ts_code, 0)
            profit_rate = 0.0
            status = "未知"
            
            if est_cost > 0 and curr_price > 0:
                profit_rate = (curr_price - est_cost) / est_cost
                if profit_rate < -0.1: status = "Deep Lock (深套)"
                elif -0.1 <= profit_rate <= 0: status = "Trapped (被套)"
                elif 0 < profit_rate <= 0.2: status = "Profit (盈利)"
                else: status = "High Profit (高利)"

            f_date = hist_dates.get(ts_code, None)
            analysis_text = self.generate_change_analysis(row, prev_row, h_cost, curr_price, f_date)
            
            is_latest = (idx == total_records - 1)

            res = {
                "ts_code": ts_code,
                "name": row.get('name', ''),
                "holder_name": row['holder_name'],
                "period_end": row['end_date'],
                "hold_amount": row['hold_amount'],
                "est_cost": round(est_cost, 2),
                "curr_price": curr_price,
                "profit_rate": round(profit_rate, 4),
                "status": status,
                "cost_source": cost_method,
                "first_buy_date": f_date, 
                "change_analysis": analysis_text,
                "is_latest": is_latest,
                "update_time": datetime.datetime.now()
            }
            results.append(res)
            prev_row = row
            
        return results

    def analyze_positions(self):
        print(">>> 🕵️‍♂️ 开始分析国家队持仓成本 (文案微调版 v4.2)...")
        
        latest_prices = self.get_all_latest_prices()
        hist_costs, hist_dates = self.get_history_info()
        
        sql = """
        SELECT s.*, b.name 
        FROM nt_shareholders s
        LEFT JOIN stock_basic b ON s.ts_code = b.ts_code
        WHERE s.ann_date > '2022-01-01' 
        ORDER BY s.ts_code, s.holder_name, s.end_date
        """
        df_all = pd.read_sql(sql, self.engine)
        print(f"📊 原始持仓记录: {len(df_all)} 条")
        
        grouped = df_all.groupby(['ts_code', 'holder_name'])
        final_results = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for _, group in grouped:
                futures.append(executor.submit(self.process_group, group, latest_prices, hist_costs, hist_dates))
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="深度分析中"):
                res_list = future.result()
                final_results.extend(res_list)

        if final_results:
            print(f"✅ 分析完成，生成 {len(final_results)} 条分析报告...")
            df_res = pd.DataFrame(final_results)
            
            cols_needed = ['display_val', 'display_amount', 'change_pct_display']
            for c in cols_needed:
                if c not in df_res.columns: df_res[c] = 0
            
            df_res.to_sql('nt_positions_analysis', self.engine, if_exists='replace', index=False)
            print("🚀 数据入库成功！(Table Replaced)")
        else:
            print("⚠️ 未生成结果")

if __name__ == "__main__":
    analyzer = NationalTeamAnalyzer()
    analyzer.analyze_positions()
