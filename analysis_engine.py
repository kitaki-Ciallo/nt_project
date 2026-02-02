# -*- coding: utf-8 -*-
"""
核心分析引擎 v4.4 (支持单账户独立成本 + 财报期对比提示)
更新日志：
- [优化] 变动分析文案增加对比日期，例如 "(较2025-06-30)".
"""
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
COST_DISCOUNT = 0.95
MAX_WORKERS = 10

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class NationalTeamAnalyzer:
    def __init__(self):
        self.engine = create_engine(DB_URL, pool_size=20, max_overflow=0)
        
    def get_all_latest_prices(self):
        sql = "SELECT DISTINCT ON (ts_code) ts_code, close FROM nt_market_data ORDER BY ts_code, trade_date DESC"
        try:
            df = pd.read_sql(sql, self.engine)
            return dict(zip(df['ts_code'], df['close']))
        except: return {}

    def get_history_info(self):
        print(">>> 正在加载精细化考古档案...")
        try:
            sql = "SELECT ts_code, holder_name, hist_cost, first_buy_date FROM nt_history_cost"
            df = pd.read_sql(sql, self.engine)
            cost_map = {}
            date_map = {}
            for _, row in df.iterrows():
                key = (row['ts_code'], row['holder_name'])
                cost_map[key] = row['hist_cost']
                if row['first_buy_date']:
                    date_map[key] = pd.to_datetime(row['first_buy_date'])
            return cost_map, date_map
        except: return {}, {}

    def get_quarter_vwap(self, ts_code, end_date):
        start_date = end_date - datetime.timedelta(days=90)
        try:
            with self.engine.connect() as conn:
                sql = text("SELECT sum(amount), sum(vol) FROM nt_market_data WHERE ts_code = :c AND trade_date >= :s AND trade_date <= :e")
                res = conn.execute(sql, {"c": ts_code, "s": start_date, "e": end_date}).fetchone()
                if res and res[1] and res[1] > 0: return float(res[0]) / (float(res[1]) * 100)
        except: pass
        return 0.0

    def generate_change_analysis(self, row, prev_row, hist_cost, curr_price, first_buy_date):
        current_date = pd.to_datetime(row['end_date'])
        
        # 1. 新进或无历史记录情况
        if prev_row is None:
            is_new = True
            if first_buy_date and pd.notnull(first_buy_date):
                if (current_date - first_buy_date).days > 180: is_new = False
            return "🆕 新进建仓" if is_new else "🔹 持仓未动"
        
        # 2. 有历史记录，准备对比数据
        hold_now = row['hold_amount']
        hold_prev = prev_row['hold_amount']
        
        # 🟢 获取上个财报期日期
        prev_date_str = str(prev_row['end_date'])
        compare_prefix = f"({prev_date_str}) " # 例如: (2025-06-30) 
        
        if hold_now == hold_prev: 
            return f"{compare_prefix}🔹 持仓未动"
        
        diff = hold_now - hold_prev
        pct_change = (diff / hold_prev * 100) if hold_prev > 0 else 0
        
        period_vwap = self.get_quarter_vwap(row['ts_code'], row['end_date'])
        op_cost = period_vwap * COST_DISCOUNT if period_vwap > 0 else 0
        op_cost_str = f"{op_cost:.2f}" if op_cost > 0 else "未知"
        
        vs_first = "N/A"
        if hist_cost > 0 and op_cost > 0: vs_first = f"{(op_cost - hist_cost)/hist_cost*100:+.1f}%"
        
        vs_curr = "N/A"
        if curr_price > 0 and op_cost > 0: vs_curr = f"{(curr_price - op_cost)/op_cost*100:+.1f}%"

        tag = "🔺 加仓" if diff > 0 else "🔻 减仓"
        
        # 🟢 拼接最终字符串
        return f"{compare_prefix}{tag}{abs(pct_change):.1f}% | 均价≈{op_cost_str} (较建仓{vs_first}, 现价较其{vs_curr})"

    def process_group(self, group_df, latest_prices, hist_costs, hist_dates):
        results = []
        group_df = group_df.sort_values('end_date', ascending=True)
        prev_row = None
        total = len(group_df)
        
        for idx, row in enumerate(group_df.to_dict('records')):
            ts_code = row['ts_code']
            holder = row['holder_name']
            
            key = (ts_code, holder)
            h_cost = hist_costs.get(key, 0)
            f_date = hist_dates.get(key, None)
            
            est_cost = 0.0
            cost_method = "未知"
            
            if h_cost > 0:
                est_cost = float(h_cost)
                cost_method = "⏳ 历史回溯"
            else:
                vwap = self.get_quarter_vwap(ts_code, row['end_date'])
                if vwap > 0:
                    est_cost = vwap * COST_DISCOUNT
                    cost_method = "⚡️ 近期估算"
            
            curr_price = latest_prices.get(ts_code, 0)
            profit_rate = (curr_price - est_cost) / est_cost if (est_cost > 0 and curr_price > 0) else 0.0
            
            status = "未知"
            if est_cost > 0 and curr_price > 0:
                if profit_rate < -0.1: status = "Deep Lock (深套)"
                elif -0.1 <= profit_rate <= 0: status = "Trapped (被套)"
                elif 0 < profit_rate <= 0.2: status = "Profit (盈利)"
                else: status = "High Profit (高利)"

            analysis = self.generate_change_analysis(row, prev_row, h_cost, curr_price, f_date)
            
            results.append({
                "ts_code": ts_code, "name": row.get('name', ''), "holder_name": holder,
                "period_end": row['end_date'], "hold_amount": row['hold_amount'],
                "est_cost": round(est_cost, 2), "curr_price": curr_price,
                "profit_rate": round(profit_rate, 4), "status": status,
                "cost_source": cost_method, "first_buy_date": f_date,
                "change_analysis": analysis, "is_latest": (idx == total - 1),
                "update_time": datetime.datetime.now()
            })
            prev_row = row
        return results

    def analyze_positions(self):
        print(">>> 🕵️‍♂️ 开始分析 (支持多账户独立成本)...")
        latest_prices = self.get_all_latest_prices()
        hist_costs, hist_dates = self.get_history_info()
        
        df_all = pd.read_sql("SELECT s.*, b.name FROM nt_shareholders s LEFT JOIN stock_basic b ON s.ts_code = b.ts_code WHERE s.ann_date > '2022-01-01' ORDER BY s.ts_code, s.holder_name, s.end_date", self.engine)
        
        final_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.process_group, group, latest_prices, hist_costs, hist_dates) for _, group in df_all.groupby(['ts_code', 'holder_name'])]
            for future in tqdm(as_completed(futures), total=len(futures)):
                final_results.extend(future.result())

        if final_results:
            df_res = pd.DataFrame(final_results)
            df_res.to_sql('nt_positions_analysis', self.engine, if_exists='replace', index=False)
            print("🚀 分析完成，数据已入库！")

if __name__ == "__main__":
    NationalTeamAnalyzer().analyze_positions()
