# -*- coding: utf-8 -*-
"""
单兵修复工具：针对特定股票重新计算历史建仓 (含断档重置逻辑)
"""
import requests
import pandas as pd
import datetime
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================= 配置 =================
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
# 🟢 修正后的关键词 (剔除"投资有限责任公司")
SSF_KEYWORDS = ["中央汇金", "中国证券金融", "全国社保", "基本养老", "社保基金", "汇金资管"]
# 🟢 指定要修复的股票代码
TARGET_CODE = "000957" 

class SingleStockFixer:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def get_secid(self, code):
        if str(code).startswith('6'): return f"1.{code}"
        return f"0.{code}"

    def get_kline_vwap_api(self, secid, start_date, end_date):
        """获取区间 VWAP"""
        s_str = start_date.replace("-", "")
        e_str = end_date.replace("-", "")
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": secid, "klt": "101", "fqt": "1", "lmt": "1000",
            "beg": s_str, "end": e_str,
            "fields1": "f1", "fields2": "f51,f56,f57" 
        }
        try:
            res = self.session.get(url, params=params, timeout=5)
            data = res.json()
            if not data or not data.get('data') or not data['data'].get('klines'):
                return 0
            
            klines = data['data']['klines']
            total_amt = 0.0
            total_vol = 0.0
            for k in klines:
                parts = k.split(',')
                if len(parts) >= 3:
                    total_vol += float(parts[1]) 
                    total_amt += float(parts[2]) 
            
            if total_vol > 0:
                return total_amt / (total_vol * 100)
        except: pass
        return 0

    def get_history_holders(self, secucode):
        """同时查两张表 + 扩大分页"""
        print(f"📡 正在拉取 {secucode} 的全量历史股东数据...")
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        dfs = []
        for rpt_type in ["RPT_F10_EH_HOLDERS", "RPT_F10_EH_FREEHOLDERS"]:
            params = {
                "type": rpt_type,
                "sty": "END_DATE,HOLDER_NAME,HOLD_NUM",
                "filter": f'(SECUCODE="{secucode}")',
                "p": "1", "ps": "5000", "st": "END_DATE", "sr": "1", # 🟢 ps=5000
                "source": "SELECT_SECU_DATA", "client": "WEB",
            }
            try:
                res = self.session.get(url, params=params, timeout=10)
                data = res.json()
                if data['result'] and data['result']['data']:
                    dfs.append(pd.DataFrame(data['result']['data']))
            except: pass
        
        if dfs:
            return pd.concat(dfs).drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
        return pd.DataFrame()

    def run(self):
        print(f"🚀 开始修复: {TARGET_CODE}")
        secucode = f"{TARGET_CODE}.SH" if TARGET_CODE.startswith('6') else f"{TARGET_CODE}.SZ"
        
        # 1. 获取数据
        df_holders = self.get_history_holders(secucode)
        if df_holders.empty:
            print("❌ 未获取到任何股东数据")
            return

        # 2. 过滤
        mask = df_holders['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
        nt_df = df_holders[mask].copy()
        if nt_df.empty:
            print("❌ 过滤后无国家队记录 (关键词优化生效)")
            # 如果以前有脏数据，这里应该删掉库里的记录
            return

        # 3. 计算 (含断档逻辑)
        group_df = nt_df.groupby('END_DATE')['HOLD_NUM'].sum().reset_index()
        group_df['END_DATE'] = pd.to_datetime(group_df['END_DATE'])
        group_df = group_df.sort_values('END_DATE', ascending=True)

        total_shares = 0
        total_cost_amt = 0.0
        first_buy_date = None
        last_hold_date = None 
        last_shares = 0
        
        secid = self.get_secid(TARGET_CODE)

        print("🧮 正在回溯持仓逻辑...")
        for _, row in group_df.iterrows():
            date = row['END_DATE']
            curr_shares = row['HOLD_NUM']
            
            # 🟢 断档检测
            if last_hold_date:
                days_diff = (date - last_hold_date).days
                if days_diff > 180: 
                    print(f"✂️ 发现断档: {last_hold_date.date()} -> {date.date()} (间隔 {days_diff} 天)，重置！")
                    first_buy_date = None
                    total_shares = 0
                    total_cost_amt = 0.0
                    last_shares = 0
            
            last_hold_date = date
            diff = curr_shares - last_shares
            
            if diff > 0: # 加仓
                if first_buy_date is None: 
                    first_buy_date = date
                    print(f"🏁 设定新起点: {date.date()}")
                
                # 为了速度，这里简单计算 VWAP
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

        # 4. 入库
        if first_buy_date:
            final_cost = total_cost_amt / total_shares if total_shares > 0 else 0
            print(f"✅ 最终结果: 建仓日={first_buy_date.date()}, 成本={final_cost:.4f}")
            
            sql = text("""
                INSERT INTO nt_history_cost (ts_code, hist_cost, total_invest, total_shares, first_buy_date, calc_date)
                VALUES (:ts_code, :hist_cost, :total_invest, :total_shares, :first_buy_date, NOW())
                ON CONFLICT (ts_code) DO UPDATE 
                SET hist_cost = EXCLUDED.hist_cost,
                    total_invest = EXCLUDED.total_invest,
                    total_shares = EXCLUDED.total_shares,
                    first_buy_date = EXCLUDED.first_buy_date,
                    calc_date = NOW();
            """)
            with self.engine.connect() as conn:
                conn.execute(sql, {
                    "ts_code": TARGET_CODE,
                    "hist_cost": final_cost,
                    "total_invest": total_cost_amt,
                    "total_shares": total_shares,
                    "first_buy_date": first_buy_date
                })
                conn.commit()
            print("💾 已写入数据库！")
        else:
            print("⚠️ 计算结果为空，未入库。")

if __name__ == "__main__":
    SingleStockFixer().run()
