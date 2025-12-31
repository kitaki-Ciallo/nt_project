# -*- coding: utf-8 -*-
"""
单兵 Debug: 艾罗能源 (688717) 分账户成本验证
"""
import requests
import pandas as pd
import datetime
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 配置
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
SSF_KEYWORDS = ["中央汇金", "中国证券金融", "全国社保", "基本养老", "社保基金", "汇金资管"]
TARGET_CODE = "688717"

class Debugger:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def get_history_holders(self, secucode):
        print(f"📡 拉取 {secucode} 历史持仓...")
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        dfs = []
        for rpt_type in ["RPT_F10_EH_HOLDERS", "RPT_F10_EH_FREEHOLDERS"]:
            params = {
                "type": rpt_type,
                "sty": "END_DATE,HOLDER_NAME,HOLD_NUM",
                "filter": f'(SECUCODE="{secucode}")',
                "p": "1", "ps": "5000", "st": "END_DATE", "sr": "1",
                "source": "SELECT_SECU_DATA", "client": "WEB",
            }
            try:
                res = self.session.get(url, params=params, timeout=5)
                data = res.json()
                if data['result'] and data['result']['data']:
                    dfs.append(pd.DataFrame(data['result']['data']))
            except: pass
        if dfs: return pd.concat(dfs).drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
        return pd.DataFrame()

    def calculate_single_holder(self, holder_name, holder_df):
        """核心逻辑：计算单个账户的建仓信息"""
        print(f"\n🔍 分析账户: 【{holder_name}】")
        holder_df = holder_df.sort_values('END_DATE', ascending=True)
        
        first_buy_date = None
        last_hold_date = None
        
        # 简单打印一下历史轨迹
        for _, row in holder_df.iterrows():
            date_str = row['END_DATE']
            shares = row['HOLD_NUM']
            print(f"   - {date_str}: 持仓 {shares} 股")

        # 回溯逻辑
        for _, row in holder_df.iterrows():
            date = pd.to_datetime(row['END_DATE'])
            
            # 断档检测
            if last_hold_date:
                if (date - last_hold_date).days > 180:
                    print(f"     ✂️ 发现断档，重置建仓日！")
                    first_buy_date = None
            
            last_hold_date = date
            
            if first_buy_date is None:
                first_buy_date = date
        
        print(f"   ✅ 计算出的建仓日: {first_buy_date.date()}")
        return first_buy_date

    def run(self):
        secucode = f"{TARGET_CODE}.SH" if TARGET_CODE.startswith('6') else f"{TARGET_CODE}.SZ"
        df = self.get_history_holders(secucode)
        
        # 1. 过滤社保
        mask = df['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
        nt_df = df[mask].copy()
        
        # 2. 🟢 分账户计算
        results = []
        for holder_name, group in nt_df.groupby('HOLDER_NAME'):
            f_date = self.calculate_single_holder(holder_name, group)
            if f_date:
                results.append({
                    "ts_code": TARGET_CODE,
                    "holder_name": holder_name,
                    "first_buy_date": f_date,
                    # 这里为了Debug演示，成本先填0或模拟值，重点看日期区分
                    "hist_cost": 0, "total_invest": 0, "total_shares": 0 
                })

        # 3. 入库 (写入新结构的表)
        if results:
            print("\n💾 正在写入数据库 (新表结构)...")
            for res in results:
                sql = text("""
                    INSERT INTO nt_history_cost (ts_code, holder_name, hist_cost, total_invest, total_shares, first_buy_date, calc_date)
                    VALUES (:ts_code, :holder_name, :hist_cost, :total_invest, :total_shares, :first_buy_date, NOW())
                    ON CONFLICT (ts_code, holder_name) DO UPDATE 
                    SET first_buy_date = EXCLUDED.first_buy_date,
                        calc_date = NOW();
                """)
                with self.engine.connect() as conn:
                    conn.execute(sql, res)
                    conn.commit()
            print("✅ 写入完成！")

if __name__ == "__main__":
    Debugger().run()
