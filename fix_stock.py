# -*- coding: utf-8 -*-
"""
自动化巡检修复机器人 (Auto-Fixer Robot) v1.1
修复日志：
- [Fix] 修复了 name 'time' is not defined 的报错 (补充 import time)。
"""
import requests
import pandas as pd
import datetime
import logging
import os
import time  # 🟢 补上了这个关键的库
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

# ================= 配置区域 =================
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
# 核心关键词
SSF_KEYWORDS = ["社保", "养老", "证金", "中央汇金", "全国社保", "基本养老", "中国证券金融", "社保基金", "汇金资管"]

# 日志配置
LOG_DIR = "storage"
if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "auto_fix.log"), mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("AutoFixer")

class AutoFixer:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def get_secid(self, code):
        return f"1.{code}" if str(code).startswith('6') else f"0.{code}"

    def detect_problems(self):
        """
        🕵️‍♂️ 侦测阶段：找出所有需要修复的股票
        """
        logger.info("📡 正在全盘扫描异常数据...")
        
        # 1. 查展示表：谁还在用“临时估算”？
        sql_missing = """
        SELECT DISTINCT ts_code 
        FROM nt_positions_analysis 
        WHERE cost_source LIKE '%%近期估算%%'
        """
        
        # 2. 查历史表：谁的成本是 0 (无效档案)？
        sql_zero_cost = """
        SELECT DISTINCT ts_code 
        FROM nt_history_cost 
        WHERE hist_cost = 0
        """
        
        try:
            df_missing = pd.read_sql(sql_missing, self.engine)
            df_zero = pd.read_sql(sql_zero_cost, self.engine)
            
            # 合并去重
            codes = list(set(df_missing['ts_code'].tolist() + df_zero['ts_code'].tolist()))
            codes.sort()
            
            logger.info(f"🧐 发现 {len(df_missing)} 只显示'近期估算'，{len(df_zero)} 只档案无效(0)。")
            logger.info(f"🎯 最终锁定目标: {len(codes)} 只")
            return codes
        except Exception as e:
            logger.error(f"侦测失败: {e}")
            return []

    def get_kline_vwap_api(self, secid, start_date, end_date):
        """获取区间 VWAP"""
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
        """核心计算逻辑"""
        holder_df = holder_df.sort_values('END_DATE', ascending=True)
        total_shares = 0; total_cost_amt = 0.0
        first_buy_date = None; last_hold_date = None; last_shares = 0
        
        for _, row in holder_df.iterrows():
            date = pd.to_datetime(row['END_DATE'])
            curr_shares = float(row['HOLD_NUM'])
            
            # 断档检测 (180天)
            if last_hold_date and (date - last_hold_date).days > 180:
                first_buy_date = None; total_shares = 0; total_cost_amt = 0.0; last_shares = 0
            
            last_hold_date = date
            diff = curr_shares - last_shares
            
            if diff > 0: # 加仓
                if first_buy_date is None: first_buy_date = date
                
                q_end = date.strftime("%Y-%m-%d")
                q_start = (date - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
                vwap = self.get_kline_vwap_api(secid, q_start, q_end)
                
                if vwap == 0: vwap = 0 
                
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
        secucode = f"{ts_code}.SH" if ts_code.startswith('6') else f"{ts_code}.SZ"
        
        # 1. 拉取全量历史
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        dfs = []
        for rpt in ["RPT_F10_EH_HOLDERS", "RPT_F10_EH_FREEHOLDERS"]:
            try:
                # ps=5000 保证拉取所有历史
                res = self.session.get(url, params={"type": rpt, "sty": "END_DATE,HOLDER_NAME,HOLD_NUM", "filter": f'(SECUCODE="{secucode}")', "p":1, "ps":5000, "st":"END_DATE", "sr":1}, timeout=10)
                if res.json()['result']: dfs.append(pd.DataFrame(res.json()['result']['data']))
            except: pass
        
        if not dfs: return 0
        
        df_all = pd.concat(dfs).drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
        mask = df_all['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
        nt_df = df_all[mask].copy()
        
        if nt_df.empty: return 0
        
        fixed_count = 0
        # 分账户计算
        for holder_name, group in nt_df.groupby('HOLDER_NAME'):
            cost, f_date, t_shares, t_invest = self.calculate_single_holder(group, secid)
            
            if f_date and cost > 0:
                try:
                    sql = text("""
                        INSERT INTO nt_history_cost (ts_code, holder_name, hist_cost, total_invest, total_shares, first_buy_date, calc_date)
                        VALUES (:c, :h, :cost, :inv, :sh, :fd, NOW())
                        ON CONFLICT (ts_code, holder_name) DO UPDATE 
                        SET hist_cost = EXCLUDED.hist_cost, 
                            first_buy_date = EXCLUDED.first_buy_date,
                            calc_date = NOW();
                    """)
                    with self.engine.connect() as conn:
                        conn.execute(sql, {"c": ts_code, "h": holder_name, "cost": cost, "inv": t_invest, "sh": t_shares, "fd": f_date})
                        conn.commit()
                    fixed_count += 1
                except Exception as e:
                    logger.error(f"写入失败 {ts_code}: {e}")
                    
        return fixed_count

    def run(self):
        logger.info("🚀 自动修复机器人启动 (v1.1)...")
        
        # 1. 侦测
        targets = self.detect_problems()
        if not targets:
            logger.info("✅ 数据库非常健康！没有发现'近期估算'或无效数据的股票。")
            return
            
        # 2. 修复
        logger.info(f"🔧 准备修复 {len(targets)} 只股票...")
        success_total = 0
        
        with tqdm(total=len(targets)) as pbar:
            for code in targets:
                pbar.set_description(f"Fixing {code}")
                try:
                    time.sleep(0.1)  # 🟢 这里就是之前报错的地方，现在修复了
                    count = self.fix_one_stock(code)
                    if count > 0:
                        success_total += 1
                except Exception as e:
                    logger.error(f"❌ {code} 修复异常: {e}")
                pbar.update(1)
                
        logger.info(f"🎉 任务结束！尝试修复 {len(targets)} 只，成功写入 {success_total} 只。")
        logger.info("👉 请运行 'python analysis_engine.py' 刷新 Dashboard 看效果。")

if __name__ == "__main__":
    AutoFixer().run()
