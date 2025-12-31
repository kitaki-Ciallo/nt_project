# -*- coding: utf-8 -*-
"""
多目标深度诊断工具 (v5.0)
功能：批量诊断指定股票的“近期估算”问题，定位是API缺失还是逻辑计算错误。
"""
import requests
import pandas as pd
import datetime
import os
import logging
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================= 配置区域 =================
# 🟢在此处填入有问题的股票代码
TARGET_CODES = ["000089", "000061"]

DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
SSF_KEYWORDS = ["中央汇金", "中国证券金融", "全国社保", "基本养老", "社保基金", "汇金资管"]

# 日志配置
LOG_DIR = "storage"
if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
LOG_FILE = os.path.join(LOG_DIR, "debug_trace.log")

# 简化的日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DebugMulti")

class DebugTracer:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def get_secid(self, code):
        return f"1.{code}" if str(code).startswith('6') else f"0.{code}"

    def get_kline_vwap_api(self, secid, start_date, end_date):
        """模拟获取 VWAP"""
        s_str = start_date.replace("-", "")
        e_str = end_date.replace("-", "")
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {"secid": secid, "klt": "101", "fqt": "1", "lmt": "1000", "beg": s_str, "end": e_str, "fields1": "f1", "fields2": "f51,f56,f57"}
        try:
            res = self.session.get(url, params=params, timeout=5)
            data = res.json()
            if data and data.get('data') and data['data'].get('klines'):
                total_amt = 0.0
                total_vol = 0.0
                for k in data['data']['klines']:
                    parts = k.split(',')
                    if len(parts) >= 3:
                        total_vol += float(parts[1])
                        total_amt += float(parts[2])
                
                if total_vol > 0:
                    vwap = total_amt / (total_vol * 100)
                    return vwap
                else:
                    return 0
        except Exception as e:
            logger.error(f"      ❌ API请求VWAP异常: {e}")
        return 0

    def get_history_holders(self, secucode):
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        dfs = []
        for rpt_type in ["RPT_F10_EH_HOLDERS", "RPT_F10_EH_FREEHOLDERS"]:
            params = {"type": rpt_type, "sty": "END_DATE,HOLDER_NAME,HOLD_NUM", "filter": f'(SECUCODE="{secucode}")', "p": "1", "ps": "5000", "st": "END_DATE", "sr": "1", "source": "SELECT_SECU_DATA", "client": "WEB"}
            try:
                res = self.session.get(url, params=params, timeout=10)
                data = res.json()
                if data['result'] and data['result']['data']:
                    dfs.append(pd.DataFrame(data['result']['data']))
            except: pass
        
        if dfs:
            return pd.concat(dfs).drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
        return pd.DataFrame()

    def calculate_single_holder(self, holder_name, holder_df, secid):
        holder_df = holder_df.sort_values('END_DATE', ascending=True)
        
        total_shares = 0
        total_cost_amt = 0.0
        first_buy_date = None
        last_hold_date = None 
        last_shares = 0
        
        history_log = [] # 记录轨迹用于分析
        
        for _, row in holder_df.iterrows():
            date = pd.to_datetime(row['END_DATE'])
            curr_shares = float(row['HOLD_NUM'])
            
            # 断档检测
            is_break = False
            if last_hold_date:
                days_diff = (date - last_hold_date).days
                if days_diff > 180:
                    is_break = True
                    first_buy_date = None; total_shares = 0; total_cost_amt = 0.0; last_shares = 0
            
            last_hold_date = date
            diff = curr_shares - last_shares
            
            action = "不变"
            vwap = 0
            
            if diff > 0: 
                action = "加仓"
                if first_buy_date is None: first_buy_date = date
                q_end = date.strftime("%Y-%m-%d")
                q_start = (date - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
                vwap = self.get_kline_vwap_api(secid, q_start, q_end)
                total_shares += diff
                total_cost_amt += diff * vwap
            elif diff < 0:
                action = "减仓"
                if total_shares > 0:
                    avg_cost = total_cost_amt / total_shares
                    total_cost_amt += diff * avg_cost 
                    total_shares += diff
            
            last_shares = curr_shares
            history_log.append(f"{date.date()} {action} {diff}股 (断档:{is_break}, VWAP:{vwap:.2f})")

        final_cost = total_cost_amt / total_shares if total_shares > 0 else 0
        
        # 🟢 诊断输出
        logger.info(f"   👤 股东: {holder_name}")
        logger.info(f"      - 历史轨迹: {len(history_log)} 条记录")
        # 只打印第一条和最后一条，避免刷屏，除非有问题
        if history_log:
            logger.info(f"      - 起始: {history_log[0]}")
            if len(history_log) > 1: logger.info(f"      - 最新: {history_log[-1]}")
        
        if final_cost == 0:
            logger.warning(f"      ⚠️ 计算出的成本为 0！(可能原因: 无法获取 VWAP 或 只有减仓记录)")
        else:
            logger.info(f"      ✅ 计算正常: 建仓={first_buy_date.date() if first_buy_date else 'None'}, 成本={final_cost:.4f}")

        return final_cost, first_buy_date, total_shares, total_cost_amt

    def run(self):
        logger.info(f"🚀 开始多目标诊断: {TARGET_CODES}")
        
        for code in TARGET_CODES:
            logger.info(f"\n🔎 正在分析股票: {code}")
            secucode = f"{code}.SH" if code.startswith('6') else f"{code}.SZ"
            secid = self.get_secid(code)
            
            # 1. 抓取
            df_all = self.get_history_holders(secucode)
            if df_all.empty:
                logger.error(f"   ❌ 无法获取 {code} 的股东数据！")
                continue

            # 2. 过滤
            mask = df_all['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
            nt_df = df_all[mask].copy()
            if nt_df.empty:
                logger.warning(f"   ⚠️ 关键词过滤后为空。API返回的股东示例: {df_all['HOLDER_NAME'].head(3).tolist()}")
                continue
                
            # 3. 计算 & 修复
            for holder_name, group in nt_df.groupby('HOLDER_NAME'):
                cost, f_date, t_shares, t_invest = self.calculate_single_holder(holder_name, group, secid)
                
                if f_date and cost > 0:
                    try:
                        sql = text("""
                            INSERT INTO nt_history_cost (ts_code, holder_name, hist_cost, total_invest, total_shares, first_buy_date, calc_date)
                            VALUES (:c, :h, :cost, :inv, :sh, :fd, NOW())
                            ON CONFLICT (ts_code, holder_name) DO UPDATE 
                            SET hist_cost = EXCLUDED.hist_cost, first_buy_date = EXCLUDED.first_buy_date, calc_date = NOW();
                        """)
                        with self.engine.connect() as conn:
                            conn.execute(sql, {"c": code, "h": holder_name, "cost": cost, "inv": t_invest, "sh": t_shares, "fd": f_date})
                            conn.commit()
                        logger.info(f"      💾 已写入数据库")
                    except Exception as e:
                        logger.error(f"      ❌ 数据库写入失败: {e}")
                else:
                    logger.warning(f"      ⚠️ 跳过入库 (无效数据)")

        logger.info("\n✅ 诊断结束。")

if __name__ == "__main__":
    DebugTracer().run()
