# -*- coding: utf-8 -*-
"""
后台批处理任务：国家队持仓考古 v6.0 (双模式版)
功能升级：
1. [双模式] 支持 'incremental' (默认) 和 'full' (全量强制重算) 两种模式。
2. [交易所适配] 完美支持 9/8/4 开头北交所代码。
3. [防封锁] 维持高强度伪装和随机延迟。
"""

import requests
import pandas as pd
import datetime
import os
import time
import random
import sys
from sqlalchemy import create_engine, text
from tqdm import tqdm
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================= 配置引用 =================
from config import DB_URL, SSF_KEYWORDS, PUSHPLUS_TOKEN

LOG_DIR = "storage"
if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
logging.basicConfig(filename=os.path.join(LOG_DIR, 'history_trace.log'), level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_pushplus(title, content):
    """发送 PushPlus 通知"""
    if not PUSHPLUS_TOKEN or "YOUR_" in PUSHPLUS_TOKEN: return
    url = "http://www.pushplus.plus/send"
    data = {"token": PUSHPLUS_TOKEN, "title": f"【NT_Trace】{title}", "content": content }
    try: requests.post(url, json=data, timeout=3)
    except: pass

class HistoryTracer:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://data.eastmoney.com/"
        })

    def get_pending_tasks(self, mode='incremental'):
        """
        根据模式获取任务清单
        """
        if mode == 'full':
            print("🔥 [全量模式] 正在拉取数据库中所有国家队持仓名单...")
            # 全量：只要在 shareholders 表里出现过的组合，全部重算
            sql = """
            SELECT DISTINCT ts_code, holder_name 
            FROM nt_shareholders
            """
        else:
            print("🔍 [增量模式] 正在寻找缺失成本档案的目标...")
            # 增量：只找没有成本档案的
            sql = """
            SELECT DISTINCT s.ts_code, s.holder_name 
            FROM nt_shareholders s
            LEFT JOIN nt_history_cost h ON s.ts_code = h.ts_code AND s.holder_name = h.holder_name
            WHERE h.hist_cost IS NULL OR h.hist_cost = 0
            """
        
        df = pd.read_sql(sql, self.engine)
        return df

    def get_secid(self, code):
        return f"1.{code}" if str(code).startswith('6') else f"0.{code}"

    # 🟢 交易所适配逻辑 (保持最新)
    def get_secucode(self, code):
        c = str(code)
        if "." in c: return c # 防止重复添加后缀
        if c.startswith('6'): return f"{c}.SH"
        elif c.startswith('8') or c.startswith('4') or c.startswith('9'): return f"{c}.BJ"
        else: return f"{c}.SZ"

    def get_market_data_from_db(self, ts_code):
        """从数据库一次性拉取该股票的所有历史行情"""
        try:
            sql = text("SELECT trade_date, amount, vol FROM nt_market_data WHERE ts_code = :code ORDER BY trade_date")
            df = pd.read_sql(sql, self.engine, params={"code": ts_code})
            if not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                # print(f"📊 [Debug] {ts_code} 加载了 {len(df)} 条日线数据")
            else:
                logging.warning(f"⚠️ [警告] {ts_code} 数据库中没有日线数据")
            return df
        except Exception as e:
            logging.error(f"DB Read Error {ts_code}: {e}")
            return pd.DataFrame()

    def get_history_holders(self, secucode):
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        dfs = []
        for rpt_type in ["RPT_F10_EH_HOLDERS", "RPT_F10_EH_FREEHOLDERS"]:
            params = {
                "type": rpt_type, "sty": "END_DATE,HOLDER_NAME,HOLD_NUM", 
                "filter": f'(SECUCODE="{secucode}")', "p": "1", "ps": "5000", 
                "st": "END_DATE", "sr": "1", "source": "SELECT_SECU_DATA", "client": "WEB"
            }
            try:
                time.sleep(random.uniform(0.1, 0.3))
                res = self.session.get(url, params=params, timeout=10)
                data = res.json()
                if data['result'] and data['result']['data']:
                    dfs.append(pd.DataFrame(data['result']['data']))
            except: pass
        if dfs: return pd.concat(dfs).drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
        return pd.DataFrame()

    def calculate_single_holder(self, holder_df, market_df):
        if market_df.empty:
            logging.warning(f"⚠️ [警告] {holder_df['HOLDER_NAME'].iloc[0]} 缺少日线数据，无法计算成本")
            return 0, None, 0, 0

        holder_df = holder_df.sort_values('END_DATE', ascending=True)
        total_shares = 0; total_cost_amt = 0.0
        first_buy_date = None; last_hold_date = None; last_shares = 0
        
        # 确保日期类型一致
        market_df['trade_date'] = pd.to_datetime(market_df['trade_date'])
        
        for _, row in holder_df.iterrows():
            date = pd.to_datetime(row['END_DATE'])
            curr_shares = row['HOLD_NUM']
            
            if last_hold_date and (date - last_hold_date).days > 180:
                first_buy_date = None; total_shares = 0; total_cost_amt = 0.0; last_shares = 0
            
            last_hold_date = date
            diff = curr_shares - last_shares
            
            if diff > 0: 
                if first_buy_date is None: first_buy_date = date
                # 内存计算 VWAP
                if not market_df.empty:
                    q_end = date
                    q_start = date - datetime.timedelta(days=90)
                    # 筛选时间段内的行情
                    mask = (market_df['trade_date'] >= q_start) & (market_df['trade_date'] <= q_end)
                    sub_df = market_df[mask]
                    
                    if not sub_df.empty:
                        sum_amt = sub_df['amount'].sum()
                        sum_vol = sub_df['vol'].sum()
                        vwap = sum_amt / (sum_vol * 100) if sum_vol > 0 else 0
                    else:
                        # print(f"⚠️ [Debug] {date} 前90天无行情数据 (Range: {q_start} - {q_end})")
                        vwap = 0
                else:
                    vwap = 0

                total_shares += diff
                total_cost_amt += diff * vwap
            elif diff < 0 and total_shares > 0: 
                total_cost_amt += diff * (total_cost_amt / total_shares)
                total_shares += diff
            last_shares = curr_shares
            
        final_cost = total_cost_amt / total_shares if total_shares > 0 else 0
        return final_cost, first_buy_date, total_shares, total_cost_amt

    def normalize_name(self, name):
        """标准化名称：全角转半角，去除空格"""
        if not name: return ""
        name = name.replace("（", "(").replace("）", ")")
        return name.replace(" ", "").strip()

    def is_match(self, api_name, target_name):
        """模糊匹配逻辑"""
        n_api = self.normalize_name(api_name)
        n_target = self.normalize_name(target_name)
        
        if n_api == n_target: return True
        # 包含关系匹配 (比如 '中央汇金' vs '中央汇金资产管理有限责任公司')
        if n_target in n_api or n_api in n_target: return True
        return False

    def run(self, mode='incremental'):
        # 1. 获取任务清单
        pending_df = self.get_pending_tasks(mode)
        
        if pending_df.empty:
            print("✅ 没有任务需要处理。")
            return

        target_stocks = pending_df['ts_code'].unique().tolist()
        print(f"🚀 [任务启动] 共 {len(pending_df)} 条持仓记录待处理，涉及 {len(target_stocks)} 只股票。")
        
        count = 0
        for code in tqdm(target_stocks, desc=f"Trace ({mode})"):
            try:
                secucode = self.get_secucode(code)
                
                # 1. 先从数据库加载该股票的所有行情
                market_df = self.get_market_data_from_db(code)
                if market_df.empty:
                    print(f"⚠️ SKIP {code}: No market data in DB")
                
                df_all = self.get_history_holders(secucode)
                if df_all.empty:
                    print(f"⚠️ SKIP {code}: No holder data from API (secucode={secucode})")
                
                if not df_all.empty:
                    # 获取该股票需要计算的目标股东列表
                    target_holders = pending_df[pending_df['ts_code'] == code]['holder_name'].unique().tolist()
                    
                    # --- 模糊匹配逻辑 ---
                    # 找出 df_all 中哪些行是我们需要的目标股东
                    matched_rows = []
                    for _, row in df_all.iterrows():
                        api_holder = row['HOLDER_NAME']
                        for target in target_holders:
                            if self.is_match(api_holder, target):
                                # 为了后续 groupby 正确，这里统一把 HOLDER_NAME 改为数据库里的标准名称
                                row['HOLDER_NAME'] = target 
                                matched_rows.append(row)
                                break
                    
                    if not matched_rows:
                        print(f"⚠️ SKIP {code}: No matching holders found. Targets: {target_holders[:3]}... API Sample: {df_all['HOLDER_NAME'].iloc[:3].tolist()}")

                    if matched_rows:
                        nt_df = pd.DataFrame(matched_rows)
                        
                        for holder_name, group in nt_df.groupby('HOLDER_NAME'):
                            # 2. 传入 market_df 进行内存计算
                            cost, f_date, t_shares, t_invest = self.calculate_single_holder(group, market_df)
                            
                            if f_date:
                                # 🟢 [修复] 转换 numpy 类型为 python 原生类型，防止 psycopg2 报错
                                cost = float(cost)
                                t_invest = float(t_invest)
                                t_shares = int(t_shares)

                                sql = text("""
                                    INSERT INTO nt_history_cost (ts_code, holder_name, hist_cost, total_invest, total_shares, first_buy_date, calc_date)
                                    VALUES (:c, :h, :cost, :inv, :sh, :fd, NOW())
                                    ON CONFLICT (ts_code, holder_name) DO UPDATE 
                                    SET hist_cost = EXCLUDED.hist_cost,
                                        first_buy_date = EXCLUDED.first_buy_date,
                                        calc_date = NOW();
                                """)
                                with self.engine.connect() as conn:
                                    conn.execute(sql, {"c": code, "h": holder_name, "cost": cost, "inv": t_invest, "sh": t_shares, "fd": f_date})
                                    conn.commit()
                                count += 1
                            else:
                                print(f"⚠️ SKIP {code} - {holder_name}: Calc failed (f_date is None). Shares: {t_shares}")
            except Exception as e:
                err_msg = str(e)
                logging.error(f"Error {code}: {err_msg}")
                # 🚨 严重错误报警并停止
                if "RemoteDisconnected" in err_msg or "Connection aborted" in err_msg:
                    send_pushplus("考古任务连接中断", f"检测到底层连接被断开。\nCode: {code}\n详情: {err_msg}")
                    print("🛑 检测到严重连接错误，正在终止程序...")
                    sys.exit(1)
                
        print(f"✅ 考古完成！成功处理 {count} 条档案。")

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    try:
        # 简单的参数解析
        mode = 'incremental'
        if len(sys.argv) > 1 and sys.argv[1] == 'full':
            mode = 'full'
        
        HistoryTracer().run(mode=mode)
        
        duration = datetime.datetime.now() - start_time
        send_pushplus("考古任务完成", f"历史持仓考古任务已成功执行完毕。\n模式: {mode}\n耗时: {duration}")

    except Exception as e:
        import traceback
        err_msg = traceback.format_exc()
        logging.error(err_msg)
        print(err_msg)
        send_pushplus("考古任务崩溃", f"脚本发生严重错误，已停止。\n\n{str(e)}")
        sys.exit(1)
