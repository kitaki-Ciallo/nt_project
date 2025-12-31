# -*- coding: utf-8 -*-
"""
Debug 专用脚本：000957 深度追踪 (v4.0 逻辑复刻)
功能：
1. 抓取 API 股东数据 (含流通股东)
2. 模拟分账户成本计算
3. 检查 VWAP 获取是否正常
4. 尝试写入数据库并验证
5. 日志输出到 storage/debug_000957.log
"""

import requests
import pandas as pd
import datetime
import os
import logging
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================= 配置 =================
DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
SSF_KEYWORDS = ["中央汇金", "中国证券金融", "全国社保", "基本养老", "社保基金", "汇金资管"]
TARGET_CODE = "000957"

# 配置日志 (按您建议，简化日志，只记录关键状态)
LOG_DIR = "storage"
if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
LOG_FILE = os.path.join(LOG_DIR, "debug_000957.log")

# 同时输出到控制台和文件
logger = logging.getLogger("DebugLogger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 文件处理器
fh = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
fh.setFormatter(formatter)
logger.addHandler(fh)

# 控制台处理器
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

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
                klines_count = len(data['data']['klines'])
                for k in data['data']['klines']:
                    parts = k.split(',')
                    if len(parts) >= 3:
                        total_vol += float(parts[1])
                        total_amt += float(parts[2])
                
                vwap = total_amt / (total_vol * 100) if total_vol > 0 else 0
                logger.info(f"      -> 💹 API获取VWAP成功: {start_date}~{end_date} (K线{klines_count}根) = {vwap:.2f}")
                return vwap
            else:
                logger.warning(f"      -> ⚠️ API返回空数据: {start_date}~{end_date}")
        except Exception as e:
            logger.error(f"      -> ❌ API请求异常: {e}")
        return 0

    def get_history_holders(self, secucode):
        logger.info(f"📡 [1] 拉取历史股东数据: {secucode} ...")
        url = "https://datacenter.eastmoney.com/securities/api/data/get"
        dfs = []
        for rpt_type in ["RPT_F10_EH_HOLDERS", "RPT_F10_EH_FREEHOLDERS"]:
            params = {"type": rpt_type, "sty": "END_DATE,HOLDER_NAME,HOLD_NUM", "filter": f'(SECUCODE="{secucode}")', "p": "1", "ps": "5000", "st": "END_DATE", "sr": "1", "source": "SELECT_SECU_DATA", "client": "WEB"}
            try:
                res = self.session.get(url, params=params, timeout=10)
                data = res.json()
                if data['result'] and data['result']['data']:
                    df = pd.DataFrame(data['result']['data'])
                    logger.info(f"    - {rpt_type}: 获取到 {len(df)} 条")
                    dfs.append(df)
            except Exception as e:
                logger.error(f"    - {rpt_type} 失败: {e}")
        
        if dfs:
            full = pd.concat(dfs).drop_duplicates(subset=['END_DATE', 'HOLDER_NAME'])
            logger.info(f"    ✅ 合计去重后: {len(full)} 条")
            return full
        logger.error("    ❌ 未获取到任何数据")
        return pd.DataFrame()

    def calculate_single_holder(self, holder_name, holder_df, secid):
        logger.info(f"\n🔍 [2] 分析股东: 【{holder_name}】 ({len(holder_df)}条记录)")
        holder_df = holder_df.sort_values('END_DATE', ascending=True)
        
        total_shares = 0
        total_cost_amt = 0.0
        first_buy_date = None
        last_hold_date = None 
        last_shares = 0
        
        for _, row in holder_df.iterrows():
            date = pd.to_datetime(row['END_DATE'])
            curr_shares = float(row['HOLD_NUM'])
            
            # 断档检测
            if last_hold_date:
                days_diff = (date - last_hold_date).days
                if days_diff > 180:
                    logger.info(f"    ✂️ 发现断档 ({days_diff}天)，重置计算状态！")
                    first_buy_date = None; total_shares = 0; total_cost_amt = 0.0; last_shares = 0
            
            last_hold_date = date
            diff = curr_shares - last_shares
            
            if diff > 0: # 加仓
                if first_buy_date is None: 
                    first_buy_date = date
                    logger.info(f"    🚩 设定建仓日: {date.date()}")
                
                q_end = date.strftime("%Y-%m-%d")
                q_start = (date - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
                
                # 获取 VWAP
                vwap = self.get_kline_vwap_api(secid, q_start, q_end)
                if vwap == 0:
                    logger.warning(f"    ⚠️ 警告: {date.date()} 加仓时 VWAP 为 0，可能导致总成本偏低！")
                
                total_shares += diff
                total_cost_amt += diff * vwap
                
            elif diff < 0: # 减仓
                if total_shares > 0:
                    avg_cost = total_cost_amt / total_shares
                    total_cost_amt += diff * avg_cost 
                    total_shares += diff
            
            last_shares = curr_shares

        final_cost = total_cost_amt / total_shares if total_shares > 0 else 0
        logger.info(f"    ✅ 计算结果: 建仓日={first_buy_date.date() if first_buy_date else 'None'}, 成本={final_cost:.4f}")
        return final_cost, first_buy_date, total_shares, total_cost_amt

    def run(self):
        logger.info(f"🚀 开始诊断股票: {TARGET_CODE}")
        secucode = f"{TARGET_CODE}.SH" if TARGET_CODE.startswith('6') else f"{TARGET_CODE}.SZ"
        secid = self.get_secid(TARGET_CODE)
        
        df_all = self.get_history_holders(secucode)
        if df_all.empty: return

        # 过滤关键词
        mask = df_all['HOLDER_NAME'].apply(lambda x: any(k in str(x) for k in SSF_KEYWORDS))
        nt_df = df_all[mask].copy()
        
        if nt_df.empty:
            logger.warning("❌ 关键词过滤后无数据！请检查 SSF_KEYWORDS 是否包含目标机构。")
            logger.info(f"    当前关键词: {SSF_KEYWORDS}")
            logger.info(f"    API返回的部分股东名: {df_all['HOLDER_NAME'].head(5).tolist()}")
            return

        logger.info(f"✅ 过滤后发现 {len(nt_df['HOLDER_NAME'].unique())} 个国家队账户")

        # 分账户计算
        success_count = 0
        for holder_name, group in nt_df.groupby('HOLDER_NAME'):
            cost, f_date, t_shares, t_invest = self.calculate_single_holder(holder_name, group, secid)
            
            if f_date:
                # 尝试写入数据库
                try:
                    sql = text("""
                        INSERT INTO nt_history_cost (ts_code, holder_name, hist_cost, total_invest, total_shares, first_buy_date, calc_date)
                        VALUES (:ts_code, :holder, :cost, :inv, :sh, :fdate, NOW())
                        ON CONFLICT (ts_code, holder_name) DO UPDATE 
                        SET hist_cost = EXCLUDED.hist_cost,
                            total_invest = EXCLUDED.total_invest,
                            total_shares = EXCLUDED.total_shares,
                            first_buy_date = EXCLUDED.first_buy_date,
                            calc_date = NOW();
                    """)
                    with self.engine.connect() as conn:
                        conn.execute(sql, {"ts_code": TARGET_CODE, "holder": holder_name, "cost": cost, "inv": t_invest, "sh": t_shares, "fdate": f_date})
                        conn.commit()
                    logger.info(f"    💾 [3] 数据库写入成功: {holder_name}")
                    success_count += 1
                except Exception as e:
                    logger.error(f"    ❌ 数据库写入失败: {e}")
            else:
                logger.warning(f"    ⚠️ 未找到有效建仓日，跳过入库: {holder_name}")

        logger.info(f"🎉 诊断完成，共更新 {success_count} 个账户。请查看 {LOG_FILE} 获取详情。")

if __name__ == "__main__":
    DebugTracer().run()
