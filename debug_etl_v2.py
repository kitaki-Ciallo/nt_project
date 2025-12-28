# -*- coding: utf-8 -*-
import akshare as ak
import pandas as pd
import time

# 选一只数据比较全的股票进行测试，比如 思源电气 (从你截图里选的)
TEST_CODE = "002028" 

print(f"🚀 开始调试数据抓取，目标股票: {TEST_CODE} ...\n")

# ==========================================
# 测试 1: 个股深度信息 (用于抓 PE全家桶, PB, 股息)
# ==========================================
print("--- [测试 1] ak.stock_individual_info_em (个股信息) ---")
try:
    df_info = ak.stock_individual_info_em(symbol=TEST_CODE)
    
    # 打印原始数据的前20行，方便看 Key 到底叫什么
    print("📋 接口返回的原始数据 (前20行):")
    print(df_info.head(20))
    print("-" * 30)
    
    # 尝试提取我们需要的目标字段
    data_map = dict(zip(df_info['item'], df_info['value']))
    
    target_fields = [
        "市盈率(动)", "市盈率(静)", "市盈率(TTM)", # PE 三兄弟
        "市净率", 
        "股息率", "股息率(TTM)", # 看看哪个有值
        "总市值"
    ]
    
    print("🔍 尝试提取目标字段:")
    for field in target_fields:
        val = data_map.get(field)
        print(f"   {field}: {val}  (类型: {type(val)})")

except Exception as e:
    print(f"❌ 测试 1 失败: {e}")

print("\n" + "="*40 + "\n")

# ==========================================
# 测试 2: 财务指标 (用于抓 ROE, EPS, 增长率, 净利率)
# ==========================================
print("--- [测试 2] ak.stock_financial_analysis_indicator (财务指标) ---")
try:
    df_fin = ak.stock_financial_analysis_indicator(symbol=TEST_CODE)
    
    if not df_fin.empty:
        # 取最近的一期报告 (通常是第一行)
        latest_report = df_fin.iloc[0]
        
        print(f"📋 最近一期财报日期: {latest_report['日期']}")
        
        # 打印所有列名，方便我们找 key
        print("🔑 所有可用字段名 (Columns):")
        print(latest_report.index.tolist())
        print("-" * 30)
        
        # 尝试提取我们需要的目标字段
        # 注意：这里的 key 可能会经常变，所以我们要打印出来确认
        potential_keys = {
            "ROE (净资产收益率)": ["净资产收益率(%)", "加权净资产收益率(%)", "摊薄净资产收益率(%)"],
            "EPS (每股收益)": ["每股收益(元)", "摊薄每股收益(元)", "基本每股收益(元)"],
            "利润增长 (同比)": ["净利润同比增长率(%)", "归属净利润同比增长率(%)"],
            "净利率 (利润率)": ["销售净利率(%)", "净利率(%)"]
        }
        
        print("🔍 尝试提取目标字段:")
        for label, keys in potential_keys.items():
            found = False
            for k in keys:
                if k in latest_report:
                    print(f"   ✅ 找到 {label}: Key='{k}', Value={latest_report[k]}")
                    found = True
                    break
            if not found:
                print(f"   ❌ 未找到 {label} (尝试过的Key: {keys})")
                
    else:
        print("❌ 接口返回数据为空")

except Exception as e:
    print(f"❌ 测试 2 失败: {e}")
