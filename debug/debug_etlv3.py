# -*- coding: utf-8 -*-
import akshare as ak
import pandas as pd
import time

TEST_CODE = "002028"  # 思源电气

print(f"🚀 开始第三轮调试 (多接口寻源)，目标: {TEST_CODE} ...\n")

# =========================================================
# 方案 A: 全市场实时快照 (stock_zh_a_spot_em)
# 目标: 抓取 PE(动), PB, 总市值, 现价
# =========================================================
print("--- [方案 A] stock_zh_a_spot_em (全市场快照) ---")
try:
    # 这个接口返回全市场几千条数据，我们只取我们要的那一行
    df_spot = ak.stock_zh_a_spot_em()
    row = df_spot[df_spot['代码'] == TEST_CODE]
    
    if not row.empty:
        print("✅ 成功找到该股快照数据:")
        # 打印所有列，看看有没有我们想要的
        print(row[['代码', '名称', '最新价', '市盈率-动态', '市净率', '总市值', '换手率', '量比']].to_string())
    else:
        print("❌ 快照中未找到该股票 (可能是代码格式问题?)")
except Exception as e:
    print(f"❌ 方案 A 崩溃: {e}")

print("\n" + "="*40 + "\n")

# =========================================================
# 方案 B: 财务摘要 (stock_financial_abstract)
# 目标: 抓取 EPS, 净利润(用于算增长), ROE
# =========================================================
print("--- [方案 B] stock_financial_abstract (财务摘要) ---")
try:
    df_abs = ak.stock_financial_abstract(symbol=TEST_CODE)
    
    if not df_abs.empty:
        print("✅ 成功获取财务摘要 (前5行):")
        print(df_abs.head().to_string())
        
        # 看看有哪些指标
        print("\n🔑 可用字段列表:", df_abs.columns.tolist())
    else:
        print("❌ 接口返回空数据")
except Exception as e:
    print(f"❌ 方案 B 崩溃: {e}")

print("\n" + "="*40 + "\n")

# =========================================================
# 方案 C: 关键指标 (stock_a_indicator_lg) - 乐咕网接口
# 目标: 它是获取 PE(TTM), PE(静), 股息率 的神器
# =========================================================
print("--- [方案 C] stock_a_indicator_lg (乐咕-估值指标) ---")
try:
    # 这个接口通常能拿到 TTM 和 股息率
    df_lg = ak.stock_a_indicator_lg(symbol=TEST_CODE)
    
    if not df_lg.empty:
        # 它是按日期排列的历史数据，我们要最后一行（最新）
        latest = df_lg.iloc[-1]
        print(f"✅ 成功获取估值指标 (日期: {latest['trade_date']}):")
        print(f"   PE(TTM): {latest.get('pe_ttm')}")
        print(f"   PE(Static): {latest.get('pe')}") # 这里的 pe 通常是静态或动态，需确认
        print(f"   股息率: {latest.get('dv_ratio')}")
        print(f"   总市值: {latest.get('total_mv')}")
    else:
        print("❌ 接口返回空数据")
except Exception as e:
    print(f"❌ 方案 C 崩溃: {e}")
