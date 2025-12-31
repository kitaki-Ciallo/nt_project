# -*- coding: utf-8 -*-
import akshare as ak
import pandas as pd
import datetime
import time

# 测试目标：
# 002028 (思源电气 - 之前缺数据)
# 600519 (茅台 - 数据通常最全)
# 601398 (工商银行 - 验证股息率)
TEST_CODES = ['002028', '600519', '601398']

def test_fetch_indicators(ts_code):
    print(f"\n>>> 正在测试代码: {ts_code} ...")
    start_time = time.time()
    
    data = {
        "ts_code": ts_code,
        "pe_ttm": None, "pe_dyn": None, 
        "eps": None, "roe": None, "div_rate": None
    }
    
    try:
        # 使用新接口：财务指标分析
        # start_date 设为最近 30 天，确保拿到最新的
        start_dt = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y%m%d")
        
        # 核心：调用 AkShare 接口
        df = ak.stock_financial_analysis_indicator(symbol=ts_code, start_date=start_dt)
        
        if not df.empty:
            print("   ✅ 接口返回成功，正在解析...")
            # 取第一行（最新数据）
            row = df.iloc[0]
            
            # 打印一下原始列名，方便确认
            # print(f"   [调试] 原始列名: {df.columns.tolist()}")
            
            def get_val(col_list):
                for col in col_list:
                    if col in row and pd.notna(row[col]):
                        return row[col]
                return None

            data['pe_ttm'] = get_val(['市盈率(TTM)', '市盈率-TTM'])
            data['pe_dyn'] = get_val(['市盈率(动态)', '市盈率-动态'])
            data['eps']    = get_val(['摊薄每股收益', '每股收益_摊薄'])
            data['roe']    = get_val(['净资产收益率(%)', '净资产收益率'])
            data['div_rate'] = get_val(['股息率(%)', '股息率'])
            
        else:
            print("   ❌ 接口返回为空 DataFrame")
            
    except Exception as e:
        print(f"   ❌ 发生异常: {e}")

    elapsed = time.time() - start_time
    print(f"   ⏱️ 耗时: {elapsed:.2f}秒")
    return data

if __name__ == "__main__":
    print("================ 🚀 开始 DEBUG 测试 ================")
    results = []
    for code in TEST_CODES:
        res = test_fetch_indicators(code)
        results.append(res)
    
    print("\n================ 📊 测试结果汇总 ================")
    df_res = pd.DataFrame(results)
    
    # 格式化打印
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    
    print(df_res)
    
    # 简单的验证逻辑
    print("\n-------------------------------------------------")
    if df_res['pe_ttm'].count() == 3:
        print("✅ 成功: TTM 数据全部获取！")
    else:
        print(f"⚠️ 警告: TTM 数据缺失 ({df_res['pe_ttm'].count()}/3)")
        
    if df_res['div_rate'].count() > 0:
        print("✅ 成功: 股息率获取成功！")
    else:
        print("⚠️ 警告: 股息率依然全军覆没...")
