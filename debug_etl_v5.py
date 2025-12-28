# -*- coding: utf-8 -*-
import akshare as ak
import pandas as pd
import time

# 测试目标
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
        # 【修复点】去掉 start_date 参数，直接获取全量历史
        df = ak.stock_financial_analysis_indicator(symbol=ts_code)
        
        if not df.empty:
            print("   ✅ 接口返回成功，正在清洗...")
            
            # 【关键步骤】确保按日期倒序排列，取最新的一行
            if '日期' in df.columns:
                df['日期'] = pd.to_datetime(df['日期'])
                df = df.sort_values('日期', ascending=False)
            
            # 取第一行（最新的数据）
            row = df.iloc[0]
            print(f"   📅 数据日期: {row['日期']}") 
            
            def get_val(col_list):
                for col in col_list:
                    if col in row and pd.notna(row[col]):
                        try: return float(row[col])
                        except: pass
                return None

            data['pe_ttm'] = get_val(['市盈率(TTM)', '市盈率-TTM'])
            data['pe_dyn'] = get_val(['市盈率(动态)', '市盈率-动态'])
            data['eps']    = get_val(['摊薄每股收益', '每股收益_摊薄', '每股收益'])
            data['roe']    = get_val(['净资产收益率(%)', '净资产收益率'])
            data['div_rate'] = get_val(['股息率(%)', '股息率'])
            
            # 打印一下抓到的值，方便确认
            print(f"   💰 抓取结果 -> TTM: {data['pe_ttm']}, EPS: {data['eps']}, 股息: {data['div_rate']}")
            
        else:
            print("   ❌ 接口返回为空 DataFrame")
            
    except Exception as e:
        print(f"   ❌ 发生异常: {e}")

    elapsed = time.time() - start_time
    print(f"   ⏱️ 耗时: {elapsed:.2f}秒")
    return data

if __name__ == "__main__":
    print("================ 🚀 开始 DEBUG 测试 (v5) ================")
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
