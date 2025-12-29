# -*- coding: utf-8 -*-
"""
🇨🇳 国家队监控室 v1.3 (最终交付版)
更新内容：
1. [文案] K线详情区的 "分析报告" 改为 "机构盈亏"。
2. [排版] 保持 v1.2 的紧凑型 CSS 排版。
3. [完整] 包含所有核心指标 (成本、现价、估值、时间戳等)。
"""

import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import plotly.graph_objects as go
import plotly.express as px
import fnmatch

st.set_page_config(page_title="国家队监控室 v1.3", layout="wide", page_icon="🇨🇳")

DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"
TAG_GROUPS = {
    "👑 国家队核心": ["*中央汇金*", "*证券金融*"],
    "🛡️ 社保大军": ["全国社保基金*"],
    "👴 养老金战队": ["基本养老保险基金*"],
    "🏦 险资/银行/公募": ["中国人寿*", "新华人寿*", "*银行*", "易方达*", "华夏基金*"]
}

@st.cache_resource
def get_engine():
    return create_engine(DB_URL)

def load_data_latest():
    engine = get_engine()
    
    sql = """
    SELECT 
        a.ts_code, b.name, a.holder_name, a.est_cost, a.curr_price, 
        a.profit_rate, a.status, a.period_end, a.hold_amount,
        a.cost_source, a.first_buy_date, a.change_analysis, a.update_time,
        f.pe_ttm, f.pe_dyn, f.pe_static, f.pb, f.div_rate, f.total_mv, f.div_rate_static,
        f.eps, f.roe, f.net_profit_growth
    FROM nt_positions_analysis a
    LEFT JOIN stock_basic b ON a.ts_code = b.ts_code
    LEFT JOIN nt_stock_fundamentals f ON a.ts_code = f.ts_code
    WHERE a.is_latest = true
    """
    try:
        df = pd.read_sql(sql, engine)
    except Exception as e:
        st.error(f"数据库读取失败: {e}")
        return pd.DataFrame()
    
    if not df.empty:
        df['hold_amount'] = df['hold_amount'].fillna(0)
        df['profit_rate_pct'] = df['profit_rate'] * 100
        df['period_end'] = pd.to_datetime(df['period_end'])
        df['first_buy_date'] = pd.to_datetime(df['first_buy_date'])
        df['update_time'] = pd.to_datetime(df['update_time'])
        
        df['first_buy_date'] = df['first_buy_date'].fillna(df['period_end'])
        
        def clean_status(s):
            if isinstance(s, str) and "(" in s:
                return s.split("(")[1].replace(")", "")
            return s
        df['status'] = df['status'].apply(clean_status)

        numeric_cols = ['div_rate', 'div_rate_static', 'pe_ttm', 'pe_dyn', 'pe_static', 'pb', 'total_mv', 'est_cost', 'curr_price', 'eps', 'roe', 'net_profit_growth']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['div_rate'] = df['div_rate'].fillna(0)
        if 'div_rate_static' in df.columns:
            df['div_rate_static'] = df['div_rate_static'].fillna(0)
            
        # 计算市值 (单位: 元)
        df['position_val'] = df['hold_amount'] * 10000 * df['curr_price']
        df['profit_val'] = (df['curr_price'] - df['est_cost']) * df['hold_amount'] * 10000
        
    return df

def load_kline_data(ts_code):
    engine = get_engine()
    sql = text("SELECT trade_date, open, high, low, close, vol FROM nt_market_data WHERE ts_code = :code ORDER BY trade_date ASC")
    try:
        return pd.read_sql(sql, engine, params={"code": ts_code})
    except: return pd.DataFrame()

def calculate_technical_indicators(df):
    if df.empty: return {}
    close = df['close']
    ma20 = close.rolling(window=20).mean().iloc[-1]
    ma60 = close.rolling(window=60).mean().iloc[-1]
    curr = close.iloc[-1]
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs)).iloc[-1]
    bias20 = (curr - ma20) / ma20 * 100
    return { "MA20": ma20, "MA60": ma60, "RSI": rsi, "Bias20": bias20, "Trend": "多头排列" if ma20 > ma60 else "空头排列" }

def get_eastmoney_url(ts_code):
    code = str(ts_code)
    prefix = 'bj' if code.startswith(('8','4')) else ('sh' if code.startswith('6') else 'sz')
    return f"https://quote.eastmoney.com/{prefix}{code}.html"

def safe_fmt(val, unit="", mult=1, default="N/A"):
    if pd.isna(val) or val is None: return default
    try:
        return f"{float(val)*mult:.2f}{unit}"
    except:
        return default

# ================= 侧边栏 =================
st.sidebar.title("🎛️ 战术控制台")
df_all = load_data_latest()

update_time_str = "未知"
if not df_all.empty and 'update_time' in df_all.columns:
    latest_dt = df_all['update_time'].max()
    if pd.notna(latest_dt):
        update_time_str = latest_dt.strftime("%m月%d日 %H:%M")

tag_options = ["(全部)"] + list(TAG_GROUPS.keys())
selected_tag = st.sidebar.selectbox("🏷️ 选择机构分组", tag_options)

available_holders = sorted(df_all['holder_name'].unique().tolist()) if not df_all.empty else []
default_holders = []
if selected_tag != "(全部)":
    patterns = TAG_GROUPS[selected_tag]
    for holder in available_holders:
        for pattern in patterns:
            if fnmatch.fnmatch(holder, pattern):
                default_holders.append(holder)
                break
sidebar_selection = st.sidebar.multiselect("🏛️ 机构名称", available_holders, default=default_holders)

status_list = df_all['status'].unique().tolist() if not df_all.empty else []
selected_status = st.sidebar.multiselect("📊 盈亏状态", status_list, default=status_list)
search_keyword = st.sidebar.text_input("🔍 搜索代码/名称", "")

# 钻取模式逻辑
if 'drill_target' in st.session_state and st.session_state.drill_target:
    current_holders = [st.session_state.drill_target]
    is_drill_mode = True
else:
    current_holders = sidebar_selection
    is_drill_mode = False

# --- 过滤数据 ---
filtered_df = df_all.copy()
if not df_all.empty:
    if current_holders: filtered_df = filtered_df[filtered_df['holder_name'].isin(current_holders)]
    if selected_status: filtered_df = filtered_df[filtered_df['status'].isin(selected_status)]
    if search_keyword: filtered_df = filtered_df[filtered_df['ts_code'].str.contains(search_keyword) | filtered_df['name'].str.contains(search_keyword)]

# ================= 主界面 =================
st.title("🇨🇳 国家队持仓透视系统 v1.3")
st.caption(f"🚀 数据更新于：{update_time_str}")

if "page_index" not in st.session_state: st.session_state.page_index = 0
nav_options = ["🔍 核心看板", "🏆 战绩排行榜"]
selected_tab = st.radio("", nav_options, index=st.session_state.page_index, horizontal=True, label_visibility="collapsed")

if selected_tab != nav_options[st.session_state.page_index]:
    st.session_state.page_index = nav_options.index(selected_tab)
    st.rerun()

st.divider()

if selected_tab == "🔍 核心看板":
    if is_drill_mode:
        col_back, col_msg = st.columns([1.5, 8])
        with col_back:
            if st.button("⬅️ 返回排行榜", type="primary"):
                del st.session_state.drill_target
                st.session_state.page_index = 1
                st.rerun()
        with col_msg: st.warning(f"当前正在查看单体机构：**{st.session_state.drill_target}**。")
    
    st.markdown("### 🎯 战况总览")
    if not filtered_df.empty:
        CUR_TOTAL_VAL = filtered_df['position_val'].sum()
        cur_profit = filtered_df['profit_val'].sum()
        real_yield = (cur_profit / CUR_TOTAL_VAL * 100) if CUR_TOTAL_VAL != 0 else 0
        avg_yield = filtered_df['profit_rate_pct'].mean()
        
        col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
        col_m1.metric("当前持有", f"{len(filtered_df)} 只")
        col_m2.metric("盈利 / 被套", f"{len(filtered_df[filtered_df['profit_rate'] > 0])} / {len(filtered_df[filtered_df['profit_rate'] <= 0])}")
        col_m3.metric("持仓收益率", f"{real_yield:.2f}%", delta_color="normal")
        col_m4.metric("平均收益率", f"{avg_yield:.2f}%")
        col_m5.metric("筛选总盈亏", f"{cur_profit/100000000:.2f} 亿", help=f"筛选持仓市值: {CUR_TOTAL_VAL/100000000:.2f} 亿")
    else: st.info("暂无数据。")

    st.divider()
    if not filtered_df.empty and CUR_TOTAL_VAL > 0:
        st.subheader("🍰 资金分布")
        col_pie, col_top = st.columns([2, 1])

        with col_pie:
            pie_df = filtered_df.copy().sort_values('position_val', ascending=False)
            plot_data = pie_df.iloc[:15] if len(pie_df) > 15 else pie_df
            if len(pie_df) > 15:
                others_val = pie_df.iloc[15:]['position_val'].sum()
                plot_data = pd.concat([plot_data, pd.DataFrame([{'name': '其他', 'position_val': others_val}])])

            fig_pie = px.pie(plot_data, values='position_val', names='name', title=f"市值分布 (筛选总额: {CUR_TOTAL_VAL/100000000:.2f}亿)", hole=0.45)
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            fig_pie.update_layout(height=480, margin=dict(t=100, b=0, l=20, r=20))
            st.plotly_chart(fig_pie, use_container_width=True)

        with col_top:
            st.markdown("#### 💎 重仓 Top 5 ")
            st.markdown("---")
            top5_df = filtered_df.sort_values('position_val', ascending=False).head(5)
            for i, row in top5_df.iterrows():
                rel_ratio = (row['position_val'] / CUR_TOTAL_VAL) * 100
                val_yi = row['position_val'] / 100000000
                color_css = "color:#e74c3c" if rel_ratio > 10 else ("color:#f39c12" if rel_ratio > 5 else "color:#3498db")
                bg_color = "#f0f2f6"
                st.markdown(f"""
                <div style='margin-bottom: 12px;'>
                    <div style='font-size: 1rem; font-weight: 600; color: #31333F;'>
                        {row['name']} <span style='color: #888; font-weight: 400; font-size: 0.9em;'>({row['ts_code']})</span>
                    </div>
                    <div style='display: flex; justify-content: space-between; align_items: center; margin-top: 4px;'>
                        <span style='font-size: 1.15rem; font-weight: 700; font-family: "Source Code Pro", monospace; color: #000;'>
                            {val_yi:.2f} 亿
                        </span>
                        <span style='background-color: {bg_color}; padding: 2px 8px; border-radius: 6px; font-size: 0.85rem; font-weight: 500; {color_css};'>
                            占比 {rel_ratio:.2f}%
                        </span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                st.markdown("<hr style='margin: 0.5em 0; border-top: 1px solid #f0f0f0;'>", unsafe_allow_html=True)

    st.divider()
    st.subheader("📋 持仓明细 (点击左方格子查看详情)")
    
    if not filtered_df.empty:
        display_df = filtered_df.copy()
        
        display_df['rel_weight'] = (display_df['position_val'] / CUR_TOTAL_VAL) * 100
        display_df['display_val'] = display_df['position_val'] / 100000000 
        display_df['display_amount'] = display_df['hold_amount'] * 100 

        display_df['period_end_str'] = display_df['period_end'].dt.strftime('%Y-%m-%d')
        display_df['first_buy_str'] = display_df['first_buy_date'].dt.strftime('%Y-%m-%d')

        view_cols = [
            'ts_code', 'name', 'holder_name', 'status',
            'est_cost', 'curr_price', 
            'profit_rate_pct',
            'display_amount',  
            'display_val',     
            'change_analysis', 
            'rel_weight',      
            'first_buy_str',
            'period_end_str',
            'cost_source'
        ]
        
        event = st.dataframe(display_df[view_cols], column_config={
            "ts_code": "代码", "name": "名称", "holder_name": "机构",
            "status": "状态",
            "est_cost": st.column_config.NumberColumn("成本", format="%.2f"),
            "curr_price": st.column_config.NumberColumn("现价", format="%.2f"),
            "profit_rate_pct": st.column_config.NumberColumn("盈亏率", format="%.2f%%"),
            "display_amount": st.column_config.NumberColumn("持股数(手)", format="%.0f"),
            "display_val": st.column_config.NumberColumn("市值(亿)", format="%.2f"),
            "change_analysis": st.column_config.TextColumn("🔍 较上个财报期变动", width="large"),
            "rel_weight": st.column_config.ProgressColumn("持仓权重", min_value=0, max_value=20.0, format="%.2f%%"),
            "first_buy_str": "建仓季度",
            "period_end_str": "最新财报期",
            "cost_source": "成本来源"
        }, use_container_width=True, height=600, hide_index=True, on_select="rerun", selection_mode="single-row", key="holdings_table")

        if event.selection.rows:
            idx = event.selection.rows[0]
            row = display_df.iloc[idx]
            code = row['ts_code']
            
            st.markdown("---")
            st.subheader(f"🔭 {row['name']} ({code}) 深度扫描")
            
            em_url = get_eastmoney_url(code)
            st.markdown(f"👉 **[点击跳转东方财富 F10 查看详情]({em_url})**")
            
            col_chart, col_data = st.columns([2.5, 1])

            with col_chart:
                    k_df = load_kline_data(code)
                    if not k_df.empty:
                        fig = go.Figure(data=[go.Candlestick(
                            x=k_df['trade_date'], open=k_df['open'], high=k_df['high'], low=k_df['low'], close=k_df['close'], 
                            name="日线", increasing_line_color='#ef5350', decreasing_line_color='#26a69a'
                        )])
                        line_color = "#ef5350" if row['profit_rate_pct'] > 0 else "#26a69a"
                        fig.add_hline(y=row['est_cost'], line_dash="dash", line_color=line_color, annotation_text=f"成本: {row['est_cost']:.2f}")
                        st.plotly_chart(fig, use_container_width=True)
                    else: st.warning("⚠️ 暂无K线数据")

            with col_data:
                # CSS 魔改：强制压缩间距 + 缩小字号
                st.markdown("""
                <style>
                div[data-testid="stMetricValue"] > div {
                    font-size: 1.0rem !important; 
                    font-weight: 600 !important;
                }
                div[data-testid="stMetricLabel"] label {
                    font-size: 0.8rem !important;
                }
                div[data-testid="stMetric"] {
                    margin-bottom: 2px !important;
                }
                hr {
                    margin-top: 5px !important;
                    margin-bottom: 10px !important;
                }
                div[data-testid="column"] {
                    gap: 0rem;
                }
                </style>
                """, unsafe_allow_html=True)
                
                # 🟢 修改处：标题改为 "机构盈亏"
                st.info(f"**💰 机构盈亏: {row['profit_rate_pct']:+.2f}%**")
                st.write("#### 📊 核心指标")
                
                c1, c2 = st.columns(2)
                c1.metric("成本来源", row['cost_source'])
                c2.metric("建仓时间", str(row['first_buy_str']))
                
                c3, c4 = st.columns(2)
                c3.metric("机构成本", f"{row['est_cost']:.2f}")
                c4.metric("当前现价", f"{row['curr_price']:.2f}")

                st.markdown("<hr>", unsafe_allow_html=True)
                
                c5, c6 = st.columns(2)
                main_pe = row['pe_dyn'] if pd.notna(row['pe_dyn']) else row['pe_ttm']
                pe_help = f"动态: {safe_fmt(row['pe_dyn'])}\nTTM: {safe_fmt(row['pe_ttm'])}\n静态: {safe_fmt(row['pe_static'])}"
                c5.metric("PE (市盈率)", safe_fmt(main_pe), help=pe_help)
                c6.metric("PB (市净率)", safe_fmt(row['pb']))

                c7, c8 = st.columns(2)
                c7.metric("利润增长", safe_fmt(row['net_profit_growth'], "%"))
                c8.metric("EPS", safe_fmt(row['eps']))
                
                c9, c10 = st.columns(2)
                c9.metric("ROE", safe_fmt(row['roe'], "%"))
                mv_show = f"{row['total_mv']/100000000:.2f} 亿" if pd.notna(row['total_mv']) else "N/A"
                c10.metric("总市值", mv_show) 

                c11, c12 = st.columns(2)
                div_val = row['div_rate']
                div_show = f"{div_val:.2f}%" if div_val > 0 else "-"
                c11.metric("💰 股息(TTM)", div_show)
                    
                div_static_val = row.get('div_rate_static')
                div_static_show = f"{div_static_val:.2f}%" if (pd.notna(div_static_val) and div_static_val > 0) else "-"
                c12.metric("📅 股息(静态)", div_static_show, help="基于上年度分红") 

                st.markdown("<hr>", unsafe_allow_html=True)
                
                tech = calculate_technical_indicators(k_df)
                st.write("#### 📈 技术面")
                if tech:
                    t1, t2 = st.columns(2)
                    t1.metric("RSI (14)", f"{tech['RSI']:.1f}")
                    t2.metric("乖离率", f"{tech['Bias20']:.1f}%")
                    st.caption(f"趋势: {tech['Trend']}")
        #else:
            #st.info("👈 请点击上方表格中的股票，查看【K线图】及【东财深度资料】")
    else:
        st.info("💡 暂无持仓分析数据。")

elif selected_tab == "🏆 战绩排行榜":
    st.markdown("### 🏆 各大机构操盘能力排行榜")
    if not filtered_df.empty:
        col_ctrl, col_hint = st.columns([2, 5])
        with col_ctrl: 
            sort_metric = st.radio("📊 排序依据", ["持仓收益率", "平均收益率"], horizontal=True)
        with col_hint:
            st.markdown("<br>", unsafe_allow_html=True)
            st.info("💡 **提示**：点击页面底部的 **“详细战绩数据”** 表格行，即可查看该机构的详细持仓！")

        rank_df = filtered_df.groupby('holder_name').apply(lambda x: pd.Series({
            'avg_profit': x['profit_rate_pct'].mean(), 
            'real_yield': (x['profit_val'].sum() / x['position_val'].sum() * 100) if x['position_val'].sum() != 0 else 0,
            'total_val_yi': x['position_val'].sum() / 100000000,
            'count': len(x)
        })).reset_index()
        
        target_col = 'real_yield' if sort_metric == "持仓收益率" else 'avg_profit'
        rank_df = rank_df.sort_values(target_col, ascending=True) 
        plot_df = rank_df 
        plot_df['color'] = plot_df[target_col].apply(lambda x: '#e53935' if x > 0 else '#43a047')
        dynamic_height = max(600, len(plot_df) * 30 + 100)

        fig_bar = px.bar(plot_df, x=target_col, y='holder_name', orientation='h', text_auto='.2f', title=f"机构{sort_metric}分布 (全榜单)")
        fig_bar.update_traces(marker_color=plot_df['color'], textposition='outside', texttemplate='%{value:.2f}%')
        fig_bar.update_layout(height=dynamic_height, xaxis_title=f"{sort_metric} (%)", yaxis_title=None, yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)

        st.markdown("---"); st.subheader("📊 详细战绩数据")
        clean_rank = filtered_df.groupby('holder_name').apply(lambda x: pd.Series({
            'count': len(x),
            'win': (x['profit_rate'] > 0).sum(),
            'loss': (x['profit_rate'] <= 0).sum(),
            'real_yield': (x['profit_val'].sum() / x['position_val'].sum() * 100) if x['position_val'].sum() != 0 else 0,
            'avg_profit': x['profit_rate_pct'].mean(),
            'total_val': x['position_val'].sum() / 100000000
        })).reset_index()
        clean_rank['win_loss'] = clean_rank.apply(lambda row: f"{int(row['win'])} / {int(row['loss'])}", axis=1)
        clean_rank = clean_rank.sort_values(target_col, ascending=False)
        
        rank_event = st.dataframe(clean_rank[['holder_name', 'count', 'win_loss', 'real_yield', 'avg_profit', 'total_val']], column_config={
            "holder_name": "机构名称 (点击跳转)", "count": st.column_config.NumberColumn("持仓数", format="%d"),
            "win_loss": "盈利 / 被套", "real_yield": st.column_config.NumberColumn("持仓收益率", format="%.2f%%"),
            "avg_profit": st.column_config.NumberColumn("平均收益率", format="%.2f%%"),
            "total_val": st.column_config.NumberColumn("总市值 (亿)", format="%.2f")
        }, hide_index=True, height=800, on_select="rerun", selection_mode="single-row", key="holdings_table")
        
        if rank_event.selection.rows:
            st.session_state.drill_target = clean_rank.iloc[rank_event.selection.rows[0]]['holder_name']
            st.session_state.page_index = 0
            st.rerun()
    else: st.warning("暂无持仓数据。")
