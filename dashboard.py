# -*- coding: utf-8 -*-
"""
🇨🇳 国家队逆向工程指挥部 (The Dashboard v9.3 - 变动幅度增强版)
功能：
1. [新增] 表格增加「变动幅度」列，显示增减持的百分比 (如 +12.5%, -5.0%)
2. [逻辑] 保持 v9.2 的分批核算逻辑
3. [保留] 所有之前的功能
"""

import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import plotly.graph_objects as go
import plotly.express as px
import akshare as ak
import fnmatch

# ================= 0. 全局配置 =================
st.set_page_config(page_title="国家队监控室 v9.3", layout="wide", page_icon="🇨🇳")

DB_URL = "postgresql+psycopg2://quant_user:quant_password_123@localhost:5432/national_team_db"

TAG_GROUPS = {
    "👑 国家队核心": ["*中央汇金*", "*证券金融*"],
    "🛡️ 社保大军": ["全国社保基金*"],
    "👴 养老金战队": ["基本养老保险基金*"],
    "🏦 险资/银行/公募": ["中国人寿*", "新华人寿*", "*银行*", "易方达*", "华夏基金*"]
}

# ================= 1. 数据核心函数 =================
@st.cache_resource
def get_engine():
    return create_engine(DB_URL)

def load_data_all():
    engine = get_engine()
    sql = """
    SELECT 
        a.ts_code, 
        b.name, 
        a.holder_name, 
        a.est_cost, 
        a.curr_price, 
        a.profit_rate, 
        a.status,
        a.period_end,
        s.hold_amount
    FROM nt_positions_analysis a
    LEFT JOIN stock_basic b ON a.ts_code = b.ts_code
    LEFT JOIN nt_shareholders s 
        ON a.ts_code = s.ts_code 
        AND a.holder_name = s.holder_name 
        AND a.period_end = s.end_date
    """
    df = pd.read_sql(sql, engine)
    
    if not df.empty:
        df['hold_amount'] = df['hold_amount'].fillna(0)
        df['profit_rate_pct'] = df['profit_rate'] * 100
        df['period_end'] = pd.to_datetime(df['period_end'])
    return df

def process_snapshot_data(df_raw):
    """快照数据 (去重，用于KPI/饼图)"""
    if df_raw.empty: return df_raw
    df = df_raw.sort_values(by='period_end', ascending=False)
    df = df.drop_duplicates(subset=['ts_code', 'holder_name'], keep='first')
    df['position_val'] = df['est_cost'] * df['hold_amount']
    return df

def process_detail_data(df_raw):
    """
    明细数据 (计算增减持幅度和分批权重)
    """
    if df_raw.empty: return df_raw
    
    # 1. 排序
    df = df_raw.sort_values(by=['ts_code', 'holder_name', 'period_end'], ascending=[True, True, True])
    
    # 2. 计算上期
    df['prev_hold'] = df.groupby(['ts_code', 'holder_name'])['hold_amount'].shift(1)
    
    # 3. 计算差值
    df['diff_val'] = df['hold_amount'] - df['prev_hold']
    
    # 4. 【新增】计算变动比例 (本期-上期)/上期
    # 注意：如果 prev_hold 为 0 或 NaN，结果会是 inf 或 NaN
    df['change_pct'] = df['diff_val'] / df['prev_hold']
    
    # 5. 标签逻辑
    df['display_amount'] = df['hold_amount'] 
    df['action_tag'] = '🔹 持有/减持'
    
    # A. 建仓
    mask_new = df['prev_hold'].isna()
    df.loc[mask_new, 'action_tag'] = '🆕 建仓'
    df.loc[mask_new, 'change_pct'] = np.nan # 建仓没有涨跌幅概念
    
    # B. 增持
    mask_add = (df['prev_hold'].notna()) & (df['diff_val'] > 0)
    df.loc[mask_add, 'display_amount'] = df.loc[mask_add, 'diff_val']
    df.loc[mask_add, 'action_tag'] = '🔺 增持(新进)'
    
    # C. 减持
    mask_sub = (df['prev_hold'].notna()) & (df['diff_val'] < 0)
    df.loc[mask_sub, 'action_tag'] = '🔻 减持'
    
    # 6. 显示优化
    df['display_val'] = df['est_cost'] * df['display_amount']
    # 将变动比例转换为百分数 (0.12 -> 12.0)
    df['change_pct_display'] = df['change_pct'] * 100
    
    # 7. 排序
    df = df.sort_values(by=['period_end', 'profit_rate'], ascending=[False, False])
    
    return df

def load_kline_data(ts_code):
    engine = get_engine()
    sql = text("SELECT trade_date, open, high, low, close, vol FROM nt_market_data WHERE ts_code = :code ORDER BY trade_date ASC")
    df = pd.read_sql(sql, engine, params={"code": ts_code})
    return df

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
    return {
        "MA20": ma20, "MA60": ma60, "RSI": rsi, "Bias20": bias20,
        "Trend": "多头排列" if ma20 > ma60 else "空头排列"
    }

def get_stock_fundamentals_robust(ts_code):
    info = {"PE": "N/A", "PB": "N/A", "Div": "N/A", "MV": "N/A"}
    try:
        df = ak.stock_individual_info_em(symbol=ts_code)
        data_map = dict(zip(df['item'], df['value']))
        for key in ["市盈率(TTM)", "市盈率(动)", "市盈率(动态)", "市盈率(静)"]:
            if key in data_map and str(data_map[key]) not in ["-", ""]:
                info["PE"] = f"{data_map[key]}"
                break
        if "市净率" in data_map: info["PB"] = str(data_map["市净率"])
        for key in ["股息率", "股息率(TTM)"]:
            if key in data_map: info["Div"] = f"{data_map[key]}%"
            break
        if "总市值" in data_map:
            val = data_map["总市值"]
            try: info["MV"] = f"{float(val)/100000000:.1f}亿"
            except: info["MV"] = str(val)
    except: pass
    return info

def get_eastmoney_url(ts_code):
    code = str(ts_code)
    prefix = 'bj' if code.startswith(('8','4')) else ('sh' if code.startswith('6') else 'sz')
    return f"https://quote.eastmoney.com/{prefix}{code}.html"

# ================= 2. 侧边栏 =================
st.sidebar.title("🎛️ 战术控制台")
df_all = load_data_all()

tag_options = ["(全部)"] + list(TAG_GROUPS.keys())
selected_tag = st.sidebar.selectbox("🏷️ 选择机构分组", tag_options)

available_holders = sorted(df_all['holder_name'].unique().tolist())
default_holders = []
if selected_tag != "(全部)":
    patterns = TAG_GROUPS[selected_tag]
    for holder in available_holders:
        for pattern in patterns:
            if fnmatch.fnmatch(holder, pattern):
                default_holders.append(holder)
                break
    if not default_holders: st.sidebar.warning(f"⚠️ 该分组规则未匹配到任何持仓机构")

selected_holders = st.sidebar.multiselect("🏛️ 机构名称", available_holders, default=default_holders)
status_list = df_all['status'].unique().tolist()
selected_status = st.sidebar.multiselect("📊 盈亏状态", status_list, default=status_list)
search_keyword = st.sidebar.text_input("🔍 搜索代码/名称", "")

filtered_df = df_all.copy()
if selected_holders: filtered_df = filtered_df[filtered_df['holder_name'].isin(selected_holders)]
if selected_status: filtered_df = filtered_df[filtered_df['status'].isin(selected_status)]
if search_keyword:
    filtered_df = filtered_df[filtered_df['ts_code'].str.contains(search_keyword) | filtered_df['name'].str.contains(search_keyword)]

df_snapshot = process_snapshot_data(filtered_df)
df_detail = process_detail_data(filtered_df)

# ================= 3. 主界面 =================
st.title("🇨🇳 国家队持仓透视系统 v9.3")

st.markdown("### 🎯 战况总览 (最新快照)")
col_m1, col_m2, col_m3, col_m4 = st.columns(4)

win_count = len(df_snapshot[df_snapshot['profit_rate'] > 0])
loss_count = len(df_snapshot[df_snapshot['profit_rate'] <= 0])
avg_profit = df_snapshot['profit_rate_pct'].mean()
total_val = df_snapshot['position_val'].sum() / 10000 

col_m1.metric("当前持有标的", f"{len(df_snapshot)} 只")
col_m2.metric("盈利 / 被套", f"{win_count} / {loss_count} 只")
col_m3.metric("平均盈亏率", f"{avg_profit:.2f}%", delta_color="normal")
col_m4.metric("当前筛选总市值", f"{total_val:.2f} 亿元")

st.divider()

if not df_snapshot.empty and total_val > 0:
    st.subheader("🍰 仓位权重分析 (去重后)")
    col_pie, col_list = st.columns([2, 1])
    with col_pie:
        pie_df = df_snapshot.sort_values('position_val', ascending=False)
        plot_data = pie_df.iloc[:15] if len(pie_df) > 15 else pie_df
        if len(pie_df) > 15:
            others_val = pie_df.iloc[15:]['position_val'].sum()
            plot_data = pd.concat([plot_data, pd.DataFrame([{'name': '其他', 'position_val': others_val}])])
        fig_pie = px.pie(plot_data, values='position_val', names='name', title=f"资金分布 (总: {total_val:.2f}亿)", hole=0.45)
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=350)
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_list:
        st.write("#### 🏆 重仓 Top 5")
        top5 = df_snapshot.sort_values('position_val', ascending=False).head(5)
        for i, row in top5.iterrows():
            val_yi = row['position_val'] / 10000
            pct = (row['position_val'] / (total_val * 10000)) * 100
            st.metric(label=f"{row['name']} ({row['ts_code']})", value=f"{val_yi:.2f} 亿", delta=f"占比 {pct:.1f}%")

st.divider()

st.subheader("📋 交易明细 (分批核算+变动幅度)")
st.info("💡 提示：**变动幅度** 为相对于上期持仓的增减百分比。")

display_df = df_detail[[
    'ts_code', 'name', 'holder_name', 'est_cost', 
    'curr_price', 'profit_rate_pct', 'action_tag', 'change_pct_display', 'period_end', 'display_amount', 'display_val'
]].copy()

display_df['period_end'] = display_df['period_end'].dt.strftime('%Y-%m-%d')

event = st.dataframe(
    display_df, 
    column_config={
        "ts_code": "代码",
        "name": "名称",
        "holder_name": "机构",
        "est_cost": st.column_config.NumberColumn("成本", format="%.2f"),
        "curr_price": st.column_config.NumberColumn("现价", format="%.2f"),
        "profit_rate_pct": st.column_config.NumberColumn("盈亏率", format="%.2f%%"),
        "action_tag": "变动类型",
        # 【新增】变动幅度列
        "change_pct_display": st.column_config.NumberColumn("变动幅度", format="%+.2f%%", help="相对于上期持仓的变动比例"),
        "period_end": "财报期",
        "display_amount": st.column_config.NumberColumn("权重(股)", format="%.0f"),
        "display_val": st.column_config.ProgressColumn("权重条(估算)", min_value=0, max_value=display_df['display_val'].max())
    },
    use_container_width=True, height=500, hide_index=True, on_select="rerun", selection_mode="single-row", key="holdings_table"
)

if event.selection.rows:
    idx = event.selection.rows[0]
    row = df_detail.iloc[idx]
    code = row['ts_code']
    name = row['name']
    em_url = get_eastmoney_url(code)
    
    st.markdown("---")
    st.subheader(f"🔭 {name} ({code}) 深度扫描")
    st.markdown(f"👉 **[点击这里跳转东方财富 F10 查看详情]({em_url})**")
    
    k_df = load_kline_data(code)
    tech = calculate_technical_indicators(k_df)
    
    col_chart, col_data = st.columns([2.5, 1])
    with col_chart:
        if not k_df.empty:
            fig = go.Figure(data=[go.Candlestick(x=k_df['trade_date'], open=k_df['open'], high=k_df['high'], low=k_df['low'], close=k_df['close'], name="日线")])
            cost = row['est_cost']
            color = "red" if row['profit_rate'] > 0 else "green"
            fig.add_hline(y=cost, line_dash="dash", line_color=color, annotation_text=f"本笔成本: {cost:.2f}")
            if 'MA20' in tech:
                fig.add_trace(go.Scatter(x=k_df['trade_date'], y=k_df['close'].rolling(20).mean(), mode='lines', name='MA20', line=dict(color='orange', width=1)))
            fig.update_layout(height=550, xaxis_rangeslider_visible=False, title=f"本笔盈亏: {row['profit_rate_pct']:.2f}%")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("⚠️ 暂无K线数据")

    with col_data:
        st.write("#### 📊 基本面")
        fund = get_stock_fundamentals_robust(code)
        c1, c2 = st.columns(2)
        c1.metric("PE 估值", fund["PE"])
        c2.metric("市净率", fund["PB"])
        c3, c4 = st.columns(2)
        c3.metric("股息率", fund["Div"])
        c4.metric("总市值", fund["MV"])
        st.divider()
        st.write("#### 📈 技术面")
        if tech:
            t1, t2 = st.columns(2)
            t1.metric("RSI (14)", f"{tech['RSI']:.1f}")
            t2.metric("乖离率", f"{tech['Bias20']:.1f}%")
            st.caption(f"趋势: {tech['Trend']}")
else:
    st.info("👈 请点击左侧表格中的股票，查看【K线图】及【东财深度资料】")
