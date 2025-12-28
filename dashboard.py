# -*- coding: utf-8 -*-
"""
🇨🇳 国家队逆向工程指挥部 (The Dashboard v10.7 - 终极修复版)
修复内容：
1. [交互] 完美复活“小问号” (Tooltip)，鼠标悬停显示 PE/增长率 详情。
2. [数据] 增强空值处理，数据库缺数据时显示更优雅。
3. [布局] 保持股息率独立一行，并修复所有跳转链接大小。
4. [逻辑] 补全 SQL 查询字段 (增加 pe_static)。
"""

import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import plotly.graph_objects as go
import plotly.express as px
import akshare as ak
import fnmatch

st.set_page_config(page_title="国家队监控室 v10.7", layout="wide", page_icon="🇨🇳")

# 数据库连接
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
def load_data_all():
    # 👇 这一行非常重要，千万不能少！
    engine = get_engine()
    
    # 🟢 SQL 查询：增加了 f.div_rate_static
    sql = """
    SELECT 
        a.ts_code, b.name, a.holder_name, a.est_cost, a.curr_price, 
        a.profit_rate, a.status, a.period_end, s.hold_amount,
        f.pe_dyn, f.pe_ttm, f.pe_static, f.pb, f.total_mv,
        f.eps, f.roe, f.net_profit_growth, f.div_rate, f.div_rate_static, f.net_margin
    FROM nt_positions_analysis a
    LEFT JOIN stock_basic b ON a.ts_code = b.ts_code
    LEFT JOIN nt_shareholders s ON a.ts_code = s.ts_code AND a.holder_name = s.holder_name AND a.period_end = s.end_date
    LEFT JOIN nt_stock_fundamentals f ON a.ts_code = f.ts_code
    """
    df = pd.read_sql(sql, engine)
    
    if not df.empty:
        df['hold_amount'] = df['hold_amount'].fillna(0)
        df['profit_rate_pct'] = df['profit_rate'] * 100
        df['period_end'] = pd.to_datetime(df['period_end'])
        
        # 🟢【关键修复】强制将所有数值列转为数字类型
        # 增加了 div_rate_static
        numeric_cols = ['div_rate', 'div_rate_static', 'pe_dyn', 'pe_ttm', 'pe_static', 'eps', 'roe', 'net_profit_growth', 'total_mv']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 填充空值
        df['div_rate'] = df['div_rate'].fillna(0)
        # 如果静态股息率是空，也填0或者保持NaN由展示层处理，这里填0比较稳妥
        if 'div_rate_static' in df.columns:
            df['div_rate_static'] = df['div_rate_static'].fillna(0)
        
    return df

def process_snapshot_data(df_raw):
    if df_raw.empty: return df_raw
    df = df_raw.sort_values(by='period_end', ascending=False)
    df = df.drop_duplicates(subset=['ts_code', 'holder_name'], keep='first')
    df['position_val'] = df['est_cost'] * df['hold_amount']
    df['profit_val'] = (df['curr_price'] - df['est_cost']) * df['hold_amount']
    return df

def process_detail_data(df_raw):
    if df_raw.empty: return df_raw
    df = df_raw.sort_values(by=['ts_code', 'holder_name', 'period_end'], ascending=[True, True, True])
    df['prev_hold'] = df.groupby(['ts_code', 'holder_name'])['hold_amount'].shift(1)
    df['diff_val'] = df['hold_amount'] - df['prev_hold']
    df['change_pct'] = df['diff_val'] / df['prev_hold']
    df['display_amount'] = df['hold_amount'] 
    df['action_tag'] = '🔹 持有/减持'
    
    mask_new = df['prev_hold'].isna()
    df.loc[mask_new, 'action_tag'] = '🆕 建仓'
    df.loc[mask_new, 'change_pct'] = np.nan
    
    mask_add = (df['prev_hold'].notna()) & (df['diff_val'] > 0)
    df.loc[mask_add, 'display_amount'] = df.loc[mask_add, 'diff_val']
    df.loc[mask_add, 'action_tag'] = '🔺 增持(新进)'
    
    mask_sub = (df['prev_hold'].notna()) & (df['diff_val'] < 0)
    df.loc[mask_sub, 'action_tag'] = '🔻 减持'
    
    df['display_val'] = df['est_cost'] * df['display_amount']
    df['change_pct_display'] = df['change_pct'] * 100
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
    return { "MA20": ma20, "MA60": ma60, "RSI": rsi, "Bias20": bias20, "Trend": "多头排列" if ma20 > ma60 else "空头排列" }

def get_eastmoney_url(ts_code):
    code = str(ts_code)
    prefix = 'bj' if code.startswith(('8','4')) else ('sh' if code.startswith('6') else 'sz')
    return f"https://quote.eastmoney.com/{prefix}{code}.html"

# 辅助函数：安全格式化
def safe_fmt(val, unit="", mult=1, default="N/A"):
    if pd.isna(val) or val is None: return default
    return f"{val*mult:.2f}{unit}"

# ================= 2. 侧边栏 =================
st.sidebar.title("🎛️ 战术控制台")
df_all = load_data_all()

# --- 筛选逻辑 ---
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
sidebar_selection = st.sidebar.multiselect("🏛️ 机构名称", available_holders, default=default_holders)
status_list = df_all['status'].unique().tolist()
selected_status = st.sidebar.multiselect("📊 盈亏状态", status_list, default=status_list)
search_keyword = st.sidebar.text_input("🔍 搜索代码/名称", "")

if 'drill_target' in st.session_state and st.session_state.drill_target:
    current_holders = [st.session_state.drill_target]
    is_drill_mode = True
else:
    current_holders = sidebar_selection
    is_drill_mode = False

filtered_df = df_all.copy()
if current_holders: filtered_df = filtered_df[filtered_df['holder_name'].isin(current_holders)]
if selected_status: filtered_df = filtered_df[filtered_df['status'].isin(selected_status)]
if search_keyword: filtered_df = filtered_df[filtered_df['ts_code'].str.contains(search_keyword) | filtered_df['name'].str.contains(search_keyword)]

df_snapshot = process_snapshot_data(filtered_df)
df_detail = process_detail_data(filtered_df)

# ================= 3. 主界面 =================
st.title("🇨🇳 国家队持仓透视系统 v10.7")

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
    
    st.markdown("### 🎯 战况总览 (最新快照)")
    if not df_snapshot.empty:
        total_profit_val_wan = df_snapshot['profit_val'].sum()
        total_cost_val_wan = df_snapshot['position_val'].sum()
        real_yield = (total_profit_val_wan / total_cost_val_wan * 100) if total_cost_val_wan != 0 else 0
        avg_yield = df_snapshot['profit_rate_pct'].mean()
        total_profit_yi = total_profit_val_wan / 10000
        total_val_yi = total_cost_val_wan / 10000
        col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
        col_m1.metric("当前持有", f"{len(df_snapshot)} 只")
        col_m2.metric("盈利 / 被套", f"{len(df_snapshot[df_snapshot['profit_rate'] > 0])} / {len(df_snapshot[df_snapshot['profit_rate'] <= 0])}")
        col_m3.metric("持仓收益率", f"{real_yield:.2f}%", delta_color="normal")
        col_m4.metric("平均收益率", f"{avg_yield:.2f}%")
        col_m5.metric("持仓总盈亏", f"{total_profit_yi:+.2f} 亿", help=f"当前持仓总市值: {total_val_yi:.2f} 亿")
    else: st.info("暂无数据")

    st.divider()
    if not df_snapshot.empty and 'total_val_yi' in locals() and total_val_yi > 0:
        st.subheader("🍰 资金分布")

        # 【布局】保持 2:1
        col_pie, col_top = st.columns([2, 1])

        with col_pie:
            pie_df = df_snapshot.sort_values('position_val', ascending=False)
            plot_data = pie_df.iloc[:15] if len(pie_df) > 15 else pie_df
            if len(pie_df) > 15:
                others_val = pie_df.iloc[15:]['position_val'].sum()
                plot_data = pd.concat([plot_data, pd.DataFrame([{'name': '其他', 'position_val': others_val}])])

            fig_pie = px.pie(plot_data, values='position_val', names='name',
                             title=f"市值权重分布 (总: {total_val_yi:.2f}亿)",
                             hole=0.45)

            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            fig_pie.update_layout(
                margin=dict(t=100, b=0, l=20, r=20),
                legend=dict(
                    orientation="v",
                    yanchor="middle",  # 垂直居中
                    y=0.5,

                    # 👈 1. 控制位置 (往左移)
                    xanchor="left",
                    x=0,               # 0 是最左边，想留空隙可以用 0.02

                    # 👈 2. 控制大小
                    font=dict(size=15), # 调大字体，默认大概是 12
                    itemsizing='constant' # (可选) 让图例的色块保持一致大小，不随饼图切片大小变化
                ),
                height=480,
                title=dict(y=0.95, x=0.05, xanchor='left', yanchor='top')
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        with col_top:
            st.markdown("#### 💎 重仓 Top 5")
            st.markdown("---")
            top5_df = df_snapshot.sort_values('position_val', ascending=False).head(5)
            for i, row in top5_df.iterrows():
                ratio = (row['position_val'] / total_cost_val_wan) * 100
                val_yi = row['position_val'] / 10000

                color_css = "color:#e74c3c" if ratio > 10 else ("color:#f39c12" if ratio > 5 else "color:#3498db")
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
                            ↑ 占比 {ratio:.1f}%
                        </span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                # 细分割线
                st.markdown("<hr style='margin: 0.5em 0; border-top: 1px solid #f0f0f0;'>", unsafe_allow_html=True)

    # 🟢 关键在这里！这一行必须和最上面的 'if' 保持对齐 (也是 4 个空格)
    st.divider()
    st.subheader("📋 交易明细 (点击查看K线)")
    # 【修复点】确保 div_rate 和 pe_static 在列中
    display_df = df_detail[[
        'ts_code', 'name', 'holder_name', 'est_cost', 'curr_price', 'profit_rate_pct', 
        'action_tag', 'change_pct_display', 'period_end', 'display_amount', 'display_val',
        'pe_dyn', 'pe_ttm', 'pe_static', 'pb', 'div_rate', 'eps', 'roe', 'net_profit_growth', 'total_mv' 
    ]].copy()
    
    total_visible_val = display_df['display_val'].sum()
    display_df['weight_pct'] = (display_df['display_val'] / total_visible_val * 100) if total_visible_val > 0 else 0
    display_df['period_end'] = display_df['period_end'].dt.strftime('%Y-%m-%d')

    event = st.dataframe(display_df, column_config={
        "ts_code": "代码", "name": "名称", "holder_name": "机构",
        "est_cost": st.column_config.NumberColumn("成本", format="%.2f"),
        "curr_price": st.column_config.NumberColumn("现价", format="%.2f"),
        "profit_rate_pct": st.column_config.NumberColumn("盈亏率", format="%.2f%%"),
        "div_rate": st.column_config.NumberColumn("股息率", format="%.2f%%"),
        "change_pct_display": st.column_config.NumberColumn("变动幅度", format="%+.2f%%"),
        "period_end": "财报期", 
        "display_amount": st.column_config.NumberColumn("持股数(万)", format="%.0f"),
        "weight_pct": st.column_config.ProgressColumn("仓位权重", min_value=0, max_value=100, format="%.1f%%"),
        "display_val": None,
        "pe_dyn": None, "pe_ttm": None, "pe_static": None, "pb": None, "eps": None, "roe": None, "net_profit_growth": None, "total_mv": None
    }, use_container_width=True, height=500, hide_index=True, on_select="rerun", selection_mode="single-row", key="holdings_table")

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
                    # 🟢【修改点】增加 increasing_line_color 和 decreasing_line_color
                    fig = go.Figure(data=[go.Candlestick(
                        x=k_df['trade_date'], 
                        open=k_df['open'], 
                        high=k_df['high'], 
                        low=k_df['low'], 
                        close=k_df['close'], 
                        name="日线",
                        increasing_line_color='#ef5350', # 红 (涨)
                        decreasing_line_color='#26a69a'  # 绿 (跌)
                    )])
                    
                    # 辅助线颜色也配合一下
                    line_color = "#ef5350" if row['profit_rate_pct'] > 0 else "#26a69a"
                    fig.add_hline(y=row['est_cost'], line_dash="dash", line_color=line_color, annotation_text=f"成本: {row['est_cost']:.2f}")
                    
                    fig.update_layout(height=550, xaxis_rangeslider_visible=False, title=f"本笔盈亏: {row['profit_rate_pct']:.2f}%")
                    st.plotly_chart(fig, use_container_width=True)
                else: st.warning("⚠️ 暂无K线数据")

        with col_data:
            st.write("#### 📊 基本面 & 估值")
            
            c1, c2 = st.columns(2)
            c1.metric("当前现价", f"{row['curr_price']:.2f}")
            c2.metric("机构成本", f"{row['est_cost']:.2f}")

            # 【修复点 2】复活小问号 (Tooltip) 并支持空值显示
            c3, c4 = st.columns(2)
            
            # 优先显示动态 PE，如果都没有则显示 N/A
            main_pe = row['pe_dyn'] if pd.notna(row['pe_dyn']) else row['pe_ttm']
            pe_show = safe_fmt(main_pe)
            
            # 构建小问号里的详细内容
            pe_help_str = f"""
            动态 PE: {safe_fmt(row['pe_dyn'])}\n
            TTM PE:  {safe_fmt(row['pe_ttm'])}\n
            静态 PE: {safe_fmt(row['pe_static'])}
            """
            
            c3.metric("PE (市盈率)", pe_show, help=pe_help_str)
            c4.metric("PB (市净率)", safe_fmt(row['pb']))

            c5, c6 = st.columns(2)
            # 增长率的小问号
            growth_help = "基于财务摘要计算，可能存在数据延迟。"
            c5.metric("利润增长(同比)", safe_fmt(row['net_profit_growth'], "%"), help=growth_help)
            c6.metric("EPS (每股收益)", safe_fmt(row['eps']))
            
            c7, c8 = st.columns(2)
            c7.metric("ROE (净资收益)", safe_fmt(row['roe'], "%"))
            mv_show = "N/A"
            if pd.notna(row['total_mv']): mv_show = f"{row['total_mv']/100000000:.2f} 亿"
            c8.metric("总市值", mv_show)

            st.write("") 
            c9, c10 = st.columns(2)
                
            # 股息率 (TTM)
            div_val = row['div_rate']
            div_show = f"{div_val:.2f}%" if (pd.notna(div_val) and div_val > 0) else "-"
            c9.metric("💰 股息率 (TTM)", div_show, help="滚动股息率：过去12个月分红/市值")
                
            # 🟢 股息率 (静态)
            div_static_val = row.get('div_rate_static') # 使用 .get 防止列不存在报错
            div_static_show = f"{div_static_val:.2f}%" if (pd.notna(div_static_val) and div_static_val > 0) else "-"
            c10.metric("📅 股息率 (静态)", div_static_show, help="静态股息率：上年度每股分红/当前股价") 

            st.divider()
            tech = calculate_technical_indicators(k_df)
            st.write("#### 📈 技术面")
            if tech:
                t1, t2 = st.columns(2)
                t1.metric("RSI (14)", f"{tech['RSI']:.1f}")
                t2.metric("乖离率", f"{tech['Bias20']:.1f}%")
                st.caption(f"趋势: {tech['Trend']}")

elif selected_tab == "🏆 战绩排行榜":
    st.markdown("### 🏆 各大机构操盘能力排行榜")
    if not df_snapshot.empty:
        col_ctrl, col_hint = st.columns([2, 5])
        
        with col_ctrl: 
            sort_metric = st.radio("📊 排序依据", ["持仓收益率", "平均收益率"], horizontal=True)
        
        with col_hint:
            st.markdown("<br>", unsafe_allow_html=True)
            st.info("💡 **提示**：点击页面底部的 **“详细战绩数据”** 表格行，即可查看该机构的详细持仓！")

        rank_df = df_snapshot.groupby('holder_name').apply(lambda x: pd.Series({
            'avg_profit': x['profit_rate_pct'].mean(), 
            'real_yield': (x['profit_val'].sum() / x['position_val'].sum() * 100) if x['position_val'].sum() != 0 else 0,
            'total_val_yi': x['position_val'].sum() / 10000,
            'count': len(x)
        })).reset_index()
        
        target_col = 'real_yield' if sort_metric == "持仓收益率" else 'avg_profit'
        
        rank_df = rank_df.sort_values(target_col, ascending=True) 
        plot_df = rank_df 
        
        plot_df['color'] = plot_df[target_col].apply(lambda x: '#e53935' if x > 0 else '#43a047')
        
        dynamic_height = max(600, len(plot_df) * 30 + 100)

        fig_bar = px.bar(
            plot_df, 
            x=target_col, 
            y='holder_name', 
            orientation='h', 
            text_auto='.2f', 
            title=f"机构{sort_metric}分布 (全榜单)"
        )
        fig_bar.update_traces(marker_color=plot_df['color'], textposition='outside', texttemplate='%{value:.2f}%')
        
        fig_bar.update_layout(
            height=dynamic_height, 
            xaxis_title=f"{sort_metric} (%)", 
            yaxis_title=None, 
            yaxis={'categoryorder':'total ascending'}
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        st.markdown("---"); st.subheader("📊 详细战绩数据")
        clean_rank = df_snapshot.groupby('holder_name').apply(lambda x: pd.Series({
            'count': len(x),
            'win': (x['profit_rate'] > 0).sum(),
            'loss': (x['profit_rate'] <= 0).sum(),
            'real_yield': (x['profit_val'].sum() / x['position_val'].sum() * 100) if x['position_val'].sum() != 0 else 0,
            'avg_profit': x['profit_rate_pct'].mean(),
            'total_val': x['position_val'].sum() / 10000
        })).reset_index()
        clean_rank['win_loss'] = clean_rank.apply(lambda row: f"{int(row['win'])} / {int(row['loss'])}", axis=1)
        
        clean_rank = clean_rank.sort_values(target_col, ascending=False)
        
        rank_event = st.dataframe(clean_rank[['holder_name', 'count', 'win_loss', 'real_yield', 'avg_profit', 'total_val']], column_config={
            "holder_name": "机构名称 (点击跳转)", "count": st.column_config.NumberColumn("持仓数", format="%d"),
            "win_loss": "盈利 / 被套", "real_yield": st.column_config.NumberColumn("持仓收益率", format="%.2f%%"),
            "avg_profit": st.column_config.NumberColumn("平均收益率", format="%.2f%%"),
            "total_val": st.column_config.NumberColumn("总市值 (亿)", format="%.2f")
        }, hide_index=True, height=800, on_select="rerun", selection_mode="single-row")
        
        if rank_event.selection.rows:
            st.session_state.drill_target = clean_rank.iloc[rank_event.selection.rows[0]]['holder_name']
            st.session_state.page_index = 0
            st.rerun()
    else: st.warning("暂无持仓数据。")
