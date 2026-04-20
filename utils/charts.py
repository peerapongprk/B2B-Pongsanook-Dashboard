"""utils/charts.py — All Plotly chart builders."""
from __future__ import annotations
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

PALETTE  = ["#1A56DB","#0EA371","#F59E0B","#E53E3E","#7C3AED","#0891B2","#DB2777","#64748B"]
_LAYOUT  = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Sarabun, DM Sans, sans-serif", size=11),
    margin=dict(l=16, r=16, t=40, b=16),
    legend=dict(orientation="h", font=dict(size=10)),
)

def _apply(fig, height=280, title=""):
    fig.update_layout(title=dict(text=title, font=dict(size=13)), height=height, **_LAYOUT)
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="#E2E8F0", zeroline=False)
    return fig

def fig_weekly_revenue(df: pd.DataFrame, week_col="Week", rev_col="Revenue") -> go.Figure:
    fig = go.Figure(go.Scatter(
        x=df[week_col], y=df[rev_col], mode="lines+markers",
        line=dict(color="#1A56DB", width=2),
        marker=dict(size=5),
        fill="tozeroy", fillcolor="rgba(26,86,219,0.07)",
        hovertemplate="%{x|%d %b}<br>฿%{y:,.0f}<extra></extra>",
    ))
    return _apply(fig, title="ยอดขายรายสัปดาห์")

def fig_division_bar(df: pd.DataFrame, div_col="Division", rev_col="Net Sales Amt") -> go.Figure:
    fig = px.bar(df, x=div_col, y=rev_col,
                 color=div_col, color_discrete_sequence=PALETTE,
                 text=df[rev_col].apply(lambda v: f"฿{v/1e3:.0f}k"))
    fig.update_traces(textposition="outside",
                      hovertemplate="<b>%{x}</b><br>฿%{y:,.0f}<extra></extra>")
    fig.update_layout(showlegend=False)
    return _apply(fig, title="ยอดขายตาม Division")

def fig_dept_bar(df: pd.DataFrame, dept_col="Department", rev_col="Net Sales Amt") -> go.Figure:
    fig = px.bar(df.head(12), x=rev_col, y=dept_col, orientation="h",
                 color=rev_col, color_continuous_scale=["#EBF1FF","#1A56DB"])
    fig.update_traces(hovertemplate="<b>%{y}</b><br>฿%{x:,.0f}<extra></extra>")
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    fig.update_yaxes(autorange="reversed")
    return _apply(fig, height=320, title="Top Departments")

def fig_region_donut(df: pd.DataFrame, reg_col="Region Name", rev_col="Net Sales Amt") -> go.Figure:
    fig = go.Figure(go.Pie(
        labels=df[reg_col], values=df[rev_col],
        hole=0.55, marker_colors=PALETTE,
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>฿%{value:,.0f}<br>%{percent}<extra></extra>",
    ))
    return _apply(fig, title="สัดส่วนตาม Region")

def fig_segment_donut(df: pd.DataFrame, seg_col="Customer Main Group Name", rev_col="Net Sales Amt") -> go.Figure:
    fig = go.Figure(go.Pie(
        labels=df[seg_col], values=df[rev_col],
        hole=0.55, marker_colors=["#1A56DB","#0EA371","#F59E0B"],
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>฿%{value:,.0f}<extra></extra>",
    ))
    return _apply(fig, title="สัดส่วนตาม Segment")

def fig_top_customers(df: pd.DataFrame, cust_col="Customer Name", rev_col="Revenue") -> go.Figure:
    top = df.head(15).sort_values(rev_col)
    fig = px.bar(top, x=rev_col, y=cust_col, orientation="h",
                 color=rev_col, color_continuous_scale=["#EBF1FF","#1A56DB"])
    fig.update_traces(hovertemplate="<b>%{y}</b><br>฿%{x:,.0f}<extra></extra>")
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    fig.update_yaxes(autorange="reversed")
    return _apply(fig, height=360, title="Top 15 ลูกค้า")

def fig_customer_weekly(df: pd.DataFrame, week_col="Week", rev_col="Revenue") -> go.Figure:
    fig = go.Figure(go.Bar(
        x=df[week_col], y=df[rev_col],
        marker_color="#1A56DB",
        hovertemplate="%{x|%d %b}<br>฿%{y:,.0f}<extra></extra>",
    ))
    return _apply(fig, height=220, title="ยอดซื้อรายสัปดาห์")

def fig_buying_days(df: pd.DataFrame, dow_col="DayOfWeek", name_col="DayName", cnt_col="Count") -> go.Figure:
    colors = ["#1A56DB" if v == df[cnt_col].max() else "#BFDBFE" for v in df[cnt_col]]
    fig = go.Figure(go.Bar(
        x=df[name_col], y=df[cnt_col],
        marker_color=colors,
        hovertemplate="<b>%{x}</b><br>%{y} ครั้ง<extra></extra>",
    ))
    return _apply(fig, height=200, title="วันที่ชอบซื้อ")

def fig_top_items(df: pd.DataFrame, item_col="Item", rev_col="Revenue") -> go.Figure:
    top = df.head(12).sort_values(rev_col)
    fig = px.bar(top, x=rev_col, y=item_col, orientation="h",
                 color=rev_col, color_continuous_scale=["#EBF1FF","#0EA371"])
    fig.update_traces(hovertemplate="<b>%{y}</b><br>฿%{x:,.0f}<extra></extra>")
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    fig.update_yaxes(autorange="reversed")
    return _apply(fig, height=320, title="สินค้าที่ซื้อบ่อย")

def fig_channel_pie(df: pd.DataFrame) -> go.Figure:
    colors = {"ONLINE": "#0EA371", "OFFLINE": "#1A56DB"}
    fig = go.Figure(go.Pie(
        labels=df["Channel"], values=df["Net Sales Amt"],
        hole=0.5,
        marker_colors=[colors.get(l, "#64748B") for l in df["Channel"]],
        hovertemplate="<b>%{label}</b><br>฿%{value:,.0f}<extra></extra>",
    ))
    return _apply(fig, height=220, title="Online vs Offline")

def fig_sub_channel(df: pd.DataFrame) -> go.Figure:
    top = df.head(8)
    fig = px.bar(top, x="Net Sales Amt", y="OCS Sub Sales Channel", orientation="h",
                 color="Net Sales Amt", color_continuous_scale=["#EBF1FF","#7C3AED"])
    fig.update_traces(hovertemplate="<b>%{y}</b><br>฿%{x:,.0f}<extra></extra>")
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    fig.update_yaxes(autorange="reversed")
    return _apply(fig, height=260, title="Sub-channel Breakdown")
