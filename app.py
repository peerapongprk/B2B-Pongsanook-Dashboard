"""
app.py — SalesIQ · B2B Intelligence Platform
Makro Division (automated) + Lotus's (placeholder)
Run: streamlit run app.py
"""
from __future__ import annotations
import calendar
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from database.persistence  import (
    init_db, save_manual_visits, load_manual_visits, log_upload,
    save_dataset, list_datasets, load_dataset,
    set_active_dataset, get_active_dataset_id,
    delete_dataset, rename_dataset,
)
from database.schema       import MAKRO as S
from utils.data_engine     import (
    load_raw_file, clean_data, compute_kpis,
    revenue_by_region, revenue_by_division, revenue_by_dept,
    revenue_by_segment, top_customers, weekly_trend,
    channel_mix, sub_channel_mix,
    customer_weekly, customer_buying_days, customer_top_items, customer_dept_mix,
    generate_visit_plan,
)
from utils.charts import (
    fig_weekly_revenue, fig_division_bar, fig_dept_bar,
    fig_region_donut, fig_segment_donut, fig_top_customers,
    fig_customer_weekly, fig_buying_days, fig_top_items,
    fig_channel_pie, fig_sub_channel,
)
from utils.styles  import GLOBAL_CSS, kpi_card, section_header, badge, priority_badge, visit_chip
from utils.export  import export_visit_plan, export_customer_summary

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SalesIQ – Makro B2B",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)
init_db()

# ── Session state ─────────────────────────────────────────────────────────────
_DEFAULTS = {
    "df":            None,
    "active_ds_id":  None,   # active dataset id from DB
    "visit_plan":    None,
    "region_filter": "ทั้งหมด",
    "segment_filter": "ทั้งหมด",
    "micro_cust":    None,
    "cal_month":     date.today().replace(day=1),
    "division":      "Makro",
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# Auto-load active dataset from DB on first run
if st.session_state.df is None:
    active_id = get_active_dataset_id()
    if active_id:
        try:
            st.session_state.df = load_dataset(active_id)
            st.session_state.active_ds_id = active_id
        except Exception:
            pass

# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:
        st.markdown("""
        <div class="sidebar-logo">
          <div class="sidebar-logo-icon">⚡</div>
          <div class="sidebar-logo-text">
            <div class="name">SalesIQ</div>
            <div class="sub">B2B Intelligence Platform</div>
          </div>
        </div>""", unsafe_allow_html=True)

        division = st.radio("Division", ["Makro", "Lotus's"],
                            horizontal=True, label_visibility="collapsed")
        st.session_state.division = division
        st.divider()

        if division == "Makro":
            _sidebar_makro()
        else:
            st.info("🏗️ Lotus's Division — Coming Soon", icon="🏪")
            st.caption("Manual entry module กำลังพัฒนาอยู่ครับ")

def _sidebar_makro():
    st.markdown("**📂 อัปโหลดข้อมูล**")
    uploaded = st.file_uploader(
        "ลาก & วางไฟล์ CSV / Excel",
        type=["csv","xlsx","xls"],
        label_visibility="collapsed",
    )
    if uploaded:
        with st.spinner("กำลังประมวลผล…"):
            raw = load_raw_file(uploaded)
            df, warns = clean_data(raw)
            # Save to persistent DB
            label  = uploaded.name.rsplit(".", 1)[0]
            ds_id  = save_dataset(label, uploaded.name, df)
            set_active_dataset(ds_id)
            st.session_state.df           = df
            st.session_state.active_ds_id = ds_id
            st.session_state.visit_plan   = None
            st.session_state.micro_cust   = None
            log_upload(uploaded.name, len(df))
            for w in warns:
                st.warning(w)
            if not warns:
                st.success(f"✅ โหลดสำเร็จ {len(df):,} แถว — บันทึกแล้ว")

    if st.session_state.df is not None:
        st.divider()
        df = st.session_state.df

        # Active dataset badge
        datasets = list_datasets()
        active   = next((d for d in datasets if d["is_active"]), None)
        if active:
            st.markdown(
                f'<div style="background:#ECFDF5;border-radius:8px;padding:.5rem .75rem;'
                f'font-size:.75rem;color:#065F46;margin-bottom:.5rem">'
                f'📦 <b>{active["label"]}</b><br>'
                f'<span style="color:#6B7280">{active["row_count"]:,} แถว</span></div>',
                unsafe_allow_html=True
            )
        if len(datasets) > 1:
            st.caption(f"มีข้อมูลทั้งหมด {len(datasets)} ชุด → ดูที่แท็บ 🗄️")

        # Filters
        st.markdown("**🔍 กรองข้อมูล**")
        regions = ["ทั้งหมด"] + sorted(df[S.REGION].dropna().unique().tolist()) if S.REGION in df.columns else ["ทั้งหมด"]
        st.session_state.region_filter = st.selectbox("Region", regions)

        if S.CUST_GROUP in df.columns:
            segs = ["ทั้งหมด"] + sorted(df[S.CUST_GROUP].dropna().unique().tolist())
            st.session_state.segment_filter = st.selectbox("Segment", segs)

        # ── Reset filters button ───────────────────────────────────────────
        if (st.session_state.region_filter != "ทั้งหมด" or
                st.session_state.segment_filter != "ทั้งหมด"):
            if st.button("🔄 รีเซ็ตตัวกรอง", use_container_width=True, type="secondary"):
                st.session_state.region_filter  = "ทั้งหมด"
                st.session_state.segment_filter = "ทั้งหมด"
                st.rerun()

        st.divider()
        st.markdown(f'<div style="font-size:.72rem;color:#64748B">📅 ข้อมูล: {df[S.DATE].min().strftime("%d %b %Y")} – {df[S.DATE].max().strftime("%d %b %Y")}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="font-size:.72rem;color:#64748B">🔢 {len(df):,} รายการ | {df[S.CUST_NUM].nunique() if S.CUST_NUM in df.columns else "–"} ลูกค้า</div>', unsafe_allow_html=True)

render_sidebar()

# ── Main content ──────────────────────────────────────────────────────────────
if st.session_state.division == "Lotus's":
    st.title("🏪 Lotus's Division")
    st.info("โมดูล Manual Entry สำหรับ Lotus's กำลังพัฒนา — จะเปิดใช้งานเร็ว ๆ นี้")
    st.stop()

if st.session_state.df is None:
    # Landing page
    st.markdown("""
    <div style="text-align:center;padding:4rem 2rem">
      <div style="font-size:3rem">⚡</div>
      <h1 style="font-size:2rem;font-weight:700;margin:.5rem 0">SalesIQ</h1>
      <p style="font-size:1.1rem;color:#64748B;margin-bottom:2rem">B2B Intelligence Platform สำหรับทีมขาย Makro</p>
      <div style="display:inline-block;background:#EBF1FF;border-radius:12px;padding:1rem 2rem;color:#1A56DB;font-weight:600">
        ⬅️ อัปโหลดไฟล์ข้อมูลผ่าน Sidebar เพื่อเริ่มใช้งาน
      </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ── Apply filters ─────────────────────────────────────────────────────────────
df_all = st.session_state.df.copy()
df = df_all.copy()
if st.session_state.region_filter != "ทั้งหมด" and S.REGION in df.columns:
    df = df[df[S.REGION] == st.session_state.region_filter]
if st.session_state.segment_filter != "ทั้งหมด" and S.CUST_GROUP in df.columns:
    df = df[df[S.CUST_GROUP] == st.session_state.segment_filter]

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_overview, tab_micro, tab_calendar, tab_data = st.tabs([
    "📊 ภาพรวม (Macro)",
    "🔍 รายลูกค้า (Micro)",
    "📅 แผนการเยี่ยม",
    "🗄️ ฐานข้อมูล",
])

# ═══════════════════════════════════════════════════════════════════════════════
#  TAB 1 — MACRO OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
with tab_overview:
    kpis = compute_kpis(df)

    # KPI row
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1: st.markdown(kpi_card("ยอดขายรวม", f"฿{kpis['revenue']:,.0f}", "💰"), unsafe_allow_html=True)
    with k2: st.markdown(kpi_card("กำไรสุทธิ", f"฿{kpis['profit']:,.0f}", "📈"), unsafe_allow_html=True)
    with k3: st.markdown(kpi_card("Margin", f"{kpis['margin']:.1f}%", "🎯"), unsafe_allow_html=True)
    with k4: st.markdown(kpi_card("ลูกค้า Active", f"{kpis['customers']:,}", "🏪"), unsafe_allow_html=True)
    with k5: st.markdown(kpi_card("รายการซื้อ", f"{kpis['orders']:,}", "🧾"), unsafe_allow_html=True)

    st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    # Weekly trend + Division
    c1, c2 = st.columns([3, 2])
    with c1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        wkly = weekly_trend(df)
        st.plotly_chart(fig_weekly_revenue(wkly), use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        divdf = revenue_by_division(df)
        if not divdf.empty:
            st.plotly_chart(fig_division_bar(divdf), use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    # Region + Segment
    c3, c4 = st.columns(2)
    with c3:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        regdf = revenue_by_region(df)
        st.plotly_chart(fig_region_donut(regdf), use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)
    with c4:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        segdf = revenue_by_segment(df)
        if not segdf.empty:
            st.plotly_chart(fig_segment_donut(segdf), use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    # Department + Top Customers
    c5, c6 = st.columns([2, 3])
    with c5:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        deptdf = revenue_by_dept(df)
        if not deptdf.empty:
            st.plotly_chart(fig_dept_bar(deptdf), use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)
    with c6:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        topdf = top_customers(df)
        st.plotly_chart(fig_top_customers(topdf), use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    # Channel mix
    st.markdown(section_header("Channel Mix", "📡"), unsafe_allow_html=True)
    cc1, cc2 = st.columns(2)
    chandf = channel_mix(df)
    subdf  = sub_channel_mix(df)
    with cc1:
        if not chandf.empty:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            st.plotly_chart(fig_channel_pie(chandf), use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)
    with cc2:
        if not subdf.empty:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            st.plotly_chart(fig_sub_channel(subdf), use_container_width=True, config={"displayModeBar": False})
            st.markdown('</div>', unsafe_allow_html=True)

    # Export
    st.markdown("<br>", unsafe_allow_html=True)
    if st.download_button("⬇️ Export สรุปลูกค้า (Excel)",
                          data=export_customer_summary(df),
                          file_name="customer_summary.xlsx",
                          mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
        pass

# ═══════════════════════════════════════════════════════════════════════════════
#  TAB 2 — CUSTOMER DRILL-DOWN
# ═══════════════════════════════════════════════════════════════════════════════
with tab_micro:
    st.markdown(section_header("เลือกลูกค้าเพื่อดูรายละเอียด", "🔍"), unsafe_allow_html=True)

    cust_id_col = S.CUST_NUM if S.CUST_NUM in df.columns else S.CUSTOMER

    # Build display options
    cust_df = (df.groupby([cust_id_col, S.CUSTOMER])
                 .agg(Rev=(S.REVENUE,"sum"))
                 .reset_index()
                 .sort_values("Rev", ascending=False))

    cust_options = {
        row[cust_id_col]: f"{row[S.CUSTOMER]}  (฿{row['Rev']:,.0f})"
        for _, row in cust_df.iterrows()
    }

    search = st.text_input("🔎 ค้นหาชื่อลูกค้า", placeholder="พิมพ์ชื่อ...")
    if search:
        cust_options = {k: v for k, v in cust_options.items() if search.lower() in v.lower()}

    if not cust_options:
        st.info("ไม่พบลูกค้าที่ค้นหา")
        st.stop()

    selected_id = st.selectbox("เลือกลูกค้า", list(cust_options.keys()),
                               format_func=lambda k: cust_options.get(k, k))
    st.session_state.micro_cust = selected_id

    cdf = df[df[cust_id_col] == selected_id]
    cust_name = cdf[S.CUSTOMER].iloc[0]
    region    = cdf[S.REGION].iloc[0] if S.REGION in cdf.columns else ""
    segment   = cdf[S.CUST_GROUP].iloc[0] if S.CUST_GROUP in cdf.columns else ""

    # Header card
    st.markdown(f"""
    <div class="chart-card" style="display:flex;align-items:center;gap:1.5rem;padding:1.1rem 1.5rem;margin-bottom:.5rem">
      <div style="width:52px;height:52px;background:#EBF1FF;border-radius:14px;
           display:flex;align-items:center;justify-content:center;font-size:1.6rem;flex-shrink:0">🏪</div>
      <div style="flex:1">
        <div style="font-size:1.1rem;font-weight:700">{cust_name}</div>
        <div style="margin-top:.3rem">
          {badge(region,"blue") if region else ""}
          {badge(segment,"green") if segment else ""}
          {badge(f"ID: {selected_id}","gray")}
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Customer KPIs
    ck = compute_kpis(cdf)
    mk1, mk2, mk3, mk4 = st.columns(4)
    with mk1: st.markdown(kpi_card("ยอดซื้อรวม", f"฿{ck['revenue']:,.0f}", "💰"), unsafe_allow_html=True)
    with mk2: st.markdown(kpi_card("กำไร", f"฿{ck['profit']:,.0f}", "📈"), unsafe_allow_html=True)
    with mk3: st.markdown(kpi_card("รายการซื้อ", f"{ck['orders']:,}", "🧾"), unsafe_allow_html=True)
    with mk4: st.markdown(kpi_card("สินค้าที่ซื้อ", f"{ck['items']:,} รายการ", "📦"), unsafe_allow_html=True)

    st.markdown("<div style='height:.3rem'></div>", unsafe_allow_html=True)

    # Weekly + Buying days
    d1, d2 = st.columns([3, 2])
    with d1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        cw = customer_weekly(df, selected_id)
        st.plotly_chart(fig_customer_weekly(cw), use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)
    with d2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        cbd = customer_buying_days(df, selected_id)
        st.plotly_chart(fig_buying_days(cbd), use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    # Top items + Dept mix
    d3, d4 = st.columns([3, 2])
    with d3:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        items = customer_top_items(df, selected_id)
        st.plotly_chart(fig_top_items(items), use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)
    with d4:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        deptm = customer_dept_mix(df, selected_id)
        if not deptm.empty:
            st.plotly_chart(fig_dept_bar(deptm), use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    # Transaction history table
    st.markdown(section_header("ประวัติการซื้อล่าสุด", "📋"), unsafe_allow_html=True)
    show_cols = [S.DATE, S.ITEM]
    if S.DEPT in df.columns:    show_cols.append(S.DEPT)
    if S.DIVISION in df.columns: show_cols.append(S.DIVISION)
    show_cols += [S.REVENUE, S.PROFIT]
    avail_cols = [c for c in show_cols if c in cdf.columns]
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.dataframe(
        cdf[avail_cols].sort_values(S.DATE, ascending=False).head(200),
        use_container_width=True, height=300,
    )
    st.markdown('</div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  TAB 3 — VISIT PLAN CALENDAR
# ═══════════════════════════════════════════════════════════════════════════════
with tab_calendar:
    st.markdown(section_header("แผนการเยี่ยมลูกค้า (AI-Powered)", "📅"), unsafe_allow_html=True)

    # Generate plan button
    col_gen, col_export, col_nav = st.columns([2, 2, 3])
    with col_gen:
        horizon = st.selectbox("ช่วงเวลา", [14, 21, 30, 45], index=2,
                               format_func=lambda v: f"{v} วันข้างหน้า")
    with col_export:
        st.markdown("<div style='height:1.7rem'></div>", unsafe_allow_html=True)
        gen_btn = st.button("🤖 สร้างแผน AI", use_container_width=True)

    if gen_btn:
        with st.spinner(f"กำลังวิเคราะห์ {df[S.CUST_NUM].nunique() if S.CUST_NUM in df.columns else df[S.CUSTOMER].nunique()} ลูกค้า…"):
            plan = generate_visit_plan(df, horizon_days=horizon)
            st.session_state.visit_plan = plan
            if not plan.empty:
                st.success(f"✅ สร้างแผนสำเร็จ {len(plan):,} การเยี่ยม ใน {plan[S.CUSTOMER].nunique()} ลูกค้า")

    plan = st.session_state.visit_plan

    if plan is None or (isinstance(plan, pd.DataFrame) and plan.empty):
        st.info("กด **🤖 สร้างแผน AI** เพื่อให้ระบบวิเคราะห์และแนะนำแผนการเยี่ยมครับ")
    else:
        # ── Plan KPIs ─────────────────────────────────────────────────────────
        pk1, pk2, pk3, pk4 = st.columns(4)
        high_ct   = len(plan[plan["Priority"]=="High"])
        med_ct    = len(plan[plan["Priority"]=="Medium"])
        repl_ct   = len(plan[plan["Reason"]=="Replenishment"])
        manual_ct = len(plan[plan["Source"]=="Manual"]) if "Source" in plan.columns else 0
        with pk1: st.markdown(kpi_card("การเยี่ยมทั้งหมด", str(len(plan)), "📋"), unsafe_allow_html=True)
        with pk2: st.markdown(kpi_card("Priority: High", str(high_ct), "🔴"), unsafe_allow_html=True)
        with pk3: st.markdown(kpi_card("Replenishment", str(repl_ct), "🔄"), unsafe_allow_html=True)
        with pk4: st.markdown(kpi_card("Manual Add", str(manual_ct), "✏️"), unsafe_allow_html=True)

        st.markdown("<div style='height:.4rem'></div>", unsafe_allow_html=True)

        # ── Calendar navigation ────────────────────────────────────────────────
        nav1, nav2, nav3 = st.columns([1, 3, 1])
        with nav1:
            if st.button("◀ เดือนก่อน"):
                m = st.session_state.cal_month
                st.session_state.cal_month = (m.replace(day=1) - timedelta(days=1)).replace(day=1)
        with nav3:
            if st.button("เดือนหน้า ▶"):
                m = st.session_state.cal_month
                last = calendar.monthrange(m.year, m.month)[1]
                st.session_state.cal_month = m.replace(day=last) + timedelta(days=1)
        with nav2:
            MONTH_TH = ["","ม.ค.","ก.พ.","มี.ค.","เม.ย.","พ.ค.","มิ.ย.",
                        "ก.ค.","ส.ค.","ก.ย.","ต.ค.","พ.ย.","ธ.ค."]
            m = st.session_state.cal_month
            st.markdown(f"<h3 style='text-align:center;margin:0;font-size:1.1rem'>{MONTH_TH[m.month]} {m.year+543}</h3>",
                        unsafe_allow_html=True)

        # ── Build calendar HTML ───────────────────────────────────────────────
        m       = st.session_state.cal_month
        yr, mo  = m.year, m.month
        _, ndays = calendar.monthrange(yr, mo)
        first_dow = date(yr, mo, 1).weekday()  # 0=Mon
        today   = date.today()

        # Index plan by date
        plan_by_date: dict[date, list] = {}
        for _, row in plan.iterrows():
            d = row["Visit_Date"].date() if hasattr(row["Visit_Date"], "date") else pd.Timestamp(row["Visit_Date"]).date()
            if d.year == yr and d.month == mo:
                plan_by_date.setdefault(d, []).append(row)

        DOW_HEADER = ["จันทร์","อังคาร","พุธ","พฤหัส","ศุกร์","เสาร์","อาทิตย์"]
        html = ['<div class="chart-card"><table class="cal-table">',
                '<thead><tr>', *[f'<th>{h}</th>' for h in DOW_HEADER], '</tr></thead>',
                '<tbody><tr>']

        # Empty cells before day 1
        for _ in range(first_dow):
            html.append('<td></td>')

        day_count = 0
        for day_num in range(1, ndays + 1):
            d         = date(yr, mo, day_num)
            is_today  = (d == today)
            today_cls = ' style="background:#EBF1FF;"' if is_today else ""
            visits    = plan_by_date.get(d, [])

            chips = ""
            for row in visits[:4]:  # max 4 chips per day
                name = str(row[S.CUSTOMER])[:18]
                src  = row.get("Source", "AI")
                pri  = row.get("Priority", "Low")
                chips += visit_chip(name, pri, src)
            if len(visits) > 4:
                chips += f'<span class="visit-chip chip-gray">+{len(visits)-4} อื่นๆ</span>'

            html.append(f'<td{today_cls}><div class="cal-day-num">{"✦ " if is_today else ""}{day_num}</div>{chips}</td>')
            day_count += 1

            # New row each Sunday or end of month
            if d.weekday() == 6 and day_num < ndays:
                html.append('</tr><tr>')

        # Pad end of last row
        last_dow = date(yr, mo, ndays).weekday()
        for _ in range(6 - last_dow):
            html.append('<td></td>')

        html.append('</tr></tbody></table></div>')
        st.markdown("".join(html), unsafe_allow_html=True)

        # ── Legend ────────────────────────────────────────────────────────────
        st.markdown("""
        <div style="display:flex;gap:1rem;margin:.5rem 0;flex-wrap:wrap;font-size:.72rem">
          <span class="visit-chip chip-high"  style="display:inline-block">🔴 High Priority</span>
          <span class="visit-chip chip-medium" style="display:inline-block">🟡 Medium Priority</span>
          <span class="visit-chip chip-low"   style="display:inline-block">🟢 Low / Cadence</span>
          <span class="visit-chip chip-manual" style="display:inline-block">🟣 Manual</span>
        </div>
        """, unsafe_allow_html=True)

        # ── Plan table + region/priority filter ──────────────────────────────
        st.markdown(section_header("รายการเยี่ยมทั้งหมด", "📋"), unsafe_allow_html=True)

        tf1, tf2, tf3 = st.columns(3)
        with tf1:
            pri_opts = ["ทั้งหมด"] + sorted(plan["Priority"].unique().tolist())
            pri_filter = st.selectbox("Priority", pri_opts, key="plan_pri")
        with tf2:
            reason_opts = ["ทั้งหมด"] + sorted(plan["Reason"].unique().tolist())
            rea_filter = st.selectbox("เหตุผล", reason_opts, key="plan_rea")
        with tf3:
            reg_col_in_plan = S.REGION if S.REGION in plan.columns else None
            if reg_col_in_plan:
                reg_opts = ["ทั้งหมด"] + sorted(plan[S.REGION].dropna().unique().tolist())
                reg_plan_filter = st.selectbox("Region", reg_opts, key="plan_reg")
            else:
                reg_plan_filter = "ทั้งหมด"

        plan_show = plan.copy()
        if pri_filter != "ทั้งหมด":
            plan_show = plan_show[plan_show["Priority"] == pri_filter]
        if rea_filter != "ทั้งหมด":
            plan_show = plan_show[plan_show["Reason"] == rea_filter]
        if reg_plan_filter != "ทั้งหมด" and S.REGION in plan_show.columns:
            plan_show = plan_show[plan_show[S.REGION] == reg_plan_filter]

        plan_show["Visit_Date"] = plan_show["Visit_Date"].dt.strftime("%d %b %Y")
        display_cols = ["Visit_Date", S.CUSTOMER, S.REGION, S.CUST_GROUP,
                        "Priority", "Reason", "Items_To_Discuss", "Source"]
        avail_plan_cols = [c for c in display_cols if c in plan_show.columns]

        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.dataframe(plan_show[avail_plan_cols], use_container_width=True, height=320)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── Manual visit add ──────────────────────────────────────────────────
        st.markdown(section_header("เพิ่มการเยี่ยมแบบ Manual", "✏️"), unsafe_allow_html=True)
        with st.expander("➕ เพิ่มการเยี่ยมใหม่"):
            with st.form("manual_visit_form", clear_on_submit=True):
                mc1, mc2 = st.columns(2)
                with mc1:
                    cust_list = sorted(df[S.CUSTOMER].unique().tolist())
                    sel_cust  = st.selectbox("ลูกค้า", cust_list)
                    visit_date = st.date_input("วันที่เยี่ยม", value=date.today() + timedelta(days=1))
                with mc2:
                    priority   = st.selectbox("Priority", ["High","Medium","Low"])
                    reason     = st.text_input("เหตุผล", value="Manual visit")
                items_discuss = st.text_input("สินค้าที่จะพูดถึง (optional)")
                submit = st.form_submit_button("💾 บันทึก", use_container_width=True)
                if submit:
                    cust_row = df[df[S.CUSTOMER] == sel_cust].iloc[0]
                    new_row = {
                        S.CUST_NUM if S.CUST_NUM in plan.columns else S.CUSTOMER: cust_row.get(S.CUST_NUM, sel_cust),
                        S.CUSTOMER:  sel_cust,
                        S.REGION:    cust_row.get(S.REGION, ""),
                        S.CUST_GROUP: cust_row.get(S.CUST_GROUP, ""),
                        "Visit_Date": pd.Timestamp(visit_date),
                        "Priority":  priority,
                        "Reason":    reason,
                        "Items_To_Discuss": items_discuss,
                        "Source":    "Manual",
                    }
                    st.session_state.visit_plan = pd.concat(
                        [plan, pd.DataFrame([new_row])], ignore_index=True
                    ).sort_values("Visit_Date")
                    save_manual_visits(st.session_state.visit_plan)
                    st.success(f"✅ เพิ่มการเยี่ยม {sel_cust} วันที่ {visit_date.strftime('%d %b %Y')} แล้ว")
                    st.rerun()

        # ── Export ────────────────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            "⬇️ Export แผนการเยี่ยม (Excel)",
            data=export_visit_plan(plan),
            file_name="visit_plan.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

# ═══════════════════════════════════════════════════════════════════════════════
#  TAB 4 — DATA MANAGER (ฐานข้อมูลกลาง)
# ═══════════════════════════════════════════════════════════════════════════════
with tab_data:
    st.markdown(section_header("ฐานข้อมูลกลาง", "🗄️"), unsafe_allow_html=True)

    # ── Upload new dataset ────────────────────────────────────────────────────
    with st.expander("➕ เพิ่มชุดข้อมูลใหม่", expanded=(len(list_datasets()) == 0)):
        up_col1, up_col2 = st.columns([3, 1])
        with up_col1:
            new_label = st.text_input("ชื่อชุดข้อมูล",
                                      placeholder="เช่น ข้อมูล Q2 2026, Makro รายเดือน มี.ค.")
        with up_col2:
            st.markdown("<div style='height:1.7rem'></div>", unsafe_allow_html=True)

        new_file = st.file_uploader(
            "ไฟล์ Excel (.xlsx / .xls) หรือ CSV",
            type=["xlsx", "xls", "csv"],
            label_visibility="collapsed",
            key="db_uploader",
        )
        if new_file:
            auto_label = new_label.strip() or new_file.name.rsplit(".", 1)[0]
            if st.button("💾 บันทึกเข้าฐานข้อมูล", type="primary", use_container_width=True):
                with st.spinner("กำลังประมวลผลและบันทึก…"):
                    raw = load_raw_file(new_file)
                    df_new, warns = clean_data(raw)
                    ds_id = save_dataset(auto_label, new_file.name, df_new)
                    for w in warns:
                        st.warning(w)
                    st.success(f"✅ บันทึก **{auto_label}** สำเร็จ — {len(df_new):,} แถว")
                    st.rerun()

    st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    # ── Dataset list ──────────────────────────────────────────────────────────
    datasets = list_datasets()

    if not datasets:
        st.info("ยังไม่มีชุดข้อมูล — อัปโหลดไฟล์ด้านบนเพื่อเริ่มต้น")
    else:
        active_id = get_active_dataset_id()
        st.markdown(f"**{len(datasets)} ชุดข้อมูล** ในระบบ", )

        for ds in datasets:
            is_active = ds["id"] == active_id
            border    = "2px solid #1A56DB" if is_active else "1px solid #E2E8F0"
            bg        = "#F0F7FF" if is_active else "#FFFFFF"

            st.markdown(
                f'<div style="background:{bg};border:{border};border-radius:12px;'
                f'padding:.9rem 1.1rem;margin-bottom:.5rem">',
                unsafe_allow_html=True
            )

            row_left, row_right = st.columns([5, 3])
            with row_left:
                active_badge = " 🟢 **ใช้งานอยู่**" if is_active else ""
                st.markdown(f"**{ds['label']}**{active_badge}")
                date_range = f"{ds['date_min']} → {ds['date_max']}" if ds['date_min'] else "ไม่ระบุช่วงวันที่"
                st.caption(f"📁 {ds['filename']}  |  🔢 {ds['row_count']:,} แถว  |  📅 {date_range}  |  ⏱ {ds['uploaded_at'][:16]}")

            with row_right:
                btn_c1, btn_c2, btn_c3 = st.columns(3)
                with btn_c1:
                    if not is_active:
                        if st.button("✅ ใช้งาน", key=f"activate_{ds['id']}", use_container_width=True):
                            with st.spinner("กำลังโหลด…"):
                                set_active_dataset(ds["id"])
                                st.session_state.df           = load_dataset(ds["id"])
                                st.session_state.active_ds_id = ds["id"]
                                st.session_state.visit_plan   = None
                                st.session_state.micro_cust   = None
                                st.session_state.region_filter  = "ทั้งหมด"
                                st.session_state.segment_filter = "ทั้งหมด"
                            st.rerun()
                    else:
                        st.markdown('<div style="text-align:center;font-size:.8rem;color:#1A56DB;padding-top:.4rem">✅ Active</div>', unsafe_allow_html=True)

                with btn_c2:
                    # Rename inline
                    with st.popover("✏️", ):
                        new_name = st.text_input("ชื่อใหม่", value=ds["label"], key=f"rename_input_{ds['id']}")
                        if st.button("บันทึก", key=f"rename_save_{ds['id']}"):
                            rename_dataset(ds["id"], new_name)
                            st.rerun()

                with btn_c3:
                    if not is_active:
                        with st.popover("🗑️"):
                            st.markdown(f"ลบ **{ds['label']}** ?")
                            if st.button("ยืนยันลบ", key=f"del_{ds['id']}", type="primary"):
                                delete_dataset(ds["id"])
                                st.rerun()
                    else:
                        st.markdown('<div style="text-align:center;font-size:.8rem;color:#9CA3AF;padding-top:.4rem">—</div>', unsafe_allow_html=True)

            st.markdown('</div>', unsafe_allow_html=True)

        # ── Summary stats ──────────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(section_header("สรุปข้อมูล Active Dataset", "📊"), unsafe_allow_html=True)
        if st.session_state.df is not None:
            adf = st.session_state.df
            sc1, sc2, sc3, sc4 = st.columns(4)
            with sc1: st.markdown(kpi_card("แถวข้อมูล", f"{len(adf):,}", "🔢"), unsafe_allow_html=True)
            with sc2: st.markdown(kpi_card("จำนวนลูกค้า", f"{adf['Customer Number'].nunique() if 'Customer Number' in adf.columns else '–':,}", "🏪"), unsafe_allow_html=True)
            with sc3: st.markdown(kpi_card("สินค้า (SKU)", f"{adf['Item'].nunique() if 'Item' in adf.columns else '–':,}", "📦"), unsafe_allow_html=True)
            with sc4: st.markdown(kpi_card("คอลัมน์", f"{len(adf.columns)}", "📋"), unsafe_allow_html=True)

            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            st.markdown("**ตัวอย่างข้อมูล (5 แถวแรก)**")
            st.dataframe(adf.head(5), use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
