"""app.py — SalesIQ v6 · Batch-based Master DB"""
from __future__ import annotations
import calendar
from datetime import date, timedelta
import pandas as pd
import streamlit as st

from database.persistence import (
    init_db, save_manual_visits,
    save_batch, list_batches, load_batch, load_combined_df,
    delete_batch, rename_batch, check_date_overlap,
    # legacy aliases still work
    save_dataset, list_datasets, load_dataset,
    set_active_dataset, get_active_dataset_id,
    delete_dataset, rename_dataset,
    list_users, save_user, delete_user,
    get_user_customers, set_user_customers,
)
from database.schema   import MAKRO as S
from utils.data_engine import (
    load_raw_file, clean_data, compute_kpis,
    revenue_by_region, revenue_by_division, revenue_by_dept,
    revenue_by_segment, top_customers, weekly_trend,
    channel_mix, sub_channel_mix,
    customer_weekly, customer_buying_days, customer_top_items, customer_dept_mix,
    generate_visit_plan_constrained, recommend_items,
    identify_potential_customers, identify_at_risk_customers, DEFAULT_RISK_THRESHOLDS,
)
from utils.charts import (
    fig_weekly_revenue, fig_division_bar, fig_dept_bar,
    fig_region_donut, fig_segment_donut, fig_top_customers,
    fig_customer_weekly, fig_buying_days, fig_top_items,
    fig_channel_pie, fig_sub_channel,
)
from utils.styles  import GLOBAL_CSS, kpi_card, section_header, badge, priority_badge, visit_chip
from utils.export  import export_visit_plan, export_customer_summary

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="SalesIQ", page_icon="⚡", layout="wide",
                   initial_sidebar_state="collapsed")
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)
st.markdown("""
<style>
/* Top nav bar */
.top-nav {
  display:flex; align-items:center; gap:.5rem;
  background:#1E293B; border-radius:12px;
  padding:.6rem 1.2rem; margin-bottom:1.2rem;
}
.nav-logo { color:#fff; font-weight:700; font-size:1.1rem; margin-right:auto; }
.nav-logo span { color:#60A5FA; }
.stButton > button[data-nav] { background:transparent; border:none; color:#CBD5E1;
  font-size:.88rem; padding:.35rem .9rem; border-radius:8px; transition:.15s; }
.stButton > button[data-nav]:hover { background:#334155; color:#fff; }
.stButton > button[data-nav-active] { background:#1A56DB; color:#fff !important; }
/* Alert level cards */
.alert-3 { border-left:4px solid #EF4444 !important; }
.alert-2 { border-left:4px solid #F97316 !important; }
.alert-1 { border-left:4px solid #EAB308 !important; }
/* potential card */
.pot-card { border-left:4px solid #10B981 !important; }
</style>
""", unsafe_allow_html=True)
init_db()

# ── Session state ──────────────────────────────────────────────────────────────
_D = {
    "page":              "home",
    "prev_page":         "home",
    "df":                None,
    "pending_upload":    None,   # {"label","filename","df","overlaps"} รอยืนยัน overlap
    "selected_user_id":  None,
    "selected_user":     None,
    "detail_cust_id":    None,
    "detail_cust_name":  None,
    "visit_plan":        None,
    "cal_month":         date.today().replace(day=1),
    "my_page_sub":       "overview",
    "risk_thresholds":   dict(DEFAULT_RISK_THRESHOLDS),
    "pot_df":            None,
    "risk_df":           None,
    "pot_page":          0,
    "risk_filter":       "ทั้งหมด",
}
for k, v in _D.items():
    if k not in st.session_state:
        st.session_state[k] = v

# Auto-load Master DB (combined all batches)
if st.session_state.df is None:
    try:
        combined = load_combined_df()
        if combined is not None:
            st.session_state.df = combined
    except Exception:
        pass

def _reload_master():
    """Reload combined df from all batches and reset caches."""
    try:
        combined = load_combined_df()
        st.session_state.df      = combined
        st.session_state.pot_df  = None
        st.session_state.risk_df = None
        st.session_state.visit_plan = None
    except Exception:
        pass

# ── Helpers ────────────────────────────────────────────────────────────────────
def go(page: str):
    st.session_state.prev_page = st.session_state.page
    st.session_state.page = page
    st.rerun()

def go_customer(cust_id, cust_name: str, origin: str):
    st.session_state.prev_page  = origin
    st.session_state.page       = "customer_detail"
    st.session_state.detail_cust_id   = cust_id
    st.session_state.detail_cust_name = cust_name
    st.rerun()

def df_for_user() -> pd.DataFrame:
    """Return df filtered to current user's portfolio (or full df if home)."""
    df = st.session_state.df
    if df is None:
        return pd.DataFrame()
    uid = st.session_state.selected_user_id
    if uid is None:
        return df
    custs = get_user_customers(uid)
    if not custs:
        return df.iloc[0:0]  # empty same schema
    col = S.CUST_NUM if S.CUST_NUM in df.columns else S.CUSTOMER
    return df[df[col].isin(custs)]

# ── Top Navigation ─────────────────────────────────────────────────────────────
def render_nav():
    pg = st.session_state.page
    cols = st.columns([3, 1, 1, 1, 1])
    with cols[0]:
        st.markdown('<div style="padding:.4rem 0;font-size:1.15rem;font-weight:700;color:#1E293B">⚡ <span style="color:#1A56DB">Sales</span>IQ</div>',
                    unsafe_allow_html=True)

    nav_items = [
        ("cols[1]", "🏠 หน้าหลัก", "home"),
        ("cols[2]", "👤 My Page",   "my_page"),
        ("cols[3]", "🗄️ Database",  "database"),
    ]
    for col_ref, label, target in nav_items:
        col_idx = int(col_ref.split("[")[1].rstrip("]"))
        with cols[col_idx]:
            active_style = "background:#1A56DB;color:#fff;border:none;" if pg == target else "border:1px solid #E2E8F0;"
            if st.button(label, key=f"nav_{target}",
                         use_container_width=True):
                go(target)

    # User selector (always visible)
    with cols[4]:
        users = list_users()
        opts  = {"— ภาพรวม —": None}
        opts.update({u["name"]: u["id"] for u in users})
        cur_name = st.session_state.selected_user or "— ภาพรวม —"
        sel = st.selectbox("👤", list(opts.keys()),
                           index=list(opts.keys()).index(cur_name) if cur_name in opts else 0,
                           label_visibility="collapsed",
                           key="nav_user_sel")
        if sel != (st.session_state.selected_user or "— ภาพรวม —"):
            st.session_state.selected_user    = sel if sel != "— ภาพรวม —" else None
            st.session_state.selected_user_id = opts[sel]
            st.session_state.visit_plan       = None
            st.rerun()

    st.markdown("<hr style='margin:.3rem 0 1rem;border-color:#E2E8F0'>", unsafe_allow_html=True)

render_nav()

# ═══════════════════════════════════════════════════════════════════════════════
#  SHARED COMPONENT: KPI + Charts overview
# ═══════════════════════════════════════════════════════════════════════════════
def render_overview_charts(df: pd.DataFrame):
    kpis = compute_kpis(df)
    k1,k2,k3,k4,k5 = st.columns(5)
    with k1: st.markdown(kpi_card("ยอดขายรวม",   f"฿{kpis['revenue']:,.0f}","💰"), unsafe_allow_html=True)
    with k2: st.markdown(kpi_card("กำไรสุทธิ",   f"฿{kpis['profit']:,.0f}", "📈"), unsafe_allow_html=True)
    with k3: st.markdown(kpi_card("Margin",       f"{kpis['margin']:.1f}%",  "🎯"), unsafe_allow_html=True)
    with k4: st.markdown(kpi_card("ลูกค้า Active",f"{kpis['customers']:,}", "🏪"), unsafe_allow_html=True)
    with k5: st.markdown(kpi_card("รายการซื้อ",  f"{kpis['orders']:,}",    "🧾"), unsafe_allow_html=True)
    st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    c1,c2 = st.columns([3,2])
    with c1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.plotly_chart(fig_weekly_revenue(weekly_trend(df)), use_container_width=True, config={"displayModeBar":False})
        st.markdown('</div>', unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        divdf = revenue_by_division(df)
        if not divdf.empty:
            st.plotly_chart(fig_division_bar(divdf), use_container_width=True, config={"displayModeBar":False})
        st.markdown('</div>', unsafe_allow_html=True)

    c3,c4 = st.columns(2)
    with c3:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.plotly_chart(fig_region_donut(revenue_by_region(df)), use_container_width=True, config={"displayModeBar":False})
        st.markdown('</div>', unsafe_allow_html=True)
    with c4:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        segdf = revenue_by_segment(df)
        if not segdf.empty:
            st.plotly_chart(fig_segment_donut(segdf), use_container_width=True, config={"displayModeBar":False})
        st.markdown('</div>', unsafe_allow_html=True)

    c5,c6 = st.columns([2,3])
    with c5:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        deptdf = revenue_by_dept(df)
        if not deptdf.empty:
            st.plotly_chart(fig_dept_bar(deptdf), use_container_width=True, config={"displayModeBar":False})
        st.markdown('</div>', unsafe_allow_html=True)
    with c6:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.plotly_chart(fig_top_customers(top_customers(df)), use_container_width=True, config={"displayModeBar":False})
        st.markdown('</div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  SHARED COMPONENT: Customer detail (full drill-down)
# ═══════════════════════════════════════════════════════════════════════════════
def render_customer_detail_panel(df_full: pd.DataFrame, cust_id, origin: str):
    cust_col = S.CUST_NUM if S.CUST_NUM in df_full.columns else S.CUSTOMER
    cdf = df_full[df_full[cust_col] == cust_id]
    if cdf.empty:
        st.warning("ไม่พบข้อมูลลูกค้า")
        return
    cust_name = cdf[S.CUSTOMER].iloc[0]
    region    = cdf[S.REGION].iloc[0] if S.REGION in cdf.columns else ""
    segment   = cdf[S.CUST_GROUP].iloc[0] if S.CUST_GROUP in cdf.columns else ""

    # Back button
    if st.button("← กลับ", key="back_btn"):
        st.session_state.page = origin
        st.rerun()

    st.markdown(f"""
    <div class="chart-card" style="display:flex;align-items:center;gap:1.5rem;padding:1.1rem 1.5rem;margin-bottom:.5rem">
      <div style="width:52px;height:52px;background:#EBF1FF;border-radius:14px;
           display:flex;align-items:center;justify-content:center;font-size:1.6rem;flex-shrink:0">🏪</div>
      <div style="flex:1">
        <div style="font-size:1.1rem;font-weight:700">{cust_name}</div>
        <div style="margin-top:.3rem">
          {badge(region,"blue") if region else ""}
          {badge(segment,"green") if segment else ""}
          {badge(f"ID: {cust_id}","gray")}
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    ck = compute_kpis(cdf)
    mk1,mk2,mk3,mk4 = st.columns(4)
    with mk1: st.markdown(kpi_card("ยอดซื้อรวม",  f"฿{ck['revenue']:,.0f}","💰"), unsafe_allow_html=True)
    with mk2: st.markdown(kpi_card("กำไร",        f"฿{ck['profit']:,.0f}", "📈"), unsafe_allow_html=True)
    with mk3: st.markdown(kpi_card("รายการซื้อ",  f"{ck['orders']:,}",    "🧾"), unsafe_allow_html=True)
    with mk4: st.markdown(kpi_card("สินค้าที่ซื้อ",f"{ck['items']:,} SKU","📦"), unsafe_allow_html=True)

    d1,d2 = st.columns([3,2])
    with d1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.plotly_chart(fig_customer_weekly(customer_weekly(df_full, cust_id)), use_container_width=True, config={"displayModeBar":False})
        st.markdown('</div>', unsafe_allow_html=True)
    with d2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.plotly_chart(fig_buying_days(customer_buying_days(df_full, cust_id)), use_container_width=True, config={"displayModeBar":False})
        st.markdown('</div>', unsafe_allow_html=True)

    d3,d4 = st.columns([3,2])
    with d3:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.plotly_chart(fig_top_items(customer_top_items(df_full, cust_id)), use_container_width=True, config={"displayModeBar":False})
        st.markdown('</div>', unsafe_allow_html=True)
    with d4:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        deptm = customer_dept_mix(df_full, cust_id)
        if not deptm.empty:
            st.plotly_chart(fig_dept_bar(deptm), use_container_width=True, config={"displayModeBar":False})
        st.markdown('</div>', unsafe_allow_html=True)

    # Recommendations
    rc1,rc2 = st.columns(2)
    with rc1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown("**🔄 สินค้าที่ซื้อประจำ (Top 10)**")
        items = customer_top_items(df_full, cust_id, n=10)
        if not items.empty:
            for _, r in items.iterrows():
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;padding:.3rem 0;'
                    f'border-bottom:1px solid #F1F5F9;font-size:.82rem">'
                    f'<span>📦 {r[S.ITEM]}</span>'
                    f'<span style="color:#1A56DB;font-weight:600">฿{r["Revenue"]:,.0f}</span></div>',
                    unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    with rc2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown("**✨ สินค้าแนะนำ (Collaborative Filtering)**")
        with st.spinner("กำลังวิเคราะห์…"):
            recs = recommend_items(df_full, cust_id, n=10)
        if recs:
            for rec in recs:
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;padding:.3rem 0;'
                    f'border-bottom:1px solid #F1F5F9;font-size:.82rem">'
                    f'<span>💡 {rec["item"]}</span>'
                    f'<span style="color:#059669;font-size:.72rem">ลูกค้าคล้ายกัน {rec["score"]} ราย</span></div>',
                    unsafe_allow_html=True)
        else:
            st.caption("ไม่พบลูกค้าที่ pattern คล้ายกันเพียงพอ")
        st.markdown('</div>', unsafe_allow_html=True)

    # History table
    st.markdown(section_header("ประวัติการซื้อล่าสุด", "📋"), unsafe_allow_html=True)
    show_cols = [c for c in [S.DATE, S.ITEM, S.DEPT, S.DIVISION, S.REVENUE, S.PROFIT] if c in cdf.columns]
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.dataframe(cdf[show_cols].sort_values(S.DATE, ascending=False).head(200),
                 use_container_width=True, height=300)
    st.markdown('</div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: CUSTOMER DETAIL
# ═══════════════════════════════════════════════════════════════════════════════
if st.session_state.page == "customer_detail":
    df_full = st.session_state.df
    if df_full is not None and st.session_state.detail_cust_id is not None:
        render_customer_detail_panel(df_full,
                                     st.session_state.detail_cust_id,
                                     st.session_state.prev_page)
    else:
        st.warning("ไม่พบข้อมูล")
        if st.button("← กลับ"):
            go("home")
    st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: HOME
# ═══════════════════════════════════════════════════════════════════════════════
if st.session_state.page == "home":
    df = st.session_state.df

    if df is None:
        st.markdown("""
        <div style="text-align:center;padding:4rem 2rem">
          <div style="font-size:3rem">⚡</div>
          <h1 style="font-size:2rem;font-weight:700">SalesIQ</h1>
          <p style="color:#64748B;margin-bottom:2rem">B2B Intelligence Platform สำหรับทีมขาย Makro</p>
          <div style="display:inline-block;background:#EBF1FF;border-radius:12px;padding:1rem 2rem;color:#1A56DB;font-weight:600">
            ไปที่ <b>🗄️ Database</b> เพื่ออัปโหลดข้อมูลเริ่มต้น
          </div>
        </div>""", unsafe_allow_html=True)
        c1,c2,c3 = st.columns([2,2,2])
        with c2:
            if st.button("🗄️ ไปที่ Database", use_container_width=True, type="primary"):
                go("database")
        st.stop()

    # ── Section 1: Overview ───────────────────────────────────────────────────
    st.markdown(section_header("ภาพรวมทั้งหมด", "📊"), unsafe_allow_html=True)
    render_overview_charts(df)

    # Channel mix
    cc1,cc2 = st.columns(2)
    with cc1:
        chandf = channel_mix(df)
        if not chandf.empty:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            st.plotly_chart(fig_channel_pie(chandf), use_container_width=True, config={"displayModeBar":False})
            st.markdown('</div>', unsafe_allow_html=True)
    with cc2:
        subdf = sub_channel_mix(df)
        if not subdf.empty:
            st.markdown('<div class="chart-card">', unsafe_allow_html=True)
            st.plotly_chart(fig_sub_channel(subdf), use_container_width=True, config={"displayModeBar":False})
            st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

    # ── Section 2: Potential Customers ───────────────────────────────────────
    st.markdown(section_header("🌟 ร้านค้า Potential 200 อันดับแรก", ""), unsafe_allow_html=True)
    st.caption("คัดจาก: ยอดขายเติบโต + ความถี่ซื้อเพิ่มขึ้น + กลุ่มสินค้าขยาย (Low base high growth ได้ bonus)")

    if st.button("🔄 คำนวณ Potential", key="calc_pot"):
        with st.spinner("กำลังวิเคราะห์…"):
            st.session_state.pot_df = identify_potential_customers(df, n=200)

    pot_df = st.session_state.pot_df
    if pot_df is None:
        st.info("กด **🔄 คำนวณ Potential** เพื่อวิเคราะห์ครั้งแรก")
    elif pot_df.empty:
        st.warning("ไม่พบข้อมูลเพียงพอสำหรับการวิเคราะห์ (ต้องการข้อมูลอย่างน้อย 60 วัน)")
    else:
        cust_col = S.CUST_NUM if S.CUST_NUM in pot_df.columns else S.CUSTOMER
        PAGE_SIZE = 20
        total_pages = max(1, (len(pot_df) - 1) // PAGE_SIZE + 1)
        pg = st.session_state.pot_page
        slice_df = pot_df.iloc[pg*PAGE_SIZE : (pg+1)*PAGE_SIZE]

        st.markdown(f'<div style="font-size:.78rem;color:#64748B;margin-bottom:.5rem">แสดง {pg*PAGE_SIZE+1}–{min((pg+1)*PAGE_SIZE, len(pot_df))} จาก {len(pot_df)} ราย</div>', unsafe_allow_html=True)

        for idx, row in slice_df.iterrows():
            rank = idx + 1
            growth_color = "#10B981" if row["Rev_Growth"] >= 0 else "#EF4444"
            st.markdown(f"""
            <div class="chart-card pot-card" style="display:flex;align-items:center;gap:1rem;padding:.75rem 1rem;margin-bottom:.4rem">
              <div style="font-size:1.1rem;font-weight:700;color:#94A3B8;min-width:2rem">#{rank}</div>
              <div style="flex:1">
                <div style="font-weight:600;font-size:.9rem">{row[S.CUSTOMER]}</div>
                <div style="font-size:.75rem;color:#64748B;margin-top:.15rem">
                  {badge(row.get(S.REGION,""),"blue") if row.get(S.REGION) else ""}
                  {badge(row.get(S.CUST_GROUP,""),"green") if row.get(S.CUST_GROUP) else ""}
                </div>
              </div>
              <div style="text-align:right;min-width:6rem">
                <div style="color:{growth_color};font-weight:700;font-size:.95rem">+{row["Rev_Growth"]:.1f}%</div>
                <div style="font-size:.72rem;color:#64748B">revenue growth</div>
              </div>
              <div style="text-align:right;min-width:5rem">
                <div style="font-weight:600;font-size:.88rem">฿{row["Rev_Recent"]:,.0f}</div>
                <div style="font-size:.72rem;color:#64748B">30 วันล่าสุด</div>
              </div>
              <div style="text-align:right;min-width:4rem">
                <div style="font-weight:700;color:#1A56DB">{row["Potential_Score"]:.0f}</div>
                <div style="font-size:.72rem;color:#64748B">score</div>
              </div>
            </div>""", unsafe_allow_html=True)

            if st.button("ดูรายละเอียด →", key=f"pot_detail_{idx}"):
                go_customer(row[cust_col], row[S.CUSTOMER], "home")

        # Pagination
        pc1,pc2,pc3 = st.columns([1,3,1])
        with pc1:
            if pg > 0 and st.button("◀ ก่อนหน้า", use_container_width=True):
                st.session_state.pot_page -= 1; st.rerun()
        with pc2:
            st.markdown(f"<div style='text-align:center;font-size:.8rem;color:#64748B;padding-top:.5rem'>หน้า {pg+1} / {total_pages}</div>", unsafe_allow_html=True)
        with pc3:
            if pg < total_pages - 1 and st.button("ถัดไป ▶", use_container_width=True):
                st.session_state.pot_page += 1; st.rerun()

    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

    # ── Section 3: At-Risk Customers ─────────────────────────────────────────
    st.markdown(section_header("⚠️ ลูกค้าที่น่าเป็นห่วง", ""), unsafe_allow_html=True)

    # Threshold settings
    with st.expander("⚙️ ปรับ Alert Threshold (คลิกเพื่อแก้ไข)"):
        t = st.session_state.risk_thresholds
        tc1,tc2,tc3 = st.columns(3)
        with tc1:
            st.markdown("**🟡 Level 1 — Watch**")
            t["L1_rev_drop"]  = st.slider("ยอดลด (%)",  5, 40, int(t["L1_rev_drop"]),  key="t_L1r")
            t["L1_gap_ratio"] = st.slider("ห่าง (x รอบ)", 1.0, 3.0, float(t["L1_gap_ratio"]), 0.1, key="t_L1g")
        with tc2:
            st.markdown("**🟠 Level 2 — Warning**")
            t["L2_rev_drop"]  = st.slider("ยอดลด (%)",  15, 60, int(t["L2_rev_drop"]), key="t_L2r")
            t["L2_gap_ratio"] = st.slider("ห่าง (x รอบ)", 1.5, 4.0, float(t["L2_gap_ratio"]), 0.1, key="t_L2g")
        with tc3:
            st.markdown("**🔴 Level 3 — Critical**")
            t["L3_rev_drop"]  = st.slider("ยอดลด (%)",  30, 90, int(t["L3_rev_drop"]), key="t_L3r")
            t["L3_gap_ratio"] = st.slider("ห่าง (x รอบ)", 2.0, 6.0, float(t["L3_gap_ratio"]), 0.1, key="t_L3g")
            t["no_buy_days"]  = st.slider("ไม่ซื้อ (วัน)", 30, 120, int(t["no_buy_days"]),     key="t_nbd")
        st.session_state.risk_thresholds = t
        rc1,rc2 = st.columns(2)
        with rc1:
            if st.button("🔄 Reset ค่าเริ่มต้น", use_container_width=True):
                st.session_state.risk_thresholds = dict(DEFAULT_RISK_THRESHOLDS)
                st.rerun()
        with rc2:
            if st.button("🔍 คำนวณ At-Risk ใหม่", type="primary", use_container_width=True):
                with st.spinner("กำลังวิเคราะห์…"):
                    st.session_state.risk_df = identify_at_risk_customers(df, st.session_state.risk_thresholds)

    if st.session_state.risk_df is None:
        if st.button("🔍 คำนวณ At-Risk", key="calc_risk"):
            with st.spinner("กำลังวิเคราะห์…"):
                st.session_state.risk_df = identify_at_risk_customers(df, st.session_state.risk_thresholds)
        st.info("กด **🔍 คำนวณ At-Risk** เพื่อวิเคราะห์")
    elif (isinstance(st.session_state.risk_df, pd.DataFrame) and st.session_state.risk_df.empty):
        st.success("✅ ไม่พบลูกค้าที่น่าเป็นห่วงในขณะนี้")
    else:
        risk_df = st.session_state.risk_df
        cust_col = S.CUST_NUM if S.CUST_NUM in risk_df.columns else S.CUSTOMER

        # Summary KPIs
        rk1,rk2,rk3 = st.columns(3)
        with rk1: st.markdown(kpi_card("🔴 Critical", str(len(risk_df[risk_df["Alert_Level"]==3])), "🔴"), unsafe_allow_html=True)
        with rk2: st.markdown(kpi_card("🟠 Warning",  str(len(risk_df[risk_df["Alert_Level"]==2])), "🟠"), unsafe_allow_html=True)
        with rk3: st.markdown(kpi_card("🟡 Watch",    str(len(risk_df[risk_df["Alert_Level"]==1])), "🟡"), unsafe_allow_html=True)

        # Filter
        risk_filter = st.radio("แสดง:", ["ทั้งหมด","🔴 Critical","🟠 Warning","🟡 Watch"],
                               horizontal=True, key="risk_filter_radio")
        show_risk = risk_df.copy()
        if risk_filter != "ทั้งหมด":
            lv = {"🔴 Critical":3, "🟠 Warning":2, "🟡 Watch":1}[risk_filter]
            show_risk = show_risk[show_risk["Alert_Level"] == lv]

        alert_cls = {3:"alert-3", 2:"alert-2", 1:"alert-1"}
        for idx, row in show_risk.iterrows():
            cls = alert_cls[row["Alert_Level"]]
            st.markdown(f"""
            <div class="chart-card {cls}" style="display:flex;align-items:center;gap:1rem;padding:.75rem 1rem;margin-bottom:.4rem">
              <div style="min-width:7rem;font-weight:700;font-size:.85rem">{row["Alert_Label"]}</div>
              <div style="flex:1">
                <div style="font-weight:600;font-size:.9rem">{row[S.CUSTOMER]}</div>
                <div style="font-size:.75rem;color:#64748B;margin-top:.1rem">
                  {badge(row.get(S.REGION,""),"blue") if row.get(S.REGION) else ""}
                  ซื้อล่าสุด: {row["Last_Buy"]}
                </div>
              </div>
              <div style="text-align:right;min-width:6rem">
                <div style="color:#EF4444;font-weight:700">-{row["Rev_Drop_Pct"]:.0f}%</div>
                <div style="font-size:.72rem;color:#64748B">ยอดลด</div>
              </div>
              <div style="text-align:right;min-width:5rem">
                <div style="font-weight:600">{row["Days_Since"]} วัน</div>
                <div style="font-size:.72rem;color:#64748B">ไม่ซื้อ</div>
              </div>
              <div style="font-size:.75rem;color:#64748B;max-width:10rem">{row["Risk_Reason"]}</div>
            </div>""", unsafe_allow_html=True)

            if st.button("ดูรายละเอียด →", key=f"risk_detail_{idx}"):
                go_customer(row[cust_col], row[S.CUSTOMER], "home")

    st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: MY PAGE
# ═══════════════════════════════════════════════════════════════════════════════
if st.session_state.page == "my_page":
    uid = st.session_state.selected_user_id
    uname = st.session_state.selected_user

    if uid is None:
        st.info("เลือกชื่อของคุณจาก dropdown **👤** มุมขวาบนก่อนครับ")
        st.stop()

    # Sub navigation
    sub = st.session_state.my_page_sub
    sc1,sc2,sc3 = st.columns(3)
    for col, label, key in [(sc1,"📊 ภาพรวม","overview"),(sc2,"🔍 รายลูกค้า","customers"),(sc3,"📋 Portfolio + แผนเยี่ยม","portfolio")]:
        with col:
            btn_type = "primary" if sub == key else "secondary"
            if st.button(label, key=f"sub_{key}", use_container_width=True, type=btn_type):
                st.session_state.my_page_sub = key
                st.rerun()
    st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    df_user = df_for_user()
    df_full = st.session_state.df

    # ── Sub: Overview ──────────────────────────────────────────────────────
    if st.session_state.my_page_sub == "overview":
        if df_user.empty:
            st.info("ยังไม่มีลูกค้าในพอร์ต — ไปที่ **📋 Portfolio** เพื่อเพิ่มลูกค้าครับ")
        else:
            st.markdown(section_header(f"ภาพรวมพอร์ต — {uname}", "📊"), unsafe_allow_html=True)
            render_overview_charts(df_user)

    # ── Sub: Customers ─────────────────────────────────────────────────────
    elif st.session_state.my_page_sub == "customers":
        if df_user.empty:
            st.info("ยังไม่มีลูกค้าในพอร์ต")
        else:
            st.markdown(section_header("รายลูกค้าในพอร์ต", "🔍"), unsafe_allow_html=True)
            cust_col = S.CUST_NUM if S.CUST_NUM in df_user.columns else S.CUSTOMER
            cust_df = (df_user.groupby([cust_col, S.CUSTOMER])
                              .agg(Rev=(S.REVENUE,"sum"))
                              .reset_index().sort_values("Rev",ascending=False))
            search = st.text_input("🔎 ค้นหา", placeholder="พิมพ์ชื่อลูกค้า…")
            if search:
                cust_df = cust_df[cust_df[S.CUSTOMER].str.contains(search, case=False, na=False)]
            for _, row in cust_df.iterrows():
                c1,c2,c3 = st.columns([5,2,1])
                with c1:
                    st.markdown(f"**{row[S.CUSTOMER]}**")
                with c2:
                    st.markdown(f"฿{row['Rev']:,.0f}")
                with c3:
                    if st.button("ดู →", key=f"my_cust_{row[cust_col]}"):
                        go_customer(row[cust_col], row[S.CUSTOMER], "my_page")

    # ── Sub: Portfolio + Visit Plan ────────────────────────────────────────
    elif st.session_state.my_page_sub == "portfolio":
        if df_full is None:
            st.warning("ยังไม่มีข้อมูล")
        else:
            cust_col = S.CUST_NUM if S.CUST_NUM in df_full.columns else S.CUSTOMER
            all_custs = (df_full.groupby([cust_col, S.CUSTOMER])
                                .agg(
                                    Rev=(S.REVENUE,"sum"),
                                    Orders=(S.DATE,"count"),
                                    LastBuy=(S.DATE,"max"),
                                    Depts=(S.DEPT,"nunique") if S.DEPT in df_full.columns else (S.REVENUE,"count"),
                                )
                                .reset_index()
                                .sort_values("Rev",ascending=False))
            all_custs["LastBuy"] = pd.to_datetime(all_custs["LastBuy"]).dt.strftime("%d %b %Y")

            current_custs = get_user_customers(uid)
            all_custs["In_Portfolio"] = all_custs[cust_col].isin(current_custs)

            st.markdown(section_header("จัดการ Portfolio", "📋"), unsafe_allow_html=True)
            st.caption(f"มี {len(current_custs)} ลูกค้าในพอร์ตของคุณ | ทั้งหมด {len(all_custs)} ราย")

            # Filter controls
            pf1,pf2,pf3 = st.columns(3)
            with pf1:
                port_filter = st.radio("แสดง:", ["ทั้งหมด","ในพอร์ต","นอกพอร์ต"], horizontal=True, key="port_view")
            with pf2:
                port_search = st.text_input("🔎 ค้นหา", placeholder="ชื่อลูกค้า…", key="port_search")
            with pf3:
                region_list = ["ทั้งหมด"] + sorted(df_full[S.REGION].dropna().unique().tolist()) if S.REGION in df_full.columns else ["ทั้งหมด"]
                port_region = st.selectbox("Region", region_list, key="port_region")

            disp = all_custs.copy()
            if port_filter == "ในพอร์ต": disp = disp[disp["In_Portfolio"]]
            elif port_filter == "นอกพอร์ต": disp = disp[~disp["In_Portfolio"]]
            if port_search:
                disp = disp[disp[S.CUSTOMER].str.contains(port_search, case=False, na=False)]
            if port_region != "ทั้งหมด" and S.REGION in df_full.columns:
                region_map = df_full.groupby(cust_col)[S.REGION].first()
                disp = disp[disp[cust_col].map(region_map) == port_region]

            # Column headers
            hc = st.columns([3,2,1,1,1,1])
            for h,c in zip(["ลูกค้า","ยอดขายรวม","คำสั่งซื้อ","ซื้อล่าสุด","",""],hc):
                c.markdown(f"**{h}**" if h else "")

            for _, row in disp.head(100).iterrows():
                rc = st.columns([3,2,1,1,1,1])
                in_port = row["In_Portfolio"]
                with rc[0]:
                    st.markdown(f"{'🟢 ' if in_port else ''}{row[S.CUSTOMER]}")
                with rc[1]:
                    st.markdown(f"฿{row['Rev']:,.0f}")
                with rc[2]:
                    st.markdown(str(int(row["Orders"])))
                with rc[3]:
                    st.markdown(str(row["LastBuy"]))
                with rc[4]:
                    btn_label = "➖ ออก" if in_port else "➕ เข้า"
                    btn_type  = "secondary" if in_port else "primary"
                    if st.button(btn_label, key=f"toggle_{row[cust_col]}", use_container_width=True, type=btn_type):
                        if in_port:
                            new_list = [c for c in current_custs if c != str(row[cust_col])]
                        else:
                            new_list = list(current_custs) + [str(row[cust_col])]
                        set_user_customers(uid, new_list)
                        st.rerun()
                with rc[5]:
                    if st.button("ดู →", key=f"port_detail_{row[cust_col]}"):
                        go_customer(row[cust_col], row[S.CUSTOMER], "my_page")

            # Visit plan section
            st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
            st.markdown(section_header("แผนการเยี่ยม", "📅"), unsafe_allow_html=True)

            if df_user.empty:
                st.info("เพิ่มลูกค้าเข้าพอร์ตก่อนเพื่อสร้างแผน")
            else:
                vp1,vp2,vp3,vp4 = st.columns([2,2,2,2])
                with vp1: horizon = st.selectbox("ช่วงเวลา", [14,21,30,45], index=2, format_func=lambda v:f"{v} วัน")
                with vp2: vpw = st.number_input("ลูกค้า/อาทิตย์", 1, 100, 20, key="vpw")
                with vp3: vpd = st.number_input("ลูกค้า/วัน",     1,  20,  5, key="vpd")
                with vp4:
                    st.markdown("<div style='height:1.7rem'></div>", unsafe_allow_html=True)
                    if st.button("🤖 สร้างแผน AI", type="primary", use_container_width=True):
                        with st.spinner("กำลังวิเคราะห์…"):
                            plan = generate_visit_plan_constrained(df_user, horizon, vpw, vpd)
                            st.session_state.visit_plan = plan
                            if not plan.empty:
                                st.success(f"✅ {len(plan)} การเยี่ยม ใน {plan[S.CUSTOMER].nunique()} ลูกค้า")

                if (st.session_state.visit_plan is not None and
                        not st.session_state.visit_plan.empty):
                    plan = st.session_state.visit_plan
                    plan_show = plan.copy()
                    plan_show["Visit_Date"] = plan_show["Visit_Date"].dt.strftime("%d %b %Y")
                    disp_cols = [c for c in ["Visit_Date",S.CUSTOMER,S.REGION,"Priority","Reason","Items_To_Discuss"] if c in plan_show.columns]
                    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
                    st.dataframe(plan_show[disp_cols], use_container_width=True, height=320)
                    st.markdown('</div>', unsafe_allow_html=True)
                    st.download_button("⬇️ Export Excel",
                                       data=export_visit_plan(plan),
                                       file_name="visit_plan.xlsx",
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: DATABASE
# ═══════════════════════════════════════════════════════════════════════════════
if st.session_state.page == "database":
    st.markdown(section_header("ฐานข้อมูลกลาง (Master DB)", "🗄️"), unsafe_allow_html=True)

    # ── Master DB Summary ────────────────────────────────────────────────────
    batches = list_batches()
    if batches and st.session_state.df is not None:
        mdf = st.session_state.df
        total_rows  = len(mdf)
        total_custs = mdf[S.CUST_NUM].nunique() if S.CUST_NUM in mdf.columns else mdf[S.CUSTOMER].nunique()
        date_min    = batches[0].get("date_min","–")
        date_max    = batches[-1].get("date_max","–")

        st.markdown('<div class="chart-card" style="border-left:4px solid #1A56DB">', unsafe_allow_html=True)
        sm1,sm2,sm3,sm4 = st.columns(4)
        with sm1: st.markdown(kpi_card("Batch ทั้งหมด", str(len(batches)), "📦"), unsafe_allow_html=True)
        with sm2: st.markdown(kpi_card("แถวข้อมูลรวม", f"{total_rows:,}", "🔢"), unsafe_allow_html=True)
        with sm3: st.markdown(kpi_card("ลูกค้าทั้งหมด", f"{total_custs:,}", "🏪"), unsafe_allow_html=True)
        with sm4: st.markdown(kpi_card("ช่วงข้อมูล", f"{date_min[:7]} → {date_max[:7]}", "📅"), unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # Timeline visual
        st.markdown("<div style='margin:.75rem 0 .3rem;font-size:.82rem;font-weight:600;color:#374151'>📅 Timeline ของข้อมูลที่มีอยู่</div>", unsafe_allow_html=True)
        timeline_html = '<div style="display:flex;gap:.3rem;flex-wrap:wrap;margin-bottom:.75rem">'
        for b in batches:
            dr = f"{b.get('date_min','')[:7]} → {b.get('date_max','')[:7]}" if b.get('date_min') else "ไม่ระบุ"
            timeline_html += (f'<div style="background:#EBF1FF;border:1px solid #BFDBFE;border-radius:6px;'
                              f'padding:.25rem .65rem;font-size:.75rem;color:#1A56DB">'
                              f'<b>{b["label"]}</b> {dr}</div>')
        timeline_html += '</div>'
        st.markdown(timeline_html, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    # ── Overlap warning / confirmation UI ────────────────────────────────────
    if st.session_state.pending_upload is not None:
        pu = st.session_state.pending_upload
        st.warning(f"⚠️ ไฟล์ **{pu['filename']}** มีช่วงวันที่ทับซ้อนกับ batch ที่มีอยู่แล้ว:")
        for ov in pu["overlaps"]:
            st.markdown(f"- **{ov['label']}** ({ov.get('date_min','')[:7]} → {ov.get('date_max','')[:7]}) — {ov['row_count']:,} แถว")
        st.markdown("ถ้ายืนยัน ระบบจะ **ลบ batch เดิมที่ทับซ้อน** แล้วใส่ batch ใหม่แทน")
        cc1,cc2 = st.columns(2)
        with cc1:
            if st.button("✅ ยืนยัน Replace", type="primary", use_container_width=True, key="confirm_replace"):
                with st.spinner("กำลัง replace…"):
                    for ov in pu["overlaps"]:
                        delete_batch(ov["id"])
                    save_batch(pu["label"], pu["filename"], pu["df"])
                    st.session_state.pending_upload = None
                    _reload_master()
                st.success("✅ Replace สำเร็จ — Master DB อัปเดตแล้ว")
                st.rerun()
        with cc2:
            if st.button("✖️ ยกเลิก", use_container_width=True, key="cancel_replace"):
                st.session_state.pending_upload = None
                st.rerun()
        st.divider()

    # ── Upload new batch ─────────────────────────────────────────────────────
    with st.expander("➕ เพิ่ม Batch ใหม่เข้า Master DB", expanded=(len(batches)==0)):
        ul1,ul2 = st.columns([3,1])
        with ul1:
            new_label = st.text_input("ชื่อ Batch", placeholder="เช่น ม.ค. 2026, มี.ค. 2026, Q1 2026")
        new_file = st.file_uploader("Excel (.xlsx) หรือ CSV",
                                    type=["xlsx","xls","csv"],
                                    label_visibility="collapsed", key="db_up")
        if new_file:
            auto_label = new_label.strip() or new_file.name.rsplit(".",1)[0]
            if st.button("🔍 ตรวจสอบและบันทึก", type="primary", use_container_width=True, key="db_save"):
                with st.spinner("กำลังประมวลผล…"):
                    raw       = load_raw_file(new_file)
                    df_new, warns = clean_data(raw)
                    for w in warns: st.warning(w)
                    date_min, date_max = (str(df_new["Date"].min().date()), str(df_new["Date"].max().date())) if "Date" in df_new.columns else (None, None)
                    overlaps  = check_date_overlap(date_min, date_max) if date_min else []

                if overlaps:
                    st.session_state.pending_upload = {
                        "label": auto_label, "filename": new_file.name,
                        "df": df_new, "overlaps": overlaps,
                    }
                    st.rerun()
                else:
                    with st.spinner("กำลังบันทึก…"):
                        save_batch(auto_label, new_file.name, df_new)
                        _reload_master()
                    st.success(f"✅ เพิ่ม **{auto_label}** สำเร็จ — {len(df_new):,} แถว | Master DB อัปเดตแล้ว")
                    st.rerun()

    st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    # ── Batch list ───────────────────────────────────────────────────────────
    batches = list_batches()
    if not batches:
        st.info("ยังไม่มี Batch — อัปโหลดไฟล์ด้านบนเพื่อเริ่มต้นครับ")
    else:
        st.markdown(f"**{len(batches)} Batch** ใน Master DB (เรียงตามวันที่เก่า → ใหม่)")
        for b in batches:
            dr = f"{b.get('date_min','')[:10]} → {b.get('date_max','')[:10]}" if b.get("date_min") else "ไม่ระบุช่วงวัน"
            st.markdown('<div class="chart-card" style="padding:.8rem 1.1rem;margin-bottom:.4rem">', unsafe_allow_html=True)
            bl,br = st.columns([5,3])
            with bl:
                st.markdown(f"📦 **{b['label']}**")
                st.caption(f"📁 {b['filename']}  |  🔢 {b['row_count']:,} แถว  |  📅 {dr}  |  ⏱ {b['uploaded_at'][:16]}")
            with br:
                bc1,bc2 = st.columns(2)
                with bc1:
                    with st.popover("✏️ เปลี่ยนชื่อ"):
                        nn = st.text_input("ชื่อใหม่", value=b["label"], key=f"ren_{b['id']}")
                        if st.button("บันทึก", key=f"rs_{b['id']}"):
                            rename_batch(b["id"], nn); st.rerun()
                with bc2:
                    with st.popover("🗑️ ลบ Batch นี้"):
                        st.markdown(f"ลบ **{b['label']}** ออกจาก Master DB?")
                        st.caption("ข้อมูลในช่วงนี้จะหายไปจาก Master DB ทั้งหมด")
                        if st.button("ยืนยันลบ", key=f"del_{b['id']}", type="primary"):
                            with st.spinner("กำลังลบและอัปเดต Master DB…"):
                                delete_batch(b["id"])
                                _reload_master()
                            st.success(f"✅ ลบ {b['label']} แล้ว — Master DB อัปเดต")
                            st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

    # ── User / Salesperson management ────────────────────────────────────────
    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
    st.markdown(section_header("จัดการชื่อเซลล์", "👤"), unsafe_allow_html=True)
    with st.expander("➕ เพิ่มเซลล์ใหม่"):
        nc1,nc2 = st.columns([4,1])
        with nc1: nu_name = st.text_input("ชื่อ", placeholder="เช่น นิค, ทีม A", key="nu_name")
        with nc2:
            st.markdown("<div style='height:1.7rem'></div>", unsafe_allow_html=True)
            if st.button("เพิ่ม", type="primary", use_container_width=True, key="add_user_db"):
                if nu_name.strip():
                    save_user(nu_name.strip()); st.rerun()
    for u in list_users():
        uc1,uc2 = st.columns([5,1])
        with uc1: st.markdown(f"👤 **{u['name']}** — {len(get_user_customers(u['id']))} ลูกค้า")
        with uc2:
            with st.popover("🗑️"):
                st.markdown(f"ลบ {u['name']}?")
                if st.button("ยืนยัน", key=f"del_u_{u['id']}", type="primary"):
                    delete_user(u["id"]); st.rerun()
    st.stop()
