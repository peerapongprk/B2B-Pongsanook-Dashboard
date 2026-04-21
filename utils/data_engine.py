"""utils/data_engine.py — Data cleaning, KPI computation, Visit Plan algorithm."""
from __future__ import annotations
import pandas as pd
import numpy as np
from datetime import date, timedelta
from database.schema import MAKRO as S

DOW_TH = ["จ","อ","พ","พฤ","ศ","ส","อา"]
DOW_EN = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

# ── File loading ──────────────────────────────────────────────────────────────
def load_raw_file(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    return pd.read_excel(uploaded_file)

# ── Cleaning ──────────────────────────────────────────────────────────────────
def clean_data(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    warnings = []
    df = df.copy()

    # Normalise column names via alias map
    alias = {k.lower(): v for k, v in S.ALIASES.items()}
    rename = {}
    for col in df.columns:
        mapped = alias.get(col.lower().strip())
        if mapped and col != mapped:
            rename[col] = mapped
    if rename:
        df.rename(columns=rename, inplace=True)

    # Parse date
    if S.DATE in df.columns:
        df[S.DATE] = pd.to_datetime(df[S.DATE], errors="coerce")
        bad = df[S.DATE].isna().sum()
        if bad:
            warnings.append(f"⚠️ ลบ {bad:,} แถวที่วันที่ไม่ถูกต้อง")
        df = df[df[S.DATE].notna()]

    # Numeric columns
    for col in [S.REVENUE, S.PROFIT]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Derived columns
    if S.DATE in df.columns:
        df[S.WEEK]     = df[S.DATE].dt.to_period("W").apply(lambda p: p.start_time)
        df[S.DOW]      = df[S.DATE].dt.dayofweek
        df[S.DOW_NAME] = df[S.DOW].map(lambda d: DOW_TH[d])

    # Check required columns
    missing = [c for c in S.REQUIRED_AFTER_CLEAN if c not in df.columns]
    if missing:
        warnings.append(f"⚠️ ไม่พบคอลัมน์: {', '.join(missing)}")

    return df.reset_index(drop=True), warnings

# ── KPIs ──────────────────────────────────────────────────────────────────────
def compute_kpis(df: pd.DataFrame) -> dict:
    rev   = df[S.REVENUE].sum()
    profit = df[S.PROFIT].sum() if S.PROFIT in df.columns else 0
    orders = len(df)
    custs  = df[S.CUST_NUM].nunique() if S.CUST_NUM in df.columns else df[S.CUSTOMER].nunique()
    items  = df[S.ITEM].nunique() if S.ITEM in df.columns else 0
    margin = (profit / rev * 100) if rev else 0
    aov    = rev / orders if orders else 0
    return dict(revenue=rev, profit=profit, orders=orders,
                customers=custs, items=items, margin=margin, aov=aov)

def revenue_by_region(df: pd.DataFrame) -> pd.DataFrame:
    return (df.groupby(S.REGION)[S.REVENUE].sum()
              .reset_index().sort_values(S.REVENUE, ascending=False))

def revenue_by_division(df: pd.DataFrame) -> pd.DataFrame:
    if S.DIVISION not in df.columns:
        return pd.DataFrame()
    return (df.groupby(S.DIVISION)[S.REVENUE].sum()
              .reset_index().sort_values(S.REVENUE, ascending=False))

def revenue_by_dept(df: pd.DataFrame) -> pd.DataFrame:
    if S.DEPT not in df.columns:
        return pd.DataFrame()
    return (df.groupby(S.DEPT)[S.REVENUE].sum()
              .reset_index().sort_values(S.REVENUE, ascending=False))

def revenue_by_segment(df: pd.DataFrame) -> pd.DataFrame:
    if S.CUST_GROUP not in df.columns:
        return pd.DataFrame()
    return (df.groupby(S.CUST_GROUP)[S.REVENUE].sum()
              .reset_index().sort_values(S.REVENUE, ascending=False))

def top_customers(df: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    grp_cols = [S.CUST_NUM, S.CUSTOMER] if S.CUST_NUM in df.columns else [S.CUSTOMER]
    if S.REGION in df.columns:
        grp_cols.append(S.REGION)
    if S.CUST_GROUP in df.columns:
        grp_cols.append(S.CUST_GROUP)
    return (df.groupby(grp_cols)
              .agg(Revenue=(S.REVENUE,"sum"), Orders=(S.DATE,"count"))
              .reset_index()
              .sort_values("Revenue", ascending=False)
              .head(n))

def weekly_trend(df: pd.DataFrame) -> pd.DataFrame:
    return (df.groupby(S.WEEK)
              .agg(Revenue=(S.REVENUE,"sum"), Orders=(S.DATE,"count"))
              .reset_index()
              .sort_values(S.WEEK))

def channel_mix(df: pd.DataFrame) -> pd.DataFrame:
    if S.CHANNEL_FLAG not in df.columns:
        return pd.DataFrame()
    return (df.groupby(S.CHANNEL_FLAG)[S.REVENUE].sum()
              .reset_index().rename(columns={S.CHANNEL_FLAG:"Channel"}))

def sub_channel_mix(df: pd.DataFrame) -> pd.DataFrame:
    if S.SUB_CHANNEL not in df.columns:
        return pd.DataFrame()
    return (df.groupby(S.SUB_CHANNEL)[S.REVENUE].sum()
              .reset_index()
              .sort_values(S.REVENUE, ascending=False))

# ── Customer drill-down ───────────────────────────────────────────────────────
def customer_weekly(df: pd.DataFrame, cust_num) -> pd.DataFrame:
    mask = df[S.CUST_NUM] == cust_num if S.CUST_NUM in df.columns else df[S.CUSTOMER] == cust_num
    return weekly_trend(df[mask])

def customer_buying_days(df: pd.DataFrame, cust_num) -> pd.DataFrame:
    mask = df[S.CUST_NUM] == cust_num if S.CUST_NUM in df.columns else df[S.CUSTOMER] == cust_num
    cdf  = df[mask]
    days = (cdf.groupby(S.DOW)[S.DATE]
               .count()
               .reset_index()
               .rename(columns={S.DATE: "Count"}))
    days[S.DOW_NAME] = days[S.DOW].map(lambda d: DOW_TH[d])
    return days.sort_values(S.DOW)

def customer_top_items(df: pd.DataFrame, cust_num, n: int = 15) -> pd.DataFrame:
    mask = df[S.CUST_NUM] == cust_num if S.CUST_NUM in df.columns else df[S.CUSTOMER] == cust_num
    return (df[mask].groupby(S.ITEM)
                    .agg(Revenue=(S.REVENUE,"sum"), Orders=(S.DATE,"count"))
                    .reset_index()
                    .sort_values("Revenue", ascending=False)
                    .head(n))

def customer_dept_mix(df: pd.DataFrame, cust_num) -> pd.DataFrame:
    if S.DEPT not in df.columns:
        return pd.DataFrame()
    mask = df[S.CUST_NUM] == cust_num if S.CUST_NUM in df.columns else df[S.CUSTOMER] == cust_num
    return (df[mask].groupby(S.DEPT)[S.REVENUE].sum()
                    .reset_index().sort_values(S.REVENUE, ascending=False))

# ── Visit Plan algorithm ──────────────────────────────────────────────────────
def generate_visit_plan(df: pd.DataFrame, horizon_days: int = 30) -> pd.DataFrame:
    """
    For each customer:
    1. Compute median purchase gap per item → predict next OOS date
    2. Snap visit to customer's top buying day(s) within ±3 days
    3. Add cadence visits every ~14 days if no replenishment visit nearby
    Returns a DataFrame with one visit row per recommended visit.
    """
    today     = pd.Timestamp(date.today())
    horizon   = today + pd.Timedelta(days=horizon_days)
    records   = []

    cust_id_col = S.CUST_NUM if S.CUST_NUM in df.columns else S.CUSTOMER

    for cust_num, cdf in df.groupby(cust_id_col):
        cust_name   = cdf[S.CUSTOMER].iloc[0]
        region      = cdf[S.REGION].iloc[0] if S.REGION in cdf.columns else ""
        cust_group  = cdf[S.CUST_GROUP].iloc[0] if S.CUST_GROUP in cdf.columns else ""

        # Preferred buying days (most common weekdays)
        dow_counts   = cdf.groupby(S.DOW)[S.DATE].count().sort_values(ascending=False)
        pref_days    = dow_counts.index.tolist()[:2]  # top-2 weekdays

        # Per-item replenishment prediction
        oos_items: list[tuple] = []   # (oos_date, item_name)
        for item, idf in cdf.groupby(S.ITEM):
            dates = idf[S.DATE].drop_duplicates().sort_values()
            if len(dates) < 2:
                continue
            gaps = dates.diff().dt.days.dropna()
            cycle = gaps.median()
            if cycle <= 0:
                continue
            last = dates.iloc[-1]
            predicted_oos = last + pd.Timedelta(days=cycle)
            if today <= predicted_oos <= horizon + pd.Timedelta(days=7):
                oos_items.append((predicted_oos, item))

        # Group OOS items that fall within ±3 days of each other
        visit_groups: dict[pd.Timestamp, list] = {}
        for oos_date, item in sorted(oos_items):
            visit_target = oos_date - pd.Timedelta(days=2)
            # Snap to preferred buying day within ±3 days
            best_day = visit_target
            best_dist = 999
            for d in range(-3, 4):
                candidate = visit_target + pd.Timedelta(days=d)
                if candidate.dayofweek in pref_days:
                    if abs(d) < best_dist:
                        best_day  = candidate
                        best_dist = abs(d)
            visit_day = best_day if best_dist < 999 else visit_target
            visit_day = max(visit_day, today + pd.Timedelta(days=1))

            # Merge into existing group if within 2 days
            merged = False
            for existing in list(visit_groups.keys()):
                if abs((visit_day - existing).days) <= 2:
                    visit_groups[existing].append(item)
                    merged = True
                    break
            if not merged:
                visit_groups[visit_day] = [item]

        # Create visit records from groups
        for visit_day, items in visit_groups.items():
            if visit_day > horizon:
                continue
            priority = "High" if len(items) >= 2 else "Medium"
            records.append({
                cust_id_col:     cust_num,
                S.CUSTOMER:      cust_name,
                S.REGION:        region,
                S.CUST_GROUP:    cust_group,
                "Visit_Date":    visit_day,
                "Priority":      priority,
                "Reason":        "Replenishment",
                "Items_To_Discuss": ", ".join(items[:3]) + ("…" if len(items)>3 else ""),
                "Source":        "AI",
            })

        # Cadence fill: add a baseline visit every 14 days if no visit nearby
        if not records or all(r[S.CUSTOMER] != cust_name for r in records):
            cadence_date = today + pd.Timedelta(days=7)
            while cadence_date <= horizon:
                snap = cadence_date
                for d in range(0, 7):
                    cand = cadence_date + pd.Timedelta(days=d)
                    if cand.dayofweek in pref_days:
                        snap = cand; break
                snap = max(snap, today + pd.Timedelta(days=1))
                if snap <= horizon:
                    records.append({
                        cust_id_col:     cust_num,
                        S.CUSTOMER:      cust_name,
                        S.REGION:        region,
                        S.CUST_GROUP:    cust_group,
                        "Visit_Date":    snap,
                        "Priority":      "Low",
                        "Reason":        "Cadence",
                        "Items_To_Discuss": "",
                        "Source":        "AI",
                    })
                cadence_date += pd.Timedelta(days=14)

    if not records:
        return pd.DataFrame()
    plan = pd.DataFrame(records)
    plan["Visit_Date"] = pd.to_datetime(plan["Visit_Date"])
    return plan.sort_values("Visit_Date").reset_index(drop=True)

# ── Product recommendations (collaborative filtering) ─────────────────────────
def recommend_items(df: pd.DataFrame, cust_num, n: int = 8) -> list[dict]:
    """
    Collaborative filtering:
    1. หาลูกค้าที่มี purchase overlap กับ target ≥ 2 รายการ
    2. สินค้าที่ลูกค้าคล้ายกันซื้อ แต่ target ยังไม่ซื้อ = recommendation
    Returns list of {item, score, market_revenue}
    """
    cust_col = S.CUST_NUM if S.CUST_NUM in df.columns else S.CUSTOMER
    target_mask  = df[cust_col] == cust_num
    target_items = set(df[target_mask][S.ITEM].unique())
    if not target_items:
        return []

    similar: list[tuple] = []
    for other, odf in df[~target_mask].groupby(cust_col):
        other_items = set(odf[S.ITEM].unique())
        overlap = len(target_items & other_items)
        if overlap >= 2:
            similar.append((overlap, other_items))

    if not similar:
        return []

    similar.sort(key=lambda x: -x[0])
    item_scores: dict = {}
    for overlap_score, other_items in similar[:20]:
        for item in (other_items - target_items):
            item_scores[item] = item_scores.get(item, 0) + overlap_score

    top = sorted(item_scores.items(), key=lambda x: -x[1])[:n]
    results = []
    for item, score in top:
        mkt_rev = df[df[S.ITEM] == item][S.REVENUE].sum()
        results.append({"item": item, "score": score, "market_revenue": mkt_rev})
    return results


# ── Visit Plan with daily/weekly constraints ──────────────────────────────────
def generate_visit_plan_constrained(
    df: pd.DataFrame,
    horizon_days: int = 30,
    visits_per_week: int = 20,
    visits_per_day: int  = 5,
) -> pd.DataFrame:
    """
    Generate visit plan respecting capacity constraints.
    Priority: High → Medium → Low → Cadence
    Slides visits forward day-by-day until a slot opens.
    """
    raw = generate_visit_plan(df, horizon_days)
    if raw.empty:
        return raw

    today   = pd.Timestamp(date.today())
    horizon = today + pd.Timedelta(days=horizon_days)

    pri_order = {"High": 0, "Medium": 1, "Low": 2, "Cadence": 3}
    raw["_p"] = raw["Priority"].map(pri_order).fillna(4)
    raw = raw.sort_values(["_p", "Visit_Date"]).drop(columns=["_p"]).reset_index(drop=True)

    day_used:  dict = {}   # date → count
    week_used: dict = {}   # "YYYY-WW" → count
    assigned:  list = []

    for _, row in raw.iterrows():
        placed = False
        for offset in range(horizon_days + 7):
            candidate = row["Visit_Date"] + pd.Timedelta(days=offset)
            if candidate > horizon:
                break
            wk = candidate.strftime("%Y-%W")
            if (day_used.get(candidate, 0) < visits_per_day and
                    week_used.get(wk, 0) < visits_per_week):
                r = row.copy()
                r["Visit_Date"] = candidate
                assigned.append(r)
                day_used[candidate]  = day_used.get(candidate, 0) + 1
                week_used[wk]        = week_used.get(wk, 0) + 1
                placed = True
                break
        # If no slot found within horizon, skip

    if not assigned:
        return pd.DataFrame()
    result = pd.DataFrame(assigned)
    result["Visit_Date"] = pd.to_datetime(result["Visit_Date"])
    return result.sort_values("Visit_Date").reset_index(drop=True)

# ── Potential Customer Scoring ────────────────────────────────────────────────
def identify_potential_customers(df: pd.DataFrame, n: int = 200) -> pd.DataFrame:
    """
    Score every customer on 3 signals:
      1. Revenue growth   : recent 30d vs previous 30d (%)
      2. Frequency growth : purchases/week recent vs historical
      3. Dept breadth     : unique depts in recent 30d vs prior
    Composite score → top-n returned, sorted descending.
    Low-base high-growth customers get a bonus multiplier.
    """
    today   = df[S.DATE].max()
    cut30   = today - pd.Timedelta(days=30)
    cut60   = today - pd.Timedelta(days=60)
    cust_col = S.CUST_NUM if S.CUST_NUM in df.columns else S.CUSTOMER

    records = []
    for cust_id, cdf in df.groupby(cust_col):
        cust_name  = cdf[S.CUSTOMER].iloc[0]
        region     = cdf[S.REGION].iloc[0] if S.REGION in cdf.columns else ""
        segment    = cdf[S.CUST_GROUP].iloc[0] if S.CUST_GROUP in cdf.columns else ""

        recent = cdf[cdf[S.DATE] >= cut30]
        prior  = cdf[(cdf[S.DATE] >= cut60) & (cdf[S.DATE] < cut30)]

        rev_recent = recent[S.REVENUE].sum()
        rev_prior  = prior[S.REVENUE].sum()
        rev_total  = cdf[S.REVENUE].sum()

        # Revenue growth %
        if rev_prior > 0:
            rev_growth = (rev_recent - rev_prior) / rev_prior * 100
        elif rev_recent > 0:
            rev_growth = 100.0  # new buyer in last 30d
        else:
            continue  # no recent activity

        # Frequency growth (purchases per week)
        freq_recent = recent[S.DATE].nunique() / 4.3
        freq_prior  = prior[S.DATE].nunique() / 4.3 if not prior.empty else 0
        freq_delta  = freq_recent - freq_prior

        # Dept breadth
        dept_recent = recent[S.DEPT].nunique() if S.DEPT in recent.columns else 0
        dept_prior  = prior[S.DEPT].nunique() if S.DEPT in prior.columns and not prior.empty else 0
        dept_delta  = dept_recent - dept_prior

        # Composite score (weighted)
        score = (
            max(rev_growth, 0) * 0.5 +
            max(freq_delta, 0) * 20 +
            max(dept_delta, 0) * 10
        )

        # Low-base high-growth bonus (small total rev but strong % growth)
        median_rev = df.groupby(cust_col)[S.REVENUE].sum().median()
        if rev_total < median_rev * 0.5 and rev_growth > 30:
            score *= 1.4

        if score > 0:
            records.append({
                cust_col:      cust_id,
                S.CUSTOMER:    cust_name,
                S.REGION:      region,
                S.CUST_GROUP:  segment,
                "Rev_Total":   rev_total,
                "Rev_Recent":  rev_recent,
                "Rev_Growth":  round(rev_growth, 1),
                "Freq_Recent": round(freq_recent, 2),
                "Freq_Delta":  round(freq_delta, 2),
                "Dept_Recent": dept_recent,
                "Potential_Score": round(score, 1),
            })

    if not records:
        return pd.DataFrame()
    result = pd.DataFrame(records)
    return result.sort_values("Potential_Score", ascending=False).head(n).reset_index(drop=True)


# ── At-Risk Customer Detection ────────────────────────────────────────────────
DEFAULT_RISK_THRESHOLDS = {
    "L1_rev_drop":  15,   # % revenue drop → Level 1 (Yellow)
    "L2_rev_drop":  35,   # % revenue drop → Level 2 (Orange)
    "L3_rev_drop":  60,   # % revenue drop → Level 3 (Red)
    "L1_gap_ratio": 1.5,  # last-purchase gap / normal cycle → L1
    "L2_gap_ratio": 2.0,
    "L3_gap_ratio": 3.0,
    "no_buy_days":  60,   # auto Level 3 if not bought in N days
}

def identify_at_risk_customers(df: pd.DataFrame, thresholds: dict | None = None) -> pd.DataFrame:
    """
    3-level risk detection:
      Red    (3): revenue drop ≥ L3 OR gap_ratio ≥ L3 OR no purchase ≥ no_buy_days
      Orange (2): revenue drop ≥ L2 OR gap_ratio ≥ L2
      Yellow (1): revenue drop ≥ L1 OR gap_ratio ≥ L1
    Returns DataFrame sorted by alert_level desc, days_overdue desc.
    """
    t = {**DEFAULT_RISK_THRESHOLDS, **(thresholds or {})}
    today    = df[S.DATE].max()
    cut30    = today - pd.Timedelta(days=30)
    cut60    = today - pd.Timedelta(days=60)
    cust_col = S.CUST_NUM if S.CUST_NUM in df.columns else S.CUSTOMER

    records = []
    for cust_id, cdf in df.groupby(cust_col):
        cust_name = cdf[S.CUSTOMER].iloc[0]
        region    = cdf[S.REGION].iloc[0] if S.REGION in cdf.columns else ""
        segment   = cdf[S.CUST_GROUP].iloc[0] if S.CUST_GROUP in cdf.columns else ""
        last_buy  = cdf[S.DATE].max()
        days_since = (today - last_buy).days

        # Normal buying cycle (median gap between purchases)
        dates = cdf[S.DATE].drop_duplicates().sort_values()
        if len(dates) >= 2:
            gaps = dates.diff().dt.days.dropna()
            normal_cycle = gaps.median()
        else:
            normal_cycle = 30  # assume monthly

        gap_ratio = days_since / max(normal_cycle, 1)

        # Revenue comparison
        rev_recent = cdf[cdf[S.DATE] >= cut30][S.REVENUE].sum()
        rev_prior  = cdf[(cdf[S.DATE] >= cut60) & (cdf[S.DATE] < cut30)][S.REVENUE].sum()
        rev_drop   = ((rev_prior - rev_recent) / rev_prior * 100) if rev_prior > 0 else 0
        rev_drop   = max(rev_drop, 0)

        # Determine alert level
        level = 0
        reasons = []
        if days_since >= t["no_buy_days"] or rev_drop >= t["L3_rev_drop"] or gap_ratio >= t["L3_gap_ratio"]:
            level = 3
            if days_since >= t["no_buy_days"]: reasons.append(f"ไม่ซื้อ {days_since} วัน")
            if rev_drop >= t["L3_rev_drop"]:   reasons.append(f"ยอดลด {rev_drop:.0f}%")
        elif rev_drop >= t["L2_rev_drop"] or gap_ratio >= t["L2_gap_ratio"]:
            level = 2
            if gap_ratio >= t["L2_gap_ratio"]: reasons.append(f"ห่างหาย {gap_ratio:.1f}x รอบปกติ")
            if rev_drop >= t["L2_rev_drop"]:   reasons.append(f"ยอดลด {rev_drop:.0f}%")
        elif rev_drop >= t["L1_rev_drop"] or gap_ratio >= t["L1_gap_ratio"]:
            level = 1
            if gap_ratio >= t["L1_gap_ratio"]: reasons.append(f"ห่างหาย {gap_ratio:.1f}x รอบปกติ")
            if rev_drop >= t["L1_rev_drop"]:   reasons.append(f"ยอดลด {rev_drop:.0f}%")

        if level == 0:
            continue

        records.append({
            cust_col:       cust_id,
            S.CUSTOMER:     cust_name,
            S.REGION:       region,
            S.CUST_GROUP:   segment,
            "Alert_Level":  level,
            "Alert_Label":  {3: "🔴 Critical", 2: "🟠 Warning", 1: "🟡 Watch"}[level],
            "Rev_Drop_Pct": round(rev_drop, 1),
            "Days_Since":   days_since,
            "Normal_Cycle": round(normal_cycle, 0),
            "Gap_Ratio":    round(gap_ratio, 2),
            "Rev_Recent":   rev_recent,
            "Rev_Prior":    rev_prior,
            "Last_Buy":     last_buy.strftime("%d %b %Y"),
            "Risk_Reason":  ", ".join(reasons) if reasons else "trend ลดลง",
        })

    if not records:
        return pd.DataFrame()
    return (pd.DataFrame(records)
            .sort_values(["Alert_Level", "Days_Since"], ascending=[False, False])
            .reset_index(drop=True))
