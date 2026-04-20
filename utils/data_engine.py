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
