"""utils/export.py — Excel export helpers."""
from __future__ import annotations
import io
import pandas as pd

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    return buf.getvalue()

def export_visit_plan(plan_df: pd.DataFrame) -> bytes:
    export = plan_df.copy()
    if "Visit_Date" in export.columns:
        export["Visit_Date"] = export["Visit_Date"].dt.strftime("%Y-%m-%d")
    return to_excel_bytes(export)

def export_customer_summary(df: pd.DataFrame) -> bytes:
    from database.schema import MAKRO as S
    cust_id = S.CUST_NUM if S.CUST_NUM in df.columns else S.CUSTOMER
    grp_cols = [cust_id, S.CUSTOMER]
    if S.REGION in df.columns: grp_cols.append(S.REGION)
    if S.CUST_GROUP in df.columns: grp_cols.append(S.CUST_GROUP)
    summary = (df.groupby(grp_cols)
                 .agg(Revenue=(S.REVENUE,"sum"),
                      Profit=(S.PROFIT,"sum") if S.PROFIT in df.columns else (S.REVENUE,"count"),
                      Orders=(S.DATE,"count"),
                      First_Purchase=(S.DATE,"min"),
                      Last_Purchase=(S.DATE,"max"))
                 .reset_index()
                 .sort_values("Revenue", ascending=False))
    return to_excel_bytes(summary)
