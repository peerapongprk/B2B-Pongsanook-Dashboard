"""database/persistence.py — SQLite persistence for visit plan overrides."""
from __future__ import annotations
import sqlite3, os
from pathlib import Path
from typing import Optional
import pandas as pd

DB_PATH = Path(__file__).parent.parent / "data" / "salesiq.db"

def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    conn = _connect()
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS visit_plan_override (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_num  TEXT    NOT NULL,
            customer_name TEXT    NOT NULL,
            region        TEXT,
            visit_date    TEXT    NOT NULL,
            reason        TEXT    DEFAULT 'Manual',
            items_to_discuss TEXT,
            priority      TEXT    DEFAULT 'Medium',
            source        TEXT    DEFAULT 'Manual',
            created_at    TEXT    DEFAULT (date('now')),
            is_deleted    INTEGER DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS upload_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            filename    TEXT NOT NULL,
            row_count   INTEGER NOT NULL,
            uploaded_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit(); conn.close()

def save_manual_visits(plan_df: pd.DataFrame) -> None:
    manual = plan_df[plan_df.get("Source", pd.Series(["AI"]*len(plan_df))) == "Manual"] if "Source" in plan_df.columns else pd.DataFrame()
    if manual.empty:
        return
    conn = _connect(); cur = conn.cursor()
    cur.execute("DELETE FROM visit_plan_override WHERE source='Manual'")
    for _, row in manual.iterrows():
        cur.execute("""INSERT INTO visit_plan_override
            (customer_num,customer_name,region,visit_date,reason,items_to_discuss,priority,source)
            VALUES (?,?,?,?,?,?,?,?)""",
            (str(row.get("Customer Number","")), row.get("Customer Name",""),
             row.get("Region Name",""),
             row["Visit_Date"].strftime("%Y-%m-%d") if hasattr(row["Visit_Date"],"strftime") else str(row["Visit_Date"]),
             row.get("Reason","Manual"), row.get("Items_To_Discuss",""),
             row.get("Priority","Medium"), "Manual"))
    conn.commit(); conn.close()

def load_manual_visits() -> pd.DataFrame:
    conn = _connect()
    df = pd.read_sql_query(
        "SELECT * FROM visit_plan_override WHERE is_deleted=0 AND source='Manual'",
        conn)
    conn.close()
    if not df.empty:
        df["Visit_Date"] = pd.to_datetime(df["visit_date"])
    return df

def log_upload(filename: str, row_count: int) -> None:
    conn = _connect()
    conn.execute("INSERT INTO upload_log (filename,row_count) VALUES (?,?)", (filename, row_count))
    conn.commit(); conn.close()
