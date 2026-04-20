"""
database/persistence.py
-----------------------
Storage backend with automatic routing:
  Supabase (PostgreSQL + Storage)  <- ถ้ามี SUPABASE_URL / SUPABASE_KEY ใน st.secrets
  SQLite local                     <- fallback สำหรับ dev หรือยังไม่ตั้งค่า
"""
from __future__ import annotations
import sqlite3, io, gzip, pickle, uuid
from pathlib import Path
from typing import Optional
import pandas as pd

DB_PATH = Path(__file__).parent.parent / "data" / "salesiq.db"

# ── Backend detection ─────────────────────────────────────────────────────────
def _use_supabase() -> bool:
    try:
        import streamlit as st
        return bool(st.secrets.get("SUPABASE_URL") and st.secrets.get("SUPABASE_KEY"))
    except Exception:
        return False

def _sb():
    import streamlit as st
    from supabase import create_client
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

def _df_to_bytes(df: pd.DataFrame) -> bytes:
    return gzip.compress(pickle.dumps(df, protocol=5))

def _bytes_to_df(data: bytes) -> pd.DataFrame:
    return pickle.loads(gzip.decompress(data))

def _date_range(df):
    if "Date" in df.columns:
        try:
            return str(df["Date"].min().date()), str(df["Date"].max().date())
        except Exception:
            pass
    return None, None

def _sqlite_connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# ── Init ──────────────────────────────────────────────────────────────────────
def init_db() -> None:
    if _use_supabase():
        try:
            _sb().table("datasets").select("id").limit(1).execute()
        except Exception as e:
            import streamlit as st
            st.error(f"Supabase error: {e}")
        return
    conn = _sqlite_connect(); cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS datasets (
        id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT NOT NULL,
        filename TEXT NOT NULL, row_count INTEGER NOT NULL, col_count INTEGER NOT NULL,
        date_min TEXT, date_max TEXT, is_active INTEGER DEFAULT 0,
        storage_path TEXT, uploaded_at TEXT DEFAULT (datetime('now')), data_blob BLOB)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS visit_plan_override (
        id INTEGER PRIMARY KEY AUTOINCREMENT, customer_num TEXT NOT NULL,
        customer_name TEXT NOT NULL, region TEXT, visit_date TEXT NOT NULL,
        reason TEXT DEFAULT 'Manual', items_to_discuss TEXT,
        priority TEXT DEFAULT 'Medium', source TEXT DEFAULT 'Manual',
        created_at TEXT DEFAULT (date('now')), is_deleted INTEGER DEFAULT 0)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS upload_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT NOT NULL,
        row_count INTEGER NOT NULL, uploaded_at TEXT DEFAULT (datetime('now')))""")
    conn.commit(); conn.close()

# ── Dataset CRUD ──────────────────────────────────────────────────────────────
def save_dataset(label: str, filename: str, df: pd.DataFrame) -> int:
    date_min, date_max = _date_range(df)
    blob = _df_to_bytes(df)
    if _use_supabase():
        sb = _sb()
        path = f"datasets/{uuid.uuid4()}.pkl.gz"
        sb.storage.from_("salesiq").upload(path, blob, {"content-type": "application/octet-stream"})
        r = sb.table("datasets").insert({
            "label": label, "filename": filename, "row_count": len(df),
            "col_count": len(df.columns), "date_min": date_min, "date_max": date_max,
            "is_active": False, "storage_path": path,
        }).execute()
        return r.data[0]["id"]
    conn = _sqlite_connect(); cur = conn.cursor()
    cur.execute("INSERT INTO datasets (label,filename,row_count,col_count,date_min,date_max,is_active,data_blob) VALUES (?,?,?,?,?,?,0,?)",
                (label, filename, len(df), len(df.columns), date_min, date_max, blob))
    new_id = cur.lastrowid; conn.commit(); conn.close()
    return new_id

def list_datasets() -> list[dict]:
    if _use_supabase():
        r = _sb().table("datasets").select("id,label,filename,row_count,col_count,date_min,date_max,is_active,uploaded_at").order("uploaded_at", desc=True).execute()
        return r.data or []
    conn = _sqlite_connect()
    rows = conn.execute("SELECT id,label,filename,row_count,col_count,date_min,date_max,is_active,uploaded_at FROM datasets ORDER BY uploaded_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def load_dataset(dataset_id: int) -> pd.DataFrame:
    if _use_supabase():
        sb = _sb()
        r = sb.table("datasets").select("storage_path").eq("id", dataset_id).single().execute()
        blob = sb.storage.from_("salesiq").download(r.data["storage_path"])
        return _bytes_to_df(blob)
    conn = _sqlite_connect()
    row = conn.execute("SELECT data_blob FROM datasets WHERE id=?", (dataset_id,)).fetchone()
    conn.close()
    if not row: raise ValueError(f"Dataset {dataset_id} not found")
    return _bytes_to_df(bytes(row["data_blob"]))

def set_active_dataset(dataset_id: int) -> None:
    if _use_supabase():
        sb = _sb()
        sb.table("datasets").update({"is_active": False}).neq("id", 0).execute()
        sb.table("datasets").update({"is_active": True}).eq("id", dataset_id).execute()
        return
    conn = _sqlite_connect()
    conn.execute("UPDATE datasets SET is_active=0")
    conn.execute("UPDATE datasets SET is_active=1 WHERE id=?", (dataset_id,))
    conn.commit(); conn.close()

def get_active_dataset_id() -> Optional[int]:
    if _use_supabase():
        r = _sb().table("datasets").select("id").eq("is_active", True).limit(1).execute()
        return r.data[0]["id"] if r.data else None
    conn = _sqlite_connect()
    row = conn.execute("SELECT id FROM datasets WHERE is_active=1").fetchone()
    conn.close()
    return row["id"] if row else None

def delete_dataset(dataset_id: int) -> None:
    if _use_supabase():
        sb = _sb()
        r = sb.table("datasets").select("storage_path").eq("id", dataset_id).single().execute()
        if r.data and r.data.get("storage_path"):
            try: sb.storage.from_("salesiq").remove([r.data["storage_path"]])
            except Exception: pass
        sb.table("datasets").delete().eq("id", dataset_id).execute()
        return
    conn = _sqlite_connect()
    conn.execute("DELETE FROM datasets WHERE id=?", (dataset_id,))
    conn.commit(); conn.close()

def rename_dataset(dataset_id: int, new_label: str) -> None:
    if _use_supabase():
        _sb().table("datasets").update({"label": new_label}).eq("id", dataset_id).execute()
        return
    conn = _sqlite_connect()
    conn.execute("UPDATE datasets SET label=? WHERE id=?", (new_label, dataset_id))
    conn.commit(); conn.close()

# ── Visit plan ────────────────────────────────────────────────────────────────
def save_manual_visits(plan_df: pd.DataFrame) -> None:
    manual = plan_df[plan_df["Source"] == "Manual"] if "Source" in plan_df.columns else pd.DataFrame()
    if manual.empty: return
    if _use_supabase():
        sb = _sb()
        sb.table("visit_plan_override").delete().eq("source", "Manual").execute()
        rows = [{"customer_num": str(r.get("Customer Number","")),
                 "customer_name": r.get("Customer Name",""),
                 "region": r.get("Region Name",""),
                 "visit_date": r["Visit_Date"].strftime("%Y-%m-%d") if hasattr(r["Visit_Date"],"strftime") else str(r["Visit_Date"]),
                 "reason": r.get("Reason","Manual"),
                 "items_to_discuss": r.get("Items_To_Discuss",""),
                 "priority": r.get("Priority","Medium"), "source": "Manual"}
                for _, r in manual.iterrows()]
        if rows: sb.table("visit_plan_override").insert(rows).execute()
        return
    conn = _sqlite_connect(); cur = conn.cursor()
    cur.execute("DELETE FROM visit_plan_override WHERE source='Manual'")
    for _, row in manual.iterrows():
        cur.execute("INSERT INTO visit_plan_override (customer_num,customer_name,region,visit_date,reason,items_to_discuss,priority,source) VALUES (?,?,?,?,?,?,?,?)",
                    (str(row.get("Customer Number","")), row.get("Customer Name",""), row.get("Region Name",""),
                     row["Visit_Date"].strftime("%Y-%m-%d") if hasattr(row["Visit_Date"],"strftime") else str(row["Visit_Date"]),
                     row.get("Reason","Manual"), row.get("Items_To_Discuss",""), row.get("Priority","Medium"), "Manual"))
    conn.commit(); conn.close()

def load_manual_visits() -> pd.DataFrame:
    if _use_supabase():
        r = _sb().table("visit_plan_override").select("*").eq("is_deleted", 0).eq("source","Manual").execute()
        df = pd.DataFrame(r.data or [])
        if not df.empty: df["Visit_Date"] = pd.to_datetime(df["visit_date"])
        return df
    conn = _sqlite_connect()
    df = pd.read_sql_query("SELECT * FROM visit_plan_override WHERE is_deleted=0 AND source='Manual'", conn)
    conn.close()
    if not df.empty: df["Visit_Date"] = pd.to_datetime(df["visit_date"])
    return df

def log_upload(filename: str, row_count: int) -> None:
    if _use_supabase():
        try: _sb().table("upload_log").insert({"filename": filename, "row_count": row_count}).execute()
        except Exception: pass
        return
    conn = _sqlite_connect()
    conn.execute("INSERT INTO upload_log (filename,row_count) VALUES (?,?)", (filename, row_count))
    conn.commit(); conn.close()

# ══════════════════════════════════════════════════════════════════════════════
#  USER & PORTFOLIO MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════
def _ensure_user_tables_sqlite():
    conn = _sqlite_connect(); cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        created_at TEXT DEFAULT (datetime('now')))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS user_customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        customer_num TEXT NOT NULL,
        customer_name TEXT,
        added_at TEXT DEFAULT (datetime('now')),
        UNIQUE(user_id, customer_num))""")
    conn.commit(); conn.close()

def list_users() -> list[dict]:
    if _use_supabase():
        r = _sb().table("users").select("*").order("name").execute()
        return r.data or []
    _ensure_user_tables_sqlite()
    conn = _sqlite_connect()
    rows = conn.execute("SELECT * FROM users ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def save_user(name: str) -> int:
    if _use_supabase():
        r = _sb().table("users").insert({"name": name}).execute()
        return r.data[0]["id"]
    _ensure_user_tables_sqlite()
    conn = _sqlite_connect(); cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO users (name) VALUES (?)", (name,))
    conn.commit()
    uid = conn.execute("SELECT id FROM users WHERE name=?", (name,)).fetchone()["id"]
    conn.close(); return uid

def delete_user(user_id: int) -> None:
    if _use_supabase():
        _sb().table("user_customers").delete().eq("user_id", user_id).execute()
        _sb().table("users").delete().eq("id", user_id).execute()
        return
    _ensure_user_tables_sqlite()
    conn = _sqlite_connect()
    conn.execute("DELETE FROM user_customers WHERE user_id=?", (user_id,))
    conn.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit(); conn.close()

def get_user_customers(user_id: int) -> list[str]:
    """Return list of customer_num strings for a user."""
    if _use_supabase():
        r = (_sb().table("user_customers")
               .select("customer_num")
               .eq("user_id", user_id)
               .execute())
        return [row["customer_num"] for row in (r.data or [])]
    _ensure_user_tables_sqlite()
    conn = _sqlite_connect()
    rows = conn.execute("SELECT customer_num FROM user_customers WHERE user_id=?", (user_id,)).fetchall()
    conn.close()
    return [r["customer_num"] for r in rows]

def set_user_customers(user_id: int, customer_nums: list[str], customer_names: dict[str,str] | None = None) -> None:
    """Replace all customers for a user."""
    names = customer_names or {}
    if _use_supabase():
        sb = _sb()
        sb.table("user_customers").delete().eq("user_id", user_id).execute()
        if customer_nums:
            rows = [{"user_id": user_id, "customer_num": c, "customer_name": names.get(c,"")} for c in customer_nums]
            sb.table("user_customers").insert(rows).execute()
        return
    _ensure_user_tables_sqlite()
    conn = _sqlite_connect(); cur = conn.cursor()
    cur.execute("DELETE FROM user_customers WHERE user_id=?", (user_id,))
    for c in customer_nums:
        cur.execute("INSERT OR IGNORE INTO user_customers (user_id, customer_num, customer_name) VALUES (?,?,?)",
                    (user_id, c, names.get(c,"")))
    conn.commit(); conn.close()
