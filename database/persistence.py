"""
database/persistence.py  v6
---------------------------
Batch-based storage:
  • แต่ละ upload = 1 batch (เก็บแยก)
  • Master DB   = concat ทุก batch เรียงตาม Date
  • ลบ batch    → Master อัปเดตอัตโนมัติ
  • overlap     → ตรวจพบและ replace ได้

Backend routing:
  Supabase  ← ถ้ามี SUPABASE_URL / SUPABASE_KEY ใน st.secrets
  SQLite    ← fallback (local / dev)
"""
from __future__ import annotations
import sqlite3, io, gzip, pickle, uuid
from pathlib import Path
from typing import Optional
import pandas as pd

DB_PATH = Path(__file__).parent.parent / "data" / "salesiq.db"

# ── Backend detection ──────────────────────────────────────────────────────────
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

def _date_range(df: pd.DataFrame):
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

# ── Init ───────────────────────────────────────────────────────────────────────
def init_db() -> None:
    if _use_supabase():
        try:
            _sb().table("data_batches").select("id").limit(1).execute()
        except Exception as e:
            import streamlit as st
            st.error(f"Supabase error: {e}")
        return

    conn = _sqlite_connect(); cur = conn.cursor()
    # Batch table (replaces datasets)
    cur.execute("""CREATE TABLE IF NOT EXISTS data_batches (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        label        TEXT    NOT NULL,
        filename     TEXT    NOT NULL,
        row_count    INTEGER NOT NULL,
        col_count    INTEGER NOT NULL,
        date_min     TEXT,
        date_max     TEXT,
        storage_path TEXT,
        uploaded_at  TEXT DEFAULT (datetime('now')),
        data_blob    BLOB)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS visit_plan_override (
        id INTEGER PRIMARY KEY AUTOINCREMENT, customer_num TEXT NOT NULL,
        customer_name TEXT NOT NULL, region TEXT, visit_date TEXT NOT NULL,
        reason TEXT DEFAULT 'Manual', items_to_discuss TEXT,
        priority TEXT DEFAULT 'Medium', source TEXT DEFAULT 'Manual',
        created_at TEXT DEFAULT (date('now')), is_deleted INTEGER DEFAULT 0)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        created_at TEXT DEFAULT (datetime('now')))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS user_customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
        customer_num TEXT NOT NULL, customer_name TEXT,
        added_at TEXT DEFAULT (datetime('now')),
        UNIQUE(user_id, customer_num))""")
    conn.commit(); conn.close()

# ══════════════════════════════════════════════════════════════════════════════
#  BATCH CRUD
# ══════════════════════════════════════════════════════════════════════════════

def check_date_overlap(date_min: str, date_max: str) -> list[dict]:
    """Return list of existing batches whose date range overlaps with [date_min, date_max]."""
    batches = list_batches()
    overlapping = []
    for b in batches:
        if not b.get("date_min") or not b.get("date_max"):
            continue
        # Overlap condition: NOT (b.max < new.min OR b.min > new.max)
        if not (b["date_max"] < date_min or b["date_min"] > date_max):
            overlapping.append(b)
    return overlapping


def save_batch(label: str, filename: str, df: pd.DataFrame) -> int:
    """Save a new batch. Returns new batch ID."""
    date_min, date_max = _date_range(df)
    blob = _df_to_bytes(df)

    if _use_supabase():
        sb = _sb()
        path = f"batches/{uuid.uuid4()}.pkl.gz"
        sb.storage.from_("salesiq").upload(
            path, blob, {"content-type": "application/octet-stream"})
        r = sb.table("data_batches").insert({
            "label": label, "filename": filename,
            "row_count": len(df), "col_count": len(df.columns),
            "date_min": date_min, "date_max": date_max,
            "storage_path": path,
        }).execute()
        return r.data[0]["id"]

    conn = _sqlite_connect(); cur = conn.cursor()
    cur.execute("""INSERT INTO data_batches
        (label, filename, row_count, col_count, date_min, date_max, data_blob)
        VALUES (?,?,?,?,?,?,?)""",
        (label, filename, len(df), len(df.columns), date_min, date_max, blob))
    new_id = cur.lastrowid; conn.commit(); conn.close()
    return new_id


def list_batches() -> list[dict]:
    """Return all batches metadata sorted by date_min."""
    if _use_supabase():
        r = (_sb().table("data_batches")
               .select("id,label,filename,row_count,col_count,date_min,date_max,uploaded_at")
               .order("date_min", desc=False)
               .execute())
        return r.data or []

    conn = _sqlite_connect()
    rows = conn.execute("""SELECT id,label,filename,row_count,col_count,
        date_min,date_max,uploaded_at FROM data_batches
        ORDER BY date_min ASC""").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def load_batch(batch_id: int) -> pd.DataFrame:
    """Load a single batch as DataFrame."""
    if _use_supabase():
        sb = _sb()
        r = (sb.table("data_batches").select("storage_path")
               .eq("id", batch_id).single().execute())
        blob = sb.storage.from_("salesiq").download(r.data["storage_path"])
        return _bytes_to_df(blob)

    conn = _sqlite_connect()
    row = conn.execute("SELECT data_blob FROM data_batches WHERE id=?",
                       (batch_id,)).fetchone()
    conn.close()
    if not row: raise ValueError(f"Batch {batch_id} not found")
    return _bytes_to_df(bytes(row["data_blob"]))


def load_combined_df() -> Optional[pd.DataFrame]:
    """Load ALL batches, concat and sort by Date → the Master DB."""
    batches = list_batches()
    if not batches:
        return None
    frames = []
    for b in batches:
        try:
            df = load_batch(b["id"])
            df["_batch_id"]    = b["id"]
            df["_batch_label"] = b["label"]
            frames.append(df)
        except Exception:
            continue
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    if "Date" in combined.columns:
        combined = combined.sort_values("Date").reset_index(drop=True)
    return combined


def delete_batch(batch_id: int) -> None:
    """Delete a batch (removes from storage + DB)."""
    if _use_supabase():
        sb = _sb()
        r = (sb.table("data_batches").select("storage_path")
               .eq("id", batch_id).single().execute())
        if r.data and r.data.get("storage_path"):
            try: sb.storage.from_("salesiq").remove([r.data["storage_path"]])
            except Exception: pass
        sb.table("data_batches").delete().eq("id", batch_id).execute()
        return

    conn = _sqlite_connect()
    conn.execute("DELETE FROM data_batches WHERE id=?", (batch_id,))
    conn.commit(); conn.close()


def rename_batch(batch_id: int, new_label: str) -> None:
    if _use_supabase():
        _sb().table("data_batches").update({"label": new_label}).eq("id", batch_id).execute()
        return
    conn = _sqlite_connect()
    conn.execute("UPDATE data_batches SET label=? WHERE id=?", (new_label, batch_id))
    conn.commit(); conn.close()


# ── Legacy aliases (backward compat with old app.py calls) ────────────────────
def save_dataset(label, filename, df): return save_batch(label, filename, df)
def list_datasets(): return list_batches()
def load_dataset(i): return load_batch(i)
def set_active_dataset(i): pass          # no-op — master is always all batches
def get_active_dataset_id(): return -1 if list_batches() else None
def delete_dataset(i): return delete_batch(i)
def rename_dataset(i, l): return rename_batch(i, l)


# ══════════════════════════════════════════════════════════════════════════════
#  VISIT PLAN
# ══════════════════════════════════════════════════════════════════════════════
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
                    (str(row.get("Customer Number","")), row.get("Customer Name",""),
                     row.get("Region Name",""),
                     row["Visit_Date"].strftime("%Y-%m-%d") if hasattr(row["Visit_Date"],"strftime") else str(row["Visit_Date"]),
                     row.get("Reason","Manual"), row.get("Items_To_Discuss",""),
                     row.get("Priority","Medium"), "Manual"))
    conn.commit(); conn.close()


def load_manual_visits() -> pd.DataFrame:
    if _use_supabase():
        r = _sb().table("visit_plan_override").select("*").eq("is_deleted",0).eq("source","Manual").execute()
        df = pd.DataFrame(r.data or [])
        if not df.empty: df["Visit_Date"] = pd.to_datetime(df["visit_date"])
        return df
    conn = _sqlite_connect()
    df = pd.read_sql_query("SELECT * FROM visit_plan_override WHERE is_deleted=0 AND source='Manual'", conn)
    conn.close()
    if not df.empty: df["Visit_Date"] = pd.to_datetime(df["visit_date"])
    return df


def log_upload(filename: str, row_count: int) -> None:
    pass  # replaced by save_batch tracking


# ══════════════════════════════════════════════════════════════════════════════
#  USERS & PORTFOLIOS
# ══════════════════════════════════════════════════════════════════════════════
def _ensure_user_tables_sqlite():
    conn = _sqlite_connect(); cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
        created_at TEXT DEFAULT (datetime('now')))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS user_customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
        customer_num TEXT NOT NULL, customer_name TEXT,
        added_at TEXT DEFAULT (datetime('now')), UNIQUE(user_id,customer_num))""")
    conn.commit(); conn.close()

def list_users() -> list[dict]:
    if _use_supabase():
        r = _sb().table("users").select("*").order("name").execute()
        return r.data or []
    _ensure_user_tables_sqlite()
    conn = _sqlite_connect()
    rows = conn.execute("SELECT * FROM users ORDER BY name").fetchall()
    conn.close(); return [dict(r) for r in rows]

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
    if _use_supabase():
        r = _sb().table("user_customers").select("customer_num").eq("user_id", user_id).execute()
        return [row["customer_num"] for row in (r.data or [])]
    _ensure_user_tables_sqlite()
    conn = _sqlite_connect()
    rows = conn.execute("SELECT customer_num FROM user_customers WHERE user_id=?", (user_id,)).fetchall()
    conn.close(); return [r["customer_num"] for r in rows]

def set_user_customers(user_id: int, customer_nums: list[str], customer_names: dict | None = None) -> None:
    names = customer_names or {}
    customer_nums = [str(c) for c in customer_nums]  # always string
    if _use_supabase():
        sb = _sb()
        sb.table("user_customers").delete().eq("user_id", user_id).execute()
        if customer_nums:
            sb.table("user_customers").insert(
                [{"user_id": user_id, "customer_num": c,
                  "customer_name": names.get(c, names.get(int(c) if c.isdigit() else c, ""))}
                 for c in customer_nums]
            ).execute()
        return
    _ensure_user_tables_sqlite()
    conn = _sqlite_connect(); cur = conn.cursor()
    cur.execute("DELETE FROM user_customers WHERE user_id=?", (user_id,))
    for c in customer_nums:
        cur.execute("INSERT OR IGNORE INTO user_customers (user_id,customer_num,customer_name) VALUES (?,?,?)",
                    (user_id, c, names.get(c,"")))
    conn.commit(); conn.close()
