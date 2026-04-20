-- ════════════════════════════════════════════════════════════════════
--  SalesIQ — Supabase Setup Script
--  รันใน Supabase Dashboard → SQL Editor → New Query → Run
-- ════════════════════════════════════════════════════════════════════

-- 1. Datasets metadata
CREATE TABLE IF NOT EXISTS datasets (
    id           SERIAL PRIMARY KEY,
    label        TEXT    NOT NULL,
    filename     TEXT    NOT NULL,
    row_count    INTEGER NOT NULL,
    col_count    INTEGER NOT NULL,
    date_min     TEXT,
    date_max     TEXT,
    is_active    BOOLEAN DEFAULT FALSE,
    storage_path TEXT,
    uploaded_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Visit plan overrides
CREATE TABLE IF NOT EXISTS visit_plan_override (
    id               SERIAL PRIMARY KEY,
    customer_num     TEXT NOT NULL,
    customer_name    TEXT NOT NULL,
    region           TEXT,
    visit_date       TEXT NOT NULL,
    reason           TEXT DEFAULT 'Manual',
    items_to_discuss TEXT,
    priority         TEXT DEFAULT 'Medium',
    source           TEXT DEFAULT 'Manual',
    created_at       DATE DEFAULT CURRENT_DATE,
    is_deleted       INTEGER DEFAULT 0
);

-- 3. Upload log
CREATE TABLE IF NOT EXISTS upload_log (
    id          SERIAL PRIMARY KEY,
    filename    TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    uploaded_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. Row Level Security (แนะนำ — ป้องกันการ query โดยไม่มี key)
ALTER TABLE datasets             ENABLE ROW LEVEL SECURITY;
ALTER TABLE visit_plan_override  ENABLE ROW LEVEL SECURITY;
ALTER TABLE upload_log           ENABLE ROW LEVEL SECURITY;

-- อนุญาต service_role (ใช้ anon key ของ Streamlit)
CREATE POLICY "allow_all_datasets"            ON datasets            FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "allow_all_visit_override"      ON visit_plan_override FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "allow_all_upload_log"          ON upload_log          FOR ALL USING (true) WITH CHECK (true);
