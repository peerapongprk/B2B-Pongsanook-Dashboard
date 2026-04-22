-- ════════════════════════════════════════════════════════════
--  SalesIQ v6 — Supabase Setup
--  รันใน SQL Editor → New Query → Run All
-- ════════════════════════════════════════════════════════════

-- 1. Batch store (แทน datasets เดิม)
CREATE TABLE IF NOT EXISTS data_batches (
    id           SERIAL PRIMARY KEY,
    label        TEXT    NOT NULL,
    filename     TEXT    NOT NULL,
    row_count    INTEGER NOT NULL,
    col_count    INTEGER NOT NULL,
    date_min     TEXT,
    date_max     TEXT,
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

-- 3. Users (salespeople)
CREATE TABLE IF NOT EXISTS users (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. User portfolios
CREATE TABLE IF NOT EXISTS user_customers (
    id            SERIAL PRIMARY KEY,
    user_id       INTEGER REFERENCES users(id) ON DELETE CASCADE,
    customer_num  TEXT NOT NULL,
    customer_name TEXT,
    added_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, customer_num)
);

-- 5. RLS policies
ALTER TABLE data_batches        ENABLE ROW LEVEL SECURITY;
ALTER TABLE visit_plan_override ENABLE ROW LEVEL SECURITY;
ALTER TABLE users               ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_customers      ENABLE ROW LEVEL SECURITY;

CREATE POLICY "open_data_batches"    ON data_batches        FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "open_visit_override"  ON visit_plan_override FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "open_users"           ON users               FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "open_user_customers"  ON user_customers      FOR ALL USING (true) WITH CHECK (true);

-- 6. Storage policy (salesiq bucket)
CREATE POLICY "open_salesiq_storage"
ON storage.objects FOR ALL TO anon, authenticated
USING (bucket_id = 'salesiq') WITH CHECK (bucket_id = 'salesiq');
