"""utils/styles.py — Premium CSS + HTML component helpers."""

GLOBAL_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Sarabun:wght@300;400;500;600;700&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] { font-family: 'Sarabun', 'DM Sans', sans-serif !important; }

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1.5rem 2rem 3rem !important; max-width: 1400px !important; }
[data-testid="stSidebar"] > div:first-child { padding-top: 1rem; }

/* ── CSS Variables ── */
:root {
  --primary:    #1A56DB;
  --primary-lt: #EBF1FF;
  --success:    #0EA371;
  --danger:     #E53E3E;
  --warning:    #F59E0B;
  --bg:         #F1F5F9;
  --card:       #FFFFFF;
  --text:       #1E293B;
  --muted:      #64748B;
  --border:     #E2E8F0;
  --shadow:     0 1px 3px rgba(0,0,0,.08), 0 1px 2px rgba(0,0,0,.04);
}

/* ── Card ── */
.kpi-card {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 14px;
  padding: 1.1rem 1.3rem;
  box-shadow: var(--shadow);
  transition: box-shadow .2s;
}
.kpi-card:hover { box-shadow: 0 4px 12px rgba(0,0,0,.1); }
.kpi-icon { font-size: 1.4rem; margin-bottom: .4rem; }
.kpi-label { font-size: .72rem; color: var(--muted); font-weight: 500; letter-spacing: .04em; text-transform: uppercase; }
.kpi-value { font-size: 1.6rem; font-weight: 700; color: var(--text); line-height: 1.15; margin: .15rem 0 0; }
.kpi-delta { font-size: .72rem; margin-top: .2rem; }
.kpi-delta.up   { color: var(--success); }
.kpi-delta.down { color: var(--danger); }

/* ── Chart card ── */
.chart-card {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 14px;
  padding: 1rem 1.2rem;
  box-shadow: var(--shadow);
  margin-bottom: .75rem;
}

/* ── Section header ── */
.section-header {
  display: flex; align-items: center; gap: .5rem;
  font-size: 1.05rem; font-weight: 600; color: var(--text);
  margin: 1.5rem 0 .75rem;
  padding-bottom: .4rem;
  border-bottom: 2px solid var(--primary-lt);
}

/* ── Badge ── */
.badge {
  display: inline-block; padding: 2px 10px; border-radius: 20px;
  font-size: .7rem; font-weight: 600; letter-spacing: .03em;
}
.badge-blue   { background:#EBF1FF; color:#1A56DB; }
.badge-green  { background:#ECFDF5; color:#059669; }
.badge-red    { background:#FEF2F2; color:#DC2626; }
.badge-amber  { background:#FFFBEB; color:#D97706; }
.badge-gray   { background:#F1F5F9; color:#475569; }

/* ── Sidebar ── */
.sidebar-logo {
  display: flex; align-items: center; gap: .75rem;
  padding: .75rem 1rem 1rem;
  border-bottom: 1px solid var(--border);
  margin-bottom: 1rem;
}
.sidebar-logo-icon {
  width: 40px; height: 40px;
  background: var(--primary); color: white;
  border-radius: 10px; display: flex;
  align-items: center; justify-content: center;
  font-size: 1.2rem;
}
.sidebar-logo-text .name { font-weight: 700; font-size: .95rem; color: var(--text); }
.sidebar-logo-text .sub  { font-size: .7rem; color: var(--muted); }

/* ── Buttons ── */
.stButton > button {
  border-radius: 8px !important;
  font-weight: 500 !important;
  transition: all .15s !important;
}
.stButton > button:hover { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(26,86,219,.25) !important; }

/* ── Priority colours ── */
.priority-high   { color:#DC2626; font-weight:600; }
.priority-medium { color:#D97706; font-weight:600; }
.priority-low    { color:#059669; font-weight:600; }

/* ── Upload zone ── */
[data-testid="stFileUploader"] {
  border: 2px dashed var(--primary) !important;
  border-radius: 14px !important;
  background: var(--primary-lt) !important;
  padding: 1rem !important;
}

/* ── Calendar table ── */
.cal-table { width:100%; border-collapse:collapse; }
.cal-table th {
  background: var(--primary); color: white;
  padding: .5rem .4rem; font-size:.78rem;
  text-align:center; border-radius:0;
}
.cal-table td {
  border: 1px solid var(--border);
  vertical-align: top; padding: .3rem;
  min-height: 70px; font-size:.72rem;
}
.cal-day-num { font-weight:700; font-size:.8rem; color:var(--muted); }
.cal-today td.today { background:#EBF1FF; }
.visit-chip {
  border-radius: 4px; padding: 2px 5px;
  margin: 1px 0; font-size: .65rem;
  display: block; white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis;
}
.chip-high   { background:#FEE2E2; color:#991B1B; }
.chip-medium { background:#FEF3C7; color:#92400E; }
.chip-low    { background:#D1FAE5; color:#065F46; }
.chip-manual { background:#EDE9FE; color:#5B21B6; }
</style>
"""

def kpi_card(label: str, value: str, icon: str = "", delta: str = "", delta_up: bool = True) -> str:
    delta_html = ""
    if delta:
        cls = "up" if delta_up else "down"
        arrow = "▲" if delta_up else "▼"
        delta_html = f'<div class="kpi-delta {cls}">{arrow} {delta}</div>'
    return f"""
<div class="kpi-card">
  <div class="kpi-icon">{icon}</div>
  <div class="kpi-label">{label}</div>
  <div class="kpi-value">{value}</div>
  {delta_html}
</div>"""

def section_header(title: str, icon: str = "") -> str:
    return f'<div class="section-header">{icon}&nbsp;{title}</div>'

def badge(text: str, color: str = "blue") -> str:
    return f'<span class="badge badge-{color}">{text}</span>'

def priority_badge(p: str) -> str:
    color = {"High":"red","Medium":"amber","Low":"green"}.get(p,"gray")
    return badge(p, color)

def visit_chip(name: str, priority: str, source: str = "AI") -> str:
    cls = "chip-manual" if source == "Manual" else f"chip-{priority.lower()}"
    return f'<span class="visit-chip {cls}" title="{name}">{name}</span>'
