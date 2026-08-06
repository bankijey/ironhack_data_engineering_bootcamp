from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import html
import os
import sqlite3
import pandas as pd
import logging

log = logging.getLogger(__name__)

DATA_DIR = "/opt/airflow/data/quickcart/incoming"
EXPECTED_FILES = ["customers.csv", "orders.csv", "products.csv", "clickstream.csv"]

# Absolute path -- a bare "quick_cart.db" would land in whatever cwd the worker
# happens to have. Lives under the bind mount so it survives container restarts.
STAGING_DB = "/opt/airflow/data/warehouse/quick_cart.db"

GOLD_DIR = "/opt/airflow/data/gold"
DASHBOARD_HTML = f"{GOLD_DIR}/quickcart_dashboard.html"

# Inactivity gap that starts a new clickstream session, in minutes.
SESSION_GAP_MIN = 30

# Problems are written here instead of raised, so a bad batch reports itself
# without halting the pipeline. The send_email_alert DAG reads this table.
ISSUES_DDL = """
CREATE TABLE IF NOT EXISTS data_issues (
    run_id     TEXT,
    detected_at TEXT DEFAULT CURRENT_TIMESTAMP,
    stage      TEXT,
    severity   TEXT,
    target     TEXT,
    issue      TEXT,
    row_count  INTEGER
);
"""

SCHEMA = """
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS clickstream;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id   TEXT PRIMARY KEY,
    customer_name TEXT,
    email         TEXT,
    city          TEXT,
    signup_date   TEXT
);

CREATE TABLE products (
    product_id    TEXT PRIMARY KEY,
    product_name  TEXT,
    category      TEXT,
    supplier_id   TEXT,
    cost_price    REAL,
    selling_price REAL
);

CREATE TABLE orders (
    order_id       TEXT PRIMARY KEY,
    customer_id    TEXT REFERENCES customers(customer_id),
    product_id     TEXT REFERENCES products(product_id),
    order_date     TEXT,
    quantity       INTEGER,
    unit_price     REAL,
    payment_status TEXT
);

CREATE TABLE clickstream (
    event_id        TEXT,
    customer_id     TEXT, --REFERENCES customers(customer_id),
    event_type      TEXT,
    page_url        TEXT,
    event_timestamp TEXT,
    device_type     TEXT
);
"""

# Indexes are created *after* loading -- cheaper than maintaining them per-insert.
INDEXES = """
CREATE INDEX idx_orders_customer   ON orders(customer_id);
CREATE INDEX idx_orders_product    ON orders(product_id);
CREATE INDEX idx_orders_date       ON orders(order_date);
CREATE INDEX idx_click_customer_ts ON clickstream(customer_id, event_timestamp);
CREATE INDEX idx_click_type        ON clickstream(event_type);
"""

# Transformation runs in SQL against the staging tables and writes a parallel
# *_clean set, so staging keeps the raw values for reference.
#
# Two notes on the SQL:
#   - SQLite has no regex, so "strip digits from email" is a nested REPLACE over
#     0-9, and the event_type normalisation is a nested REPLACE chain applied
#     outermost-last (lower/trim -> drop _ - space -> fix serach/clik typos).
#   - event_timestamp arrives in three formats. The slash format is DD/MM/YYYY:
#     its first component reaches 31 while the second never exceeds 7, so
#     day-first is the only reading the data supports. Naive values are treated
#     as UTC, matching pd.to_datetime(..., utc=True).
TRANSFORM = """
DROP TABLE IF EXISTS clickstream_rejected;
DROP TABLE IF EXISTS clickstream_clean;
DROP TABLE IF EXISTS orders_clean;
DROP TABLE IF EXISTS products_clean;
DROP TABLE IF EXISTS customers_clean;

CREATE TABLE customers_clean (
    customer_id   INTEGER PRIMARY KEY,
    customer_name TEXT,
    email         TEXT,
    city          TEXT,
    signup_date   TEXT
);

INSERT INTO customers_clean
SELECT
    CAST(customer_id AS INTEGER),
    customer_name,
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        LOWER(TRIM(email)),
        '0', ''), '1', ''), '2', ''), '3', ''), '4', ''),
        '5', ''), '6', ''), '7', ''), '8', ''), '9', ''),
    -- Q2.1 standardise city: trim, then title-case. Every city in this source is
    -- a single word, so upper-first/lower-rest is the correct canonical form and
    -- makes 'LONDON'/'london' collapse onto 'London' if they ever show up.
    UPPER(SUBSTR(TRIM(city), 1, 1)) || LOWER(SUBSTR(TRIM(city), 2)),
    DATE(signup_date)
FROM customers;

CREATE TABLE products_clean (
    product_id    INTEGER PRIMARY KEY,
    product_name  TEXT,
    category      TEXT,
    supplier_id   INTEGER,
    cost_price    REAL,
    selling_price REAL
);

INSERT INTO products_clean
SELECT
    CAST(product_id AS INTEGER),
    product_name,
    category,
    CAST(supplier_id AS INTEGER),
    cost_price,
    selling_price
FROM products;

CREATE TABLE orders_clean (
    order_id       INTEGER PRIMARY KEY,
    customer_id    INTEGER REFERENCES customers_clean(customer_id),
    product_id     INTEGER REFERENCES products_clean(product_id),
    order_date     TEXT,
    quantity       INTEGER,
    unit_price     REAL,
    payment_status TEXT
);

INSERT INTO orders_clean
SELECT
    CAST(order_id AS INTEGER),
    CAST(customer_id AS INTEGER),
    CAST(product_id AS INTEGER),
    DATE(order_date),
    quantity,
    unit_price,
    payment_status
FROM orders;

-- event_id is not unique (36 ids are reused by genuinely different events), so
-- the clean table gets a surrogate key and keeps every event.
CREATE TABLE clickstream_clean (
    event_key       INTEGER PRIMARY KEY,
    event_id        INTEGER,
    customer_id     INTEGER,
    event_type      TEXT,
    page_url        TEXT,
    event_timestamp TEXT,
    device_type     TEXT
);

-- Q2.4 quarantine rather than delete, so corrupted rows stay auditable.
CREATE TABLE clickstream_rejected (
    event_id        INTEGER,
    customer_id     INTEGER,
    event_type      TEXT,
    page_url        TEXT,
    event_timestamp TEXT,
    device_type     TEXT,
    reject_reason   TEXT
);

DROP VIEW IF EXISTS clickstream_norm;
CREATE VIEW clickstream_norm AS
SELECT
    CAST(event_id AS INTEGER) AS event_id,
    CAST(customer_id AS INTEGER) AS customer_id,
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        LOWER(TRIM(event_type)),
        '_', ''), '-', ''), ' ', ''),
        'serach', 'search'), 'clik', 'click') AS event_type,
    page_url,
    DATETIME(
        CASE
            WHEN event_timestamp LIKE '____-__-__T__:__:__Z'
                THEN REPLACE(REPLACE(event_timestamp, 'T', ' '), 'Z', '')
            WHEN event_timestamp LIKE '____-__-__ __:__:__'
                THEN event_timestamp
            WHEN event_timestamp LIKE '__/__/____ __:__'
                THEN substr(event_timestamp, 7, 4) || '-'
                  || substr(event_timestamp, 4, 2) || '-'
                  || substr(event_timestamp, 1, 2) || ' '
                  || substr(event_timestamp, 12, 5) || ':00'
        END
    ) AS event_timestamp,
    -- Q2.3 missing values: device_type is absent on 424 rows. Filled with
    -- 'unknown' so it stays countable in the device split instead of vanishing.
    COALESCE(LOWER(device_type), 'unknown') AS device_type
FROM clickstream;

INSERT INTO clickstream_clean
    (event_id, customer_id, event_type, page_url, event_timestamp, device_type)
SELECT event_id, customer_id, event_type, page_url, event_timestamp, device_type
FROM clickstream_norm
WHERE event_timestamp IS NOT NULL
  AND page_url IS NOT NULL;

INSERT INTO clickstream_rejected
SELECT event_id, customer_id, event_type, page_url, event_timestamp, device_type,
       CASE
           WHEN event_timestamp IS NULL THEN 'unparseable event_timestamp'
           WHEN page_url IS NULL        THEN 'missing page_url'
       END
FROM clickstream_norm
WHERE event_timestamp IS NULL
   OR page_url IS NULL;

CREATE INDEX idx_c_orders_customer   ON orders_clean(customer_id);
CREATE INDEX idx_c_orders_product    ON orders_clean(product_id);
CREATE INDEX idx_c_orders_date       ON orders_clean(order_date);
CREATE INDEX idx_c_click_customer_ts ON clickstream_clean(customer_id, event_timestamp);
CREATE INDEX idx_c_click_type        ON clickstream_clean(event_type);
"""

CLEAN_TABLES = ["customers_clean", "products_clean", "orders_clean",
                "clickstream_clean", "clickstream_rejected"]

# Load order matters: parents before children, or the FKs abort the insert.
STAGING_TABLES = [
    ("customers", "customers.csv"),
    ("products", "products.csv"),
    ("orders", "orders.csv"),
    ("clickstream", "clickstream.csv"),
]

def _ds(context):
    """Airflow 3 manual runs may carry no logical_date, so context['ds'] can be
    absent. Fall back to the run's own dates before the wall clock."""
    ds = context.get("ds")
    if ds:
        return str(ds)
    dr = context.get("dag_run")
    for attr in ("logical_date", "run_after", "start_date"):
        value = getattr(dr, attr, None)
        if value:
            return value.strftime("%Y-%m-%d")
    return datetime.utcnow().strftime("%Y-%m-%d")


def _record_issue(conn, run_id, stage, severity, target, issue, row_count=0):
    """Log a problem to data_issues instead of raising, so the run continues."""
    conn.execute(ISSUES_DDL)
    conn.execute(
        "INSERT INTO data_issues (run_id, stage, severity, target, issue, row_count)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, stage, severity, target, issue, row_count),
    )
    log.warning("[%s] %s | %s: %s (%s rows)", severity, stage, target, issue, row_count)


def alert_on_failure(context):
    """on_failure_callback: record a hard task failure so the alert reports it."""
    ti = context["task_instance"]
    try:
        conn = sqlite3.connect(STAGING_DB)
        try:
            _record_issue(
                conn, context["run_id"], ti.task_id, "ERROR", ti.task_id,
                f"task failed: {str(context.get('exception'))[:300]}", 0,
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        # Never let the callback mask the original failure.
        log.exception("could not record task failure for %s", ti.task_id)


default_args = {
    "owner": "quickcart_data-eng",
    "retries": 3,                              
    "retry_delay": timedelta(minutes=2),       
    "retry_exponential_backoff": True,         
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    # A hard failure still gets recorded and alerted rather than swallowed.
    "on_failure_callback": alert_on_failure,
}

# def validate_files():
#     df = pd.read_csv(RAW_FILE)

#     if df.isnull().values.any():
#         raise ValueError("Null values detected!")

#     if (df["Price"] < 0).any():
#         raise ValueError("Negative prices found!")

#     print("Validation successful!")
    
def load_staging(**context):
    """Load the raw CSVs into staging tables."""
    os.makedirs(os.path.dirname(STAGING_DB), exist_ok=True)
    conn = sqlite3.connect(STAGING_DB)
    try:
        conn.executescript(SCHEMA)
        counts = {}
        for table, fname in STAGING_TABLES:
            df = pd.read_csv(f"{DATA_DIR}/{fname}")
            df.to_sql(table, conn, if_exists="append", index=False)
            counts[table] = len(df)
            log.info("Loaded %s: %d rows from %s", table, len(df), fname)

        conn.executescript(INDEXES)
        conn.commit()
        log.info("Staging load complete -> %s | %s", STAGING_DB, counts)
        
    finally:
        conn.close()


def transform(**context):
    """Build the *_clean tables from staging, in SQL."""
    conn = sqlite3.connect(STAGING_DB)
    try:
        conn.executescript(TRANSFORM)

        counts = {}
        for table in CLEAN_TABLES:
            counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            log.info("Built %s: %d rows", table, counts[table])

        run_id = context["run_id"]

        # Q2.4 corrupted rows, quarantined by reason.
        for reason, n in conn.execute(
            "SELECT reject_reason, COUNT(*) FROM clickstream_rejected GROUP BY 1"
        ).fetchall():
            _record_issue(conn, run_id, "transform", "WARNING", "clickstream",
                          f"{n} rows quarantined: {reason}", n)

        # Q2.1 city standardisation -- report how many values the clean-up moved.
        changed = conn.execute(
            "SELECT COUNT(*) FROM customers c JOIN customers_clean cc"
            "  ON CAST(c.customer_id AS INTEGER) = cc.customer_id"
            " WHERE c.city <> cc.city"
        ).fetchone()[0]
        log.info("city standardisation changed %d of %d rows", changed,
                 counts.get("customers_clean", 0))

        # Q2.3 missing values that were filled rather than dropped.
        filled = conn.execute(
            "SELECT COUNT(*) FROM clickstream_clean WHERE device_type = 'unknown'"
        ).fetchone()[0]
        if filled:
            _record_issue(conn, run_id, "transform", "INFO", "clickstream",
                          f"{filled} rows had no device_type, filled with 'unknown'", filled)

        anon = conn.execute(
            "SELECT COUNT(*) FROM clickstream_clean WHERE customer_id IS NULL"
        ).fetchone()[0]
        if anon:
            _record_issue(conn, run_id, "transform", "INFO", "clickstream",
                          f"{anon} events have no customer_id and cannot be sessionised", anon)

        event_types = [r[0] for r in conn.execute(
            "SELECT DISTINCT event_type FROM clickstream_clean ORDER BY event_type"
        )]
        log.info("event_type normalised to %d values: %s", len(event_types), event_types)

        conn.commit()
        log.info("Transform complete -> %s | %s", STAGING_DB, counts)
    finally:
        conn.close()


GOLD_SQL = {
    # Q3.1 most visited pages
    "top_pages": """
        SELECT page_url, COUNT(*) AS views,
               ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM clickstream_clean), 2) AS pct
        FROM clickstream_clean
        GROUP BY page_url
        ORDER BY views DESC
    """,
    # Q3.4 mobile vs desktop traffic
    "device_split": """
        SELECT device_type, COUNT(*) AS events,
               ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM clickstream_clean), 2) AS pct
        FROM clickstream_clean
        GROUP BY device_type
        ORDER BY events DESC
    """,
    "event_mix": """
        SELECT event_type, COUNT(*) AS events,
               ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM clickstream_clean), 2) AS pct
        FROM clickstream_clean
        GROUP BY event_type
        ORDER BY events DESC
    """,
}

# Q3.2 session counts and Q3.3 bounce rate. A session breaks after
# SESSION_GAP_MIN minutes of inactivity for a given customer. Anonymous events
# (no customer_id) cannot be attributed to a session, so they are excluded here
# and reported separately.
SESSIONS_SQL = f"""
WITH ordered AS (
    SELECT customer_id, event_timestamp,
           LAG(event_timestamp) OVER (
               PARTITION BY customer_id ORDER BY event_timestamp
           ) AS prev_ts
    FROM clickstream_clean
    WHERE customer_id IS NOT NULL
), flagged AS (
    SELECT customer_id, event_timestamp,
           CASE
               WHEN prev_ts IS NULL THEN 1
               WHEN (julianday(event_timestamp) - julianday(prev_ts)) * 1440.0
                    > {SESSION_GAP_MIN} THEN 1
               ELSE 0
           END AS is_session_start
    FROM ordered
), sessioned AS (
    SELECT customer_id,
           SUM(is_session_start) OVER (
               PARTITION BY customer_id ORDER BY event_timestamp
               ROWS UNBOUNDED PRECEDING
           ) AS session_no
    FROM flagged
), per_session AS (
    SELECT customer_id, session_no, COUNT(*) AS events
    FROM sessioned
    GROUP BY customer_id, session_no
)
SELECT COUNT(*)                                        AS sessions,
       COUNT(DISTINCT customer_id)                     AS customers,
       SUM(CASE WHEN events = 1 THEN 1 ELSE 0 END)     AS bounced,
       ROUND(100.0 * SUM(CASE WHEN events = 1 THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                       AS bounce_rate_pct,
       ROUND(AVG(events), 2)                           AS avg_events_per_session
FROM per_session
"""


def _bar_rows(rows, label_i=0, value_i=1, pct_i=2):
    """Render result rows as a CSS bar chart -- no external libs, email-safe."""
    if not rows:
        return "<p class='muted'>no data</p>"
    top = max(r[value_i] for r in rows) or 1
    out = []
    for r in rows:
        label = html.escape(str(r[label_i]) if r[label_i] is not None else "(none)")
        width = 100.0 * r[value_i] / top
        out.append(
            f"<tr><td class='lbl'>{label}</td>"
            f"<td class='barcell'><span class='bar' style='width:{width:.1f}%'></span></td>"
            f"<td class='num'>{r[value_i]:,}</td>"
            f"<td class='num muted'>{r[pct_i]}%</td></tr>"
        )
    return "<table class='bars'>" + "".join(out) + "</table>"


def build_gold(**context):
    """Q3 analytics + the gold HTML dashboard, including this run's issues."""
    os.makedirs(GOLD_DIR, exist_ok=True)
    conn = sqlite3.connect(STAGING_DB)
    try:
        conn.execute(ISSUES_DDL)
        run_id = context["run_id"]
        ds = _ds(context)

        results = {name: conn.execute(sql).fetchall() for name, sql in GOLD_SQL.items()}
        sessions, customers, bounced, bounce_pct, avg_events = conn.execute(
            SESSIONS_SQL
        ).fetchone()

        total_events = conn.execute("SELECT COUNT(*) FROM clickstream_clean").fetchone()[0]
        rejected = conn.execute("SELECT COUNT(*) FROM clickstream_rejected").fetchone()[0]

        device = {d: (n, p) for d, n, p in results["device_split"]}
        mobile_pct = device.get("mobile", (0, 0))[1]
        desktop_pct = device.get("desktop", (0, 0))[1]

        issues = conn.execute(
            "SELECT severity, stage, target, issue, row_count FROM data_issues"
            " WHERE run_id = ? ORDER BY CASE severity WHEN 'ERROR' THEN 0"
            " WHEN 'WARNING' THEN 1 ELSE 2 END, stage",
            (run_id,),
        ).fetchall()

        kpis = [
            ("Total events", f"{total_events:,}"),
            ("Sessions", f"{sessions:,}"),
            ("Bounce rate", f"{bounce_pct}%"),
            ("Mobile traffic", f"{mobile_pct}%"),
            ("Desktop traffic", f"{desktop_pct}%"),
            ("Rows quarantined", f"{rejected:,}"),
        ]
        kpi_html = "".join(
            f"<div class='kpi'><div class='kpi-v'>{v}</div>"
            f"<div class='kpi-l'>{html.escape(l)}</div></div>"
            for l, v in kpis
        )

        if issues:
            rows = "".join(
                f"<tr class='sev-{html.escape(s.lower())}'><td>{html.escape(s)}</td>"
                f"<td>{html.escape(st)}</td><td>{html.escape(str(t))}</td>"
                f"<td>{html.escape(i)}</td>"
                f"<td class='num'>{n or ''}</td></tr>"
                for s, st, t, i, n in issues
            )
            issues_html = (
                "<table class='grid'><thead><tr><th>Severity</th><th>Stage</th>"
                "<th>Target</th><th>Issue</th><th>Rows</th></tr></thead>"
                f"<tbody>{rows}</tbody></table>"
            )
        else:
            issues_html = "<p class='ok'>No issues recorded for this run.</p>"

        n_err = sum(1 for i in issues if i[0] == "ERROR")
        n_warn = sum(1 for i in issues if i[0] == "WARNING")

        page = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>QuickCart — gold dashboard</title>
<style>
 body{{font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif;color:#1a1a1a;
      background:#f6f7f9;margin:0;padding:24px}}
 .wrap{{max-width:900px;margin:0 auto}}
 h1{{font-size:20px;margin:0 0 4px}} h2{{font-size:15px;margin:28px 0 10px}}
 .muted{{color:#6b7280}} .ok{{color:#047857}}
 .kpis{{display:flex;flex-wrap:wrap;gap:10px;margin:16px 0}}
 .kpi{{flex:1 1 130px;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:12px}}
 .kpi-v{{font-size:22px;font-weight:600}} .kpi-l{{font-size:12px;color:#6b7280}}
 table{{border-collapse:collapse;width:100%;background:#fff}}
 .grid th,.grid td{{border:1px solid #e5e7eb;padding:6px 8px;text-align:left;font-size:13px}}
 .grid th{{background:#f3f4f6}}
 .bars td{{padding:3px 6px;font-size:13px;border:0}}
 .bars .lbl{{width:150px}} .barcell{{width:auto}}
 .bar{{display:inline-block;height:12px;background:#2563eb;border-radius:3px}}
 .num{{text-align:right;white-space:nowrap}}
 .sev-error td{{background:#fef2f2}} .sev-warning td{{background:#fffbeb}}
 .banner{{padding:10px 12px;border-radius:8px;margin:12px 0;
          background:{'#fef2f2' if n_err else '#fffbeb' if n_warn else '#ecfdf5'};
          border:1px solid {'#fecaca' if n_err else '#fde68a' if n_warn else '#a7f3d0'}}}
</style></head><body><div class="wrap">
<h1>QuickCart — gold dashboard</h1>
<p class="muted">Run <code>{html.escape(run_id)}</code> · logical date {ds}
 · generated {datetime.utcnow():%Y-%m-%d %H:%M} UTC</p>

<div class="banner"><strong>{n_err} error(s), {n_warn} warning(s)</strong>
 recorded this run. The pipeline completed regardless — nothing was halted.</div>

<div class="kpis">{kpi_html}</div>

<h2>Data issues</h2>
{issues_html}

<h2>Q3.1 — Most visited pages</h2>
{_bar_rows(results['top_pages'])}

<h2>Q3.2 / Q3.3 — Sessions and bounce rate</h2>
<table class="grid"><tbody>
<tr><th>Sessions</th><td class="num">{sessions:,}</td></tr>
<tr><th>Distinct customers</th><td class="num">{customers:,}</td></tr>
<tr><th>Bounced (single-event) sessions</th><td class="num">{bounced:,}</td></tr>
<tr><th>Bounce rate</th><td class="num">{bounce_pct}%</td></tr>
<tr><th>Avg events per session</th><td class="num">{avg_events}</td></tr>
</tbody></table>
<p class="muted">Session = events for one customer split on a
 {SESSION_GAP_MIN}-minute inactivity gap. This source scatters event timestamps
 randomly across months with no clustering, so nearly every event lands in its own
 session and the bounce rate is correspondingly extreme — a property of the data,
 not the query.</p>

<h2>Q3.4 — Mobile vs desktop traffic</h2>
{_bar_rows(results['device_split'])}

<h2>Event mix</h2>
{_bar_rows(results['event_mix'])}
</div></body></html>"""

        with open(DASHBOARD_HTML, "w", encoding="utf-8") as fh:
            fh.write(page)

        log.info("Gold dashboard -> %s | sessions=%s bounce=%s%% mobile=%s%% desktop=%s%%",
                 DASHBOARD_HTML, sessions, bounce_pct, mobile_pct, desktop_pct)
        context["ti"].xcom_push(key="dashboard_path", value=DASHBOARD_HTML)
        context["ti"].xcom_push(key="issue_counts", value={"errors": n_err, "warnings": n_warn})
        conn.commit()
    finally:
        conn.close()


def validate(**context):
    """Schema + basic quality gate. Raise to fail the run (and alert)."""
    ds = _ds(context)
    problems = []

    checks = {
        "orders.csv": ["order_id", "customer_id", "product_id",
                       "order_date", "quantity", "unit_price", "payment_status"],
        "customers.csv": ["customer_id", "customer_name", "email", "city", "signup_date"],
        "products.csv": ["product_id", "product_name", "category",
                         "supplier_id", "cost_price", "selling_price"],
        "clickstream.csv": ["event_id", "customer_id", "event_type",
                            "page_url", "event_timestamp", "device_type"],
    }

    for fname, cols in checks.items():
        df = pd.read_csv(f"{DATA_DIR}/{fname}")
        log.info("Validating %s: %d rows", fname, len(df))

        missing = set(cols) - set(df.columns)
        if missing:
            problems.append(("ERROR", fname, f"missing columns {sorted(missing)}", 0))

        key = cols[0]
        if key in df.columns and df[key].duplicated().any():
            n = int(df[key].duplicated().sum())
            problems.append(("WARNING", fname, f"{n} duplicate {key}", n))

        if fname == "orders.csv" and "quantity" in df.columns:
            bad = int((df["quantity"] <= 0).sum())
            if bad:
                problems.append(("ERROR", fname, f"{bad} rows with quantity <= 0", bad))

        # DATE MISMATCH: order_date runs years past the run date. Reported, not
        # dropped -- the rows are otherwise intact and it may be a source bug.
        if fname == "orders.csv" and "order_date" in df.columns:
            od = pd.to_datetime(df["order_date"], errors="coerce")
            unparsed = int(od.isna().sum())
            if unparsed:
                problems.append(("ERROR", fname, f"{unparsed} unparseable order_date", unparsed))
            future = int((od > pd.Timestamp(ds)).sum())
            if future:
                span = f"{od.max().date()}"
                problems.append((
                    "WARNING", fname,
                    f"{future} orders dated after the run date {ds} (latest {span})",
                    future,
                ))

        # Mixed timestamp formats are a mismatch worth surfacing at validation
        # time, since the day-first reading is an assumption the transform makes.
        if fname == "clickstream.csv" and "event_timestamp" in df.columns:
            slash = int(df["event_timestamp"].astype(str).str.contains("/").sum())
            if slash:
                problems.append((
                    "WARNING", fname,
                    f"{slash} rows use DD/MM/YYYY while the rest are ISO 8601 "
                    f"-- parsed day-first",
                    slash,
                ))

    conn = sqlite3.connect(STAGING_DB)
    try:
        conn.execute(ISSUES_DDL)
        conn.execute("DELETE FROM data_issues WHERE run_id = ?", (context["run_id"],))
        for severity, target, issue, n in problems:
            _record_issue(conn, context["run_id"], "validate", severity, target, issue, n)
        conn.commit()
    finally:
        conn.close()

    log.info("Validation finished for %s: %d issue(s) recorded", ds, len(problems))




with DAG(
    dag_id="quickcart_ingestion_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    # SENSOR: wait until all four files to land (by waiting for the last/largest one).
    wait_for_files = FileSensor(
        task_id="wait_for_files",
        filepath=f"{DATA_DIR}/clickstream.csv",   # last/biggest to arrive
        fs_conn_id="fs_default",
        poke_interval=30,                          # check every 30 s
        timeout= 10800,                        # give up after 3 h
        mode="reschedule",                          # free the worker between pokes
    )
    
    load_staging_task = PythonOperator(
        task_id="load_staging",
        python_callable=load_staging
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform
    )

    build_gold_task = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold
    )

    # all_done: the alert must go out even when an upstream task hard-failed,
    # which is the whole point of alerting instead of halting.
    send_alert = TriggerDagRunOperator(
        task_id="send_alert",
        trigger_dag_id="send_email_alert",
        trigger_rule="all_done",
        wait_for_completion=False,
        reset_dag_run=True,
        # No {{ ds }} here: Airflow 3 manual runs have no logical_date, so the
        # template is undefined and the trigger fails. run_id is all the alert needs.
        conf={"run_id": "{{ run_id }}"},
    )

    (wait_for_files >> validate_task >> load_staging_task
     >> transform_task >> build_gold_task >> send_alert)