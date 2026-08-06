from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.hooks.smtp import SmtpHook
from datetime import datetime, timedelta
import html
import os
import sqlite3
import logging

log = logging.getLogger(__name__)

STAGING_DB = "/opt/airflow/data/warehouse/quick_cart.db"
DASHBOARD_HTML = "/opt/airflow/data/gold/quickcart_dashboard.html"
ALERT_EMAIL_TO = os.environ["ALERT_EMAIL_TO"]

default_args = {
    "owner": "analytics_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def build_alert(**context):
    """Assemble the alert body from the gold dashboard plus this run's issues.

    Triggered by quickcart_ingestion_pipeline with the upstream run_id in conf.
    Task is named send_email_alert to match the DAG it lives in.
    Reports problems; it does not re-raise them.
    """
    conf = (context["dag_run"].conf or {})
    upstream_run = conf.get("run_id")

    errors = warnings = 0
    summary_rows = ""

    if os.path.exists(STAGING_DB):
        conn = sqlite3.connect(STAGING_DB)
        try:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
            if "data_issues" in tables:
                if upstream_run:
                    rows = conn.execute(
                        "SELECT severity, stage, target, issue, row_count FROM data_issues"
                        " WHERE run_id = ? ORDER BY CASE severity WHEN 'ERROR' THEN 0"
                        " WHEN 'WARNING' THEN 1 ELSE 2 END",
                        (upstream_run,),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT severity, stage, target, issue, row_count FROM data_issues"
                        " ORDER BY detected_at DESC LIMIT 50"
                    ).fetchall()

                errors = sum(1 for r in rows if r[0] == "ERROR")
                warnings = sum(1 for r in rows if r[0] == "WARNING")
                summary_rows = "".join(
                    f"<tr><td>{html.escape(s)}</td><td>{html.escape(st)}</td>"
                    f"<td>{html.escape(str(t))}</td><td>{html.escape(i)}</td>"
                    f"<td align='right'>{n or ''}</td></tr>"
                    for s, st, t, i, n in rows
                )
            else:
                log.info("no data_issues table yet -- nothing to report")
        finally:
            conn.close()
    else:
        log.warning("staging DB not found at %s", STAGING_DB)

    # The gold dashboard is the body when it exists; the issue table is prepended
    # so the alert is readable even if the dashboard failed to build.
    dashboard = ""
    if os.path.exists(DASHBOARD_HTML):
        with open(DASHBOARD_HTML, encoding="utf-8") as fh:
            dashboard = fh.read()
    else:
        log.warning("dashboard not found at %s -- sending issue summary only",
                    DASHBOARD_HTML)

    if summary_rows:
        header = (
            "<h2>Pipeline issues</h2>"
            "<table border='1' cellpadding='6' cellspacing='0'>"
            "<tr><th>Severity</th><th>Stage</th><th>Target</th>"
            "<th>Issue</th><th>Rows</th></tr>"
            f"{summary_rows}</table>"
        )
    else:
        header = "<p>No data issues recorded for this run.</p>"

    body = dashboard if dashboard else f"<html><body>{header}</body></html>"

    subject = (
        f"QuickCart pipeline — {errors} error(s), {warnings} warning(s)"
        if (errors or warnings)
        else "QuickCart pipeline — clean run"
    )

    log.info("Alert prepared: %s (dashboard attached: %s)", subject, bool(dashboard))

    # Sent from this task rather than handed to an EmailOperator via XCom: the
    # dashboard body runs to several KB and the XCom round-trip failed on it.
    with SmtpHook() as smtp:
        smtp.send_email_smtp(
            to=ALERT_EMAIL_TO,
            subject=subject,
            html_content=body,
            files=[DASHBOARD_HTML] if dashboard else None,
        )
    log.info("Alert sent to %s", ALERT_EMAIL_TO)


with DAG(
    dag_id="send_email_alert",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # triggered by quickcart_ingestion_pipeline
    catchup=False,
    default_args=default_args,
) as dag:

    send_alert = PythonOperator(
        task_id="send_alert",
        python_callable=build_alert,
        retries=3,
    )
