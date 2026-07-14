# ~/airflow/dags/kibot_daily_babysitter.py
"""
Kibot_daily_babysitter — auto-recovery DAG for missed / failed Kibot_daily_pipeline runs.

Runs once a day at 22:00 UTC (after the main DAG's 10:00 UTC fire + 10x1h retry
window has finished at ~20:00 UTC). Looks back N business days for any logical_date
that isn't `success` in the main DAG. For each such date:

  1. Skip if Kibot hasn't published the archives yet (next babysitter run will retry).
  2. Skip if the Stocks or ETFs archive is malformed (7z "Tail Size" warning, or
     `Daily/` or `Intraday/` subdir missing). Tomorrow's babysitter will retry,
     hoping Kibot has fixed/replaced the archive.
  3. Otherwise: delete the failed dag_run + cascade + summary files, then trigger
     the main DAG fresh for that date. The triggered runs queue behind
     max_active_runs=1 on the main DAG and process serially.

Designed to be safe to re-run: deletes are scoped to the specific dag_run + cascade,
triggers are idempotent via Airflow's UNIQUE constraint on (dag_id, logical_date)
which we explicitly clear first.
"""
import logging
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, date, timedelta

log = logging.getLogger(__name__)

for path in (
    "/home/magrino_bini/Code_Repos/py_algotrading_infrastructure",
):
    if path not in sys.path:
        sys.path.append(path)

from airflow import DAG
from airflow.operators.python import PythonOperator


MAIN_DAG_ID = "Kibot_daily_pipeline"
AIRFLOW_HOME = "/home/magrino_bini/airflow"
AIRFLOW_DB = f"{AIRFLOW_HOME}/airflow.db"
LOOKBACK_BUSINESS_DAYS = 10

ARCHIVE_BASE = "/home/magrino_bini/sberg_share_us1/Historical_data/Equities/US/Kibot/Dump/Updates"
CDF_STAGE_ROOTS = [
    "/home/magrino_bini/sberg_share_us1/Historical_data/Equities/US/Kibot/Kibot_cdf_by_date/3_All_Stocks_daily",
    "/home/magrino_bini/sberg_share_us1/Historical_data/Equities/US/Kibot/Kibot_cdf_by_date/7_All_ETFs_daily",
    "/home/magrino_bini/sberg_share_us1/Historical_data/Equities/US/Kibot/Kibot_cdf_by_date/1_All_Stocks_1min",
    "/home/magrino_bini/sberg_share_us1/Historical_data/Equities/US/Kibot/Kibot_cdf_by_date/5_All_ETFs_1min",
]

# Minimum bytes a healthy archive should have — Kibot publishes ~500-1000 MB
# Stocks and ~300-500 MB ETFs. Catches "thin partial" publications that pass
# structural RAR validation but contain almost no data (per-file size <100 B).
MIN_ARCHIVE_BYTES = {
    "stocks": 50 * 1024 * 1024,
    "etfs":   30 * 1024 * 1024,
}


def _stocks_path(d: date) -> str:
    return f"{ARCHIVE_BASE}/All Stocks/Daily/{d.strftime('%Y%m%d')}.exe"


def _etfs_path(d: date) -> str:
    return f"{ARCHIVE_BASE}/All ETFs/Daily/{d.strftime('%Y%m%d')}.exe"


def _archive_healthy(path: str) -> tuple:
    """Run 7z l and check for malformed-archive signals. Returns (healthy, reason)."""
    # Size floor — catches "thin partial" publications that pass structural
    # validation (well-formed RAR, both subdirs present) but contain almost
    # no actual data per file.
    kind = "stocks" if "/All Stocks/" in path else ("etfs" if "/All ETFs/" in path else None)
    if kind is not None:
        try:
            sz = os.path.getsize(path)
        except OSError as e:
            return False, f"stat failed: {e}"
        floor = MIN_ARCHIVE_BYTES[kind]
        if sz < floor:
            return False, f"size below floor: {sz:,} B < {floor:,} B ({kind})"
    try:
        result = subprocess.run(["7z", "l", path], capture_output=True, text=True, check=False)
    except FileNotFoundError:
        return False, "7z binary not found"
    if result.returncode != 0:
        return False, f"7z l exit={result.returncode}"
    out = result.stdout
    # Detect tail-data malformed pattern (the case we hit with 2026-06-11 / 06-12).
    if "WARNINGS" in out and "Tail Size" in out:
        for line in out.splitlines():
            if "Tail Size" in line:
                return False, f"malformed archive (tail data): {line.strip()}"
    # Confirm both Daily/ and Intraday/ subdirs present in the listing.
    has_daily = " Daily/" in out
    has_intraday = " Intraday/" in out
    if not (has_daily and has_intraday):
        return False, f"missing subdir(s): Daily={has_daily} Intraday={has_intraday}"
    return True, "ok"


def _get_main_dag_state_for_date(d: date) -> str:
    """Return state of the most-recent dag_run for the given calendar date
    (matches any time component on logical_date), or None if no dag_run exists."""
    conn = sqlite3.connect(AIRFLOW_DB)
    cur = conn.cursor()
    cur.execute(
        "SELECT state FROM dag_run WHERE dag_id=? AND substr(logical_date,1,10)=? "
        "ORDER BY start_date DESC LIMIT 1",
        (MAIN_DAG_ID, d.isoformat()),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def _delete_runs_for_date(d: date) -> int:
    """Delete every dag_run for the given calendar date (any time component) +
    cascade across task-related tables. Returns count of runs deleted.

    Same approach as backfill_dates.py — we know this works and avoids the
    orphan-xcom collisions we hit when relying on Airflow's own cascade."""
    conn = sqlite3.connect(AIRFLOW_DB)
    cur = conn.cursor()
    cur.execute(
        "SELECT run_id FROM dag_run WHERE dag_id=? AND substr(logical_date,1,10)=?",
        (MAIN_DAG_ID, d.isoformat()),
    )
    run_ids = [r[0] for r in cur.fetchall()]
    for run_id in run_ids:
        for tbl in ("xcom", "task_instance_history", "task_reschedule",
                    "rendered_task_instance_fields", "task_map", "task_instance"):
            try:
                cur.execute(f"DELETE FROM {tbl} WHERE run_id=? AND dag_id=?",
                            (run_id, MAIN_DAG_ID))
            except sqlite3.OperationalError:
                try:
                    cur.execute(f"DELETE FROM {tbl} WHERE run_id=?", (run_id,))
                except sqlite3.OperationalError:
                    pass
        cur.execute("DELETE FROM dag_run WHERE dag_id=? AND run_id=?",
                    (MAIN_DAG_ID, run_id))
    # Orphan xcom sweep — SQLite rowid reuse can cause collisions otherwise.
    cur.execute(
        "DELETE FROM xcom WHERE dag_id=? AND dag_run_id NOT IN "
        "(SELECT id FROM dag_run WHERE dag_id=?)",
        (MAIN_DAG_ID, MAIN_DAG_ID),
    )
    conn.commit()
    conn.close()
    return len(run_ids)


def _delete_bad_archives(d: date) -> list:
    """Remove stocks + etfs archives for date d so the next FTP pull will
    re-download them. Used when integrity check flags malformed/truncated
    archives — usually the result of an aborted FTP transfer that left a
    partial file on disk; deletion forces _should_download_file to re-pull."""
    removed = []
    for p in (_stocks_path(d), _etfs_path(d)):
        if os.path.exists(p):
            try:
                os.remove(p)
                removed.append(p)
            except OSError as e:
                log.warning(f"could not remove {p}: {e}")
    return removed


def _delete_summary_files(d: date) -> int:
    """Remove processing_summary_<YYYYMMDD>.csv from each stage root so the
    main pipeline's extract task re-extracts (otherwise it short-circuits via
    the 'skip if summary exists' check)."""
    ymd = d.strftime("%Y%m%d")
    n = 0
    for root in CDF_STAGE_ROOTS:
        p = os.path.join(root, f"processing_summary_{ymd}.csv")
        if os.path.exists(p):
            try:
                os.remove(p)
                n += 1
            except OSError as e:
                log.warning(f"could not remove {p}: {e}")
    return n


def _trigger_main_dag(d: date) -> str:
    """Trigger the main DAG via the airflow CLI with logical_date=d 00:00:00.
    Returns the new run_id (most recent for that logical_date)."""
    env = os.environ.copy()
    env["AIRFLOW_HOME"] = AIRFLOW_HOME
    result = subprocess.run(
        ["airflow", "dags", "trigger", MAIN_DAG_ID, "--logical-date", d.isoformat(),
         "-o", "plain"],
        capture_output=True, text=True, env=env, check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"trigger failed for {d}: rc={result.returncode} "
            f"stderr={result.stderr.strip()[:300]}"
        )
    # Resolve new run_id via the DB.
    import time
    time.sleep(2)
    conn = sqlite3.connect(AIRFLOW_DB)
    cur = conn.cursor()
    cur.execute(
        "SELECT run_id FROM dag_run WHERE dag_id=? AND substr(logical_date,1,10)=? "
        "ORDER BY start_date DESC NULLS LAST LIMIT 1",
        (MAIN_DAG_ID, d.isoformat()),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else "(unknown)"


def babysit(**context):
    """Single task body. Detects, validates, retriggers."""
    # Step 1 — find non-success weekday dates in the lookback window.
    today = date.today()
    candidates = []
    i = 1
    while len(candidates) < LOOKBACK_BUSINESS_DAYS and i < LOOKBACK_BUSINESS_DAYS * 2 + 4:
        d = today - timedelta(days=i)
        i += 1
        if d.weekday() >= 5:
            continue   # weekends are not trading days
        state = _get_main_dag_state_for_date(d)
        if state != "success":
            candidates.append((d, state))

    log.info(f"=== babysit: {len(candidates)} non-success date(s) in lookback "
             f"(last {LOOKBACK_BUSINESS_DAYS} business days) ===")
    for d, s in candidates:
        log.info(f"  {d}  current_state={s}")

    triggered, skipped = [], []
    for d, state in candidates:
        stk, etf = _stocks_path(d), _etfs_path(d)
        if not (os.path.exists(stk) and os.path.exists(etf)):
            reason = (f"missing archive(s): "
                      f"stocks={'✓' if os.path.exists(stk) else '✗'} "
                      f"etfs={'✓' if os.path.exists(etf) else '✗'}")
            log.info(f"  {d}: SKIP — {reason} (Kibot hasn't published; will retry next run)")
            skipped.append((d, reason))
            continue

        s_ok, s_reason = _archive_healthy(stk)
        e_ok, e_reason = _archive_healthy(etf)
        if not (s_ok and e_ok):
            reason = f"malformed: stocks={s_reason} | etfs={e_reason}"
            deleted = _delete_bad_archives(d)
            log.info(f"  {d}: SKIP — {reason}; deleted {len(deleted)} bad archive(s) "
                     f"to force re-pull on next FTP run")
            skipped.append((d, reason))
            continue

        try:
            n_runs = _delete_runs_for_date(d)
            n_sum = _delete_summary_files(d)
            log.info(f"  {d}: cleared {n_runs} dag_run(s), {n_sum} summary file(s); triggering...")
            run_id = _trigger_main_dag(d)
            log.info(f"  {d}: TRIGGERED run_id={run_id}")
            triggered.append(d)
        except Exception as e:
            log.error(f"  {d}: trigger failed: {type(e).__name__}: {e}")
            skipped.append((d, f"trigger error: {e}"))

    log.info(f"=== babysit done: triggered={len(triggered)} "
             f"skipped={len(skipped)} ===")
    return {
        "triggered": [d.isoformat() for d in triggered],
        "skipped": [(d.isoformat(), r) for d, r in skipped],
    }


default_args = {
    "owner": "Pulse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="Kibot_daily_babysitter",
    default_args=default_args,
    description="Auto-recover missed / failed Kibot_daily_pipeline runs in the last N business days",
    schedule="0 22 * * *",                # 22:00 UTC — after main DAG retries finish
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["kibot", "babysitter"],
) as dag:

    babysit_task = PythonOperator(
        task_id="babysit",
        python_callable=babysit,
    )
