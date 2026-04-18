#!/usr/bin/env python3
"""
powerbi_refresh_script.py

- Loads environment variables from Arish_qa_data_validation.env (or DOTENV_PATH).
- Exposes refresh_powerbi_and_log(...) which performs:
    1) Get OAuth token
    2) Insert latest (previous) refresh record into MySQL
    3) Trigger a new refresh
    4) Poll briefly and insert 'New' refresh record(s) into MySQL
- Provides run_func(...) wrapper for Airflow PythonOperator.
- When run directly, prints verbose logs and tracebacks for debugging.
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv, find_dotenv

# ---------------------------
# Logging config
# ---------------------------
logger = logging.getLogger("powerbi_refresh")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


# ---------------------------
# Dotenv loading
# ---------------------------
def _load_env_file(env_filename: str = "Arish_qa_data_validation.env") -> None:
    """
    Load environment variables from DOTENV_PATH if provided, otherwise load env_filename
    from same directory as this script or search for it using find_dotenv.
    """
    dotenv_override = os.getenv("DOTENV_PATH")
    if dotenv_override and os.path.exists(dotenv_override):
        load_dotenv(dotenv_override)
        logger.info("Loaded env from DOTENV_PATH: %s", dotenv_override)
        return

    # Try path next to this file
    base_dir = os.path.dirname(__file__)
    local_path = os.path.join(base_dir, env_filename)
    if os.path.exists(local_path):
        load_dotenv(local_path)
        logger.info("Loaded env from local file: %s", local_path)
        return

    # Last resort: try to find it up the tree
    found = find_dotenv(env_filename, raise_error_if_not_found=False)
    if found:
        load_dotenv(found)
        logger.info("Loaded env from discovered file: %s", found)
        return

    logger.warning("No %s found. Falling back to system environment variables.", env_filename)


# ---------------------------
# HTTP session helper
# ---------------------------
def make_session(retries: int = 3, backoff: float = 1.0, timeout: int = 10) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"])
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.request_timeout = timeout  # not used by requests directly; use explicit timeouts in calls
    return s


# ---------------------------
# Core workflow function
# ---------------------------
def refresh_powerbi_and_log(
    env_filename: str = "Arish_qa_data_validation.env",
    poll_seconds: int = 90,
    poll_interval: int = 8,
    http_retries: int = 3,
    http_timeout: int = 15,
) -> None:
    """
    Perform the Power BI refresh workflow and log results to MySQL.

    - env_filename: name of the dotenv file to load (if not using DOTENV_PATH).
    - poll_seconds / poll_interval: how long to poll for new refresh result (best-effort).
    - http_retries / http_timeout: controls requests retry and request timeout.
    """
    _load_env_file(env_filename)

    # Load required env vars
    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    TENANT_ID = os.getenv("TENANT_ID")
    WORKSPACE_ID = os.getenv("WORKSPACE_ID")
    DATASET_ID = os.getenv("DATASET_ID")
    DASHBOARD_NAME = os.getenv("DASHBOARD_NAME", "")

    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")

    # Basic validation
    missing = [k for k, v in {
        "CLIENT_ID": CLIENT_ID, "CLIENT_SECRET": CLIENT_SECRET, "TENANT_ID": TENANT_ID,
        "WORKSPACE_ID": WORKSPACE_ID, "DATASET_ID": DATASET_ID,
        "DB_HOST": DB_HOST, "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD, "DB_NAME": DB_NAME
    }.items() if not v]

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    AUTH_PARAMS = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://analysis.windows.net/powerbi/api/.default",
        "grant_type": "client_credentials"
    }

    session = make_session(retries=http_retries, backoff=1.0, timeout=http_timeout)

    # Internal helpers
    def get_oauth_token() -> Optional[str]:
        try:
            resp = session.post(TOKEN_URL, data=AUTH_PARAMS, timeout=http_timeout)
            resp.raise_for_status()
            token = resp.json().get("access_token")
            if not token:
                logger.error("OAuth response missing access_token: %s", resp.text)
                return None
            logger.debug("Obtained OAuth token (len=%d)", len(token))
            return token
        except requests.RequestException as e:
            logger.exception("Failed to get OAuth token: %s", e)
            return None

    def log_to_mysql(dataset_id: str, dashboard_name: str, completed_time: Optional[datetime],
                     status: str, message: str, request_id: Optional[str]) -> None:
        conn = None
        cursor = None
        try:
            conn = mysql.connector.connect(
                host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
            )
            cursor = conn.cursor()
            sql = """
            INSERT INTO powerbi_refresh_log
            (dataset_id, dashboard_name, completed_time, status, message, request_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            # MySQL DATETIME is naive; store UTC naive datetime if provided
            completed_val = None
            if completed_time:
                if completed_time.tzinfo is not None:
                    completed_val = completed_time.astimezone(timezone.utc).replace(tzinfo=None)
                else:
                    completed_val = completed_time
            cursor.execute(sql, (
                dataset_id,
                dashboard_name,
                completed_val,
                status,
                message,
                request_id
            ))
            conn.commit()
            logger.info("Logged to MySQL: status=%s request_id=%s completed_time=%s",
                        status, request_id, completed_val)
        except Exception as e:
            logger.exception("Error logging to MySQL: %s", e)
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            # re-raise? we choose to raise so callers can mark failure
            raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def fetch_latest_refresh(token: str) -> Optional[Dict[str, Any]]:
        headers = {"Authorization": f"Bearer {token}"}
        # request the latest refresh explicitly
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/refreshes?$top=1&$orderby=startTime desc"
        try:
            resp = session.get(url, headers=headers, timeout=http_timeout)
            resp.raise_for_status()
            items = resp.json().get("value", [])
            if not items:
                logger.warning("No refresh history found for dataset %s", DATASET_ID)
                return None
            return items[0]
        except requests.RequestException as e:
            logger.exception("Failed to fetch refresh history: %s", e)
            return None

    def insert_refresh_record(token: str, prefix: str = "History") -> None:
        latest = fetch_latest_refresh(token)
        if not latest:
            logger.info("%s: no refresh record available", prefix)
            return

        status = latest.get("status", "Unknown")
        end_time = latest.get("endTime") or latest.get("startTime")
        request_id = latest.get("requestId") or latest.get("id") or None
        service_exception = latest.get("serviceException")
        if isinstance(service_exception, dict):
            message = service_exception.get("exceptionMessage") or service_exception.get("message") or str(service_exception)
        else:
            message = str(service_exception) if service_exception else "Success"

        completed_time = None
        if end_time:
            try:
                completed_time = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
            except Exception:
                try:
                    completed_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
                except Exception:
                    logger.exception("Failed to parse endTime: %s", end_time)
                    completed_time = None

        log_to_mysql(DATASET_ID, DASHBOARD_NAME, completed_time, status, f"{prefix}: {message}", request_id)

    def trigger_refresh(token: str) -> bool:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/refreshes"
        try:
            resp = session.post(url, headers=headers, json={}, timeout=http_timeout)
            # Power BI returns 202 for accepted; sometimes 200/201 may appear
            if resp.status_code in (202, 200, 201):
                logger.info("Triggered refresh for dataset %s, code=%s", DATASET_ID, resp.status_code)
                return True
            else:
                logger.error("Failed to trigger refresh: status=%s body=%s", resp.status_code, resp.text)
                return False
        except requests.RequestException as e:
            logger.exception("Exception when triggering refresh: %s", e)
            return False

    # ---------------------------
    # Execute workflow
    # ---------------------------
    token = get_oauth_token()
    if not token:
        raise RuntimeError("Failed to obtain OAuth token")

    # Insert previous/latest record
    try:
        insert_refresh_record(token, prefix="Previous")
    except Exception:
        logger.exception("Error inserting previous refresh record (continuing)")

    # Trigger refresh
    if not trigger_refresh(token):
        raise RuntimeError("Failed to trigger Power BI refresh")

    # Poll for short time to try to capture the new refresh outcome (best-effort)
    deadline = time.time() + poll_seconds
    while time.time() < deadline:
        try:
            insert_refresh_record(token, prefix="New")
        except Exception:
            logger.exception("Error inserting 'New' refresh record (continuing)")
        latest = fetch_latest_refresh(token)
        if latest:
            status = (latest.get("status") or "").lower()
            # stop early if completed
            if status and status not in ("running", "queued"):
                logger.info("Latest refresh finished with status: %s", status)
                break
        time.sleep(poll_interval)

    logger.info("Power BI refresh workflow finished.")


# ---------------------------
# Airflow-friendly wrapper
# ---------------------------
def run_func(**context):
    """
    Airflow PythonOperator entry point.
    Example: PythonOperator(python_callable=run_func)
    """
    # You can pass env_filename via Airflow Variables or as kwargs in op_args if desired
    return refresh_powerbi_and_log()


# ---------------------------
# Manual test runner
# ---------------------------
if __name__ == "__main__":
    import traceback
    import sys
    # Make console logs verbose for manual run
    logger.setLevel(logging.DEBUG)
    print("🔹 Running refresh_powerbi_and_log() manually (verbose)...\n")
    try:
        # Use default env file name; set DOTENV_PATH to override if needed.
        refresh_powerbi_and_log()
        print("\n✅ Completed without exception.")
    except Exception as e:
        print("\n❌ Exception raised while running refresh_powerbi_and_log():", repr(e))
        traceback.print_exc()
        sys.exit(1)
