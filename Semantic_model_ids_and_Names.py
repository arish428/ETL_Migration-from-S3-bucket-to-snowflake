# ============================================================
# Microsoft Fabric / Power BI
# → Export ALL Semantic Model IDs & Names from ALL Workspaces
# ============================================================

import os
import requests
import pandas as pd
from dotenv import load_dotenv
from requests.exceptions import RequestException

# =========================
# LOAD ENV VARIABLES
# =========================

load_dotenv("Arish_qa_data_validation.env")

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

OUTPUT_FILE = "all_semantic_models.csv"
REQUEST_TIMEOUT = 30  # seconds

# =========================
# AUTHENTICATION
# =========================

def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://analysis.windows.net/powerbi/api/.default"
    }
    response = requests.post(url, data=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()["access_token"]

# =========================
# FETCH WORKSPACES
# =========================

def fetch_workspaces(token):
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://api.powerbi.com/v1.0/myorg/groups"

    workspaces = []
    while url:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        workspaces.extend(data.get("value", []))
        url = data.get("@odata.nextLink")

    return workspaces

# =========================
# FETCH SEMANTIC MODELS
# =========================

def fetch_all_models(token, workspace):
    headers = {"Authorization": f"Bearer {token}"}
    ws_id = workspace["id"]
    ws_name = workspace.get("name", "Unknown")

    url = f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/datasets"
    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    datasets = response.json().get("value", [])

    records = []
    for d in datasets:
        records.append({
            "workspace_id": ws_id,
            "workspace_name": ws_name,
            "semantic_model_id": d.get("id"),
            "semantic_model_name": d.get("name"),
            "is_refreshable": d.get("isRefreshable"),
            "configured_by": d.get("configuredBy")
        })

    return records

# =========================
# MAIN
# =========================

def main():
    print("Starting FULL Semantic Model export...")

    token = get_access_token()
    workspaces = fetch_workspaces(token)

    total_ws = len(workspaces)
    print(f"Total workspaces found: {total_ws}")

    all_records = []
    skipped = 0

    for idx, ws in enumerate(workspaces, start=1):
        ws_name = ws.get("name", "Unknown")
        print(f"[{idx}/{total_ws}] Processing workspace: {ws_name}")

        try:
            records = fetch_all_models(token, ws)
            all_records.extend(records)
        except RequestException as e:
            skipped += 1
            print(f"⚠️ Skipped workspace due to error: {ws_name}")
            continue

    if not all_records:
        print("No semantic models found.")
        return

    df = pd.DataFrame(all_records)
    df.drop_duplicates(inplace=True)
    df.sort_values(by=["workspace_name", "semantic_model_name"], inplace=True)

    df.to_csv(OUTPUT_FILE, index=False)

    print("\n✅ CSV exported successfully:", OUTPUT_FILE)
    print("Total semantic models:", len(df))
    print("Skipped workspaces:", skipped)

if __name__ == "__main__":
    main()
