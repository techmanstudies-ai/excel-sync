import requests
import pandas as pd
import gspread
import os
import json
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials

# ================================
# 🔐 CONFIG (FROM GITHUB SECRETS)
# ================================

TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
USER_EMAIL = os.environ["USER_EMAIL"]
FILE_ID = os.environ["FILE_ID"]
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]

# state file
LAST_SYNC_FILE = "last_sync.txt"

# table mapping
TABLE_MAPPING = {
    "IVDTL_Table": "IVDTL",
    "CN_Table": "CN",
    "IV_Table": "IV",
    "Client_Table": "Client"
}

# ================================
# 🔐 GOOGLE AUTH
# ================================

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=scope
)

client = gspread.authorize(creds)
spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

# ================================
# 🔐 MICROSOFT GRAPH TOKEN
# ================================

token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

token_data = {
    "client_id": CLIENT_ID,
    "scope": "https://graph.microsoft.com/.default",
    "client_secret": CLIENT_SECRET,
    "grant_type": "client_credentials"
}

token_res = requests.post(token_url, data=token_data, timeout=30)
token_res.raise_for_status()

token_json = token_res.json()
if "access_token" not in token_json:
    raise Exception(f"Cannot get access token: {token_json}")

access_token = token_json["access_token"]

headers = {
    "Authorization": f"Bearer {access_token}"
}

# ================================
# 🕒 HELPER: TIME
# ================================

def parse_graph_datetime(dt_str: str) -> datetime:
    """
    Convert Graph datetime like 2026-04-07T01:23:45Z into timezone-aware datetime.
    """
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

def read_last_sync_time() -> datetime:
    """
    Read last sync time from last_sync.txt.
    If file not found, use very old datetime.
    """
    if not os.path.exists(LAST_SYNC_FILE):
        return datetime(2000, 1, 1, tzinfo=timezone.utc)

    with open(LAST_SYNC_FILE, "r", encoding="utf-8") as f:
        content = f.read().strip()

    if not content:
        return datetime(2000, 1, 1, tzinfo=timezone.utc)

    return parse_graph_datetime(content)

def save_last_sync_time(dt_str: str) -> None:
    """
    Save last sync time into last_sync.txt
    """
    with open(LAST_SYNC_FILE, "w", encoding="utf-8") as f:
        f.write(dt_str)

# ================================
# 📄 HELPER: EXCEL LAST MODIFIED
# ================================

def get_excel_last_modified() -> str:
    """
    Get Excel file last modified time from Microsoft Graph
    """
    file_meta_url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/items/{FILE_ID}"
        f"?$select=id,name,lastModifiedDateTime"
    )

    res = requests.get(file_meta_url, headers=headers, timeout=30)
    res.raise_for_status()

    data = res.json()

    if "lastModifiedDateTime" not in data:
        raise Exception(f"Cannot get lastModifiedDateTime: {data}")

    return data["lastModifiedDateTime"]

# ================================
# 🔄 SYNC FUNCTION
# ================================

def sync_table(table_name: str, sheet_name: str) -> None:
    print(f"🔄 Syncing {table_name}...")

    rows_url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/items/"
        f"{FILE_ID}/workbook/tables/{table_name}/rows"
    )
    rows_res = requests.get(rows_url, headers=headers, timeout=60)
    rows_res.raise_for_status()

    rows_json = rows_res.json()
    row_items = rows_json.get("value", [])
    rows = [r["values"][0] for r in row_items]

    cols_url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/items/"
        f"{FILE_ID}/workbook/tables/{table_name}/columns"
    )
    cols_res = requests.get(cols_url, headers=headers, timeout=60)
    cols_res.raise_for_status()

    cols_json = cols_res.json()
    columns = [c["name"] for c in cols_json.get("value", [])]

    df = pd.DataFrame(rows, columns=columns)

    print(f"   Rows: {len(df)}")

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")

    worksheet.clear()

    if len(df) == 0:
        worksheet.update([columns])
    else:
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    print(f"   ✅ Synced to {sheet_name}")

# ================================
# 🚀 MAIN
# ================================

def main():
    print("====================================")
    print("🚀 START EXCEL TO GOOGLE SYNC")
    print("====================================")

    excel_last_modified_str = get_excel_last_modified()
    excel_last_modified = parse_graph_datetime(excel_last_modified_str)

    last_sync_time = read_last_sync_time()

    print(f"📄 Excel last modified : {excel_last_modified_str}")
    print(f"🕒 Last sync time      : {last_sync_time.isoformat()}")

    if excel_last_modified <= last_sync_time:
        print("🟡 No changes detected. Skip sync.")
        print("====================================")
        return

    print("🟢 Changes detected. Start syncing...")

    for table, sheet in TABLE_MAPPING.items():
        sync_table(table, sheet)

    save_last_sync_time(excel_last_modified_str)

    print(f"📝 Updated {LAST_SYNC_FILE} to: {excel_last_modified_str}")
    print("🎉 ALL DONE")
    print("====================================")

if __name__ == "__main__":
    main()
