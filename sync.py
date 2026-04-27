import requests
import pandas as pd
import gspread
import os
import json
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials

# ================================
#  CONFIG (FROM GITHUB SECRETS)
# ================================

TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
USER_EMAIL = os.environ["USER_EMAIL"]

ESBFILE_ID = os.environ["ESBFILE_ID"]
ESBGOOGLE_SHEET_ID = os.environ["ESBGOOGLE_SHEET_ID"]

TESTFILE_ID = os.environ["FILE_ID"]
TEST_GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]

# ====================================================

CFIS_FILE_ID = os.environ["CFIS_FILE_ID"]
LUBRICANT_FILE_ID = os.environ["LUBRICANT_FILE_ID"]
INCENTIVE_FILE_ID = os.environ["INCENTIVE_FILE_ID"]
PENPEC_GOOGLE_SHEET_ID = os.environ["PENPEC_GOOGLE_SHEET_ID"]

# ================================
# MULTI CONFIG
# ================================

SYNC_CONFIGS = [
    {
        "name": "ESB",
        "file_id": ESBFILE_ID,
        "google_sheet_id": ESBGOOGLE_SHEET_ID,
        "last_sync_file": "last_sync_esb.txt",
        "table_mapping": {
            "IVDTL_Table": "IVDTL",
            "CN_Table": "CN",
            "IV_Table": "IV",
            "Client_Table": "Client"
        }
    },
    {
        "name": "TEST",
        "file_id": TESTFILE_ID,
        "google_sheet_id": TEST_GOOGLE_SHEET_ID,
        "last_sync_file": "last_sync_test.txt",
        "table_mapping": {
            "InvoiceTable": "Invoice_Header",
            "InvoiceDetailTable": "Invoice_Detail"
        }
    },
    {
        "name": "CFIS",
        "file_id": CFIS_FILE_ID,
        "google_sheet_id": PENPEC_GOOGLE_SHEET_ID,
        "last_sync_file": "last_sync_CFIS.txt",
        "table_mapping": {
            "CF_Table": "CF",
            "IS_Table": "IS",
            "CR_Table": "Loyalty Liters- CR & B-Infinite",
            "RP_Table": "Redeemed Points"
        }
    },
    {
        "name": "LUBRICANT",
        "file_id": LUBRICANT_FILE_ID,
        "google_sheet_id": PENPEC_GOOGLE_SHEET_ID,
        "last_sync_file": "last_sync_LUBRICANT.txt",
        "table_mapping": {
            "Lubricant_Table": "Lubricant",
        }
    },
    {
        "name": "INCENTIVE",
        "file_id": INCENTIVE_FILE_ID,
        "google_sheet_id": PENPEC_GOOGLE_SHEET_ID,
        "last_sync_file": "last_sync_INCENTIVE.txt",
        "table_mapping": {
            "Table1": "Related Incentive",
        }
    }
]

# ================================
# GOOGLE AUTH
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

gspread_client = gspread.authorize(creds)

# ================================
# MICROSOFT GRAPH TOKEN
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
# TIME HELPERS
# ================================

def parse_graph_datetime(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

def read_last_sync_time(last_sync_file: str) -> datetime:
    if not os.path.exists(last_sync_file):
        return datetime(2000, 1, 1, tzinfo=timezone.utc)

    with open(last_sync_file, "r", encoding="utf-8") as f:
        content = f.read().strip()

    if not content:
        return datetime(2000, 1, 1, tzinfo=timezone.utc)

    return parse_graph_datetime(content)

def save_last_sync_time(last_sync_file: str, dt_str: str) -> None:
    with open(last_sync_file, "w", encoding="utf-8") as f:
        f.write(dt_str)

# ================================
# EXCEL LAST MODIFIED
# ================================

def get_excel_last_modified(file_id: str) -> str:
    file_meta_url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/items/{file_id}"
        f"?$select=id,name,lastModifiedDateTime"
    )

    res = requests.get(file_meta_url, headers=headers, timeout=30)
    res.raise_for_status()

    data = res.json()

    if "lastModifiedDateTime" not in data:
        raise Exception(f"Cannot get lastModifiedDateTime: {data}")

    return data["lastModifiedDateTime"]

# ================================
# SYNC ONE TABLE
# ================================

def sync_table(file_id: str, spreadsheet, table_name: str, sheet_name: str) -> None:
    print(f"🔄 Syncing {table_name}...")

    rows_url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/items/"
        f"{file_id}/workbook/tables/{table_name}/rows"
    )
    rows_res = requests.get(rows_url, headers=headers, timeout=60)
    rows_res.raise_for_status()

    rows_json = rows_res.json()
    row_items = rows_json.get("value", [])
    rows = [r["values"][0] for r in row_items]

    cols_url = (
        f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/items/"
        f"{file_id}/workbook/tables/{table_name}/columns"
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
# PROCESS ONE EXCEL
# ================================

def process_one_config(config: dict) -> None:
    name = config["name"]
    file_id = config["file_id"]
    google_sheet_id = config["google_sheet_id"]
    last_sync_file = config["last_sync_file"]
    table_mapping = config["table_mapping"]

    print("====================================")
    print(f"📦 START CONFIG: {name}")
    print("====================================")

    spreadsheet = gspread_client.open_by_key(google_sheet_id)

    excel_last_modified_str = get_excel_last_modified(file_id)
    excel_last_modified = parse_graph_datetime(excel_last_modified_str)

    last_sync_time = read_last_sync_time(last_sync_file)

    print(f"📄 Excel last modified : {excel_last_modified_str}")
    print(f"🕒 Last sync time      : {last_sync_time.isoformat()}")

    if excel_last_modified <= last_sync_time:
        print("🟡 No changes detected. Skip sync.")
        print("====================================")
        return

    print("🟢 Changes detected. Start syncing...")

    for table, sheet in table_mapping.items():
        sync_table(file_id, spreadsheet, table, sheet)

    save_last_sync_time(last_sync_file, excel_last_modified_str)

    print(f"📝 Updated {last_sync_file} to: {excel_last_modified_str}")
    print(f"🎉 {name} DONE")
    print("====================================")

# ================================
# MAIN
# ================================

def main():
    print("🚀 START MULTI EXCEL TO GOOGLE SYNC")

    for config in SYNC_CONFIGS:
        process_one_config(config)

    print("🎉 ALL CONFIGS DONE")

if __name__ == "__main__":
    main()
