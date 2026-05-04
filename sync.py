import requests
import pandas as pd
import gspread
import os
import json
import time
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ================================
# RETRY FUNCTION (NEW)
# ================================
def retry(func, retries=5, delay=5):
    for i in range(retries):
        try:
            return func()
        except APIError as e:
            if "503" in str(e):
                print(f"⚠️ 503 error, retry {i+1}/{retries}...")
                time.sleep(delay * (i + 1))
            else:
                raise
    raise Exception("❌ Max retries reached")

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

CFIS_FILE_ID = os.environ["CFIS_FILE_ID"]
LUBRICANT_FILE_ID = os.environ["LUBRICANT_FILE_ID"]
INCENTIVE_FILE_ID = os.environ["INCENTIVE_FILE_ID"]
FUEL_FILE_ID = os.environ["FUEL_FILE_ID"]
AGING_FILE_ID = os.environ["AGING_FILE_ID"]
PENPEC_GOOGLE_SHEET_ID = os.environ["PENPEC_GOOGLE_SHEET_ID"]

PENPEC_TENANT_ID = os.environ["PENPEC_TENANT_ID"]
PENPEC_CLIENT_ID = os.environ["PENPEC_CLIENT_ID"]
PENPEC_CLIENT_SECRET = os.environ["PENPEC_CLIENT_SECRET"]
PENPEC_USER_EMAIL = os.environ["PENPEC_USER_EMAIL"]

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
            "Lubricant_Table": "Lubricant"
        }
    },
    {
        "name": "INCENTIVE",
        "file_id": INCENTIVE_FILE_ID,
        "google_sheet_id": PENPEC_GOOGLE_SHEET_ID,
        "last_sync_file": "last_sync_INCENTIVE.txt",
        "table_mapping": {
            "Table1": "Related Incentive"
        }
    },
    {
        "name": "FUEL",
        "file_id": FUEL_FILE_ID,
        "google_sheet_id": PENPEC_GOOGLE_SHEET_ID,
        "last_sync_file": "last_sync_FUEL.txt",
        "table_mapping": {
            "Item_ScoreCard": "SO Fuel"
        }
    },
    {
        "name": "AGING",
        "file_id": AGING_FILE_ID,
        "google_sheet_id": PENPEC_GOOGLE_SHEET_ID,
        "last_sync_file": "last_sync_AGING.txt",
        "table_mapping": {
            "AR_IV__C1": "Aging C1",
            "AR_IV__C2": "Aging C2"
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

def get_headers(tenant, client_id, client_secret):
    token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"

    token_data = {
        "client_id": client_id,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }

    res = requests.post(token_url, data=token_data, timeout=30)
    res.raise_for_status()

    token = res.json().get("access_token")

    if not token:
        raise Exception(f"❌ Cannot get token: {res.json()}")

    return {
        "Authorization": f"Bearer {token}"
    }

# ================================
# TIME HELPERS
# ================================

def parse_graph_datetime(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

def read_last_sync_time(last_sync_file: str) -> datetime:
    if not os.path.exists(last_sync_file):
        return datetime(2000, 1, 1, tzinfo=timezone.utc)
    return parse_graph_datetime(open(last_sync_file).read().strip())

def save_last_sync_time(last_sync_file: str, dt_str: str) -> None:
    with open(last_sync_file, "w") as f:
        f.write(dt_str)

# ================================
# EXCEL LAST MODIFIED
# ================================

def get_excel_last_modified(file_id: str, headers, user_email) -> str:
    url = f"https://graph.microsoft.com/v1.0/users/{user_email}/drive/items/{file_id}?$select=lastModifiedDateTime"
    return requests.get(url, headers=headers).json()["lastModifiedDateTime"]

# ================================
# SYNC ONE TABLE
# ================================

def sync_table(file_id, spreadsheet, table_name, sheet_name, headers, user_email):
    print(f"🔄 Syncing {table_name}...")

    rows_url = f"https://graph.microsoft.com/v1.0/users/{user_email}/drive/items/{file_id}/workbook/tables/{table_name}/rows"
    cols_url = f"https://graph.microsoft.com/v1.0/users/{user_email}/drive/items/{file_id}/workbook/tables/{table_name}/columns"

    rows = [r["values"][0] for r in requests.get(rows_url, headers=headers).json()["value"]]
    cols = [c["name"] for c in requests.get(cols_url, headers=headers).json()["value"]]

    df = pd.DataFrame(rows, columns=cols)

    print(f"   Rows: {len(df)}")

    worksheet = retry(lambda: spreadsheet.worksheet(sheet_name)) \
        if sheet_name in [ws.title for ws in spreadsheet.worksheets()] \
        else spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")

    retry(lambda: worksheet.clear())

    if len(df) == 0:
        retry(lambda: worksheet.update([cols]))
    else:
        retry(lambda: worksheet.update([df.columns.tolist()] + df.values.tolist()))

    print(f"   ✅ Synced to {sheet_name}")

# ================================
# PROCESS ONE CONFIG
# ================================

def process_one_config(config):
    print(f"\n📦 START CONFIG: {config['name']}")

    # 👇 判断是不是 AGING（用 Penpec）
    if config["name"] == "AGING":
        headers = get_headers(PENPEC_TENANT_ID, PENPEC_CLIENT_ID, PENPEC_CLIENT_SECRET)
        user_email = PENPEC_USER_EMAIL
    else:
        headers = get_headers(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
        user_email = USER_EMAIL

    spreadsheet = retry(lambda: gspread_client.open_by_key(config["google_sheet_id"]))

    excel_last_modified = parse_graph_datetime(
        get_excel_last_modified(config["file_id"], headers, user_email)
    )

    last_sync_time = read_last_sync_time(config["last_sync_file"])

    print(f"📄 Excel last modified : {excel_last_modified}")
    print(f"🕒 Last sync time      : {last_sync_time}")

    if excel_last_modified <= last_sync_time:
        print("🟡 No changes detected. Skip sync.")
        return

    print("🟢 Changes detected. Start syncing...")

    for table, sheet in config["table_mapping"].items():
        sync_table(config["file_id"], spreadsheet, table, sheet, headers, user_email)

    save_last_sync_time(config["last_sync_file"], excel_last_modified.isoformat())
    print(f"🎉 {config['name']} DONE")
# ================================
# MAIN
# ================================

def main():
    print("🚀 START MULTI EXCEL TO GOOGLE SYNC")

    for config in SYNC_CONFIGS:
        process_one_config(config)
        print("⏳ Sleeping 3 seconds to avoid API limit...")
        time.sleep(3)  # 👈 关键

    print("🎉 ALL CONFIGS DONE")

if __name__ == "__main__":
    main()
