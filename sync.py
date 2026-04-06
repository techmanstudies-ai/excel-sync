import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ================================
# 🔐 CONFIG
# ================================

TENANT_ID = "38ea5a79-e934-48d1-a5d0-43b67f2959e2"
CLIENT_ID = "e6da8248-5255-4694-a0f6-cba2b6ada7af"
CLIENT_SECRET = "puU8Q~QRHbcniFJ1e6PzVNTrlqwn2jTMKaHsVa~y"

USER_EMAIL = "office1@tftechman.onmicrosoft.com"
FILE_ID = "53989FB3-B76F-4D18-8F2C-B27EE392E97D"

GOOGLE_SHEET_NAME = "Copy of Test"

# table mapping
TABLE_MAPPING = {
    "InvoiceTable": "Invoice_Header",
    "InvoiceDetailTable": "Invoice_Detail"
}

# ================================
# 🔐 GOOGLE AUTH
# ================================

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(
    "credentials.json",
    scopes=scope
)

client = gspread.authorize(creds)
spreadsheet = client.open_by_key("1YFSKluOs-XSwvg7bAFlfjp85K4u2fSDOub_jF4V2GrU")

# ================================
# 🔐 GET TOKEN
# ================================

token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

token_data = {
    "client_id": CLIENT_ID,
    "scope": "https://graph.microsoft.com/.default",
    "client_secret": CLIENT_SECRET,
    "grant_type": "client_credentials"
}

token_res = requests.post(token_url, data=token_data)
access_token = token_res.json()["access_token"]

headers = {
    "Authorization": f"Bearer {access_token}"
}

# ================================
# 🔄 SYNC FUNCTION
# ================================

def sync_table(table_name, sheet_name):
    print(f"🔄 Syncing {table_name}...")

    # 1️⃣ GET EXCEL DATA
    url = f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/items/{FILE_ID}/workbook/tables/{table_name}/rows"
    res = requests.get(url, headers=headers)
    data = res.json()["value"]

    # extract values
    rows = [r["values"][0] for r in data]

    # get columns
    col_url = f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/items/{FILE_ID}/workbook/tables/{table_name}/columns"
    col_res = requests.get(col_url, headers=headers)
    columns = [c["name"] for c in col_res.json()["value"]]

    # dataframe
    df = pd.DataFrame(rows, columns=columns)

    print(f"   Rows: {len(df)}")

    # 2️⃣ WRITE TO GOOGLE SHEET (OVERWRITE)
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")

    worksheet.clear()

    worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    print(f"   ✅ Synced to {sheet_name}")

# ================================
# 🚀 RUN ALL
# ================================

for table, sheet in TABLE_MAPPING.items():
    sync_table(table, sheet)

print("🎉 ALL DONE")