import requests
import pandas as pd
import gspread
import os
import json
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

# table mapping
TABLE_MAPPING = {
    "InvoiceTable": "Invoice_Header",
    "InvoiceDetailTable": "Invoice_Detail"
}

# ================================
# 🔐 GOOGLE AUTH (FROM SECRET JSON)
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
# 🔐 GET TOKEN (MICROSOFT GRAPH)
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

    url = f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/items/{FILE_ID}/workbook/tables/{table_name}/rows"
    res = requests.get(url, headers=headers)
    data = res.json()["value"]

    rows = [r["values"][0] for r in data]

    col_url = f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/drive/items/{FILE_ID}/workbook/tables/{table_name}/columns"
    col_res = requests.get(col_url, headers=headers)
    columns = [c["name"] for c in col_res.json()["value"]]

    df = pd.DataFrame(rows, columns=columns)

    print(f"   Rows: {len(df)}")

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
