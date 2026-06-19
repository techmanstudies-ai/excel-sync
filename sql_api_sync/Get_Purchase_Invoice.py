import requests
import json
import os
import time
import gspread
from requests_aws4auth import AWS4Auth
from google.oauth2.service_account import Credentials

# ==========================================
# CONFIG
# ==========================================

DOCUMENT_TYPE = "purchaseinvoice"
TARGET_YEAR = 2026

ACCESS_KEY = os.environ["SQL_ACCESS_KEY"]
SECRET_KEY = os.environ["SQL_SECRET_KEY"]

REGION = "ap-southeast-5"
SERVICE = "sqlaccount"

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
BASE_URL = f"https://api.sql.my/{DOCUMENT_TYPE}"

JUMP_SIZE = 500
LIMIT = 50
AUTO_PUSH_EVERY = 500
MAX_RETRY = 5

WORKSHEET_NAME = "Purchase_Invoice"

# Save progress/log in same folder as script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PROGRESS_FILE = os.path.join(
    BASE_DIR,
    f"progress_{DOCUMENT_TYPE}_{TARGET_YEAR}.json"
)

# ==========================================
# AUTH
# ==========================================

auth = AWS4Auth(
    ACCESS_KEY,
    SECRET_KEY,
    REGION,
    SERVICE
)

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=scope
)

gc = gspread.authorize(creds)

spreadsheet = gc.open_by_key(SPREADSHEET_ID)
sheet = spreadsheet.worksheet(WORKSHEET_NAME)

print("Connected to Google Sheet.")

# ==========================================
# API REQUEST WITH RETRY
# ==========================================

def safe_request(url):

    for i in range(MAX_RETRY):

        try:

            response = requests.get(
                url,
                auth=auth,
                timeout=30
            )

            if response.status_code == 200:
                return response

            print(
                f"API status {response.status_code}, "
                f"retry {i+1}/{MAX_RETRY}"
            )

        except Exception as e:

            print(
                f"Request error: {e}, "
                f"retry {i+1}/{MAX_RETRY}"
            )

        time.sleep(2 * (i + 1))

    return None

# ==========================================
# LOAD EXISTING DOCNO
# ==========================================

existing_docnos = set()

records = sheet.col_values(1)

if len(records) > 1:
    existing_docnos = set(records[1:])

print(f"Existing invoices in sheet: {len(existing_docnos)}")

# ==========================================
# PROGRESS LOG ONLY
# ==========================================

def save_progress(offset, pushed_count):

    with open(PROGRESS_FILE, "w") as f:

        json.dump(
            {
                "last_checked_offset": offset,
                "target_year": TARGET_YEAR,
                "pushed_count_this_run": pushed_count,
                "note": "This file is only for log. Script does not resume from this offset."
            },
            f,
            indent=4
        )

# ==========================================
# FIND TARGET YEAR START
# ==========================================

def find_year_start():

    print("\nPhase 1: Jump searching for target year...")

    offset = 0
    last_valid_offset = 0

    while True:

        url = f"{BASE_URL}?offset={offset}&limit=1"

        response = safe_request(url)

        if not response:

            print(
                f"Cannot fetch offset {offset}. "
                f"Using last valid offset {last_valid_offset}."
            )

            return last_valid_offset

        data = response.json().get("data", [])

        if not data:

            print("No data found during jump search.")

            return last_valid_offset

        docdate = data[0].get("docdate", "")

        if not docdate:

            offset += JUMP_SIZE
            continue

        try:

            doc_year = int(docdate[:4])

        except ValueError:

            offset += JUMP_SIZE
            continue

        print(f"Jump offset {offset}: {docdate}")

        if doc_year >= TARGET_YEAR:

            start_offset = max(0, offset - JUMP_SIZE)

            print(
                f"Target year located. "
                f"Rewinding to offset {start_offset}"
            )

            return start_offset

        last_valid_offset = offset

        offset += JUMP_SIZE

# ==========================================
# PUSH FUNCTION
# ==========================================

def push_rows(rows):

    if not rows:
        return 0

    if not sheet.get_all_values():

        sheet.append_row([
            "DocNo",
            "DocDate",
            "SupplierCode",
            "SupplierName",
            "Currency",
            "DocAmount",
            "LocalAmount",
            "Status",
            "Cancelled",
            "CreatedDate"
        ])

    sheet.append_rows(
        rows,
        value_input_option="RAW"
    )

    print(f"Pushed {len(rows)} rows to Google Sheet")

    return len(rows)

# ==========================================
# DOWNLOAD PHASE
# ==========================================

offset = find_year_start()

print("\nPhase 2: Downloading purchase invoices...")
print("This run will scan from year start and skip existing DocNo.")

batch_rows = []
total_pushed = 0

while True:

    print(f"\nFetching offset {offset}...")

    url = f"{BASE_URL}?offset={offset}&limit={LIMIT}"

    response = safe_request(url)

    if not response:

        print(f"Cannot fetch offset {offset}. Stopping safely.")

        break

    headers = response.json().get("data", [])

    if not headers:

        print("No more records.")

        break

    stop_all = False

    for inv in headers:

        docdate = inv.get("docdate", "")

        if not docdate:
            continue

        try:

            doc_year = int(docdate[:4])

        except ValueError:

            continue

        if doc_year < TARGET_YEAR:
            continue

        if doc_year > TARGET_YEAR:

            stop_all = True
            break

        docno = inv.get("docno")

        if not docno:
            continue

        if docno in existing_docnos:
            continue

        row = [
            docno,
            docdate,
            inv.get("code"),
            inv.get("companyname"),
            inv.get("currencycode"),
            inv.get("docamt"),
            inv.get("localdocamt"),
            inv.get("status"),
            inv.get("cancelled"),
            inv.get("creationdate")
        ]

        batch_rows.append(row)

        existing_docnos.add(docno)

    if len(batch_rows) >= AUTO_PUSH_EVERY:

        total_pushed += push_rows(batch_rows)

        batch_rows = []

    save_progress(
        offset,
        total_pushed + len(batch_rows)
    )

    if stop_all:

        print(f"\nFinished target year {TARGET_YEAR}.")

        break

    offset += LIMIT

    time.sleep(0.05)

# ==========================================
# FINAL PUSH
# ==========================================

total_pushed += push_rows(batch_rows)

save_progress(offset, total_pushed)

print("\nSync complete.")
print(f"Total new rows pushed this run: {total_pushed}")