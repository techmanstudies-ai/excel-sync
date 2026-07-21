import json
import os
import time

import gspread
import requests
from google.oauth2.service_account import Credentials
from requests_aws4auth import AWS4Auth

# ==========================================
# CONFIG
# ==========================================

DOCUMENT_TYPE = "salescreditnote"
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

WORKSHEET_NAME = "Sales_Credit_Note"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROGRESS_FILE = os.path.join(
    BASE_DIR,
    f"progress_{DOCUMENT_TYPE}_{TARGET_YEAR}.json"
)

HEADERS = [
    "DocNo",
    "DocDate",
    "CustomerCode",
    "CustomerName",
    "Currency",
    "DocAmount",
    "LocalAmount",
    "Status",
    "Cancelled",
    "CreatedDate",
    "DocKey"
]

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

creds_dict = json.loads(os.environ["SQL_GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=scope
)

gc = gspread.authorize(creds)
spreadsheet = gc.open_by_key(SPREADSHEET_ID)

try:
    sheet = spreadsheet.worksheet(WORKSHEET_NAME)
except gspread.WorksheetNotFound:
    sheet = spreadsheet.add_worksheet(
        title=WORKSHEET_NAME,
        rows=1000,
        cols=len(HEADERS)
    )

print(f"Connected to worksheet: {WORKSHEET_NAME}")
print(f"Downloading Sales Credit Notes for {TARGET_YEAR}")

# ==========================================
# API REQUEST WITH RETRY
# ==========================================

def safe_request(url):
    for attempt in range(1, MAX_RETRY + 1):
        try:
            response = requests.get(
                url,
                auth=auth,
                timeout=30
            )

            if response.status_code == 200:
                return response

            print(
                f"API status {response.status_code} "
                f"on attempt {attempt}/{MAX_RETRY}"
            )

            if response.text:
                print(response.text[:1000])

        except requests.RequestException as error:
            print(
                f"Request error on attempt "
                f"{attempt}/{MAX_RETRY}: {error}"
            )

        time.sleep(2 * attempt)

    return None

# ==========================================
# DATE/YEAR HELPER
# ==========================================

def get_document_year(docdate):
    if not docdate:
        return None

    text = str(docdate).strip()

    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])

    if "/" in text:
        parts = text.split("/")
        if len(parts) == 3 and parts[2][:4].isdigit():
            return int(parts[2][:4])

    return None

# ==========================================
# ENSURE HEADER
# ==========================================

def ensure_header():
    first_row = sheet.row_values(1)

    if not first_row:
        sheet.update(
            range_name=f"A1:K1",
            values=[HEADERS]
        )
        print("Sales Credit Note header created.")
        return

    existing = [
        str(value).strip()
        for value in first_row[:len(HEADERS)]
    ]

    required = [
        str(value).strip()
        for value in HEADERS
    ]

    if existing == required:
        return

    first_cell = str(first_row[0]).strip() if first_row else ""

    if first_cell and first_cell != "DocNo":
        sheet.insert_row(
            HEADERS,
            index=1,
            value_input_option="RAW"
        )
        print("Header inserted above existing data.")
        return

    sheet.update(
        range_name=f"A1:K1",
        values=[HEADERS]
    )
    print("Existing header corrected.")

ensure_header()

# ==========================================
# LOAD EXISTING DOCNO
# ==========================================

existing_docnos = set()

records = sheet.col_values(1)

if len(records) > 1:
    existing_docnos = {
        str(docno).strip()
        for docno in records[1:]
        if str(docno).strip()
    }

print(f"Existing Sales Credit Notes: {len(existing_docnos)}")

# ==========================================
# PROGRESS LOG
# ==========================================

def save_progress(offset, pushed_count):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as file:
        json.dump(
            {
                "document_type": DOCUMENT_TYPE,
                "worksheet_name": WORKSHEET_NAME,
                "last_checked_offset": offset,
                "target_year": TARGET_YEAR,
                "pushed_count_this_run": pushed_count,
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "note": (
                    "Log only. Script scans from target-year start "
                    "and skips existing DocNo."
                )
            },
            file,
            indent=4,
            ensure_ascii=False
        )

# ==========================================
# FIND TARGET YEAR START
# ==========================================

def find_year_start():
    print("\nPhase 1: Jump searching for target year...")

    offset = 0
    last_valid_offset = 0

    while True:
        response = safe_request(
            f"{BASE_URL}?offset={offset}&limit=1"
        )

        if not response:
            print(
                f"Cannot fetch offset {offset}. "
                f"Using last valid offset {last_valid_offset}."
            )
            return last_valid_offset

        try:
            data = response.json().get("data", [])
        except ValueError:
            print("API returned invalid JSON.")
            return last_valid_offset

        if not data:
            print("No data found during jump search.")
            return last_valid_offset

        docdate = data[0].get("docdate", "")
        doc_year = get_document_year(docdate)

        if doc_year is None:
            offset += JUMP_SIZE
            continue

        print(f"Jump offset {offset}: {docdate}")

        if doc_year >= TARGET_YEAR:
            start_offset = max(0, offset - JUMP_SIZE)
            print(f"Target year located. Rewinding to offset {start_offset}")
            return start_offset

        last_valid_offset = offset
        offset += JUMP_SIZE

# ==========================================
# PUSH FUNCTION
# ==========================================

def push_rows(rows):
    if not rows:
        return 0

    ensure_header()

    sheet.append_rows(
        rows,
        value_input_option="RAW"
    )

    print(f"Pushed {len(rows)} rows to Google Sheet")
    return len(rows)

# ==========================================
# DOWNLOAD
# ==========================================

offset = find_year_start()

print("\nPhase 2: Downloading Sales Credit Notes...")

batch_rows = []
total_pushed = 0
total_checked = 0

while True:
    print(f"\nFetching Sales Credit Note offset {offset}...")

    response = safe_request(
        f"{BASE_URL}?offset={offset}&limit={LIMIT}"
    )

    if not response:
        print(f"Cannot fetch offset {offset}. Stopping safely.")
        break

    try:
        credit_notes = response.json().get("data", [])
    except ValueError:
        print(f"Invalid JSON at offset {offset}. Stopping safely.")
        break

    if not credit_notes:
        print("No more records.")
        break

    stop_all = False

    for credit_note in credit_notes:
        total_checked += 1

        docdate = credit_note.get("docdate", "")
        doc_year = get_document_year(docdate)

        if doc_year is None:
            continue

        if doc_year < TARGET_YEAR:
            continue

        if doc_year > TARGET_YEAR:
            stop_all = True
            break

        docno = str(credit_note.get("docno", "")).strip()

        if not docno or docno in existing_docnos:
            continue

        batch_rows.append(
            [
                docno,
                docdate,
                credit_note.get("code"),
                credit_note.get("companyname"),
                credit_note.get("currencycode"),
                credit_note.get("docamt"),
                credit_note.get("localdocamt"),
                credit_note.get("status"),
                credit_note.get("cancelled"),
                credit_note.get("creationdate"),
                credit_note.get("dockey")
            ]
        )

        existing_docnos.add(docno)

    if len(batch_rows) >= AUTO_PUSH_EVERY:
        total_pushed += push_rows(batch_rows)
        batch_rows = []

    save_progress(
        offset,
        total_pushed + len(batch_rows)
    )

    print(
        f"Checked: {total_checked} | "
        f"Waiting: {len(batch_rows)} | "
        f"Pushed: {total_pushed}"
    )

    if stop_all:
        print(f"\nFinished target year {TARGET_YEAR}.")
        break

    offset += LIMIT
    time.sleep(0.05)

total_pushed += push_rows(batch_rows)
save_progress(offset, total_pushed)

print("\nSales Credit Note sync complete.")
print(f"Total records checked: {total_checked}")
print(f"Total new rows pushed: {total_pushed}")
