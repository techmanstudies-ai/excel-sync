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

WORKSHEET_NAME = "Sales_Credit_Note_Detail"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROGRESS_FILE = os.path.join(
    BASE_DIR,
    f"progress_{DOCUMENT_TYPE}_detail_{TARGET_YEAR}.json"
)

HEADERS = [
    "UniqueKey",
    "DocNo",
    "DocDate",
    "DocKey",
    "DtlKey",
    "Sequence",
    "CustomerCode",
    "CustomerName",
    "ItemCode",
    "Description",
    "Description2",
    "Qty",
    "UOM",
    "Rate",
    "UnitPrice",
    "Discount",
    "Amount",
    "LocalAmount",
    "TaxCode",
    "TaxRate",
    "TaxAmount",
    "LocalTaxAmount",
    "Location",
    "BatchNo",
    "Project",
    "Account",
    "FromDocType",
    "FromDocKey",
    "FromDtlKey",
    "Remark1",
    "Remark2"
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
print(f"Downloading Sales Credit Note details for {TARGET_YEAR}")

# ==========================================
# API REQUEST
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
# HELPERS
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


def make_unique_key(docno, detail_key, sequence, itemcode, description):
    if detail_key not in (None, ""):
        return f"{docno}|DTLKEY:{detail_key}"

    if sequence not in (None, ""):
        return f"{docno}|SEQ:{sequence}"

    return f"{docno}|{itemcode or ''}|{description or ''}"


def extract_detail_lines(detail_json):
    data = detail_json.get("data", [])

    if isinstance(data, list):
        header = data[0] if data else {}
    elif isinstance(data, dict):
        header = data
    else:
        header = {}

    for key in [
        "sdsdocdetail",
        "sdsDocDetail",
        "docdetail",
        "details",
        "detail"
    ]:
        lines = header.get(key, [])
        if isinstance(lines, list) and lines:
            return lines

    return []

# ==========================================
# ENSURE HEADER
# ==========================================

def ensure_header():
    first_row = sheet.row_values(1)

    if not first_row:
        sheet.update(
            range_name=f"A1:AE1",
            values=[HEADERS]
        )
        print("Sales Credit Note Detail header created.")
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

    if first_cell and first_cell != "UniqueKey":
        sheet.insert_row(
            HEADERS,
            index=1,
            value_input_option="RAW"
        )
        print("Detail header inserted above existing data.")
        return

    sheet.update(
        range_name=f"A1:AE1",
        values=[HEADERS]
    )
    print("Existing detail header corrected.")

ensure_header()

# ==========================================
# LOAD EXISTING KEYS
# ==========================================

existing_keys = set()

records = sheet.col_values(1)

if len(records) > 1:
    existing_keys = {
        str(value).strip()
        for value in records[1:]
        if str(value).strip()
    }

print(f"Existing Sales Credit Note detail rows: {len(existing_keys)}")

# ==========================================
# PROGRESS
# ==========================================

def save_progress(offset, pushed_count, checked_documents):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as file:
        json.dump(
            {
                "document_type": DOCUMENT_TYPE,
                "worksheet_name": WORKSHEET_NAME,
                "last_checked_offset": offset,
                "target_year": TARGET_YEAR,
                "checked_documents_this_run": checked_documents,
                "pushed_count_this_run": pushed_count,
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
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
            return last_valid_offset

        try:
            data = response.json().get("data", [])
        except ValueError:
            return last_valid_offset

        if not data:
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
# PUSH
# ==========================================

def push_rows(rows):
    if not rows:
        return 0

    ensure_header()

    sheet.append_rows(
        rows,
        value_input_option="RAW"
    )

    print(f"Pushed {len(rows)} detail rows")
    return len(rows)

# ==========================================
# DOWNLOAD
# ==========================================

offset = find_year_start()

print("\nPhase 2: Downloading Sales Credit Note details...")

batch_rows = []
total_pushed = 0
checked_documents = 0
checked_lines = 0

while True:
    print(f"\nFetching Sales Credit Note header offset {offset}...")

    response = safe_request(
        f"{BASE_URL}?offset={offset}&limit={LIMIT}"
    )

    if not response:
        print(f"Cannot fetch offset {offset}. Stopping safely.")
        break

    try:
        headers = response.json().get("data", [])
    except ValueError:
        print(f"Invalid JSON at offset {offset}. Stopping safely.")
        break

    if not headers:
        print("No more records.")
        break

    stop_all = False

    for credit_note in headers:
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
        dockey = credit_note.get("dockey")

        if not docno or not dockey:
            continue

        checked_documents += 1
        print(f"Fetching details for {docno} | DocKey: {dockey}")

        detail_response = safe_request(f"{BASE_URL}/{dockey}")

        if not detail_response:
            print(f"Failed to fetch details for {docno}")
            continue

        try:
            detail_json = detail_response.json()
        except ValueError:
            print(f"Invalid detail JSON for {docno}")
            continue

        lines = extract_detail_lines(detail_json)

        if not lines:
            print(f"No detail lines found for {docno}")
            continue

        for line in lines:
            checked_lines += 1

            detail_key = line.get("dtlkey")
            sequence = line.get("seq")
            itemcode = line.get("itemcode")
            description = line.get("description")

            unique_key = make_unique_key(
                docno,
                detail_key,
                sequence,
                itemcode,
                description
            )

            if unique_key in existing_keys:
                continue

            batch_rows.append(
                [
                    unique_key,
                    docno,
                    docdate,
                    dockey,
                    detail_key,
                    sequence,
                    credit_note.get("code"),
                    credit_note.get("companyname"),
                    itemcode,
                    description,
                    line.get("description2"),
                    line.get("qty"),
                    line.get("uom"),
                    line.get("rate"),
                    line.get("unitprice"),
                    line.get("disc"),
                    line.get("amount"),
                    line.get("localamount"),
                    line.get("taxcode") or line.get("tax"),
                    line.get("taxrate"),
                    line.get("taxamt"),
                    line.get("localtaxamt"),
                    line.get("location"),
                    line.get("batchno") or line.get("batch"),
                    line.get("project"),
                    line.get("account"),
                    line.get("fromdoctype"),
                    line.get("fromdockey"),
                    line.get("fromdtlkey"),
                    line.get("remark1"),
                    line.get("remark2")
                ]
            )

            existing_keys.add(unique_key)

            if len(batch_rows) >= AUTO_PUSH_EVERY:
                total_pushed += push_rows(batch_rows)
                batch_rows = []

        time.sleep(0.05)

    save_progress(
        offset,
        total_pushed + len(batch_rows),
        checked_documents
    )

    print(
        f"Documents: {checked_documents} | "
        f"Lines: {checked_lines} | "
        f"Waiting: {len(batch_rows)} | "
        f"Pushed: {total_pushed}"
    )

    if stop_all:
        print(f"\nFinished target year {TARGET_YEAR}.")
        break

    offset += LIMIT
    time.sleep(0.05)

total_pushed += push_rows(batch_rows)

save_progress(
    offset,
    total_pushed,
    checked_documents
)

print("\nSales Credit Note Detail sync complete.")
print(f"Total documents checked: {checked_documents}")
print(f"Total detail lines checked: {checked_lines}")
print(f"Total new rows pushed: {total_pushed}")
