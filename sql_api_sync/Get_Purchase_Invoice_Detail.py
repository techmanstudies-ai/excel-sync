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

BASE_URL = f"https://api.sql.my/{DOCUMENT_TYPE}"

JUMP_SIZE = 500
LIMIT = 50
AUTO_PUSH_EVERY = 500
MAX_RETRY = 5

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
WORKSHEET_NAME = "Purchase_Invoice_Detail"

# Save progress/log in same folder as script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PROGRESS_FILE = os.path.join(
    BASE_DIR,
    f"progress_{DOCUMENT_TYPE}_detail_{TARGET_YEAR}.json"
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
# LOAD EXISTING UNIQUE KEYS
# ==========================================

existing_keys = set()

records = sheet.get_all_values()

if len(records) > 1:

    for r in records[1:]:

        if len(r) >= 6:

            docno = r[0]
            item = r[4]
            desc = r[5]

            unique_key = f"{docno}-{item}-{desc}"

            existing_keys.add(unique_key)

print(f"Existing rows in sheet: {len(existing_keys)}")

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
            "ItemCode",
            "Description",
            "Qty",
            "UnitPrice",
            "Amount"
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

print("\nPhase 2: Downloading purchase invoice details...")
print("This run will scan from year start and skip existing rows.")

batch_rows = []
total_pushed = 0

try:

    while True:

        print(f"\nFetching header batch offset {offset}...")

        url = f"{BASE_URL}?offset={offset}&limit={LIMIT}"

        response = safe_request(url)

        if not response:

            print(f"Cannot fetch offset {offset}. Stopping safely.")

            break

        headers = response.json().get("data", [])

        if not headers:

            print("No more headers.")

            break

        stop_all = False

        for h in headers:

            docdate = h.get("docdate", "")

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

            dockey = h.get("dockey")
            docno = h.get("docno")

            if not dockey or not docno:
                continue

            print(f"Fetching detail for {docno}")

            detail_url = f"{BASE_URL}/{dockey}"

            detail_response = safe_request(detail_url)

            if not detail_response:

                print(f"Failed to fetch detail for {docno}")

                continue

            detail_json = detail_response.json()

            header_block = detail_json.get("data", [])

            if isinstance(header_block, list):

                header_block = (
                    header_block[0]
                    if header_block
                    else {}
                )

            lines = []

            for key in header_block.keys():

                if "detail" in key.lower():

                    lines = header_block.get(key, [])

                    break

            for line in lines:

                item = line.get("itemcode")
                desc = line.get("description")

                unique_key = f"{docno}-{item}-{desc}"

                if unique_key in existing_keys:
                    continue

                row = [
                    docno,
                    docdate,
                    h.get("code"),
                    h.get("companyname"),
                    item,
                    desc,
                    line.get("qty"),
                    line.get("unitprice"),
                    line.get("amount")
                ]

                batch_rows.append(row)

                existing_keys.add(unique_key)

            if len(batch_rows) >= AUTO_PUSH_EVERY:

                total_pushed += push_rows(batch_rows)

                batch_rows = []

            time.sleep(0.05)

        save_progress(
            offset,
            total_pushed + len(batch_rows)
        )

        if stop_all:

            print(f"\nFinished target year {TARGET_YEAR}.")

            break

        offset += LIMIT

except KeyboardInterrupt:

    print("\nCTRL+C detected — pushing remaining rows...")

    total_pushed += push_rows(batch_rows)

    save_progress(offset, total_pushed)

    exit()

# ==========================================
# FINAL PUSH
# ==========================================

total_pushed += push_rows(batch_rows)

save_progress(offset, total_pushed)

print("\nSync complete.")
print(f"Total new rows pushed this run: {total_pushed}")