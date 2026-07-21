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

creds_dict = json.loads(
    os.environ["SQL_GOOGLE_CREDENTIALS"]
)

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=scope
)

gc = gspread.authorize(creds)

spreadsheet = gc.open_by_key(SPREADSHEET_ID)

# Create worksheet automatically if missing
try:
    sheet = spreadsheet.worksheet(WORKSHEET_NAME)
except gspread.WorksheetNotFound:
    sheet = spreadsheet.add_worksheet(
        title=WORKSHEET_NAME,
        rows=1000,
        cols=20
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

            try:
                print(response.text[:1000])
            except Exception:
                pass

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

    date_text = str(docdate).strip()

    # Format: YYYY-MM-DD
    if (
        len(date_text) >= 4
        and date_text[:4].isdigit()
    ):
        return int(date_text[:4])

    # Format: DD/MM/YYYY
    if "/" in date_text:

        date_parts = date_text.split("/")

        if (
            len(date_parts) == 3
            and date_parts[2][:4].isdigit()
        ):
            return int(date_parts[2][:4])

    return None

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

print(
    f"Existing Sales Credit Notes in sheet: "
    f"{len(existing_docnos)}"
)

# ==========================================
# PROGRESS LOG ONLY
# ==========================================

def save_progress(offset, pushed_count):

    progress_data = {
        "document_type": DOCUMENT_TYPE,
        "worksheet_name": WORKSHEET_NAME,
        "last_checked_offset": offset,
        "target_year": TARGET_YEAR,
        "pushed_count_this_run": pushed_count,
        "updated_at": time.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "note": (
            "This file is only a log. "
            "The script scans from the beginning "
            "of the target year and skips existing DocNo."
        )
    }

    with open(
        PROGRESS_FILE,
        "w",
        encoding="utf-8"
    ) as progress_file:

        json.dump(
            progress_data,
            progress_file,
            indent=4,
            ensure_ascii=False
        )

# ==========================================
# FIND TARGET YEAR START
# ==========================================

def find_year_start():

    print(
        "\nPhase 1: Jump searching "
        "for the target year..."
    )

    offset = 0
    last_valid_offset = 0

    while True:

        url = (
            f"{BASE_URL}"
            f"?offset={offset}"
            f"&limit=1"
        )

        response = safe_request(url)

        if not response:

            print(
                f"Cannot fetch offset {offset}. "
                f"Using last valid offset "
                f"{last_valid_offset}."
            )

            return last_valid_offset

        try:
            response_data = response.json()
        except ValueError:

            print(
                "API returned an invalid JSON response."
            )

            return last_valid_offset

        data = response_data.get("data", [])

        if not data:

            print(
                "No data found during jump search."
            )

            return last_valid_offset

        docdate = data[0].get("docdate", "")
        doc_year = get_document_year(docdate)

        if doc_year is None:

            print(
                f"Unable to read date at "
                f"offset {offset}: {docdate}"
            )

            offset += JUMP_SIZE
            continue

        print(
            f"Jump offset {offset}: "
            f"{docdate}"
        )

        if doc_year >= TARGET_YEAR:

            start_offset = max(
                0,
                offset - JUMP_SIZE
            )

            print(
                "Target year located. "
                f"Rewinding to offset "
                f"{start_offset}"
            )

            return start_offset

        last_valid_offset = offset
        offset += JUMP_SIZE

# ==========================================
# ENSURE HEADER
# ==========================================

def ensure_header():

    current_values = sheet.get_all_values()

    if current_values:
        return

    header = [
        "DocNo",
        "DocDate",
        "CustomerCode",
        "CustomerName",
        "Currency",
        "DocAmount",
        "LocalAmount",
        "Status",
        "Cancelled",
        "CreatedDate"
    ]

    sheet.append_row(
        header,
        value_input_option="RAW"
    )

    print("Worksheet header created.")

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

    print(
        f"Pushed {len(rows)} rows "
        "to Google Sheet"
    )

    return len(rows)

# ==========================================
# DOWNLOAD PHASE
# ==========================================

offset = find_year_start()

print(
    "\nPhase 2: Downloading "
    "Sales Credit Notes..."
)

print(
    "This run will scan from the target "
    "year start and skip existing DocNo."
)

batch_rows = []
total_pushed = 0
total_checked = 0

while True:

    print(f"\nFetching CN header offset {offset}...")

    url = (
        f"{BASE_URL}"
        f"?offset={offset}"
        f"&limit={LIMIT}"
    )

    response = safe_request(url)

    if not response:

        print(
            f"Cannot fetch offset {offset}. "
            "Stopping safely."
        )

        break

    try:
        response_data = response.json()
    except ValueError:

        print(
            f"Invalid JSON at offset {offset}. "
            "Stopping safely."
        )

        break

    headers = response_data.get("data", [])

    if not headers:

        print("No more records.")
        break

    stop_all = False

    for credit_note in headers:

        total_checked += 1

        docdate = credit_note.get(
            "docdate",
            ""
        )

        doc_year = get_document_year(docdate)

        if doc_year is None:
            continue

        if doc_year < TARGET_YEAR:
            continue

        if doc_year > TARGET_YEAR:

            stop_all = True
            break

        docno = str(
            credit_note.get("docno", "")
        ).strip()

        if not docno:
            continue

        if docno in existing_docnos:
            continue

        row = [
            docno,
            docdate,
            credit_note.get("code"),
            credit_note.get("companyname"),
            credit_note.get("currencycode"),
            credit_note.get("docamt"),
            credit_note.get("localdocamt"),
            credit_note.get("status"),
            credit_note.get("cancelled"),
            credit_note.get("creationdate")
        ]

        batch_rows.append(row)
        existing_docnos.add(docno)

    if len(batch_rows) >= AUTO_PUSH_EVERY:

        total_pushed += push_rows(
            batch_rows
        )

        batch_rows = []

    save_progress(
        offset,
        total_pushed + len(batch_rows)
    )

    print(
        f"Checked: {total_checked} | "
        f"Waiting to push: {len(batch_rows)} | "
        f"Already pushed: {total_pushed}"
    )

    if stop_all:

        print(
            f"\nFinished target year "
            f"{TARGET_YEAR}."
        )

        break

    offset += LIMIT
    time.sleep(0.05)

# ==========================================
# FINAL PUSH
# ==========================================

total_pushed += push_rows(batch_rows)

save_progress(
    offset,
    total_pushed
)

print("\nSales Credit Note sync complete.")
print(
    f"Total records checked: "
    f"{total_checked}"
)
print(
    f"Total new rows pushed this run: "
    f"{total_pushed}"
)
