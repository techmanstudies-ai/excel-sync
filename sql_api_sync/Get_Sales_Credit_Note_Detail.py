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

BASE_URL = f"https://api.sql.my/{DOCUMENT_TYPE}"

JUMP_SIZE = 500
LIMIT = 50
AUTO_PUSH_EVERY = 500
MAX_RETRY = 5

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
WORKSHEET_NAME = "Sales_Credit_Note_Detail"

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
        cols=30
    )

print(f"Connected to worksheet: {WORKSHEET_NAME}")
print(
    f"Downloading Sales Credit Note details "
    f"for {TARGET_YEAR}"
)

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

    # YYYY-MM-DD
    if (
        len(date_text) >= 4
        and date_text[:4].isdigit()
    ):
        return int(date_text[:4])

    # DD/MM/YYYY
    if "/" in date_text:

        parts = date_text.split("/")

        if (
            len(parts) == 3
            and parts[2][:4].isdigit()
        ):
            return int(parts[2][:4])

    return None

# ==========================================
# UNIQUE KEY HELPER
# ==========================================

def make_unique_key(
    docno,
    detail_key,
    sequence,
    itemcode,
    description
):

    # DTLKey is the best unique identifier
    if detail_key not in [None, ""]:
        return f"{docno}|DTLKEY:{detail_key}"

    # Fall back to document + sequence
    if sequence not in [None, ""]:
        return f"{docno}|SEQ:{sequence}"

    # Final fallback
    return (
        f"{docno}|"
        f"{itemcode or ''}|"
        f"{description or ''}"
    )

# ==========================================
# LOAD EXISTING UNIQUE KEYS
# ==========================================

existing_keys = set()

records = sheet.get_all_values()

if len(records) > 1:

    for row in records[1:]:

        # New layout:
        # A UniqueKey
        # B DocNo
        # C DocDate
        # D DocKey
        # E DtlKey
        # F Seq
        # G CustomerCode
        # H CustomerName
        # I ItemCode
        # J Description

        if len(row) >= 10:

            saved_unique_key = str(
                row[0]
            ).strip()

            if saved_unique_key:
                existing_keys.add(
                    saved_unique_key
                )

print(
    f"Existing Sales Credit Note detail rows: "
    f"{len(existing_keys)}"
)

# ==========================================
# PROGRESS LOG
# ==========================================

def save_progress(
    offset,
    pushed_count,
    checked_documents
):

    progress_data = {
        "document_type": DOCUMENT_TYPE,
        "worksheet_name": WORKSHEET_NAME,
        "last_checked_offset": offset,
        "target_year": TARGET_YEAR,
        "checked_documents_this_run": checked_documents,
        "pushed_count_this_run": pushed_count,
        "updated_at": time.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "note": (
            "This file is only a log. "
            "The script scans from the target-year "
            "starting point and skips existing detail keys."
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
        "for target year..."
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
            result = response.json()

        except ValueError:

            print(
                "API returned invalid JSON "
                "during jump search."
            )

            return last_valid_offset

        data = result.get("data", [])

        if not data:

            print(
                "No data found during jump search."
            )

            return last_valid_offset

        docdate = data[0].get(
            "docdate",
            ""
        )

        doc_year = get_document_year(
            docdate
        )

        if doc_year is None:

            print(
                f"Cannot read date at offset "
                f"{offset}: {docdate}"
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
# HEADER
# ==========================================

def ensure_header():

    if sheet.get_all_values():
        return

    headers = [
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

    sheet.append_row(
        headers,
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
        f"Pushed {len(rows)} detail rows "
        "to Google Sheet"
    )

    return len(rows)

# ==========================================
# GET DETAIL LINES
# ==========================================

def extract_detail_lines(detail_json):

    data_block = detail_json.get(
        "data",
        []
    )

    if isinstance(data_block, list):

        header_block = (
            data_block[0]
            if data_block
            else {}
        )

    elif isinstance(data_block, dict):

        header_block = data_block

    else:

        header_block = {}

    lines = header_block.get(
        "sdsdocdetail",
        []
    )

    if not lines:

        # Fallback names in case this API version
        # uses another detail collection name
        possible_names = [
            "docdetail",
            "details",
            "detail",
            "salescreditnotedetail",
            "sdscreditnotedetail"
        ]

        for name in possible_names:

            possible_lines = header_block.get(
                name,
                []
            )

            if isinstance(
                possible_lines,
                list
            ):

                if possible_lines:
                    lines = possible_lines
                    break

    if not isinstance(lines, list):
        return []

    return lines

# ==========================================
# DOWNLOAD PHASE
# ==========================================

offset = find_year_start()

print(
    "\nPhase 2: Downloading "
    "Sales Credit Note details..."
)

print(
    "The script will retrieve each credit-note "
    "header, then request its detail using DocKey."
)

batch_rows = []
total_pushed = 0
checked_documents = 0
checked_lines = 0

while True:

    print(
        f"\nFetching Credit Note "
        f"header offset {offset}..."
    )

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
        result = response.json()

    except ValueError:

        print(
            f"Invalid JSON at offset "
            f"{offset}. Stopping safely."
        )

        break

    headers = result.get("data", [])

    if not headers:

        print("No more records.")
        break

    stop_all = False

    for credit_note in headers:

        docdate = credit_note.get(
            "docdate",
            ""
        )

        doc_year = get_document_year(
            docdate
        )

        if doc_year is None:
            continue

        if doc_year < TARGET_YEAR:
            continue

        if doc_year > TARGET_YEAR:

            stop_all = True
            break

        dockey = credit_note.get(
            "dockey"
        )

        docno = str(
            credit_note.get(
                "docno",
                ""
            )
        ).strip()

        if not dockey or not docno:
            continue

        checked_documents += 1

        print(
            f"Fetching details for "
            f"{docno} | DocKey: {dockey}"
        )

        detail_url = (
            f"{BASE_URL}/{dockey}"
        )

        detail_response = safe_request(
            detail_url
        )

        if not detail_response:

            print(
                f"Failed to fetch detail "
                f"for {docno}"
            )

            continue

        try:
            detail_json = (
                detail_response.json()
            )

        except ValueError:

            print(
                f"Invalid detail JSON "
                f"for {docno}"
            )

            continue

        lines = extract_detail_lines(
            detail_json
        )

        if not lines:

            print(
                f"No detail lines found "
                f"for {docno}"
            )

            continue

        print(
            f"Found {len(lines)} detail "
            f"line(s) for {docno}"
        )

        for line in lines:

            checked_lines += 1

            detail_key = line.get(
                "dtlkey"
            )

            sequence = line.get(
                "seq"
            )

            itemcode = line.get(
                "itemcode"
            )

            description = line.get(
                "description"
            )

            unique_key = make_unique_key(
                docno,
                detail_key,
                sequence,
                itemcode,
                description
            )

            if unique_key in existing_keys:
                continue

            row = [
                unique_key,
                docno,
                docdate,
                dockey,
                detail_key,
                sequence,
                credit_note.get("code"),
                credit_note.get(
                    "companyname"
                ),
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
                (
                    line.get("taxcode")
                    or line.get("tax")
                ),
                line.get("taxrate"),
                line.get("taxamt"),
                line.get("location"),
                (
                    line.get("batchno")
                    or line.get("batch")
                ),
                line.get("project"),
                line.get("account"),
                line.get("fromdoctype"),
                line.get("fromdockey"),
                line.get("fromdtlkey"),
                line.get("remark1"),
                line.get("remark2")
            ]

            batch_rows.append(row)
            existing_keys.add(unique_key)

            if (
                len(batch_rows)
                >= AUTO_PUSH_EVERY
            ):

                total_pushed += push_rows(
                    batch_rows
                )

                batch_rows = []

        time.sleep(0.05)

    save_progress(
        offset,
        total_pushed + len(batch_rows),
        checked_documents
    )

    print(
        f"Documents checked: "
        f"{checked_documents} | "
        f"Lines checked: {checked_lines} | "
        f"Waiting to push: "
        f"{len(batch_rows)} | "
        f"Already pushed: "
        f"{total_pushed}"
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

total_pushed += push_rows(
    batch_rows
)

save_progress(
    offset,
    total_pushed,
    checked_documents
)

print(
    "\nSales Credit Note Detail "
    "sync complete."
)

print(
    f"Total documents checked: "
    f"{checked_documents}"
)

print(
    f"Total detail lines checked: "
    f"{checked_lines}"
)

print(
    f"Total new rows pushed "
    f"this run: {total_pushed}"
)
