import json
import os
import time
from typing import Any, Dict, List, Optional

import gspread
import requests
from google.oauth2.service_account import Credentials
from requests_aws4auth import AWS4Auth

# ============================================================
# CUSTOMER CREDIT NOTE HEADER SYNC
#
# Extracts the referenced invoice number from:
#     sdsknockoff[].docno
#
# Google Sheets column label:
#     RefInv
#
# The script:
#   1. Appends new Customer Credit Notes.
#   2. Updates existing rows when the same DocKey is found.
#   3. Can therefore populate RefInv for rows synced previously.
# ============================================================

DOCUMENT_TYPE = "customercreditnote"
WORKSHEET_NAME = "Customer_Credit_Note"
TARGET_YEAR = 2026

ACCESS_KEY = os.environ["SQL_ACCESS_KEY"]
SECRET_KEY = os.environ["SQL_SECRET_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

REGION = "ap-southeast-5"
SERVICE = "sqlaccount"
BASE_URL = f"https://api.sql.my/{DOCUMENT_TYPE}"

LIMIT = 50
JUMP_SIZE = 500
MAX_RETRY = 5
WRITE_BATCH_SIZE = 300

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROGRESS_FILE = os.path.join(
    BASE_DIR,
    f"progress_{DOCUMENT_TYPE}_{TARGET_YEAR}.json",
)

HEADERS = [
    "DocKey",
    "DocNo",
    "DocNoEx",
    "DocDate",
    "PostDate",
    "TaxDate",
    "CustomerCode",
    "Description",
    "Area",
    "Agent",
    "Project",
    "CurrencyCode",
    "CurrencyRate",
    "DocAmount",
    "LocalDocAmount",
    "UnappliedAmount",
    "FromDocType",
    "RefInv",
    "Cancelled",
    "Status",
    "UpdateCount",
    "LastModified",
]

# ============================================================
# AUTHENTICATION
# ============================================================

auth = AWS4Auth(
    ACCESS_KEY,
    SECRET_KEY,
    REGION,
    SERVICE,
)

google_credentials = json.loads(
    os.environ["SQL_GOOGLE_CREDENTIALS"]
)

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

credentials = Credentials.from_service_account_info(
    google_credentials,
    scopes=scopes,
)

gspread_client = gspread.authorize(credentials)
spreadsheet = gspread_client.open_by_key(SPREADSHEET_ID)

try:
    sheet = spreadsheet.worksheet(WORKSHEET_NAME)
except gspread.WorksheetNotFound:
    sheet = spreadsheet.add_worksheet(
        title=WORKSHEET_NAME,
        rows=1000,
        cols=len(HEADERS),
    )

print(f"Connected to worksheet: {WORKSHEET_NAME}")
print(f"Downloading Customer Credit Notes for {TARGET_YEAR}")

# ============================================================
# API HELPERS
# ============================================================

def safe_request(url: str) -> Optional[requests.Response]:
    """Perform a signed GET request with retry handling."""

    for attempt in range(1, MAX_RETRY + 1):
        try:
            response = requests.get(
                url,
                auth=auth,
                timeout=60,
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


def get_response_records(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the API data array safely."""

    data = payload.get("data", [])

    if isinstance(data, list):
        return [
            record
            for record in data
            if isinstance(record, dict)
        ]

    if isinstance(data, dict):
        return [data]

    return []


def get_document_year(docdate: Any) -> Optional[int]:
    """Extract the year from YYYY-MM-DD or DD/MM/YYYY values."""

    if docdate in (None, ""):
        return None

    text = str(docdate).strip()

    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])

    if "/" in text:
        parts = text.split("/")

        if (
            len(parts) == 3
            and len(parts[2]) >= 4
            and parts[2][:4].isdigit()
        ):
            return int(parts[2][:4])

    return None


def fetch_full_document(dockey: Any) -> Optional[Dict[str, Any]]:
    """
    Fetch a complete Customer Credit Note.

    The listing endpoint may not always return nested sdsknockoff data.
    Therefore, the script retrieves the full document using its DocKey.
    """

    if dockey in (None, ""):
        return None

    response = safe_request(f"{BASE_URL}/{dockey}")

    if response is None:
        return None

    try:
        records = get_response_records(response.json())
    except ValueError:
        print(f"Invalid JSON returned for DocKey {dockey}")
        return None

    return records[0] if records else None

# ============================================================
# REFERENCE INVOICE EXTRACTION
# ============================================================

def extract_ref_invoice(document: Dict[str, Any]) -> str:
    """
    Extract referenced invoice DocNo values from sdsknockoff.

    Example:
        "sdsknockoff": [
            {
                "doctype": "IV",
                "docno": "SIN-2026/2142"
            }
        ]

    Result:
        SIN-2026/2142

    If more than one invoice is linked, the values are joined with commas.
    """

    knockoff_rows = document.get("sdsknockoff", [])

    if not isinstance(knockoff_rows, list):
        return ""

    invoice_docnos: List[str] = []

    for knockoff in knockoff_rows:
        if not isinstance(knockoff, dict):
            continue

        doctype = str(
            knockoff.get("doctype", "")
        ).strip().upper()

        docno = str(
            knockoff.get("docno", "")
        ).strip()

        # Keep invoice references only.
        if doctype == "IV" and docno:
            if docno not in invoice_docnos:
                invoice_docnos.append(docno)

    return ", ".join(invoice_docnos)

# ============================================================
# GOOGLE SHEET HELPERS
# ============================================================

def ensure_header() -> None:
    """
    Create or repair row 1.

    Existing data is preserved. If row 1 contains data rather than headers,
    the correct header is inserted above it.
    """

    first_row = sheet.row_values(1)

    if not first_row:
        sheet.update(
            range_name=f"A1:V1",
            values=[HEADERS],
        )
        print("Customer Credit Note header created.")
        return

    current = [
        str(value).strip()
        for value in first_row[:len(HEADERS)]
    ]

    expected = [
        str(value).strip()
        for value in HEADERS
    ]

    if current == expected:
        return

    first_cell = str(first_row[0]).strip() if first_row else ""

    known_header_names = {
        "DocKey",
        "DocNo",
        "DocNoEx",
    }

    if first_cell and first_cell not in known_header_names:
        sheet.insert_row(
            HEADERS,
            index=1,
            value_input_option="RAW",
        )
        print("Header inserted above existing Customer Credit Note data.")
        return

    sheet.update(
        range_name=f"A1:V1",
        values=[HEADERS],
    )
    print("Customer Credit Note header corrected.")


def load_existing_rows() -> Dict[str, int]:
    """
    Return a mapping of DocKey -> Google Sheet row number.

    Row 1 is the header, so data starts from row 2.
    """

    values = sheet.get_all_values()

    existing: Dict[str, int] = {}

    for row_number, row in enumerate(values[1:], start=2):
        if not row:
            continue

        dockey = str(row[0]).strip()

        if dockey:
            existing[dockey] = row_number

    return existing


def document_to_row(document: Dict[str, Any]) -> List[Any]:
    """Convert one API Customer Credit Note into one sheet row."""

    return [
        document.get("dockey"),
        document.get("docno"),
        document.get("docnoex"),
        document.get("docdate"),
        document.get("postdate"),
        document.get("taxdate"),
        document.get("code"),
        document.get("description"),
        document.get("area"),
        document.get("agent"),
        document.get("project"),
        document.get("currencycode"),
        document.get("currencyrate"),
        document.get("docamt"),
        document.get("localdocamt"),
        document.get("unappliedamt"),
        document.get("fromdoctype"),
        extract_ref_invoice(document),
        document.get("cancelled"),
        document.get("status"),
        document.get("updatecount"),
        document.get("lastmodified"),
    ]


def append_rows(rows: List[List[Any]]) -> int:
    """Append a batch of new records below the existing data."""

    if not rows:
        return 0

    sheet.append_rows(
        rows,
        value_input_option="RAW",
    )

    print(f"Appended {len(rows)} new Customer Credit Note rows.")
    return len(rows)


def update_rows(updates: List[tuple[int, List[Any]]]) -> int:
    """
    Update existing records in batches.

    This is important for previously synced rows where RefInv was blank.
    """

    if not updates:
        return 0

    requests_data = []

    for row_number, row_values in updates:
        requests_data.append(
            {
                "range": f"A{row_number}:V{row_number}",
                "values": [row_values],
            }
        )

    sheet.batch_update(
        requests_data,
        value_input_option="RAW",
    )

    print(f"Updated {len(updates)} existing Customer Credit Note rows.")
    return len(updates)

# ============================================================
# PROGRESS
# ============================================================

def save_progress(
    offset: int,
    checked: int,
    appended: int,
    updated: int,
) -> None:
    """Write a local JSON progress log for GitHub Actions diagnostics."""

    progress = {
        "document_type": DOCUMENT_TYPE,
        "worksheet_name": WORKSHEET_NAME,
        "target_year": TARGET_YEAR,
        "last_checked_offset": offset,
        "checked_documents": checked,
        "appended_documents": appended,
        "updated_documents": updated,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    with open(
        PROGRESS_FILE,
        "w",
        encoding="utf-8",
    ) as progress_file:
        json.dump(
            progress,
            progress_file,
            indent=4,
            ensure_ascii=False,
        )

# ============================================================
# FIND TARGET YEAR
# ============================================================

def find_year_start() -> int:
    """
    Use jump search to locate the beginning of TARGET_YEAR.

    The script rewinds one jump so records near the year boundary are not lost.
    """

    print("\nPhase 1: Locating target year...")

    offset = 0
    last_valid_offset = 0

    while True:
        response = safe_request(
            f"{BASE_URL}?offset={offset}&limit=1"
        )

        if response is None:
            print(
                f"Could not read offset {offset}. "
                f"Using offset {last_valid_offset}."
            )
            return last_valid_offset

        try:
            records = get_response_records(response.json())
        except ValueError:
            print("Invalid JSON during jump search.")
            return last_valid_offset

        if not records:
            return last_valid_offset

        docdate = records[0].get("docdate")
        doc_year = get_document_year(docdate)

        print(f"Jump offset {offset}: {docdate}")

        if doc_year is None:
            offset += JUMP_SIZE
            continue

        if doc_year >= TARGET_YEAR:
            start_offset = max(0, offset - JUMP_SIZE)

            print(
                f"Target year located. "
                f"Starting from offset {start_offset}."
            )

            return start_offset

        last_valid_offset = offset
        offset += JUMP_SIZE

# ============================================================
# MAIN SYNC
# ============================================================

def main() -> None:
    ensure_header()

    existing_rows = load_existing_rows()

    print(
        f"Existing Customer Credit Note rows: "
        f"{len(existing_rows)}"
    )

    offset = find_year_start()

    pending_appends: List[List[Any]] = []
    pending_updates: List[tuple[int, List[Any]]] = []

    total_checked = 0
    total_appended = 0
    total_updated = 0

    print("\nPhase 2: Syncing Customer Credit Notes...")

    while True:
        print(f"\nFetching header offset {offset}...")

        response = safe_request(
            f"{BASE_URL}?offset={offset}&limit={LIMIT}"
        )

        if response is None:
            print(f"Unable to fetch offset {offset}. Stopping safely.")
            break

        try:
            list_documents = get_response_records(response.json())
        except ValueError:
            print(f"Invalid JSON at offset {offset}. Stopping safely.")
            break

        if not list_documents:
            print("No more Customer Credit Notes.")
            break

        reached_future_year = False

        for list_document in list_documents:
            doc_year = get_document_year(
                list_document.get("docdate")
            )

            if doc_year is None:
                continue

            if doc_year < TARGET_YEAR:
                continue

            if doc_year > TARGET_YEAR:
                reached_future_year = True
                break

            dockey = list_document.get("dockey")

            if dockey in (None, ""):
                continue

            total_checked += 1

            # Fetch the full record so sdsknockoff is available.
            full_document = fetch_full_document(dockey)

            if full_document is None:
                print(
                    f"Skipping DocKey {dockey}: "
                    "full document could not be retrieved."
                )
                continue

            row_values = document_to_row(full_document)
            dockey_text = str(dockey).strip()

            if dockey_text in existing_rows:
                row_number = existing_rows[dockey_text]

                pending_updates.append(
                    (row_number, row_values)
                )
            else:
                pending_appends.append(row_values)

            if len(pending_updates) >= WRITE_BATCH_SIZE:
                total_updated += update_rows(pending_updates)
                pending_updates = []

            if len(pending_appends) >= WRITE_BATCH_SIZE:
                total_appended += append_rows(pending_appends)
                pending_appends = []

        save_progress(
            offset=offset,
            checked=total_checked,
            appended=total_appended + len(pending_appends),
            updated=total_updated + len(pending_updates),
        )

        print(
            f"Checked: {total_checked} | "
            f"Waiting append: {len(pending_appends)} | "
            f"Waiting update: {len(pending_updates)} | "
            f"Appended: {total_appended} | "
            f"Updated: {total_updated}"
        )

        if reached_future_year:
            print(f"Finished target year {TARGET_YEAR}.")
            break

        offset += LIMIT
        time.sleep(0.05)

    total_updated += update_rows(pending_updates)
    total_appended += append_rows(pending_appends)

    save_progress(
        offset=offset,
        checked=total_checked,
        appended=total_appended,
        updated=total_updated,
    )

    print("\nCustomer Credit Note sync complete.")
    print(f"Documents checked: {total_checked}")
    print(f"New rows appended: {total_appended}")
    print(f"Existing rows updated: {total_updated}")


if __name__ == "__main__":
    main()
