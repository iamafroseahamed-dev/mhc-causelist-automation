"""
Madras High Court – Daily Cause List Downloader
================================================
Runs as a Railway scheduled background job.
Cron schedule: 30 0 * * *  →  6:00 AM IST (UTC+05:30)

Safe update strategy
--------------------
Download XML → Validate → Parse → (only on success) Clear today's rows → Insert.
If download or parse fails the existing Supabase data is left untouched.

Environment variables required
-------------------------------
  SUPABASE_URL   – e.g. https://<project>.supabase.co
  SUPABASE_KEY   – service-role or anon key (never hardcoded)
"""

import datetime
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET
from typing import Any

import requests
import urllib3
from supabase import Client, create_client

# ---------------------------------------------------------------------------
# Suppress InsecureRequestWarning – MHC uses a self-signed / mismatched cert
# ---------------------------------------------------------------------------
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Logging – structured, timestamped, written to stdout for Railway log stream
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_RETRIES: int = 3
RETRY_BASE_DELAY: int = 2          # seconds; delay = BASE ** attempt (2, 4 s)
REQUEST_TIMEOUT: int = 60          # seconds per HTTP attempt
BATCH_SIZE: int = 500              # Supabase upsert batch size

MHC_XML_URL_TEMPLATE: str = (
    "https://mhc.tn.gov.in/judis/clists/clists-madras/causelists/xml/cause_{date}.xml"
)
REQUEST_HEADERS: dict[str, str] = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/xml,text/xml,*/*",
    "Referer": "https://mhc.tn.gov.in/judis/clists/clists-madras/index.php",
}


# ---------------------------------------------------------------------------
# Step 0: Supabase client
# ---------------------------------------------------------------------------
def get_supabase_client() -> Client:
    """Build a Supabase client from environment variables.

    Raises:
        EnvironmentError: if SUPABASE_URL or SUPABASE_KEY is not set.
    """
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_KEY", "").strip()
    if not url or not key:
        raise EnvironmentError(
            "Both SUPABASE_URL and SUPABASE_KEY environment variables must be set."
        )
    return create_client(url, key)


# ---------------------------------------------------------------------------
# Step 1: Download XML with exponential-backoff retries
# ---------------------------------------------------------------------------
def download_xml(url: str) -> bytes:
    """Download the cause-list XML from *url*.

    Retries up to MAX_RETRIES times with exponential backoff.

    Args:
        url: Full URL of the XML file.

    Returns:
        Raw XML bytes on success.

    Raises:
        RuntimeError: after all retry attempts are exhausted.
    """
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Download attempt %d/%d → %s", attempt, MAX_RETRIES, url)
            response = requests.get(
                url,
                headers=REQUEST_HEADERS,
                timeout=REQUEST_TIMEOUT,
                verify=False,  # MHC uses a self-signed certificate
            )
            response.raise_for_status()
            log.info(
                "Download succeeded (HTTP %d, %d bytes).",
                response.status_code,
                len(response.content),
            )
            return response.content

        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY**attempt  # 2 s, 4 s
                log.warning(
                    "Attempt %d failed: %s — retrying in %ds…", attempt, exc, delay
                )
                time.sleep(delay)
            else:
                log.error("All %d download attempts failed.", MAX_RETRIES)

    raise RuntimeError(
        f"Failed to download XML after {MAX_RETRIES} attempts"
    ) from last_exc


# ---------------------------------------------------------------------------
# Step 2: Parse XML → list of row dicts (deduplicated)
# ---------------------------------------------------------------------------
def parse_xml(
    content: bytes,
    url: str,
    db_date: str,
) -> list[dict[str, Any]]:
    """Parse raw XML *content* into Supabase-ready row dicts.

    Args:
        content:  Raw XML bytes returned by download_xml().
        url:      Source URL stored in each row for provenance.
        db_date:  ISO-8601 date string (YYYY-MM-DD) for cause_date column.

    Returns:
        Deduplicated list of row dicts.

    Raises:
        ET.ParseError: if the XML is malformed.
    """
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        log.error("Malformed XML – cannot parse: %s", exc)
        raise

    rows: list[dict[str, Any]] = []

    for court in root.findall(".//court"):
        court_hall = court.findtext("courtno")
        judge_name = court.findtext("judge1")

        for stage in court.findall(".//stage"):
            stage_name = stage.findtext("stagename")

            for case in stage.findall(".//casedetails"):
                case_type = case.findtext("mcasetype")
                case_no = case.findtext("mcaseno")
                case_year = case.findtext("mcaseyr")

                # Build a human-readable case number when all parts are present
                case_number: str | None = None
                if case_type and case_no and case_year:
                    case_number = f"{case_type}/{case_no}/{case_year}"

                petitioner = case.findtext("pname")
                respondent = case.findtext("rname")

                row: dict[str, Any] = {
                    "cause_date": db_date,
                    "source_type": "xml",
                    "source_url": url,
                    "court_name": "Madras High Court",
                    "bench": "Chennai",
                    "court_hall": court_hall,
                    "item_number": case.findtext("serial_no"),
                    "case_number": case_number,
                    "cnr_number": None,
                    "petitioner": petitioner,
                    "respondent": respondent,
                    "party_names": f"{petitioner} vs {respondent}",
                    "judge_name": judge_name,
                    "section": case_type,
                    "district": None,
                    "prayer": None,
                    "last_hearing_or_stage": stage_name,
                    "counsel_name": case.findtext("mpadv"),
                    "raw_text": ET.tostring(case, encoding="unicode"),
                    "raw_data": {
                        "mcasetype": case_type,
                        "mcaseno": case_no,
                        "mcaseyr": case_year,
                        "mpadv": case.findtext("mpadv"),
                        "mradv": case.findtext("mradv"),
                        "case_remarks": case.findtext("case_remarks"),
                    },
                    "import_status": "imported",
                    "updated_at": datetime.datetime.now(datetime.UTC).isoformat(),
                }
                rows.append(row)

    log.info("Parsed %d raw records.", len(rows))

    # Deduplicate on the same key used for the Supabase upsert constraint
    seen: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            row["cause_date"],
            row["court_hall"] or "",
            row["item_number"] or "",
            row["case_number"] or "",
        )
        seen[key] = row

    deduped = list(seen.values())
    log.info("Deduplicated to %d records.", len(deduped))
    return deduped


# ---------------------------------------------------------------------------
# Step 3: Clear today's rows – called ONLY after a successful parse
# ---------------------------------------------------------------------------
def clear_existing_data(supabase: Client, db_date: str) -> None:
    """Delete all rows for *db_date* from daily_cause_list.

    This is intentionally called after a successful download+parse so that
    existing data is never lost if the upstream source is unavailable.

    Args:
        supabase: Authenticated Supabase client.
        db_date:  ISO-8601 date string (YYYY-MM-DD).
    """
    log.info("Clearing existing rows for %s…", db_date)
    supabase.table("daily_cause_list").delete().eq("cause_date", db_date).execute()
    log.info("Existing rows cleared.")


# ---------------------------------------------------------------------------
# Step 4: Insert records in batches
# ---------------------------------------------------------------------------
def insert_records(
    supabase: Client,
    records: list[dict[str, Any]],
    db_date: str,
) -> int:
    """Upsert *records* into daily_cause_list in batches.

    Args:
        supabase: Authenticated Supabase client.
        records:  Deduplicated list of row dicts from parse_xml().
        db_date:  Used only for the log message.

    Returns:
        Total number of records inserted/updated.

    Raises:
        Exception: propagates any Supabase client error.
    """
    total = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        log.info(
            "Inserting records %d–%d of %d…",
            i + 1,
            i + len(batch),
            len(records),
        )
        supabase.table("daily_cause_list").upsert(
            batch,
            on_conflict="cause_date,court_hall,item_number,case_number",
        ).execute()
        total += len(batch)

    log.info("Inserted %d records for %s.", total, db_date)
    return total


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def main() -> None:
    """Orchestrate the full download → parse → clear → insert pipeline.

    Safe-update guarantee: the table is only modified after a successful
    download AND parse.  Any earlier failure exits with status 1 and leaves
    the existing Supabase data intact.
    """
    start_time = datetime.datetime.now(datetime.UTC)
    log.info("=== Cause List Job Started at %s UTC ===", start_time.isoformat())

    # Build date strings for today
    today = datetime.date.today()
    file_date = today.strftime("%d%m%Y")   # DDMMYYYY  – used in the XML filename
    db_date = today.strftime("%Y-%m-%d")   # YYYY-MM-DD – stored in Supabase
    url = MHC_XML_URL_TEMPLATE.format(date=file_date)

    log.info("Target date  : %s", db_date)
    log.info("Download URL : %s", url)

    # Initialise Supabase client (validates env vars up-front)
    try:
        supabase = get_supabase_client()
    except EnvironmentError as exc:
        log.error("Configuration error: %s", exc)
        sys.exit(1)

    # ── Step 1: Download ────────────────────────────────────────────────────
    try:
        xml_content = download_xml(url)
    except RuntimeError as exc:
        log.error(
            "Download failed – retaining existing Supabase data. Error: %s", exc
        )
        sys.exit(1)

    # ── Step 2: Parse ────────────────────────────────────────────────────────
    try:
        records = parse_xml(xml_content, url, db_date)
    except ET.ParseError as exc:
        log.error(
            "XML parse error – retaining existing Supabase data. Error: %s",
            exc,
            exc_info=True,
        )
        sys.exit(1)

    if not records:
        log.warning("No records found in XML – retaining existing data. Exiting.")
        sys.exit(0)

    # ── Steps 3 & 4: Clear then Insert (only reached on successful parse) ───
    try:
        clear_existing_data(supabase, db_date)
        inserted = insert_records(supabase, records, db_date)
    except Exception as exc:
        log.error(
            "Supabase operation failed – data may be partially updated. Error: %s",
            exc,
            exc_info=True,
        )
        sys.exit(1)

    elapsed = (datetime.datetime.now(datetime.UTC) - start_time).total_seconds()
    log.info(
        "=== Job Completed in %.1fs | Inserted: %d records ===",
        elapsed,
        inserted,
    )


if __name__ == "__main__":
    main()