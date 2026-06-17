"""
Madras High Court – Daily Cause List Downloader
Railway scheduled background job
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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

MAX_RETRIES = 5
REQUEST_TIMEOUT = (20, 180)  # connect timeout, read timeout
BATCH_SIZE = 500

MHC_XML_URL_TEMPLATE = (
    "https://mhc.tn.gov.in/judis/clists/clists-madras/causelists/xml/cause_{date}.xml"
)

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/xml,text/xml,*/*",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "close",
    "Referer": "https://mhc.tn.gov.in/judis/clists/clists-madras/index.php",
}


def get_supabase_client() -> Client:
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_KEY", "").strip()

    if not url or not key:
        raise EnvironmentError(
            "Both SUPABASE_URL and SUPABASE_KEY environment variables must be set."
        )

    return create_client(url, key)


def check_xml_available(url: str) -> bool:
    try:
        response = requests.head(
            url,
            headers=REQUEST_HEADERS,
            timeout=30,
            verify=False,
            allow_redirects=True,
        )

        log.info("HEAD check status: HTTP %s", response.status_code)

        if response.status_code == 200:
            return True

        log.warning("XML not available yet. HTTP %s", response.status_code)
        return False

    except requests.RequestException as exc:
        log.warning("HEAD check failed, will still try GET. Error: %s", exc)
        return True


def download_xml(url: str) -> bytes:
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Download attempt %d/%d → %s", attempt, MAX_RETRIES, url)

            with requests.get(
                url,
                headers=REQUEST_HEADERS,
                timeout=REQUEST_TIMEOUT,
                verify=False,
                stream=True,
            ) as response:
                response.raise_for_status()

                chunks = []
                total_size = 0

                for chunk in response.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        chunks.append(chunk)
                        total_size += len(chunk)

                content = b"".join(chunks)

                log.info(
                    "Download succeeded. HTTP %s, %d bytes.",
                    response.status_code,
                    total_size,
                )

                return content

        except requests.RequestException as exc:
            last_exc = exc

            if attempt < MAX_RETRIES:
                delay = attempt * 10
                log.warning(
                    "Attempt %d failed: %s — retrying in %ds...",
                    attempt,
                    exc,
                    delay,
                )
                time.sleep(delay)
            else:
                log.error("All %d download attempts failed.", MAX_RETRIES)

    raise RuntimeError(f"Failed to download XML after {MAX_RETRIES} attempts") from last_exc


def parse_xml(content: bytes, url: str, db_date: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        log.error("Malformed XML. Cannot parse: %s", exc)
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

                case_number = None
                if case_type and case_no and case_year:
                    case_number = f"{case_type}/{case_no}/{case_year}"

                petitioner = case.findtext("pname")
                respondent = case.findtext("rname")

                row = {
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


def clear_existing_data(supabase: Client, db_date: str) -> None:
    log.info("Clearing existing rows for %s...", db_date)
    supabase.table("daily_cause_list").delete().eq("cause_date", db_date).execute()
    log.info("Existing rows cleared.")


def insert_records(
    supabase: Client,
    records: list[dict[str, Any]],
    db_date: str,
) -> int:
    total = 0

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i: i + BATCH_SIZE]

        log.info(
            "Inserting records %d-%d of %d...",
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


def main() -> None:
    start_time = datetime.datetime.now(datetime.UTC)

    log.info("=== Cause List Job Started at %s UTC ===", start_time.isoformat())

    today = datetime.date.today()

    file_date = today.strftime("%d%m%Y")
    db_date = today.strftime("%Y-%m-%d")

    url = MHC_XML_URL_TEMPLATE.format(date=file_date)

    log.info("Target date  : %s", db_date)
    log.info("Download URL : %s", url)

    try:
        supabase = get_supabase_client()
    except EnvironmentError as exc:
        log.error("Configuration error: %s", exc)
        sys.exit(1)

    if not check_xml_available(url):
        log.warning("Cause list XML not available. Existing Supabase data retained.")
        sys.exit(0)

    try:
        xml_content = download_xml(url)
    except RuntimeError as exc:
        log.error("Download failed. Existing Supabase data retained. Error: %s", exc)
        sys.exit(0)

    try:
        records = parse_xml(xml_content, url, db_date)
    except ET.ParseError as exc:
        log.error("XML parse failed. Existing Supabase data retained. Error: %s", exc)
        sys.exit(0)

    if not records:
        log.warning("No records found in XML. Existing Supabase data retained.")
        sys.exit(0)

    try:
        clear_existing_data(supabase, db_date)
        inserted = insert_records(supabase, records, db_date)
    except Exception as exc:
        log.error(
            "Supabase operation failed. Data may be partially updated. Error: %s",
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
