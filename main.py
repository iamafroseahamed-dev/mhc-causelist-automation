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

MAX_RETRIES = 2
REQUEST_TIMEOUT = (10, 30)
BATCH_SIZE = 500

MHC_XML_URL_TEMPLATE = (
    "https://mhc.tn.gov.in/judis/clists/clists-madras/causelists/xml/cause_{date}.xml"
)

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml,text/xml,text/html,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "close",
    "Referer": "https://mhc.tn.gov.in/judis/clists/clists-madras/",
}


def get_supabase_client() -> Client:
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_KEY", "").strip()

    if not url or not key:
        raise EnvironmentError(
            "Both SUPABASE_URL and SUPABASE_KEY environment variables must be set."
        )

    return create_client(url, key)


def warmup_session(session: requests.Session) -> None:
    try:
        warmup_url = "https://mhc.tn.gov.in/judis/clists/clists-madras/"

        response = session.get(
            warmup_url,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT,
            verify=False,
        )

        log.info("Warmup completed. HTTP %s", response.status_code)

    except Exception as exc:
        log.warning("Warmup failed, continuing anyway. Error: %s", exc)


def download_xml(url: str) -> bytes:
    session = requests.Session()

    warmup_session(session)

    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Download attempt %d/%d → %s", attempt, MAX_RETRIES, url)

            response = session.get(
                url,
                headers=REQUEST_HEADERS,
                timeout=REQUEST_TIMEOUT,
                verify=False,
            )

            response.raise_for_status()

            log.info(
                "Download succeeded. HTTP %s, %d bytes.",
                response.status_code,
                len(response.content),
            )

            return response.content

        except requests.RequestException as exc:
            last_exc = exc

            if attempt < MAX_RETRIES:
                delay = 10
                log.warning(
                    "Attempt %d failed: %s — retrying in %ds...",
                    attempt,
                    exc,
                    delay,
                )
                time.sleep(delay)
            else:
                log.error("All %d download attempts failed.", MAX_RETRIES)

    raise RuntimeError("MHC XML download failed") from last_exc


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


def run_for_date(target_date: datetime.date, supabase: Client) -> bool:
    file_date = target_date.strftime("%d%m%Y")
    db_date = target_date.strftime("%Y-%m-%d")

    url = MHC_XML_URL_TEMPLATE.format(date=file_date)

    log.info("Target date  : %s", db_date)
    log.info("Download URL : %s", url)

    try:
        xml_content = download_xml(url)
    except RuntimeError as exc:
        log.error("Download failed for %s. Error: %s", db_date, exc)
        return False

    try:
        records = parse_xml(xml_content, url, db_date)
    except ET.ParseError as exc:
        log.error("XML parse failed for %s. Error: %s", db_date, exc)
        return False

    if not records:
        log.warning("No records found for %s. Existing data retained.", db_date)
        return False

    try:
        clear_existing_data(supabase, db_date)
        inserted = insert_records(supabase, records, db_date)
    except Exception as exc:
        log.error(
            "Supabase operation failed for %s. Error: %s",
            db_date,
            exc,
            exc_info=True,
        )
        return False

    log.info("Completed %s. Inserted %d records.", db_date, inserted)

    return True


def main() -> None:
    start_time = datetime.datetime.now(datetime.UTC)

    log.info("=== Cause List Job Started at %s UTC ===", start_time.isoformat())

    try:
        supabase = get_supabase_client()
    except EnvironmentError as exc:
        log.error("Configuration error: %s", exc)
        sys.exit(1)

    today = datetime.date.today()

    candidate_dates = [
        today,
        today - datetime.timedelta(days=1),
    ]

    success = False

    for target_date in candidate_dates:
        if run_for_date(target_date, supabase):
            success = True
            break

    elapsed = (datetime.datetime.now(datetime.UTC) - start_time).total_seconds()

    if success:
        log.info("=== Job Completed Successfully in %.1fs ===", elapsed)
        sys.exit(0)

    log.warning(
        "=== Job Completed with no data update in %.1fs. Existing data retained. ===",
        elapsed,
    )

    sys.exit(0)


if __name__ == "__main__":
    main()
