<<<<<<< HEAD
=======
import os
import time
>>>>>>> a24eaa0e4d9c8d2256819a407e6c1d82c4972c00
import datetime
import requests
import urllib3
import xml.etree.ElementTree as ET
from supabase import create_client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SUPABASE_URL = "https://iyohifpzsqjxcrgrtsza.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Iml5b2hpZnB6c3FqeGNyZ3J0c3phIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4MTU4OTQ1MiwiZXhwIjoyMDk3MTY1NDUyfQ.BLz5-PeIc5TTjSAiYuWxnGgJYrVnqjh0RYwdirJn_50"

<<<<<<< HEAD

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

today = datetime.date.today()
# For testing:
# today = datetime.date(2026, 6, 13)

# If today is Saturday (5) or Sunday (6), advance to the next Monday
if today.weekday() == 5:  # Saturday
    today += datetime.timedelta(days=2)
elif today.weekday() == 6:  # Sunday
    today += datetime.timedelta(days=1)

file_date = today.strftime("%d%m%Y")
db_date = today.strftime("%Y-%m-%d")

url = f"https://mhc.tn.gov.in/judis/clists/clists-madras/causelists/xml/cause_{file_date}.xml"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/xml,text/xml,*/*",
    "Referer": "https://mhc.tn.gov.in/judis/clists/clists-madras/index.php"
}

print("Clearing daily_cause_list table...")

supabase.table("daily_cause_list") \
    .delete() \
    .neq("id", "00000000-0000-0000-0000-000000000000") \
    .execute()

print("Table cleared.")

print("Downloading XML:", url)

res = requests.get(url, headers=headers, timeout=60, verify=False)

if res.status_code == 404:
    print(f"No cause list published for {db_date} (HTTP 404). Skipping import.")
    exit(0)

res.raise_for_status()

root = ET.fromstring(res.content)
=======
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def download_xml(url):
    session = requests.Session()

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/xml,text/xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
        "Referer": "https://mhc.tn.gov.in/judis/clists/clists-madras/index.php",
    })

    last_error = None

    for attempt in range(1, 8):
        try:
            print(f"Download attempt {attempt}/7: {url}")

            response = session.get(
                url,
                timeout=(180, 180),
                verify=False,
                allow_redirects=True
            )

            print("HTTP status:", response.status_code)
            print("Content-Type:", response.headers.get("Content-Type"))

            response.raise_for_status()

            if not response.content or len(response.content) < 100:
                raise Exception("Empty or invalid XML response")

            return response.content

        except Exception as error:
            last_error = error
            print(f"Attempt {attempt} failed:", error)

            if attempt < 7:
                time.sleep(30)

    print("Failed to download XML after all retries.")
    print("Last error:", last_error)
    return None


def chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


today = datetime.date.today()

file_date = today.strftime("%d%m%Y")
db_date = today.strftime("%Y-%m-%d")

url = f"https://mhc.tn.gov.in/judis/clists/clists-madras/causelists/xml/cause_{file_date}.xml"

print("XML URL:", url)

xml_content = download_xml(url)

if not xml_content:
    print("No XML downloaded. Existing data not deleted.")
    exit(0)

try:
    root = ET.fromstring(xml_content)
except ET.ParseError as error:
    print("XML parsing failed:", error)
    print("Existing data not deleted.")
    exit(0)
>>>>>>> a24eaa0e4d9c8d2256819a407e6c1d82c4972c00

rows = []

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

<<<<<<< HEAD
            row = {
=======
            rows.append({
>>>>>>> a24eaa0e4d9c8d2256819a407e6c1d82c4972c00
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
<<<<<<< HEAD
                "party_names": f"{petitioner} vs {respondent}",
=======
                "party_names": f"{petitioner or ''} vs {respondent or ''}",
>>>>>>> a24eaa0e4d9c8d2256819a407e6c1d82c4972c00
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
<<<<<<< HEAD
            }

            rows.append(row)
=======
            })
>>>>>>> a24eaa0e4d9c8d2256819a407e6c1d82c4972c00

seen = {}

for row in rows:
    key = (
        row["cause_date"],
        row["court_hall"] or "",
        row["item_number"] or "",
        row["case_number"] or "",
    )
    seen[key] = row

deduped_rows = list(seen.values())

print("Parsed rows:", len(rows))
print("Deduplicated rows:", len(deduped_rows))

<<<<<<< HEAD
if deduped_rows:
    supabase.table("daily_cause_list").upsert(
        deduped_rows,
        on_conflict="cause_date,court_hall,item_number,case_number"
    ).execute()

print("XML URL:", url)
print("Inserted/Updated:", len(deduped_rows))
print("Done.")
=======
if not deduped_rows:
    print("No rows found. Existing data not deleted.")
    exit(0)

print("Clearing today's records only...")

supabase.table("daily_cause_list") \
    .delete() \
    .eq("cause_date", db_date) \
    .execute()

print("Today's old records cleared.")

for batch in chunk_list(deduped_rows, 500):
    supabase.table("daily_cause_list").upsert(
        batch,
        on_conflict="cause_date,court_hall,item_number,case_number"
    ).execute()

print("Inserted/Updated:", len(deduped_rows))
print("Done.")
>>>>>>> a24eaa0e4d9c8d2256819a407e6c1d82c4972c00
