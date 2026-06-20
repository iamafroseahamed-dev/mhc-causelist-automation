import datetime
import requests
import urllib3
import xml.etree.ElementTree as ET
from supabase import create_client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SUPABASE_URL = "https://iyohifpzsqjxcrgrtsza.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Iml5b2hpZnB6c3FqeGNyZ3J0c3phIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4MTU4OTQ1MiwiZXhwIjoyMDk3MTY1NDUyfQ.BLz5-PeIc5TTjSAiYuWxnGgJYrVnqjh0RYwdirJn_50"


supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

today = datetime.date.today()
# For testing:
# today = datetime.date(2026, 6, 13)

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
res.raise_for_status()

root = ET.fromstring(res.content)

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

if deduped_rows:
    supabase.table("daily_cause_list").upsert(
        deduped_rows,
        on_conflict="cause_date,court_hall,item_number,case_number"
    ).execute()

print("XML URL:", url)
print("Inserted/Updated:", len(deduped_rows))
print("Done.")
