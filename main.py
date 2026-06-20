import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://hcmadras.tn.gov.in",
    "Referer": "https://hcmadras.tn.gov.in/cause_list_mhc.php",
    "X-Requested-With": "XMLHttpRequest",
}

payload = {
    "cause_list_dt": "22-06-2026",
    "court_no": "05",
    "courtno_captcha": "983223",
    "submit": "SEARCH"
}

url = "https://hcmadras.tn.gov.in/cause_list_court.php"

response = session.post(
    url,
    headers=headers,
    data=payload,
    timeout=(60, 180),
    verify=False
)

print("Status:", response.status_code)
print("Content-Type:", response.headers.get("Content-Type"))
print(response.text[:2000])
