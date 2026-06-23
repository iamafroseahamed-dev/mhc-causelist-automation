import os
import re
import json
import time
import datetime
import requests
import urllib3
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from typing import Any, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from supabase import create_client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Timeline logger ────────────────────────────────────────────────────────────
_IST_TZ = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
_SCRIPT_START = datetime.datetime.now(_IST_TZ)

def log(msg: str, step: str = '') -> None:
    now      = datetime.datetime.now(_IST_TZ)
    elapsed  = (now - _SCRIPT_START).total_seconds()
    ts       = now.strftime('%H:%M:%S')
    step_tag = f'  [{step}]' if step else ''
    print(f'[{ts}  +{elapsed:6.1f}s]{step_tag}  {msg}', flush=True)

def log_section(title: str) -> None:
    now = datetime.datetime.now(_IST_TZ)
    ts  = now.strftime('%H:%M:%S')
    bar = '─' * 56
    print(f'\n┌{bar}┐', flush=True)
    print(f'│  {ts}  {title:<50}│', flush=True)
    print(f'└{bar}┘', flush=True)

SUPABASE_URL = "https://iyohifpzsqjxcrgrtsza.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Iml5b2hpZnB6c3FqeGNyZ3J0c3phIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4MTU4OTQ1MiwiZXhwIjoyMDk3MTY1NDUyfQ.BLz5-PeIc5TTjSAiYuWxnGgJYrVnqjh0RYwdirJn_50"


supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ── Pipeline constants ─────────────────────────────────────────────────────────
IST              = _IST_TZ
PAGE_SIZE        = 1000
BATCH_SIZE       = 500
ECOURTS_HIST_URL = 'https://hcservices.ecourts.gov.in/hcservices/cases_qry/o_civil_case_history.php'
ECOURTS_HOME_URL = 'https://hcservices.ecourts.gov.in/'
ECOURTS_TIMEOUT  = (5, 20)
ECOURTS_WORKERS  = 5
ECOURTS_BUDGET_S = 45
ECOURTS_RETRIES  = 3
CLA_PATTERNS     = (
    'the commissioner of land administration',
    'land administration department',
)
BASE_MATCH_COLS  = frozenset({
    'listed_date',
    'case_id', 'daily_cause_list_id',
    'case_number', 'court_hall', 'item_number',
    'judge_name', 'vc_link', 'stage', 'petitioner', 'respondent',
    'notification_status', 'created_at', 'updated_at',
})
_MONTHS = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
}


# ── Supabase REST helpers ──────────────────────────────────────────────────────

def _sb_headers(prefer: str = 'return=minimal') -> Dict[str, str]:
    return {
        'apikey':        SUPABASE_SERVICE_ROLE_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
        'Content-Type':  'application/json',
        'Prefer':        prefer,
    }


def _fetch_all(table: str, params: Dict) -> List[Dict]:
    rows: List[Dict] = []
    offset = 0
    while True:
        resp = requests.get(
            f'{SUPABASE_URL}/rest/v1/{table}',
            headers={**_sb_headers('count=none'), 'Range-Unit': 'items',
                     'Range': f'{offset}-{offset + PAGE_SIZE - 1}'},
            params=params, timeout=30,
        )
        resp.raise_for_status()
        chunk = resp.json()
        if not isinstance(chunk, list) or not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def _fetch_active_cases() -> List[Dict]:
    select_candidates = [
        'id,organization_id,case_number,cnr_number,active',
        'id,organization_id,case_number,cnr_number',
    ]

    last_error = None
    for select_expr in select_candidates:
        try:
            params = {'select': select_expr, 'active': 'eq.true'}
            cases = _fetch_all('cases', params)
            for c in cases:
                c.setdefault('ecourts_case_no', None)
                c.setdefault('active', True)
            return cases
        except Exception as exc:
            last_error = exc
            log(f'Cases fetch fallback for select={select_expr!r}: {exc}', 'match')

    if last_error:
        raise last_error
    return []


def _fetch_all_cases_for_existence() -> List[Dict]:
    select_candidates = [
        'id,organization_id,case_number,cnr_number,active',
        'id,organization_id,case_number,cnr_number',
        'id,case_number,cnr_number,active',
        'id,case_number,cnr_number',
    ]

    last_error = None
    for select_expr in select_candidates:
        try:
            params = {'select': select_expr}
            return _fetch_all('cases', params)
        except Exception as exc:
            last_error = exc
            log(f'Cases existence fetch fallback for select={select_expr!r}: {exc}', 'match')

    if last_error:
        raise last_error
    return []


# ── Case-number normalisation ──────────────────────────────────────────────────

def normalize_case_number(s: Optional[str]) -> str:
    if not s:
        return ''
    try:
        import unicodedata
        s = unicodedata.normalize('NFKC', str(s))
    except Exception:
        pass
    s = s.upper().strip()
    s = re.sub(r'(?<=[A-Z])\.(?=[A-Z(])', '', s)
    s = re.sub(r'(?<=\))\.',  '', s)
    s = re.sub(r'\.', ' ', s)
    s = re.sub(r'\bNO\b', '', s)
    s = re.sub(r'\bNUMBER\b', '', s)
    s = re.sub(r'\bOF\b', '/', s)
    s = re.sub(r'\s+', ' ', s).strip()
    parts = [p.strip() for p in re.split(r'\s*/\s*|\s+', s) if p.strip()]
    if len(parts) >= 3:
        num_idx = next((i for i, p in enumerate(parts) if re.match(r'^\d+$', p)), -1)
        if num_idx >= 1 and num_idx + 1 < len(parts):
            ct = ''.join(parts[:num_idx])
            cn = parts[num_idx].lstrip('0') or '0'
            cy = re.sub(r'\D', '', parts[num_idx + 1])
            if ct and cn and re.match(r'^\d{2,4}$', cy):
                return f'{ct}/{cn}/{cy}'
        ct = parts[0]
        cn = re.sub(r'\D', '', parts[1]).lstrip('0') or '0'
        cy = re.sub(r'\D', '', parts[2])
        if ct and cn and re.match(r'^\d{2,4}$', cy):
            return f'{ct}/{cn}/{cy}'
    return re.sub(r'[^A-Z0-9]', '', s)


def _is_for_admission(stage: Optional[str]) -> bool:
    normal = re.sub(r'\s+', ' ', (stage or '').strip().upper())
    return normal == 'FOR ADMISSION'


def normalize_judge_name(name: Optional[str]) -> str:
    return ' '.join(str(name or '').upper().split())


def _build_case_index(cases: List[Dict]) -> Dict[str, Dict]:
    indexed: Dict[str, Dict] = {}
    for c in cases:
        norm_case = normalize_case_number(c.get('case_number') or '')
        if not norm_case:
            continue
        if norm_case not in indexed:
            indexed[norm_case] = c
            continue
        # Prefer active entries when multiple rows share a normalized case number.
        existing_active = bool(indexed[norm_case].get('active'))
        incoming_active = bool(c.get('active'))
        if incoming_active and not existing_active:
            indexed[norm_case] = c
    return indexed


def _safe_create_case(case_number: str) -> Optional[Dict]:
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    payload_variants = [
        {
            'case_number': case_number,
            'active': True,
            'created_at': now_iso,
            'updated_at': now_iso,
        },
        {
            'case_number': case_number,
            'active': True,
            'updated_at': now_iso,
        },
        {
            'case_number': case_number,
            'updated_at': now_iso,
        },
        {
            'case_number': case_number,
        },
    ]

    for payload in payload_variants:
        try:
            resp = requests.post(
                f'{SUPABASE_URL}/rest/v1/cases',
                headers={**_sb_headers('return=representation'), 'Prefer': 'return=representation'},
                json=[payload],
                timeout=20,
            )

            if resp.ok:
                created_rows = resp.json() if resp.text.strip() else []
                if isinstance(created_rows, list) and created_rows:
                    return created_rows[0]
                return payload

            ct = resp.headers.get('content-type', '')
            err = resp.json() if 'json' in ct else {'message': resp.text[:200]}
            if isinstance(err, dict):
                code = err.get('code', '')
                msg = err.get('message', '')
            else:
                code = ''
                msg = str(err)

            # Retry with the next, smaller payload if a column is missing.
            if code in ('PGRST204', '42703'):
                continue

            # If duplicate key already exists, stop creating; caller should re-check index.
            if code == '23505':
                return None

            log(f'Create case failed for {case_number!r}: {err}', 'match')
            return None

        except Exception as exc:
            log(f'Create case exception for {case_number!r}: {exc}', 'match')
            return None

    return None


def _ensure_case_exists_for_admission(
    case_number: str,
    case_index_by_norm: Dict[str, Dict],
) -> Optional[Dict]:
    norm_case = normalize_case_number(case_number)
    if not norm_case:
        return None

    existing = case_index_by_norm.get(norm_case)
    if existing:
        return existing

    created = _safe_create_case(case_number)
    if not created:
        return None

    created.setdefault('case_number', case_number)
    created.setdefault('active', True)
    case_index_by_norm[norm_case] = created
    return created


# ── Date parsing ───────────────────────────────────────────────────────────────

def _parse_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if not s or s in ('\u2014', '-', 'NA', 'N/A', 'NULL', 'null', '0'):
        return None
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
        return s
    m = re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$', s)
    if m:
        return f'{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}'
    m = re.match(r'^(\d{1,2})[- ]([A-Za-z]{3})[- ](\d{4})$', s)
    if m:
        mon = _MONTHS.get(m.group(2).lower())
        if mon:
            return f'{m.group(3)}-{str(mon).zfill(2)}-{m.group(1).zfill(2)}'
    m = re.match(r'^(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})$', s, re.I)
    if m:
        mon = _MONTHS.get(m.group(2).lower()[:3])
        if mon:
            return f'{m.group(3)}-{str(mon).zfill(2)}-{m.group(1).zfill(2)}'
    return None


# ── eCourts HTML parser ────────────────────────────────────────────────────────

def _cell_text(v: Any) -> str:
    return ' '.join(str(v or '').split()).strip()


def _heading_table_map(soup: Any) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    current = ''
    for el in soup.find_all(['h1', 'h2', 'h3', 'h4', 'table']):
        if el.name in ('h1', 'h2', 'h3', 'h4'):
            t = _cell_text(el.get_text(' ', strip=True))
            if t:
                current = t
        elif el.name == 'table' and current and current not in result:
            result[current] = el
    return result


def _kv_pairs(tbl: Any) -> Dict[str, str]:
    pairs: Dict[str, str] = {}
    for tr in tbl.find_all('tr'):
        cells = tr.find_all(['th', 'td'])
        vals = [_cell_text(c.get_text(' ', strip=True)) for c in cells]
        vals = [v for v in vals if v]
        if len(vals) >= 4:
            pairs[vals[0]] = vals[1]
            pairs[vals[2]] = vals[3]
        elif len(vals) == 2:
            pairs[vals[0]] = vals[1]
    return pairs


def _parse_ecourts_html(html: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, 'html.parser')
    for tag in soup(['script', 'style', 'noscript']):
        tag.decompose()
    plain = soup.get_text('\n', strip=True).lower()
    if any(p in plain for p in [
        'no records found', 'no record found', 'case not found',
        'invalid cnr', 'cnr not found', 'no data found', 'record not found',
    ]):
        return {'_not_found': True}
    if len(plain.strip()) < 80:
        return None

    result: Dict[str, Any] = {}
    hmap = _heading_table_map(soup)

    for key, tbl in hmap.items():
        kl = key.lower()
        if 'case details' not in kl and 'case status' not in kl:
            continue
        for k, v in _kv_pairs(tbl).items():
            kk = k.lower()
            if not v or v.lower() in ('0', 'null', 'none', 'na', '\u2014'):
                continue
            if ('next' in kk and 'hearing' in kk) or ('next' in kk and 'date' in kk):
                d = _parse_date(v)
                if d:
                    result.setdefault('next_hearing_date', d)
            elif 'stage' in kk and 'case' not in kk:
                result.setdefault('latest_stage', v)
            elif 'stage of case' in kk or ('status' in kk and 'case' in kk):
                result.setdefault('latest_case_status', v)

    hkey = next(
        (k for k in hmap if 'history' in k.lower() and 'hearing' in k.lower()), None
    )
    if hkey:
        htbl = hmap[hkey]
        first_tr = htbl.find('tr')
        header_cells = first_tr.find_all(['th', 'td']) if first_tr else []
        hdrs = [_cell_text(c.get_text(' ', strip=True)).lower() for c in header_cells]

        def _col_idx(*kws: str) -> int:
            for kw in kws:
                for i, h in enumerate(hdrs):
                    if kw in h:
                        return i
            return -1

        date_col    = _col_idx('hearing date', 'date')
        biz_col     = _col_idx('purpose', 'business')
        stage_col   = _col_idx('cause list type', 'stage')
        remarks_col = _col_idx('remarks', 'remark')

        if date_col  == -1: date_col  = min(3, max(0, len(hdrs) - 1))
        if biz_col   == -1: biz_col   = min(4, max(0, len(hdrs) - 1))
        if stage_col == -1: stage_col = 0

        def _row_cell(row: List[str], idx: int) -> str:
            return row[idx] if 0 <= idx < len(row) else ''

        hearings: List[Dict[str, str]] = []
        skip_set = {'orders', 'order number', 'order no', 'order on'}
        for tr in htbl.find_all('tr')[1 if hdrs else 0:]:
            cells = tr.find_all('td')
            if not cells:
                continue
            row = [_cell_text(c.get_text(' ', strip=True)) for c in cells]
            if not any(row):
                continue
            if row[0].lower().strip() in skip_set:
                break
            date_raw = _row_cell(row, date_col)
            hearings.append({
                'date':     _parse_date(date_raw) or date_raw,
                'business': _row_cell(row, biz_col),
                'stage':    _row_cell(row, stage_col),
                'remarks':  _row_cell(row, remarks_col) if remarks_col >= 0 else '',
            })

        if hearings:
            result['hearing_history'] = hearings[:10]
            latest = hearings[0]
            if latest.get('date'):
                result['latest_hearing_date'] = _parse_date(latest['date']) or latest['date']
            result.setdefault('latest_hearing_remarks', latest.get('business', ''))
            result.setdefault('latest_stage', latest.get('stage', ''))

    return result if result else None


# ── Per-match eCourts enrichment ───────────────────────────────────────────────

def _to_text(value: Any) -> str:
    if value is None:
        return ''
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def _raw_data_dict(cl_row: Dict) -> Dict[str, Any]:
    raw_data = cl_row.get('raw_data')
    if isinstance(raw_data, dict):
        return raw_data
    if isinstance(raw_data, str):
        try:
            parsed = json.loads(raw_data)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _is_extra_case_row(cl_row: Dict) -> bool:
    raw = _raw_data_dict(cl_row)
    return bool(raw.get('is_extra'))


def _build_party_search_text(cl_row: Dict) -> str:
    parts = [
        _to_text(cl_row.get('case_number')),
        _to_text(cl_row.get('petitioner')),
        _to_text(cl_row.get('respondent')),
        _to_text(cl_row.get('prayer')),
        _to_text(cl_row.get('stage_status')),
    ]

    return ' '.join(parts).lower()


def _is_land_admin_match(cl_row: Dict) -> bool:
    text = _build_party_search_text(cl_row)
    return any(pattern in text for pattern in CLA_PATTERNS)


def _run_hearing_history_post(ecourts_case_no: str, cnr_number: str) -> requests.Response:
    return requests.post(
        ECOURTS_HIST_URL,
        data={
            'court_code': '1',
            'state_code': '10',
            'court_complex_code': '1',
            'case_no': ecourts_case_no,
            'cino': cnr_number,
            'appFlag': '',
        },
        headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': ECOURTS_HOME_URL,
            'X-Requested-With': 'XMLHttpRequest',
        },
        timeout=ECOURTS_TIMEOUT,
        verify=False,
    )


def _enrich_match(match: Dict) -> Dict:
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    cnr = (match.get('cnr_number') or '').strip()
    ecourts_case_no = (match.get('ecourts_case_no') or '').strip()

    if not cnr:
        match['ecourts_sync_status'] = 'pending_cnr_discovery'
        match['ecourts_synced_at'] = now_iso
        return match

    try:
        resp = None
        for _ in range(ECOURTS_RETRIES):
            resp = _run_hearing_history_post(ecourts_case_no, cnr)
            if resp.ok:
                break

        if resp is None or not resp.ok:
            match['ecourts_sync_status'] = 'failed'
            match['ecourts_error'] = f'HTTP {resp.status_code}' if resp is not None else 'No response'
            match['ecourts_synced_at'] = now_iso
            return match

        parsed = _parse_ecourts_html(resp.text)
        if parsed is None:
            match['ecourts_sync_status'] = 'failed'
        elif parsed.pop('_not_found', False):
            match['ecourts_sync_status'] = 'not_found'
        else:
            match.update(parsed)
            match['ecourts_sync_status'] = 'done'
            match['ecourts_error'] = None

        match['ecourts_synced_at'] = now_iso
        return match

    except Exception as exc:
        match['ecourts_sync_status'] = 'failed'
        match['ecourts_error'] = str(exc)[:200]
        match['ecourts_synced_at'] = now_iso
        return match


def _enrich_all(
    matches: List[Dict],
) -> Tuple[List[Dict], int]:
    if not matches:
        return [], 0

    enriched: List[Dict] = []
    done_count = 0
    deadline = time.monotonic() + ECOURTS_BUDGET_S

    for m in matches:
        if time.monotonic() >= deadline:
            m['ecourts_sync_status'] = 'pending_budget'
            m['ecourts_synced_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            enriched.append(m)
            continue

        result = _enrich_match(m)
        enriched.append(result)
        if result.get('ecourts_sync_status') == 'done':
            done_count += 1

    return enriched, done_count


# ── Batch upsert into today_matched_listings ───────────────────────────────────

def _safe_upsert_batch(batch: List[Dict]) -> int:
    ON_CONFLICT = 'listed_date,case_id,daily_cause_list_id'

    def _normalise_keys(rows: List[Dict]) -> List[Dict]:
        all_keys: set = set()
        for r in rows:
            all_keys.update(r.keys())
        return [{k: r.get(k) for k in all_keys} for r in rows]

    def _post(rows: List[Dict], upsert: bool) -> requests.Response:
        prefer = 'resolution=merge-duplicates,return=minimal' if upsert else 'return=minimal'
        params = {'on_conflict': ON_CONFLICT} if upsert else {}
        return requests.post(
            f'{SUPABASE_URL}/rest/v1/today_matched_listings',
            headers={**_sb_headers(), 'Prefer': prefer},
            params=params, json=_normalise_keys(rows), timeout=30,
        )

    # Keep only columns supported by today_matched_listings schema.
    working_batch = [{k: v for k, v in row.items() if k in BASE_MATCH_COLS} for row in batch]
    for _ in range(20):
        r = _post(working_batch, upsert=True)
        if r.ok:
            return len(working_batch)

        ct  = r.headers.get('content-type', '')
        err = r.json() if 'json' in ct else {}
        if not isinstance(err, dict):
            err = {}
        code = err.get('code', '')
        msg  = err.get('message', '')

        missing = None
        if code == 'PGRST204':
            m = re.search(r"find the '(\w+)' column", msg)
            if m:
                missing = m.group(1)
        elif code == '42703':
            m = re.search(r"column [\"']?(\w+)[\"']? does not exist", msg)
            if m:
                missing = m.group(1)

        if missing:
            log(f'Column {missing!r} missing - stripping and retrying.', 'match')
            working_batch = [{k: v for k, v in row.items() if k != missing} for row in working_batch]
            continue

        log(f'Upsert error {r.status_code}: {err or r.text[:200]}', 'match')
        return 0

    log('Upsert failed after repeated missing-column retries.', 'match')
    return 0


def _safe_upsert_daily_cause_batch(batch: List[Dict]) -> int:
    def _normalise_keys(rows: List[Dict]) -> List[Dict]:
        all_keys: set = set()
        for r in rows:
            all_keys.update(r.keys())
        return [{k: r.get(k) for k in all_keys} for r in rows]

    working_batch = list(batch)
    for _ in range(20):
        try:
            supabase.table("daily_cause_list").upsert(
                _normalise_keys(working_batch),
                on_conflict="cause_date,court_hall,item_number,case_number"
            ).execute()
            return len(working_batch)
        except Exception as exc:
            msg = str(exc)
            m = re.search(r"find the '(\w+)' column", msg)
            if not m:
                m = re.search(r"column [\"']?(\w+)[\"']?", msg)
            if m:
                missing = m.group(1)
                log(f"daily_cause_list missing column {missing!r} - stripping and retrying.", 'db')
                working_batch = [{k: v for k, v in row.items() if k != missing} for row in working_batch]
                continue
            raise

    log('daily_cause_list upsert failed after repeated missing-column retries.', 'db')
    return 0


def _safe_insert_vc_links_batch(batch: List[Dict]) -> int:
    def _normalise_keys(rows: List[Dict]) -> List[Dict]:
        all_keys: set = set()
        for r in rows:
            all_keys.update(r.keys())
        return [{k: r.get(k) for k in all_keys} for r in rows]

    working_batch = list(batch)
    for _ in range(20):
        try:
            supabase.table("vc_links").insert(
                _normalise_keys(working_batch)
            ).execute()
            return len(working_batch)
        except Exception as exc:
            msg = str(exc)
            m = re.search(r"find the '(\w+)' column", msg)
            if not m:
                m = re.search(r"column [\"']?(\w+)[\"']?", msg)
            if m:
                missing = m.group(1)
                log(f"vc_links missing column {missing!r} - stripping and retrying.", 'vc')
                working_batch = [{k: v for k, v in row.items() if k != missing} for row in working_batch]
                continue
            raise

    log('vc_links insert failed after repeated missing-column retries.', 'vc')
    return 0


# ── Cases master-record sync ───────────────────────────────────────────────────

def _derive_case_patch(match: Dict, today_str: str) -> Optional[Dict]:
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    patch: Dict = {'updated_at': now_iso}

    # Always record that the case appeared in today's cause list
    listed = match.get('listed_date') or today_str
    patch['last_listed_date'] = listed

    # Sync the latest cause-list stage to cases status fields.
    stage_status = (match.get('stage') or '').strip()
    if stage_status:
        patch['case_status'] = stage_status
        patch['last_hearing_update'] = stage_status

    substantive = set(patch) - {'updated_at'}
    return patch if substantive else None


def _safe_patch_case(case_id: str, patch: Dict) -> bool:
    def _do(fields: Dict) -> requests.Response:
        return requests.patch(
            f'{SUPABASE_URL}/rest/v1/cases',
            headers=_sb_headers(),
            params={'id': f'eq.{case_id}'},
            json=fields, timeout=15,
        )

    r = _do(patch)
    if r.ok:
        return True

    ct  = r.headers.get('content-type', '')
    err = r.json() if 'json' in ct else {}
    code = err.get('code', '') if isinstance(err, dict) else ''
    msg  = err.get('message', '') if isinstance(err, dict) else ''

    if code in ('PGRST204', '42703'):
        m = re.search(
            r"column (?:cases\.)?[\"']?(\w+)[\"']? does not exist|find the '(\w+)' column",
            msg,
        )
        if m:
            missing = m.group(1) or m.group(2)
            r2 = _do({k: v for k, v in patch.items() if k != missing})
            if r2.ok:
                return True

    log(f'PATCH cases id={case_id} failed: {err or r.text[:200]}', 'sync')
    return False


def _sync_cases_table(enriched_matches: List[Dict]) -> int:
    today_str = datetime.datetime.now(IST).date().isoformat()
    updated = 0
    for match in enriched_matches:
        case_id = match.get('case_id')
        if not case_id:
            continue
        patch = _derive_case_patch(match, today_str)
        if not patch:
            continue
        try:
            if _safe_patch_case(case_id, patch):
                updated += 1
                log(f'case updated id={case_id}  status={patch.get("case_status")!r}  nhd={patch.get("next_hearing_date")!r}', 'sync')
        except Exception as exc:
            log(f'cases update error case_id={case_id}: {exc}', 'sync')
    return updated


# ── Main matching + enrichment + notification pipeline ─────────────────────────

def run_matching_pipeline(listed_date: str, vc_lookup: Optional[Dict[str, str]] = None) -> None:
    """Match cause list rows by case-number or CLA party text and enrich from eCourts."""
    t0 = time.monotonic()
    vc_lookup = vc_lookup or {}
    try:
        log_section('STEP 1 — Fetch daily cause list')
        cause_list = _fetch_all('daily_cause_list', {
            'select': (
                'id,case_number,court_hall,item_number,judge_name,'
                'stage_status,petitioner,respondent,prayer'
            ),
            'cause_date': f'eq.{listed_date}',
            'order':      'court_hall.asc,item_number.asc',
        })
        if not cause_list:
            log(f'No cause list rows for {listed_date}. Skipping pipeline.', 'match')
            return
        log(f'Cause list rows: {len(cause_list)}', 'match')

        log_section('STEP 2 — Fetch tracked cases')
        cases = _fetch_active_cases()
        log(f'Active tracked cases: {len(cases)}', 'match')

        all_cases = _fetch_all_cases_for_existence()
        log(f'Total cases for existence check: {len(all_cases)}', 'match')

        log_section('STEP 3 — Build active-case lookup')
        case_by_norm = _build_case_index(cases)
        case_exists_by_norm = _build_case_index(all_cases)

        log_section('STEP 4 — Match by case number or CLA text')
        base_matches: List[Dict] = []
        for cl in cause_list:
            norm_cl_case = normalize_case_number(cl.get('case_number') or '')
            matched_case = case_by_norm.get(norm_cl_case) if norm_cl_case else None
            land_admin_match = _is_land_admin_match(cl)

            if not matched_case and not land_admin_match:
                continue

            stage_status = cl.get('stage_status')
            if not matched_case and _is_for_admission(stage_status):
                ensured = _ensure_case_exists_for_admission(
                    cl.get('case_number') or '',
                    case_exists_by_norm,
                )
                if ensured:
                    matched_case = ensured
                    if norm_cl_case:
                        case_by_norm[norm_cl_case] = ensured
                    log(f'FOR ADMISSION ensured case exists for {cl.get("case_number")!r}', 'match')
                else:
                    log(f'FOR ADMISSION case create skipped/failed for {cl.get("case_number")!r}', 'match')

            match_type = 'case_number' if matched_case else 'land_admin'
            case_number_value = matched_case.get('case_number') if matched_case else cl.get('case_number')
            case_cnr = (matched_case.get('cnr_number') or '').strip() if matched_case else ''

            if _is_extra_case_row(cl):
                log(f'MATCHED EXTRA CASE: {cl.get("case_number") or case_number_value}', 'match')
            else:
                log(f'MATCHED PARENT CASE: {cl.get("case_number") or case_number_value}', 'match')

            log(f'MATCH ({match_type})  CL={cl.get("case_number")!r} CASE={case_number_value!r} CNR={case_cnr!r}', 'match')

            item_raw = cl.get('item_number')
            normalized_judge = normalize_judge_name(cl.get('judge_name'))
            vc_link = vc_lookup.get(normalized_judge)
            if vc_link:
                log(f'VC Link matched for {cl.get("judge_name")}', 'vc')
            else:
                log(f'No VC Link found for {cl.get("judge_name")}', 'vc')
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            base_matches.append({
                'listed_date':         listed_date,
                'case_id':             matched_case.get('id') if matched_case else None,
                'daily_cause_list_id': cl['id'],
                'case_number':         case_number_value,
                'court_hall':          cl.get('court_hall'),
                'item_number':         str(item_raw).strip() if item_raw is not None else None,
                'judge_name':          cl.get('judge_name'),
                'vc_link':             vc_link,
                'stage':               cl.get('stage_status'),
                'petitioner':          cl.get('petitioner'),
                'respondent':          cl.get('respondent'),
                'notification_status': 'not_notified',
                'created_at':          now_iso,
                'updated_at':          now_iso,
            })

        log(f'Total matches found: {len(base_matches)}', 'match')
        if not base_matches:
            log('No matches. Pipeline done.', 'match')
            return

        log_section('STEP 5 — Upsert listing rows')
        inserted = 0
        for i in range(0, len(base_matches), BATCH_SIZE):
            inserted += _safe_upsert_batch(base_matches[i:i + BATCH_SIZE])
        log(f'Upserted base rows into today_matched_listings: {inserted}', 'match')

        log_section('STEP 6 — Sync cases master table')
        try:
            synced = _sync_cases_table(base_matches)
            log(f'Cases master table updated: {synced}/{len(base_matches)}', 'sync')
        except Exception as exc:
            log(f'Error (non-fatal): {exc}', 'sync')

        elapsed_total = time.monotonic() - t0
        log_section(f'PIPELINE COMPLETE  matched={inserted}  elapsed={elapsed_total:.1f}s')

    except Exception as exc:
        log(f'Pipeline error: {exc}', 'match')


today = datetime.datetime.now(IST).date() + datetime.timedelta(days=1)
# For testing:
today = datetime.date(2026, 6, 23)

# If today is Saturday (5) or Sunday (6), advance to the next Monday
if today.weekday() == 5:  # Saturday
    today += datetime.timedelta(days=2)
elif today.weekday() == 6:  # Sunday
    today += datetime.timedelta(days=1)

file_date = today.strftime("%d%m%Y")
db_date = today.strftime("%Y-%m-%d")

url = f"https://mhc.tn.gov.in/judis/clists/clists-madras/causelists/xml/cause_{file_date}.xml"

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
            log(f'Attempt {attempt}/7  {url}', 'download')

            response = session.get(
                url,
                timeout=(180, 180),
                verify=False,
                allow_redirects=True
            )

            log(f'HTTP {response.status_code}  Content-Type: {response.headers.get("Content-Type")}', 'download')

            response.raise_for_status()

            if not response.content or len(response.content) < 100:
                raise Exception("Empty or invalid XML response")

            log(f'Downloaded {len(response.content):,} bytes', 'download')
            return response.content

        except Exception as error:
            last_error = error
            log(f'Attempt {attempt} failed: {error}', 'download')

            if attempt < 7:
                log(f'Retrying in 30s...', 'download')
                time.sleep(30)

    log('Failed to download XML after all retries.', 'download')
    log(f'Last error: {last_error}', 'download')
    return None


def fetch_vc_links(vc_date: str) -> List[Dict[str, Optional[str]]]:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
        "Referer": "https://www.mhc.tn.gov.in/vclink/",
    })

    last_error = None

    for attempt in range(1, 8):
        try:
            log(f'Attempt {attempt}/7  vc_date={vc_date}', 'vc')
            response = session.post(
                'https://www.mhc.tn.gov.in/vclink/datareport.php',
                data={
                    'bench': '1',
                    'cdate': vc_date,
                },
                timeout=60,
                verify=False,
            )

            log(f'HTTP {response.status_code}  Content-Type: {response.headers.get("Content-Type")}', 'vc')
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            vc_rows: List[Dict[str, Optional[str]]] = []

            for tr in soup.find_all('tr'):
                cells = tr.find_all('td')
                if len(cells) < 3:
                    continue

                bench_no = _cell_text(cells[0].get_text(' ', strip=True))
                judge_name = _cell_text(cells[1].get_text(' ', strip=True))
                anchor = cells[2].find('a', href=True)
                vc_link = anchor['href'].strip() if anchor and anchor.get('href') else ''

                if not bench_no or not judge_name or not vc_link:
                    continue

                vc_rows.append({
                    'bench_no': bench_no,
                    'judge_name': judge_name,
                    'vc_link': vc_link,
                })

            return vc_rows

        except Exception as error:
            last_error = error
            log(f'Attempt {attempt} failed: {error}', 'vc')

            if attempt < 7:
                log('Retrying in 30s...', 'vc')
                time.sleep(30)

    log('Failed to download VC links after all retries.', 'vc')
    log(f'Last error: {last_error}', 'vc')
    return []


def build_vc_lookup(vc_rows: List[Dict[str, Optional[str]]]) -> Dict[str, str]:
    vc_lookup: Dict[str, str] = {}
    for row in vc_rows:
        normalized_judge = normalize_judge_name(row.get('judge_name'))
        vc_link = (row.get('vc_link') or '').strip()
        if not normalized_judge or not vc_link:
            continue
        vc_lookup[normalized_judge] = vc_link
    return vc_lookup


def save_vc_links(vc_date: str, vc_rows: List[Dict[str, Optional[str]]]) -> int:
    now_iso = datetime.datetime.now(datetime.UTC).isoformat()
    payload = [
        {
            'vc_date': vc_date,
            'bench_no': row.get('bench_no'),
            'judge_name': row.get('judge_name'),
            'vc_link': row.get('vc_link'),
            'created_at': now_iso,
        }
        for row in vc_rows
    ]

    inserted = 0
    for batch in chunk_list(payload, 500):
        inserted += _safe_insert_vc_links_batch(batch)

    return inserted


def chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


log_section(f'STEP 0 — Script start  ({_SCRIPT_START.strftime("%Y-%m-%d %H:%M:%S IST")})')
log(f'Target date : {db_date}', 'init')
log(f'XML URL     : {url}', 'init')

log_section('STEP 0a — Clear today\'s data from all tables')
try:
    log(f'Clearing today_matched_listings for {db_date}...', 'init')
    supabase.table('today_matched_listings').delete().eq('listed_date', db_date).execute()
    log('today_matched_listings cleared.', 'init')
except Exception as exc:
    log(f'today_matched_listings clear error (non-fatal): {exc}', 'init')
try:
    log(f'Clearing daily_cause_list for {db_date}...', 'init')
    supabase.table('daily_cause_list').delete().eq('cause_date', db_date).execute()
    log('daily_cause_list cleared.', 'init')
except Exception as exc:
    log(f'daily_cause_list clear error (non-fatal): {exc}', 'init')
try:
    log(f'Clearing vc_links for {db_date}...', 'init')
    supabase.table('vc_links').delete().eq('vc_date', db_date).execute()
    log('vc_links cleared.', 'init')
except Exception as exc:
    log(f'vc_links clear error (non-fatal): {exc}', 'init')

log_section('STEP 1 — Download MHC cause list XML')
xml_content = download_xml(url)

if not xml_content:
    log('No XML downloaded. Existing data not deleted.', 'download')
    exit(0)

log_section('STEP 2 — Parse XML')
try:
    root = ET.fromstring(xml_content)
except ET.ParseError as error:
    log(f'XML parsing failed: {error}', 'parse')
    log('Existing data not deleted.', 'parse')
    exit(0)

rows = []


for court in root.findall(".//court"):
    court_hall = court.findtext("courtno")
    judge_name = court.findtext("judge1")

    for stage in court.findall(".//stage"):
        stage_name = stage.findtext("stagename")

        for case in stage.findall(".//casedetails"):
            case_type = case.findtext("mcasetype")
            case_no   = case.findtext("mcaseno")
            case_year = case.findtext("mcaseyr")

            case_number = None
            if case_type and case_no and case_year:
                case_number = f"{case_type}/{case_no}/{case_year}"

            petitioner = case.findtext("pname")
            respondent = case.findtext("rname")
            serial_no  = case.findtext("serial_no")

            # ── Build the raw_data dict for the main case ──────────────────
            raw_data = {
                "mcasetype":    case_type,
                "mcaseno":      case_no,
                "mcaseyr":      case_year,
                "mpadv":        case.findtext("mpadv"),
                "mradv":        case.findtext("mradv"),
                "case_remarks": case.findtext("case_remarks"),
            }

            # ── Collect <extra> linked cases ───────────────────────────────
            extras = []
            for extra in case.findall("extra"):
                ex_type = extra.findtext("excasetype")
                ex_no   = extra.findtext("excaseno")
                ex_year = extra.findtext("excaseyr")
                ex_case_number = None
                if ex_type and ex_no and ex_year:
                    ex_case_number = f"{ex_type}/{ex_no}/{ex_year}"
                extras.append({
                    "case_number":    ex_case_number,
                    "excasetype":     ex_type,
                    "excaseno":       ex_no,
                    "excaseyr":       ex_year,
                    "petitioner":     extra.findtext("expname"),
                    "respondent":     extra.findtext("exrname"),
                    "petitioner_adv": extra.findtext("expadv"),
                    "respondent_adv": extra.findtext("exradv"),
                    "case_remarks":   extra.findtext("excaseremarks"),
                })

            if extras:
                raw_data["extra_cases"] = extras

            rows.append({
                "cause_date":           db_date,
                "court_name":           "Madras High Court",
                "bench":                "Chennai",
                "court_hall":           court_hall,
                "item_number":          serial_no,
                "case_number":          case_number,
                "petitioner":           petitioner,
                "respondent":           respondent,
                "judge_name":           judge_name,
                "prayer":               raw_data.get("case_remarks") or None,
                "stage_status":         stage_name,
                "updated_at":           datetime.datetime.now(datetime.UTC).isoformat(),
            })

            # ── Also insert each <extra> as its own row ────────────────────
            for idx, ex in enumerate(extras, start=1):
                if not ex["case_number"]:
                    continue   # skip malformed extras with no case number

                rows.append({
                    "cause_date":           db_date,
                    "court_name":           "Madras High Court",
                    "bench":                "Chennai",
                    "court_hall":           court_hall,
                    # extras share the parent's serial_no; suffix keeps them unique
                    "item_number":          f"{serial_no}-E{idx}" if serial_no else None,
                    "case_number":          ex["case_number"],
                    "petitioner":           ex["petitioner"],
                    "respondent":           ex["respondent"],
                    "judge_name":           judge_name,
                    "prayer":               ex.get("case_remarks") or None,
                    "stage_status":         stage_name,
                    "updated_at":           datetime.datetime.now(datetime.UTC).isoformat(),
                })

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

log(f'Parsed rows        : {len(rows)}', 'parse')
log(f'Deduplicated rows  : {len(deduped_rows)}', 'parse')

if not deduped_rows:
    log('No rows found. Existing data not deleted.', 'parse')
    exit(0)

log_section('STEP 3 — Write to Supabase daily_cause_list')

inserted_daily = 0
for batch in chunk_list(deduped_rows, 500):
    inserted_daily += _safe_upsert_daily_cause_batch(batch)

log(f'Inserted/Updated : {inserted_daily} rows', 'db')

log_section('STEP 4 — Download VC Links')
vc_rows = fetch_vc_links(db_date)
vc_lookup = build_vc_lookup(vc_rows)
log(f'VC rows found : {len(vc_rows)}', 'vc')
inserted_vc = save_vc_links(db_date, vc_rows)
log(f'Inserted VC rows : {inserted_vc}', 'vc')

# ── Step 2: Match cause list against tracked cases, enrich, and notify ─────────
run_matching_pipeline(db_date, vc_lookup)
