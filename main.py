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
ECOURTS_TIMEOUT  = (5, 20)
ECOURTS_WORKERS  = 5
ECOURTS_BUDGET_S = 45
BASE_MATCH_COLS  = frozenset({
    'listed_date', 'match_date', 'organization_id',
    'case_id', 'daily_cause_list_id',
    'case_number', 'cnr_number', 'court_hall', 'item_number',
    'judge_name', 'stage', 'petitioner', 'respondent',
    'match_type', 'match_status', 'notification_status', 'cnr_status',
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

def _enrich_match(match: Dict) -> Dict:
    cnr     = (match.get('cnr_number') or '').strip()
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if not cnr:
        match['ecourts_sync_status'] = 'pending_cnr'
        match['ecourts_synced_at']   = now_iso
        return match

    try:
        resp = requests.get(
            ECOURTS_HIST_URL,
            params={
                'state_code':           '10',
                'dist_code':            '1',
                'court_code':           '1',
                'caseStatusSearchType': 'CNRNumber',
                'cino':                 cnr,
                'national_court_code':  'HCMA01',
            },
            headers={
                'User-Agent':       'Mozilla/5.0',
                'Referer':          'https://hcservices.ecourts.gov.in/',
                'X-Requested-With': 'XMLHttpRequest',
            },
            timeout=ECOURTS_TIMEOUT,
            verify=False,
        )
        if not resp.ok:
            match['ecourts_sync_status'] = 'failed'
            match['ecourts_error']       = f'HTTP {resp.status_code}'
            match['ecourts_synced_at']   = now_iso
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
        match['ecourts_error']       = str(exc)[:200]
        match['ecourts_synced_at']   = now_iso
        return match


def _enrich_all(matches: List[Dict]) -> Tuple[List[Dict], int]:
    if not matches:
        return [], 0

    no_cnr   = [m for m in matches if not (m.get('cnr_number') or '').strip()]
    to_fetch = [m for m in matches if (m.get('cnr_number') or '').strip()]

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for m in no_cnr:
        m['ecourts_sync_status'] = 'pending_cnr'
        m['ecourts_synced_at']   = now_iso

    if not to_fetch:
        return no_cnr, 0

    enriched: List[Dict] = list(no_cnr)
    done_count = 0
    deadline   = time.monotonic() + ECOURTS_BUDGET_S

    executor = ThreadPoolExecutor(max_workers=ECOURTS_WORKERS)
    pending: Dict[Any, Dict] = {executor.submit(_enrich_match, m): m for m in to_fetch}

    try:
        while pending and time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())
            done_set, _ = wait(list(pending.keys()), timeout=remaining,
                               return_when=FIRST_COMPLETED)
            for f in done_set:
                m = pending.pop(f)
                try:
                    result = f.result()
                    enriched.append(result)
                    if result.get('ecourts_sync_status') == 'done':
                        done_count += 1
                except Exception as exc:
                    m['ecourts_sync_status'] = 'failed'
                    m['ecourts_error']       = str(exc)[:200]
                    m['ecourts_synced_at']   = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    enriched.append(m)

        for f, m in list(pending.items()):
            f.cancel()
            m['ecourts_sync_status'] = 'pending'
            enriched.append(m)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

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

    r = _post(batch, upsert=True)
    if r.ok:
        return len(batch)

    ct  = r.headers.get('content-type', '')
    err = r.json() if 'json' in ct else {}
    if not isinstance(err, dict):
        err = {}
    code = err.get('code', '')
    msg  = err.get('message', '')

    if code == '42703':
        log('WARNING: missing columns; falling back to base-only insert.', 'match')
        base_batch = [{k: v for k, v in row.items() if k in BASE_MATCH_COLS} for row in batch]
        r2 = _post(base_batch, upsert=True)
        return len(base_batch) if r2.ok else 0

    if code == 'PGRST204':
        m2 = re.search(r"find the '(\w+)' column", msg)
        if m2:
            missing = m2.group(1)
            log(f'Column {missing!r} missing - stripping and retrying.', 'match')
            stripped = [{k: v for k, v in row.items() if k != missing} for row in batch]
            r2 = _post(stripped, upsert=(missing != 'listed_date'))
            if r2.ok:
                return len(stripped)

    log(f'Upsert error {r.status_code}: {err or r.text[:200]}', 'match')
    return 0


# ── Cases master-record sync ───────────────────────────────────────────────────

def _derive_case_patch(match: Dict, today_str: str) -> Optional[Dict]:
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    patch: Dict = {'updated_at': now_iso}

    # Always record that the case appeared in today's cause list
    listed = match.get('listed_date') or today_str
    patch['last_listed_date'] = listed

    stage = (match.get('stage') or '').strip()
    if stage:
        patch['current_stage'] = stage

    # eCourts enrichment fields — only applied when sync succeeded
    if match.get('ecourts_sync_status') == 'done':
        patch['ecourts_last_synced_at'] = now_iso

        raw_status = (match.get('latest_case_status') or '').strip()
        if raw_status:
            sl = raw_status.lower()
            if 'dispos' in sl:
                patch['case_status'] = 'Disposed'
                patch['active']      = False
            elif 'pending' in sl:
                patch['case_status'] = 'Pending'
            else:
                patch['case_status'] = raw_status

        if match.get('latest_hearing_date'):
            patch['last_hearing_date'] = match['latest_hearing_date']

        lhr = (match.get('latest_hearing_remarks') or '').strip()
        if lhr:
            patch['last_hearing_update'] = lhr

        nhd = match.get('next_hearing_date')
        if nhd:
            patch['next_hearing_date'] = nhd
            if nhd >= today_str:
                patch['follow_up_status'] = 'Active'

    substantive = set(patch) - {'updated_at', 'ecourts_last_synced_at'}
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

def run_matching_pipeline(listed_date: str) -> None:
    """Match daily_cause_list against tracked cases, enrich with eCourts, and notify."""
    t0 = time.monotonic()
    try:
        # 1. Cause list rows for this date
        log_section('STEP 1 — Fetch daily cause list')
        cause_list = _fetch_all('daily_cause_list', {
            'select': ('id,case_number,cnr_number,court_hall,item_number,'
                       'judge_name,last_hearing_or_stage,petitioner,respondent'),
            'cause_date': f'eq.{listed_date}',
            'order':      'court_hall.asc,item_number.asc',
        })
        if not cause_list:
            log(f'No cause list rows for {listed_date}. Skipping pipeline.', 'match')
            return
        log(f'Cause list rows: {len(cause_list)}', 'match')

        # 2. All active tracked cases
        log_section('STEP 2 — Fetch tracked cases')
        cases = _fetch_all('cases', {
            'select': 'id,organization_id,case_number,cnr_number',
            'active': 'eq.true',
        })
        log(f'Active tracked cases: {len(cases)}', 'match')
        if not cases:
            log('No active cases to match against. Skipping.', 'match')
            return

        # 3. Build lookup maps
        cl_by_cnr:  Dict[str, Dict] = {}
        cl_by_norm: Dict[str, Dict] = {}
        for cl in cause_list:
            raw_cnr = (cl.get('cnr_number') or '').strip()
            if raw_cnr:
                cl_by_cnr[raw_cnr.lower()] = cl
            norm_cn = normalize_case_number(cl.get('case_number') or '')
            if norm_cn:
                cl_by_norm[norm_cn] = cl

        # 4. Match each case
        log_section('STEP 3 — Match cause list against cases')
        base_matches: List[Dict] = []
        seen: set = set()
        for c in cases:
            matched_cl: Optional[Dict] = None
            match_type = 'case_number'
            case_cnr = (c.get('cnr_number') or '').strip()

            if case_cnr and case_cnr.lower() in cl_by_cnr:
                matched_cl = cl_by_cnr[case_cnr.lower()]
                match_type = 'cnr'

            if not matched_cl:
                norm_c = normalize_case_number(c.get('case_number') or '')
                if norm_c and norm_c in cl_by_norm:
                    matched_cl = cl_by_norm[norm_c]

            if not matched_cl:
                continue

            pair = (c['id'], matched_cl['id'])
            if pair in seen:
                continue
            seen.add(pair)

            log(f'MATCH ({match_type})  CL={matched_cl.get("case_number")!r}  CASE={c.get("case_number")!r}  CNR={case_cnr!r}', 'match')

            item_raw = matched_cl.get('item_number')
            base_matches.append({
                'listed_date':         listed_date,
                'match_date':          listed_date,
                'organization_id':     c.get('organization_id'),
                'case_id':             c['id'],
                'daily_cause_list_id': matched_cl['id'],
                'case_number':         matched_cl.get('case_number'),
                'cnr_number':          case_cnr or None,
                'court_hall':          matched_cl.get('court_hall'),
                'item_number':         str(item_raw).strip() if item_raw is not None else None,
                'judge_name':          matched_cl.get('judge_name'),
                'stage':               matched_cl.get('last_hearing_or_stage'),
                'petitioner':          matched_cl.get('petitioner'),
                'respondent':          matched_cl.get('respondent'),
                'match_type':          match_type,
                'match_status':        'matched',
                'cnr_status':          'discovered' if case_cnr else 'not_discovered',
                'ecourts_sync_status': 'pending',
            })

        log(f'Total matches found: {len(base_matches)}', 'match')
        if not base_matches:
            log('No matches. Pipeline done.', 'match')
            return

        # 5. Enrich with eCourts hearing history
        log_section('STEP 4 — eCourts enrichment')
        log(f'Enriching {len(base_matches)} match(es) via eCourts (budget={ECOURTS_BUDGET_S}s)...', 'match')
        enriched_matches, enriched_count = _enrich_all(base_matches)
        log(f'eCourts enriched: {enriched_count}/{len(base_matches)}', 'match')

        # 6. Upsert to today_matched_listings
        log_section('STEP 5 — Upsert to today_matched_listings')
        inserted = 0
        for i in range(0, len(enriched_matches), BATCH_SIZE):
            inserted += _safe_upsert_batch(enriched_matches[i:i + BATCH_SIZE])
        log(f'Upserted to today_matched_listings: {inserted}', 'match')

        # 6b. Sync cases master table with latest court status + cause list stage
        log_section('STEP 6 — Sync cases master table')
        try:
            synced = _sync_cases_table(enriched_matches)
            log(f'Cases master table updated: {synced}/{len(enriched_matches)}', 'sync')
        except Exception as exc:
            log(f'Error (non-fatal): {exc}', 'sync')

        elapsed_total = time.monotonic() - t0
        log_section(f'PIPELINE COMPLETE  matched={inserted}  enriched={enriched_count}  elapsed={elapsed_total:.1f}s')

    except Exception as exc:
        log(f'Pipeline error: {exc}', 'match')


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


def chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


log_section(f'STEP 0 — Script start  ({_SCRIPT_START.strftime("%Y-%m-%d %H:%M:%S IST")})')
log(f'Target date : {db_date}', 'init')
log(f'XML URL     : {url}', 'init')

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
            case_no = case.findtext("mcaseno")
            case_year = case.findtext("mcaseyr")

            case_number = None
            if case_type and case_no and case_year:
                case_number = f"{case_type}/{case_no}/{case_year}"

            petitioner = case.findtext("pname")
            respondent = case.findtext("rname")

            rows.append({
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
                "party_names": f"{petitioner or ''} vs {respondent or ''}",
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
log(f'Clearing existing rows for {db_date}...', 'db')
supabase.table("daily_cause_list") \
    .delete() \
    .eq("cause_date", db_date) \
    .execute()
log('Old records cleared.', 'db')

for batch in chunk_list(deduped_rows, 500):
    supabase.table("daily_cause_list").upsert(
        batch,
        on_conflict="cause_date,court_hall,item_number,case_number"
    ).execute()

log(f'Inserted/Updated : {len(deduped_rows)} rows', 'db')

# ── Step 2: Match cause list against tracked cases, enrich, and notify ─────────
run_matching_pipeline(db_date)
