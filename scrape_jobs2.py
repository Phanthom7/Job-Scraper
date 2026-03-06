import requests
import pandas as pd
import re
import os
import json
import logging
from datetime import datetime, timedelta
import concurrent.futures
from tenacity import retry, stop_after_attempt, wait_exponential

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURATION LOADING ---
def load_json(file_name, default):
    path = os.path.join(os.path.dirname(__file__), file_name)
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load {file_name}: {e}")
        return default

# Load Targeting & Company Configs
role_cfg = load_json('include.json', {"INCLUDE_ROLES": [], "EXCLUDE_ROLES": []})
INCLUDE = role_cfg.get("INCLUDE_ROLES", [])
EXCLUDE = role_cfg.get("EXCLUDE_ROLES", [])

comp_cfg = load_json('config.json', {"GREENHOUSE_COMPANIES": [], "WORKDAY_CONFIG": []})
GREENHOUSE_COMPANIES = comp_cfg.get('GREENHOUSE_COMPANIES', [])
WORKDAY_CONFIG = comp_cfg.get('WORKDAY_CONFIG', [])

US_STATES_ABBR = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
]
LOCATION_KEYS = ["us", "usa", "united states", "remote", "anywhere"]

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

# --- UTILITY & FILTERING ---
def is_viable_job(job):
    title = job['Title'].lower()
    loc = job['Location'].lower()
    desc = job.get('Description', '').lower()
    
    # 1. Title Filter (Targeting INCLUDE, avoiding EXCLUDE)
    if not any(k in title for k in INCLUDE) or any(k in title for k in EXCLUDE):
        return False
    
    # 2. Strict Location Filter (Prevents International results like UK)
    is_us_keyword = any(k in loc for k in LOCATION_KEYS)
    is_us_format = any(f", {abbr}" in job['Location'].upper() for abbr in US_STATES_ABBR) or job['Location'].upper() in US_STATES_ABBR
    if not (is_us_keyword or is_us_format):
        return False
    
    # 3. Experience Filter (Rejects > 2 years or advanced degrees)
    if any(w in desc for w in ["phd", "p.h.d", "staff analyst", "principal", "graduating"]):
        return False
    
    years = re.findall(r'(\d{1,2})[\+]?\s*(?:\w+\s+){0,3}years?', desc)
    if any(int(y) > 2 for y in years):
        return False
        
    return True

# --- FETCHERS ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2))
def fetch_greenhouse(token):
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
    try:
        res = session.get(url, timeout=15)
        res.raise_for_status()
        valid_jobs = []
        for j in res.json().get('jobs', []):
            job_obj = {
                "Platform": "Greenhouse", "Company": token.capitalize(),
                "Title": j['title'], "Location": j['location']['name'],
                "Posted": j.get('updated_at', '')[:10],
                "Link": j['absolute_url'], "Description": j.get('content', '')
            }
            if is_viable_job(job_obj): valid_jobs.append(job_obj)
        return valid_jobs
    except Exception as e:
        logger.error(f"Greenhouse {token} error: {e}")
        return []

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2))
def fetch_workday(src):
    # Wells Fargo requires sub: 'wf' to avoid 422 error
    api_url = f"https://{src['sub']}.{src['server']}.myworkdayjobs.com/wday/cxs/{src['sub']}/{src['id']}/jobs"
    base_url = f"https://{src['sub']}.{src['server']}.myworkdayjobs.com/en-US/{src['id']}"
    payload = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": "analyst"}
    
    try:
        res = session.post(api_url, json=payload, timeout=15)
        res.raise_for_status()
        valid_jobs = []
        for j in res.json().get('jobPostings', []):
            job_obj = {
                "Platform": "Workday", "Company": src['name'],
                "Title": j['title'], "Location": j.get('locationsText', ''),
                "Posted": j.get('postedOn', 'Today'),
                "Link": base_url + j['externalPath'],
                "Description": "" # Workday requires a second call for desc; title/loc filter first
            }
            if is_viable_job(job_obj): valid_jobs.append(job_obj)
        return valid_jobs
    except Exception as e:
        logger.error(f"Workday {src['name']} error: {e}")
        return []

# --- EXECUTION ---
if __name__ == "__main__":
    all_jobs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_workday, s): s['name'] for s in WORKDAY_CONFIG}
        futures.update({executor.submit(fetch_greenhouse, t): t for t in GREENHOUSE_COMPANIES})
        
        for f in concurrent.futures.as_completed(futures):
            all_jobs.extend(f.result())

    # Date Filter (Last 14 days)
    final_list = []
    cutoff = datetime.now() - timedelta(days=14)
    for job in all_jobs:
        posted = job['Posted'].lower()
        if any(x in posted for x in ["today", "yesterday", "recent", "day"]):
            final_list.append(job)
        else:
            try:
                if datetime.strptime(job['Posted'], "%Y-%m-%d") >= cutoff:
                    final_list.append(job)
            except: pass

    if final_list:
        df = pd.DataFrame(final_list)
        # Drop description before export to keep Excel file clean
        if 'Description' in df.columns: df = df.drop(columns=['Description'])
        df.to_excel("daily_opportunities.xlsx", index=False)
        logger.info(f"Success: Saved {len(final_list)} jobs.")
    else:
        logger.info("No viable jobs found.")