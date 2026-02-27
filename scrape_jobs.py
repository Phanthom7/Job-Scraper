import requests
import pandas as pd
import time
import re
import os
import json
import logging
from datetime import datetime
import concurrent.futures
from tenacity import retry, stop_after_attempt, wait_exponential

# --- 0. LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 1. CONFIGURATION ---
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.json')
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    GREENHOUSE_COMPANIES = config_data.get('GREENHOUSE_COMPANIES', [])
    WORKDAY_CONFIG = config_data.get('WORKDAY_CONFIG', [])
    logger.info(f"Loaded {len(GREENHOUSE_COMPANIES)} Greenhouse companies and {len(WORKDAY_CONFIG)} Workday companies from config.json.")
except Exception as e:
    logger.error(f"Failed to load config.json: {e}")
    GREENHOUSE_COMPANIES = []
    WORKDAY_CONFIG = []

INCLUDE = ["analyst", "data", "analytics", "research", "scientist", 'associate']
EXCLUDE = ["senior", "sr", "lead", "manager", "vp", "director", "staff", "principal", 'phd', 'intern']
US_STATES = ['alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri', 'montana', 'nebraska', 'nevada', 'new hampshire', 'new jersey', 'new mexico', 'new york', 'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania', 'rhode island', 'south carolina', 'south dakota', 'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington', 'west virginia', 'wisconsin', 'wyoming']
LOCATION_KEYWORDS = ["us", "usa", "united states", "remote", "anywhere", "locations", "san francisco", "san jose", "lehi", "chicago", "boston", "seattle", "austin", "dallas", "houston", "atlanta", "los angeles"] + US_STATES

# Create a shared session for connection pooling
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

# --- 2. FETCHING FUNCTIONS ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_workday_with_retry(api_url, payload):
    return session.post(api_url, json=payload, headers={"Content-Type": "application/json"}, timeout=15)

def fetch_workday(source):
    api_url = f"https://{source['sub']}.{source['server']}.myworkdayjobs.com/wday/cxs/{source['sub']}/{source['id']}/jobs"
    base_url = f"https://{source['sub']}.{source['server']}.myworkdayjobs.com/en-US/{source['id']}"
    payload = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": "analyst"}
    try:
        res = fetch_workday_with_retry(api_url, payload)
        res.raise_for_status()
        data = res.json()
        jobs = [{
            "Platform": "Workday", "Company": source['name'],
            "Title": j['title'], "Location": j.get('locationsText', 'N/A'),
            "Posted": j.get('postedOn', 'Today'),
            "Description Link": base_url + j['externalPath'],
            "Apply Link": (base_url + j['externalPath']).replace("/job/", "/apply/")
        } for j in data.get('jobPostings', [])]
        logger.info(f"Workday: {source['name']} - Found {len(jobs)} jobs")
        return jobs
    except Exception as e:
        logger.error(f"Workday error for {source['name']}: {e}")
        return []

def check_experience(description):
    """Returns False if the job requires > 2 years of experience or a PhD."""
    desc = description.lower()
    
    # 1. PhD or internship Check
    if "phd" in desc:
        return False
        
    # 2. Years of Experience Check
    # Regex looks for: [number] [space] [year/years] [space] [of]
    # Matches: "3 years of", "5+ years of", "10 years of"
    experience_matches = re.findall(r'(\d+)\s*[\+]?\s*years?', desc)
    
    for match in experience_matches:
        if int(match) > 2:
            return False
            
    return True

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_greenhouse_with_retry(url):
    return session.get(url, timeout=15)

def fetch_greenhouse(token):
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
    try:
        res = fetch_greenhouse_with_retry(url)
        res.raise_for_status()
        data = res.json()
        jobs_list = []
        for j in data.get('jobs', []):
            content = j.get('content', '')
            if not check_experience(content):
                continue
            # Greenhouse format: 2026-02-10T18:59:18-05:00
            # We take the first 10 characters to get "2026-02-10"
            raw_date = j.get('updated_at', '')
            clean_date = raw_date[:10] if raw_date else "Today"
            
            jobs_list.append({
                "Platform": "Greenhouse", 
                "Company": token.capitalize(),
                "Title": j['title'], 
                "Location": j['location']['name'],
                "Posted": clean_date,  # <--- Cleaned Date
                "Description Link": j['absolute_url'], 
                "Apply Link": j['absolute_url'] + "#app"
            })
        logger.info(f"Greenhouse: {token} - Found {len(jobs_list)} viable jobs")
        return jobs_list
    except Exception as e:
        logger.error(f"Greenhouse error for {token}: {e}")
        return []


# --- 3. MAIN EXECUTION ---
if __name__ == "__main__":
    start_time = time.time()
    all_raw = []

    # Use ThreadPoolExecutor for concurrent fetching
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Submit Workday tasks
        workday_futures = {executor.submit(fetch_workday, src): src['name'] for src in WORKDAY_CONFIG}
        # Submit Greenhouse tasks
        greenhouse_futures = {executor.submit(fetch_greenhouse, token): token for token in GREENHOUSE_COMPANIES}
        
        # Combine futures
        futures = {**workday_futures, **greenhouse_futures}
        
        for future in concurrent.futures.as_completed(futures):
            company_name = futures[future]
            try:
                jobs = future.result()
                if jobs:
                    all_raw.extend(jobs)
            except Exception as exc:
                logger.error(f"{company_name} generated an exception: {exc}")

    filtered = [j for j in all_raw if any(k in j['Title'].lower() for k in INCLUDE) 
                and not any(k in j['Title'].lower() for k in EXCLUDE)]

    final_list = []
    today_date = datetime.now().date()
    
    # Track companies that returned at least one passing job
    companies_captured = set()


    for job in filtered:
        # Location Filter
        loc = job['Location'].lower()
        if not (not loc or any(k in loc for k in LOCATION_KEYWORDS) or re.search(r',\s*[A-Z]{2}', job['Location'])):
            continue

        # Date Filter
        posted_text = str(job['Posted']).lower()
        keep_job = False
        
        if re.match(r'\d{4}-\d{2}-\d{2}', posted_text):
            try:
                job_date = pd.to_datetime(posted_text).date()
                if (today_date - job_date).days <= 30: keep_job = True
            except: pass
        elif any(x in posted_text for x in ["today", "yesterday", "recent", "1 day"]):
            keep_job = True
        else:
            days_match = re.search(r'\b([0-9]|[12][0-9]|30)\b(?!\+)', posted_text) 
            if days_match: keep_job = True
            elif "30+" not in posted_text: keep_job = True

        if keep_job: 
            final_list.append(job)
            companies_captured.add(job['Company'].lower())

    end_time = time.time()
    logger.info(f"Scraping completed in {end_time - start_time:.2f} seconds.")
    
    # Verify captured companies against expected
    all_expected_companies = set(GREENHOUSE_COMPANIES + [src['name'].lower() for src in WORKDAY_CONFIG])
    missed_companies = all_expected_companies - companies_captured
    if missed_companies:
        logger.warning(f"No valid jobs found after filtering for: {', '.join(missed_companies)}")

    # --- 4. EXPORT (OVERWRITE MODE) ---
    if final_list:
        df = pd.DataFrame(final_list)
        df.to_excel("daily_opportunities.xlsx", index=False)
        logger.info(f"✅ SUCCESS: Saved {len(final_list)} jobs to daily_opportunities.xlsx!")
    else:
        logger.info("❌ No recent US jobs found.")