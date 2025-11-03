#!/usr/bin/env python3
import os
import time
import hashlib
import requests
from bs4 import BeautifulSoup
import pandas as pd
import subprocess

# --- CONFIGURATION ---
FBREF_URL = "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures"
HFW_REPO = "HFW-App"      # folder name after cloning your fork
OUTPUT_DIR = "weekly_scores"
CACHE_DIR = "fbref_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

# Sports-Reference policy: <= 10 requests per minute to FBref
MAX_REQUESTS_PER_MIN = 10
REQUEST_WINDOW_SECONDS = 60.0

# Simple request timestamp tracker (in-memory)
_request_timestamps = []

# AbstractAPI settings: provide key via environment var ABSTRACTAPI_KEY
# (instructions below for GitHub Actions)
ABSTRACTAPI_KEY = os.environ.get("ABSTRACTAPI_KEY", "").strip()
ABSTRACTAPI_ENDPOINT = "https://scrape.abstractapi.com/v1/"

# ----------------- helpers -----------------
def _cache_path_for_url(url: str) -> str:
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return os.path.join(CACHE_DIR, f"{h}.html")

def _purge_old_timestamps():
    now = time.time()
    cutoff = now - REQUEST_WINDOW_SECONDS
    # keep only timestamps inside window
    while _request_timestamps and _request_timestamps[0] <= cutoff:
        _request_timestamps.pop(0)

def _sleep_to_obey_rate_limit():
    """
    Ensure we don't exceed MAX_REQUESTS_PER_MIN in a rolling window.
    If too many, sleep long enough for earliest timestamp to fall out.
    """
    _purge_old_timestamps()
    if len(_request_timestamps) >= MAX_REQUESTS_PER_MIN:
        now = time.time()
        earliest = _request_timestamps[0]
        wait = (earliest + REQUEST_WINDOW_SECONDS) - now + 0.5
        if wait > 0:
            print(f"Rate limit reached — sleeping {wait:.1f}s to honor FBref policy")
            time.sleep(wait)
        _purge_old_timestamps()

def _record_request_timestamp():
    _request_timestamps.append(time.time())

# ----------------- fetch via AbstractAPI -----------------
def fetch_html_via_abstractapi(url: str, use_cache_seconds: int = 3600, max_retries: int = 3):
    """
    Fetch the given URL via AbstractAPI scrape endpoint, with disk cache and backoff.
    - use_cache_seconds: reuse cached HTML younger than this value (seconds).
    - returns HTML text or None on failure.
    """
    if not ABSTRACTAPI_KEY:
        print("⚠️ ABSTRACTAPI_KEY not set in environment. Set it and retry.")
        return None

    cache_path = _cache_path_for_url(url)
    if os.path.exists(cache_path):
        age = time.time() - os.path.getmtime(cache_path)
        if age <= use_cache_seconds:
            print(f"Using cached HTML for {url} (age {int(age)}s)")
            with open(cache_path, "r", encoding="utf-8") as f:
                return f.read()
        else:
            print(f"Cached HTML is stale (age {int(age)}s) — will re-fetch via AbstractAPI")

    attempt = 0
    backoff = 1.0
    while attempt < max_retries:
        attempt += 1
        # Respect Sports-Reference policy before making the request
        _sleep_to_obey_rate_limit()

        params = {"api_key": ABSTRACTAPI_KEY, "url": url}
        print(f"Fetching via AbstractAPI: {url} (attempt {attempt}/{max_retries})")
        try:
            resp = requests.get(ABSTRACTAPI_ENDPOINT, params=params, timeout=30)
        except Exception as e:
            print(f"Network error on AbstractAPI request: {e} — backing off {backoff}s")
            time.sleep(backoff)
            backoff *= 2.0
            continue

        status = getattr(resp, "status_code", None)
        print(f"Status Code: {status}")
        # record timestamp as we did make a request to fetch the page (counts against the policy)
        _record_request_timestamp()

        if status == 200 and resp.text and "Premier League" in resp.text:
            try:
                with open(cache_path, "w", encoding="utf-8") as f:
                    f.write(resp.text)
                print(f"✅ Saved fetched HTML to {cache_path}")
            except Exception as e:
                print(f"Warning: could not write cache file: {e}")
            return resp.text

        if status in (403, 429):
            # blocked or rate-limited — back off and retry conservatively
            print(f"Received HTTP {status} from AbstractAPI/fbref; backing off {backoff}s")
            time.sleep(backoff)
            backoff *= 2.0
            continue
        elif status and 500 <= status < 600:
            print(f"Server error {status}; retrying after {backoff}s")
            time.sleep(backoff)
            backoff *= 2.0
            continue
        else:
            print(f"Unexpected status {status}; not retrying further.")
            break

    print(f"⚠️ Failed to load {url} via AbstractAPI after {max_retries} attempts.")
    return None

# ----------------- parsing helpers (unchanged logic) -----------------
def find_premier_league_table(soup):
    """
    FBref wraps the schedule table in a season-specific div like:
      <div id="div_sched_2025-2026_9_1"> ... <table> ... </table> ... </div>
    '9' is the competition id for the Premier League.
    """
    div_container = soup.find("div", id=lambda x: x and x.startswith("div_sched_") and "_9_" in x)
    if not div_container:
        print("⚠️ Could not find Premier League schedule div (id starts with 'div_sched_' and contains '_9_').")
        return None
    table = div_container.find("table")
    if not table:
        print("⚠️ Could not find schedule table inside the div.")
        return None
    return table

def get_latest_completed_week_from_soup(soup):
    table = find_premier_league_table(soup)
    if not table:
        return None
    weeks = set()
    for row in table.find_all("tr"):
        wk = row.find("td", {"data-stat": "week"})
        score = row.find("td", {"data-stat": "score"})
        if wk and score and score.text.strip():
            try:
                weeks.add(int(wk.text.strip()))
            except ValueError:
                continue
    latest = max(weeks) if weeks else None
    print(f"Latest completed week detected: {latest}")
    return latest

def get_links_by_week_from_soup(soup, target_week):
    table = find_premier_league_table(soup)
    if not table:
        return []
    links = []
    for row in table.find_all("tr"):
        wk_cell = row.find("td", {"data-stat": "week"})
        if not wk_cell:
            continue
        try:
            week = int(wk_cell.text.strip())
        except ValueError:
            continue
        if week == target_week:
            match_link = row.find("a", string="Match Report")
            if match_link and match_link.get("href"):
                links.append("https://fbref.com" + match_link["href"])
    print(f"Found {len(links)} match reports for Week {target_week}")
    return links

# ----------------- run scoring + combine (unchanged) -----------------
def run_match(link, idx):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"match_{idx}.csv")
    print(f"→ Running scoring.py for {link}")
    subprocess.run(
        ["python3", "scoring.py", link, output_file],
        cwd=HFW_REPO,
        check=True
    )
    # scoring.py writes output inside HFW_REPO/weekly_scores — return that path
    return os.path.join(HFW_REPO, output_file)

def combine_results(csv_paths, out_name):
    dfs = [pd.read_csv(p) for p in csv_paths if os.path.exists(p)]
    if not dfs:
        print("⚠️ No CSVs found to combine.")
        return None
    combined = pd.concat(dfs, ignore_index=True)
    combined.to_csv(out_name, index=False)
    return out_name

# ----------------- main workflow -----------------
if __name__ == "__main__":
    # Fetch schedule HTML via AbstractAPI (cached)
    html = fetch_html_via_abstractapi(FBREF_URL, use_cache_seconds=60*60)  # 1 hour cache
    if not html:
        print("⚠️ No HTML returned; exiting.")
        raise SystemExit(0)

    soup = BeautifulSoup(html, "html.parser")
    target_week = get_latest_completed_week_from_soup(soup)
    if not target_week:
        print("⚠️ No completed matchweek detected. Exiting.")
        raise SystemExit(0)

    match_links = get_links_by_week_from_soup(soup, target_week)
    if not match_links:
        print(f"⚠️ No Premier League matches found for Week {target_week}. Exiting.")
        raise SystemExit(0)

    csvs = []
    for i, link in enumerate(match_links):
        print(f"Processing match {i+1}/{len(match_links)}: {link}")
        csv_path = run_match(link, i)
        csvs.append(csv_path)

    out_csv = os.path.join(os.getcwd(), f"Matchweek_{target_week}.csv")
    print(f"Saving combined file to {out_csv}")
    combined = combine_results(csvs, out_csv)

    if combined:
        print(f"✅ All done. Combined results → {out_csv}")
    else:
        print("⚠️ No combined file created.")
