import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date, timedelta
import subprocess
import os

FBREF_BASE = "https://fbref.com/en/matches"
HFW_REPO = "HFW-App"  # folder name after cloning your fork
OUTPUT_DIR = "weekly_scores"

def get_last_saturday():
    today = date.today()
    offset = (today.weekday() + 2) % 7  # Saturday = 5
    return today - timedelta(days=offset)

def get_premier_league_links(fbref_date):
    url = f"{FBREF_BASE}/{fbref_date.strftime('%Y-%m-%d')}"
    print(f"Fetching matches from {url}")
    headers = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/118.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"⚠️ Failed to load page for {fbref_date}, status {resp.status_code}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    links = []
    for row in soup.select("table#sched_all tr"):
        comp = row.select_one("td.competition")
        if not comp or "Premier League" not in comp.text:
            continue
        match_link = row.find("a", string="Match Report")
        if match_link:
            links.append("https://fbref.com" + match_link["href"])
    return links

def run_match(link, idx):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"match_{idx}.csv")
    print(f"→ Running scoring.py for {link}")
    subprocess.run(
        ["python3", "scoring.py", link, output_file],
        cwd=HFW_REPO,
        check=True
    )
    return os.path.join(HFW_REPO, output_file)

def combine_results(csv_paths, out_name):
    dfs = [pd.read_csv(p) for p in csv_paths]
    combined = pd.concat(dfs, ignore_index=True)
    combined.to_csv(out_name, index=False)
    return out_name

if __name__ == "__main__":
    today = date.today()
    # get most recent Monday
    
    monday = today - timedelta(days=today.weekday())
    week_dates = [monday - timedelta(days=i) for i in range(1, 8)]  # last 7 days
    match_links = []
    
    for d in week_dates:
        daily_links = get_premier_league_links(d)
        if daily_links:
            print(f"{d}: found {len(daily_links)} matches")
            match_links += daily_links

    print(f"\nTotal Premier League matches found: {len(match_links)}")
    if not match_links:
        print("⚠️ No Premier League matches found — skipping CSV creation.")
        import sys
        sys.exit(0)
        
    csvs = []
    for i, link in enumerate(match_links):
        print(f"Processing match {i+1}/{len(match_links)}: {link}")
        csv_path = run_match(link, i)
        csvs.append(csv_path)
        
    out_csv = os.path.join(os.getcwd(), f"Matchweek_{monday.strftime('%Y-%m-%d')}.csv")
    print(f"Saving combined file to {out_csv}")
    combine_results(csvs, out_csv)

    print(f"✅ All done. Combined results → {out_csv}")
