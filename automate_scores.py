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
    resp = requests.get(url)
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
    saturday = get_last_saturday()
    sunday = saturday + timedelta(days=1)

    # Collect Saturday + Sunday matches
    match_links = get_premier_league_links(saturday)
    match_links += get_premier_league_links(sunday)

    print(f"\nFound {len(match_links)} Premier League matches")

    if not match_links:
        print("⚠️ No Premier League matches found — skipping CSV creation.")
        exit(0)

    csvs = []
    for i, link in enumerate(match_links):
        print(f"Processing match {i+1}/{len(match_links)}: {link}")
        csv_path = run_match(link, i)
        csvs.append(csv_path)

    out_csv = f"Matchweek_{saturday.strftime('%Y-%m-%d')}.csv"
    combine_results(csvs, out_csv)

    print(f"✅ All done. Combined results → {out_csv}")
