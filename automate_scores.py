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
    offset = (today.weekday() + 2) % 7
    return today - timedelta(days=offset)

def get_premier_league_links(fbref_date):
    url = f"{FBREF_BASE}/{fbref_date.strftime('%Y-%m-%d')}"
    resp = requests.get(url)
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
    match_links = get_premier_league_links(saturday)

    print(f"Found {len(match_links)} Premier League matches")

    csvs = []
    for i, link in enumerate(match_links):
        print(f"Processing match {i+1}/{len(match_links)}: {link}")
        csv_path = run_match(link, i)
        csvs.append(csv_path)

    out_csv = f"Matchweek_{saturday.strftime('%Y-%m-%d')}.csv"
    combine_results(csvs, out_csv)

    print(f"✅ All done. Combined results → {out_csv}")
