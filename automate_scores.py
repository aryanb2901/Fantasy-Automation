import requests
from bs4 import BeautifulSoup
import pandas as pd
import subprocess
import os

# --- CONFIGURATION ---
FBREF_URL = "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures"
HFW_REPO = "HFW-App"      # folder name after cloning your fork
OUTPUT_DIR = "weekly_scores"

# --- CORE FUNCTIONS ---

def get_latest_completed_week():
    """Return the most recent completed Premier League matchweek number."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0.0.0 Safari/537.36"
        )
    }

    print(f"Fetching schedule page from {FBREF_URL}")
    resp = requests.get(FBREF_URL, headers=headers)
    if resp.status_code != 200:
        print(f"⚠️ Failed to load schedule page, status {resp.status_code}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "sched_ks_9_2025"}) or soup.find("table", {"id": "sched_all"})
    if not table:
        print("⚠️ Could not find schedule table.")
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


def get_premier_league_links_by_week(target_week):
    """Return all Match Report links for a specific Premier League week."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0.0.0 Safari/537.36"
        )
    }

    resp = requests.get(FBREF_URL, headers=headers)
    if resp.status_code != 200:
        print(f"⚠️ Failed to load schedule page, status {resp.status_code}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "sched_ks_9_2025"}) or soup.find("table", {"id": "sched_all"})
    if not table:
        print("⚠️ Could not find schedule table.")
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
            if match_link:
                href = match_link["href"]
                links.append("https://fbref.com" + href)

    print(f"Found {len(links)} match reports for Week {target_week}")
    return links


def run_match(link, idx):
    """Run scoring.py for a single match."""
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
    """Combine all match CSVs into a single file."""
    dfs = [pd.read_csv(p) for p in csv_paths if os.path.exists(p)]
    if not dfs:
        print("⚠️ No CSVs found to combine.")
        return None
    combined = pd.concat(dfs, ignore_index=True)
    combined.to_csv(out_name, index=False)
    return out_name


# --- MAIN WORKFLOW ---
if __name__ == "__main__":
    target_week = get_latest_completed_week()
    if not target_week:
        print("⚠️ No completed matchweek detected. Exiting.")
        raise SystemExit(0)

    match_links = get_premier_league_links_by_week(target_week)
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
