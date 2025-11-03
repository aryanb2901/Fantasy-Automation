import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import subprocess
import os

# --- CONFIGURATION ---
FBREF_URL = "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures"
HFW_REPO = "HFW-App"      # folder name after cloning your fork
OUTPUT_DIR = "weekly_scores"

# --- FETCH FUNCTION ---
def fetch_html(url):
    """Fetch page content using Cloudscraper to bypass Cloudflare."""
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False}
    )
    print(f"Fetching schedule page from {url}")
    resp = scraper.get(url)
    print(f"Status Code: {resp.status_code}")
    if resp.status_code != 200:
        print(f"⚠️ Failed to load {url}, status {resp.status_code}")
        return None
    return resp.text


# --- CORE FUNCTIONS ---
def get_latest_completed_week():
    """Return the most recent completed Premier League matchweek number."""
    html = fetch_html(FBREF_URL)
    if not html:
        return None

    # Save fetched HTML for debugging (optional)
    with open("debug_fbref.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("✅ Saved fetched HTML to debug_fbref.html")

    soup = BeautifulSoup(html, "html.parser")

    # ✅ Find the div that contains the Premier League schedule table dynamically
    div_container = soup.find("div", id=lambda x: x and x.startswith("div_sched_") and "_9_" in x)
    if not div_container:
        print("⚠️ Could not find Premier League schedule div.")
        return None

    table = div_container.find("table")
    if not table:
        print("⚠️ Could not find schedule table inside the div.")
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
    html = fetch_html(FBREF_URL)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    # ✅ Find the div that contains the Premier League schedule table dynamically
    div_container = soup.find("div", id=lambda x: x and x.startswith("div_sched_") and "_9_" in x)
    if not div_container:
        print("⚠️ Could not find Premier League schedule div.")
        return []

    table = div_container.find("table")
    if not table:
        print("⚠️ Could not find schedule table inside the div.")
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
