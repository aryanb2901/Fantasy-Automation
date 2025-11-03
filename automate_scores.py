import time
import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import subprocess
import os

# --- CONFIGURATION ---
FBREF_URL = "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures"
HFW_REPO = "HFW-App"      # folder name after cloning your fork
OUTPUT_DIR = "weekly_scores"

# --- FETCH (single fetch per run, with retry) ---
def fetch_html(url, attempts=3, delay=2.0):
    """Fetch page content using Cloudscraper (chrome/darwin), with headers and simple retry."""
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False}
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }

    for i in range(1, attempts + 1):
        print(f"Fetching schedule page from {url} (attempt {i}/{attempts})")
        resp = scraper.get(url, headers=headers)
        print(f"Status Code: {resp.status_code}")
        if resp.status_code == 200 and resp.text and "Premier League" in resp.text:
            return resp.text
        time.sleep(delay)

    print(f"⚠️ Failed to load {url} after {attempts} attempts.")
    return None

# --- HTML PARSING HELPERS ---
def find_schedule_table(soup: BeautifulSoup):
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

def get_latest_completed_week_from_soup(soup: BeautifulSoup):
    """Parse the latest completed matchweek from an already-fetched soup."""
    table = find_schedule_table(soup)
    if not table:
        return None

    weeks = set()
    for row in table.find_all("tr"):
        wk = row.find("td", {"data-stat": "week"})
        score = row.find("td", {"data-stat": "score"})
        # Only count rows where a result exists
        if wk and score and score.text.strip():
            try:
                weeks.add(int(wk.text.strip()))
            except ValueError:
                continue

    latest = max(weeks) if weeks else None
    print(f"Latest completed week detected: {latest}")
    return latest

def get_links_by_week_from_soup(soup: BeautifulSoup, target_week: int):
    """Collect Match Report links for the given week from an already-fetched soup."""
    table = find_schedule_table(soup)
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

# --- scoring.py runner + combiner ---
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
    # Fetch once, reuse everywhere (prevents double-hit 403s)
    html = fetch_html(FBREF_URL)
    if not html:
        print("⚠️ No HTML returned; exiting.")
        raise SystemExit(0)

    # Save for troubleshooting (optional)
    try:
        with open("debug_fbref.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("✅ Saved fetched HTML to debug_fbref.html")
    except Exception as e:
        print(f"⚠️ Could not save debug HTML: {e}")

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
