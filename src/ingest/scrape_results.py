# src/ingest/scrape_results.py
"""
Fetches 100 m results from World Athletics,
cleans the table, and saves it as Parquet.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path

URL = "https://worldathletics.org/records/all-time-toplists/sprints/100-metres/outdoor/men/senior"

print("Fetching data...")

# Fetch the page
response = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"})
response.raise_for_status()

soup = BeautifulSoup(response.text, "lxml")

# Extract all table rows
rows = soup.select("table tbody tr")
data = []
for row in rows:
    cols = [c.get_text(strip=True) for c in row.select("td")]
    if cols:
        data.append(cols)

# Convert to DataFrame
df = pd.DataFrame(data)

# Assign column names safely
possible_cols = [
    "rank", "perf", "wind", "competitor", "dob",
    "nat", "pos", "venue", "date", "resultscore", "competition"
]
if len(df.columns) <= len(possible_cols):
    df.columns = possible_cols[:len(df.columns)]

print(f"{len(df)} rows fetched.")

# Save the data
Path("data/processed").mkdir(parents=True, exist_ok=True)
out_path = Path("data/processed/results.parquet")
df.to_parquet(out_path, index=False)

print(f"✅ Saved to {out_path.resolve()}")

