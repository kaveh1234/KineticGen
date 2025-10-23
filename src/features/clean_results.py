# src/features/clean_results.py
"""
Cleans and normalizes the raw 100 m results dataset.
"""

import pandas as pd
from pathlib import Path

input_path = Path("data/processed/results.parquet")
output_path = Path("data/processed/results_clean.parquet")

print("🔍 Loading raw results...")
df = pd.read_parquet(input_path)

# Fix misaligned columns from scraping (venue/date/resultscore swapped)
if "venue" in df.columns and df["venue"].astype(str).str.strip().eq("").any():
    print("🩺 Fixing column alignment...")
    df["venue"] = df["date"]            # real venues were stored under 'date'
    df["date"] = df["resultscore"]      # real dates under 'resultscore'
    df["resultscore"] = df["competition"]  # shift resultscore
    df["competition"] = None            # clear unused


# Drop rows with missing performance
df = df[df["perf"].notna() & (df["perf"] != "")]

# Convert performance to seconds (float)
def parse_perf(p):
    try:
        return float(p)
    except ValueError:
        return None

df["perf"] = df["perf"].apply(parse_perf)

# Clean wind column (e.g., "+0.9" → 0.9)
def parse_wind(w):
    if not isinstance(w, str) or w.strip() == "":
        return None
    try:
        return float(w.replace("+", ""))
    except ValueError:
        return None

if "wind" in df.columns:
    df["wind"] = df["wind"].apply(parse_wind)

# Drop rows missing perf or wind
df = df.dropna(subset=["perf"])
df = df.reset_index(drop=True)

print(f"✅ Cleaned dataset: {len(df)} rows remaining.")
df.to_parquet(output_path, index=False)
print(f"💾 Saved to {output_path.resolve()}")

