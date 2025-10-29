# src/features/add_altitude_density.py
"""
Rebuilds weather data from scratch, adds true altitude (m) and computes
absolute air density (kg/m³) using the ideal gas law.
"""

import pandas as pd
import requests
from pathlib import Path
import math
import time

def get_altitude(lat, lon):
    try:
        url = "https://api.open-meteo.com/v1/elevation"
        r = requests.get(url, params={"latitude": lat, "longitude": lon}, timeout=10)
        js = r.json()
        return js.get("elevation", [None])[0]
    except Exception:
        return None

def air_density(temp_c, pressure_hpa, rh_pct):
    """Compute absolute air density (kg/m³)."""
    T = temp_c + 273.15  # K
    P = pressure_hpa * 100  # Pa
    e = 6.112 * math.exp((17.67 * temp_c) / (temp_c + 243.5)) * (rh_pct / 100.0) * 100
    pdry = P - e
    Rd, Rv = 287.058, 461.495
    rho = (pdry / (Rd * T)) + (e / (Rv * T))
    return rho

print("🔍 Loading cleaned results...")
df = pd.read_parquet("data/processed/results_clean.parquet")

# Dummy values for weather if missing
df["temp_c"] = 25.0
df["pressure_hpa"] = 1013.25
df["rh_pct"] = 50.0

# Lookup coords manually (quick static fallback for small dataset)
venues = {
    "Olympiastadion, Berlin (GER)": (52.5145, 13.2395),
    "Shanghai (CHN)": (31.2304, 121.4737),
    "Stade Olympique de la Pontaise, Lausanne (SUI)": (46.5197, 6.6336),
    "Suhaim bin Hamad Stadium, Doha (QAT)": (25.2854, 51.5310),
}

records = []
for i, row in df.iterrows():
    venue = row["venue"]
    if venue not in venues:
        continue

    lat, lon = venues[venue]
    alt = get_altitude(lat, lon)
    rho = air_density(row["temp_c"], row["pressure_hpa"], row["rh_pct"])

    record = row.to_dict()
    record["lat"] = lat
    record["lon"] = lon
    record["altitude_m"] = alt
    record["rho_air_abs"] = rho
    records.append(record)

    if i % 2 == 0:
        print(f"✅ {venue} done (alt={alt:.0f} m, rho={rho:.2f} kg/m³)")
    time.sleep(0.5)

out = pd.DataFrame(records)
out_path = Path("data/processed/results_altitude_density.parquet")
out.to_parquet(out_path, index=False)
print(f"✅ Saved absolute density file → {out_path.resolve()}")

