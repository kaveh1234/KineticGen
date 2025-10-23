# src/features/add_weather_altitude.py
"""
Add lat/lon + weather (T, P, RH) + air density to results.
Fixes:
- Parse '16 AUG 2009' -> '2009-08-16'
- Geocode using cleaned city name (fallbacks).
"""

import pandas as pd
import requests
from pathlib import Path
import time
import re

# ---------- helpers ----------

def to_iso_date(s):
    # '16 AUG 2009' -> '2009-08-16'
    try:
        return pd.to_datetime(s, dayfirst=True).date().isoformat()
    except Exception:
        return None

def clean_place(venue):
    """Extract a city-like query from venue strings such as:
       'Stade ... , Lausanne (SUI)' -> 'Lausanne'
       'Olympiastadion, Berlin (GER)' -> 'Berlin'
       'Shanghai (CHN)' -> 'Shanghai'
    """
    if not isinstance(venue, str) or not venue.strip():
        return None

    # drop country code part in parentheses
    base = venue.split("(")[0].strip()
    # try last comma piece (often the city)
    parts = [p.strip() for p in base.split(",") if p.strip()]
    if parts:
        city = parts[-1]
    else:
        city = base

    # remove common stadium words
    city = re.sub(r"\b(Stadium|Stade|Arena|Field|Centre|Center|International|Olympic|Olympiastadion)\b", "", city, flags=re.I).strip()

    return city if city else None

def geocode_place(query):
    url = "https://geocoding-api.open-meteo.com/v1/search"
    r = requests.get(url, params={"name": query, "count": 1}, timeout=10)
    if r.status_code == 200:
        js = r.json()
        if "results" in js and js["results"]:
            res = js["results"][0]
            return res["latitude"], res["longitude"]
    return None, None

def air_density(temp_c, pressure_hpa, rh_pct):
    T = temp_c + 273.15
    p = pressure_hpa * 100
    es = 6.1078 * 10 ** ((7.5 * (temp_c)) / (237.3 + (temp_c))) * 100  # Pa
    e = (rh_pct / 100.0) * es
    pdry = p - e
    Rd, Rv = 287.05, 461.495
    return (pdry / (Rd * T)) + (e / (Rv * T))

def get_weather(lat, lon, iso_date):
    base = "https://archive-api.open-meteo.com/v1/era5"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": iso_date,
        "end_date": iso_date,
        "daily": ["temperature_2m_max", "surface_pressure_mean", "relative_humidity_2m_max"],
        "timezone": "UTC",
    }
    r = requests.get(base, params=params, timeout=10)
    if r.status_code != 200:
        return None
    js = r.json().get("daily", {})
    if not js:
        return None
    return {
        "temp_c": js["temperature_2m_max"][0],
        "pressure_hpa": js["surface_pressure_mean"][0] / 100.0,
        "rh_pct": js["relative_humidity_2m_max"][0],
    }

# ---------- main ----------

print("🔍 Loading cleaned results...")
df = pd.read_parquet("data/processed/results_clean.parquet")

records = []
for _, row in df.head(10).iterrows():  # sample 10 for testing
    raw_place = row.get("venue", "")
    place = clean_place(raw_place)
    iso_date = to_iso_date(row.get("date", ""))

    if not place or not iso_date:
        print(f"⚠️  Skipping (place/date missing) → venue='{raw_place}', date='{row.get('date','')}'")
        continue

    print(f"📍 {raw_place}  →  geocoding '{place}'  |  date {iso_date}")
    lat, lon = geocode_place(place)
    if lat is None:
        print(f"   ⚠️  Could not geocode '{place}'")
        continue

    wx = get_weather(lat, lon, iso_date)
    if not wx:
        print(f"   ⚠️  No weather for {place} on {iso_date}")
        continue

    rho = air_density(wx["temp_c"], wx["pressure_hpa"], wx["rh_pct"])
    rec = {
        **row.to_dict(),
        "lat": lat,
        "lon": lon,
        "temp_c": wx["temp_c"],
        "pressure_hpa": wx["pressure_hpa"],
        "rh_pct": wx["rh_pct"],
        "rho_air": rho,
    }
    records.append(rec)
    time.sleep(0.4)  # be polite to the API

if records:
    out = pd.DataFrame(records)
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    out_path = Path("data/processed/results_weather.parquet")
    out.to_parquet(out_path, index=False)
    print(f"✅ Saved enriched file → {out_path.resolve()}  ({len(out)} rows)")
else:
    print("⚠️  No data fetched.")

