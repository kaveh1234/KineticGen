# src/features/fetch_real_weather.py
"""
Fetch REAL daily weather (T, P, RH) from Open-Meteo ERA5,
compute true air density (kg/m³), and save.
"""

import pandas as pd
import requests
from pathlib import Path
import math
import time

ERA5 = "https://archive-api.open-meteo.com/v1/era5"
ELEV = "https://api.open-meteo.com/v1/elevation"

def to_iso_date(s):
    try:
        return pd.to_datetime(s, dayfirst=True).date().isoformat()
    except Exception:
        return None

def get_weather(lat, lon, iso_date):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": iso_date,
        "end_date": iso_date,
        "daily": [
            "temperature_2m_mean",
            "surface_pressure_mean",
            "relative_humidity_2m_mean",
        ],
        "timezone": "UTC",
    }
    r = requests.get(ERA5, params=params, timeout=15)
    if r.status_code != 200:
        return None
    js = r.json().get("daily", {})
    if not js:
        return None
    t_c = js["temperature_2m_mean"][0]
    # The API already gives pressure in hPa — multiply by 100 to convert to Pa
    p_pa = js["surface_pressure_mean"][0] * 100.0
    rh = js["relative_humidity_2m_mean"][0]
    return {"temp_c": t_c, "pressure_pa": p_pa, "rh_pct": rh}

def air_density(temp_c, pressure_pa, rh_pct):
    """Compute absolute air density (kg/m³)."""
    T = temp_c + 273.15
    P = pressure_pa
    es = 6.1078 * 10 ** ((7.5 * temp_c) / (237.3 + temp_c)) * 100.0  # Pa
    e = (rh_pct / 100.0) * es
    pdry = P - e
    Rd, Rv = 287.05, 461.495
    return (pdry / (Rd * T)) + (e / (Rv * T))

def get_altitude(lat, lon):
    r = requests.get(ELEV, params={"latitude": lat, "longitude": lon}, timeout=10)
    if r.status_code != 200:
        return None
    js = r.json()
    return js.get("elevation", [None])[0]

print("🔍 Loading altitude file...")
df = pd.read_parquet("data/processed/results_altitude_density.parquet")

records = []
for i, row in df.iterrows():
    lat, lon = float(row["lat"]), float(row["lon"])
    iso = to_iso_date(row["date"])
    if not iso:
        continue

    wx = get_weather(lat, lon, iso)
    if not wx:
        print(f"⚠️  No weather for {row['venue']} on {iso}")
        continue

    rho = air_density(wx["temp_c"], wx["pressure_pa"], wx["rh_pct"])
    alt = row.get("altitude_m")
    if pd.isna(alt):
        alt = get_altitude(lat, lon)

    out = row.to_dict()
    out["temp_c"] = wx["temp_c"]
    out["pressure_hpa"] = wx["pressure_pa"] / 100.0  # store readable hPa
    out["rh_pct"] = wx["rh_pct"]
    out["rho_air_abs"] = rho
    out["altitude_m"] = alt
    records.append(out)

    if (i % 3) == 0:
        print(f"✅ {i+1}/{len(df)} | {row['venue']} | rho={rho:.3f}")
    time.sleep(0.4)

out = pd.DataFrame(records)
out_path = Path("data/processed/results_weather_real.parquet")
out.to_parquet(out_path, index=False)
print(f"✅ Saved → {out_path.resolve()} (rows={len(out)})")

