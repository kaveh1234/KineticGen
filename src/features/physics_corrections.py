# src/features/physics_corrections.py
"""
Compute physics-corrected (neutral) 100 m times using wind, altitude, and air density.
"""

import pandas as pd
import numpy as np
from pathlib import Path

print("🔍 Loading altitude + density data...")
df = pd.read_parquet("data/processed/results_altitude_density.parquet")

# --- Constants ---
RHO_REF = 1.225   # sea-level air density (kg/m³)
ALT_SCALE = 0.00012  # time improvement per meter altitude (s/m)
WIND_COEFF = 0.045   # time improvement per m/s tailwind
RHO_COEFF = 0.25     # correction factor for density deviation

# --- Clean data ---
for c in ["perf", "wind", "altitude_m", "rho_air_abs"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

df = df.dropna(subset=["perf", "wind", "altitude_m", "rho_air_abs"]).copy()

# --- Physics model ---
def neutral_time(row):
    base = row["perf"]
    wind_corr = WIND_COEFF * row["wind"]
    alt_corr = ALT_SCALE * row["altitude_m"]
    rho_corr = RHO_COEFF * (RHO_REF - row["rho_air_abs"])
    return base + wind_corr - alt_corr + rho_corr

df["t_neutral"] = df.apply(neutral_time, axis=1)

# --- Sanity check ---
print(df[["venue", "perf", "wind", "altitude_m", "rho_air_abs", "t_neutral"]].head())

# --- Save ---
out_path = Path("data/processed/results_physics_refined.parquet")
df.to_parquet(out_path, index=False)
print(f"✅ Saved refined physics-corrected results → {out_path.resolve()}")

