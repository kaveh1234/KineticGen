# src/features/physics_corrections.py
"""
Compute aerodynamic & environmental corrections for 100m results.
Outputs neutral (wind, altitude, density) performance estimates.
"""

import pandas as pd
import numpy as np
from pathlib import Path

def wind_altitude_correction(v_perf, wind, altitude_m, rho_air):
    """
    Empirical model combining wind, altitude, and air density corrections.
    Based on Mureika (2001) and simplified drag-energy scaling.
    """
    rho0 = 1.225  # sea-level standard air density (kg/m³)
    rho_ratio = rho_air / rho0

    # Altitude factor (lower air density → faster)
    alt_corr = 0.000125 * altitude_m  # ≈0.0125 s per 100 m per 1000 m altitude

    # Wind factor (tailwind reduces time)
    wind_corr = -0.045 * wind  # 0.045 s per m/s (approx)

    # Air density scaling: faster if thinner air
    density_corr = -0.5 * (1 - rho_ratio) * 0.05  # mild density adjustment

    # Total correction (positive → slower, negative → faster)
    total_corr = alt_corr + wind_corr + density_corr
    t0 = v_perf + total_corr
    return t0, alt_corr, wind_corr, density_corr

def main():
    df = pd.read_parquet("data/processed/results_weather.parquet")

    # ensure numeric
    df["perf"] = pd.to_numeric(df["perf"], errors="coerce")
    df["wind"] = pd.to_numeric(df["wind"], errors="coerce").fillna(0)
    df["rho_air"] = pd.to_numeric(df["rho_air"], errors="coerce").fillna(1.225)
    df["altitude_m"] = np.nan_to_num(df.get("altitude_m", np.zeros(len(df))), nan=0.0)

    rows = []
    for _, r in df.iterrows():
        t0, alt_c, wind_c, rho_c = wind_altitude_correction(
            r["perf"], r["wind"], r["altitude_m"], r["rho_air"]
        )
        rows.append({
            **r.to_dict(),
            "t_neutral": round(t0, 3),
            "corr_alt": round(alt_c, 4),
            "corr_wind": round(wind_c, 4),
            "corr_rho": round(rho_c, 4)
        })

    out = pd.DataFrame(rows)
    Path("data/processed").mkdir(exist_ok=True)
    out.to_parquet("data/processed/results_physics.parquet", index=False)
    print("✅ Physics corrections computed → data/processed/results_physics.parquet")

if __name__ == "__main__":
    main()

