# notebooks/analysis.py
"""
Basic exploratory analysis of corrected 100 m data.
Plots distributions and relationships between perf, wind, rho_air, altitude, t_neutral.
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

df = pd.read_parquet("data/processed/results_physics.parquet")

# --- sanity ---
print(df.describe()[["perf","wind","rho_air","t_neutral"]])

# --- scatterplots ---
plt.scatter(df["wind"], df["perf"] - df["t_neutral"], alpha=0.7)
plt.xlabel("Wind (m/s)")
plt.ylabel("Raw – Neutral time (s)")
plt.title("Wind effect on performance")
plt.grid(True)
plt.show()

plt.scatter(df["rho_air"], df["t_neutral"], alpha=0.7)
plt.xlabel("Air density (kg/m³)")
plt.ylabel("Neutral time (s)")
plt.title("Air-density vs corrected performance")
plt.grid(True)
plt.show()

# --- simple regression ---
import statsmodels.api as sm
X = df[["wind","rho_air"]].copy()
X["const"] = 1
y = df["perf"]
model = sm.OLS(y, X, missing='drop').fit()
print(model.summary())

