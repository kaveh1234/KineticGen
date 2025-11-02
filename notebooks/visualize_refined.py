# notebooks/visualize_refined.py
"""
Visualize refined physics corrections:
- Raw vs Neutralized times
- Wind vs ΔTime
- Air density vs ΔTime
- Altitude vs ΔTime
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_parquet("data/processed/results_physics_refined.parquet")
df["delta"] = df["t_neutral"] - df["perf"]

# --- 1️⃣ Raw vs Neutral times ---
plt.figure(figsize=(10,6))
plt.scatter(df["perf"], df["t_neutral"], color="steelblue", alpha=0.8)
plt.plot([df["perf"].min(), df["perf"].max()],
         [df["perf"].min(), df["perf"].max()],
         "r--", label="y = x")
plt.xlabel("Raw performance (s)")
plt.ylabel("Neutral performance (s)")
plt.title("Raw vs Physics-Corrected 100m Times")
plt.legend()
plt.grid(True)
plt.show()

# --- 2️⃣ Wind vs ΔTime ---
plt.figure(figsize=(8,5))
plt.scatter(df["wind"], df["delta"], color="orange")
m, b = np.polyfit(df["wind"], df["delta"], 1)
plt.plot(df["wind"], m*df["wind"] + b, "r--", label=f"Slope = {m:.3f} s/(m/s)")
plt.xlabel("Wind (m/s)")
plt.ylabel("ΔTime (Neutral - Raw) [s]")
plt.title("Effect of Wind on 100m Performance")
plt.legend()
plt.grid(True)
plt.show()

# --- 3️⃣ Air density vs ΔTime ---
plt.figure(figsize=(8,5))
plt.scatter(df["rho_air_abs"], df["delta"], color="green")
plt.xlabel("Air density (kg/m³)")
plt.ylabel("ΔTime (Neutral - Raw) [s]")
plt.title("Effect of Air Density on Performance")
plt.grid(True)
plt.show()

# --- 4️⃣ Altitude vs ΔTime ---
plt.figure(figsize=(8,5))
plt.scatter(df["altitude_m"], df["delta"], color="purple")
plt.xlabel("Altitude (m)")
plt.ylabel("ΔTime (Neutral - Raw) [s]")
plt.title("Effect of Altitude on Performance")
plt.grid(True)
plt.show()

