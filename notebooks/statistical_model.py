# notebooks/statistical_model.py
"""
Statistical modeling of environmental effects on sprint performance.
Cleans data and fits multiple linear regression to estimate contributions
from wind, air density, and altitude to ΔTime (neutral - raw).
"""

import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
import seaborn as sns

# Load data
df = pd.read_parquet("data/processed/results_physics_refined.parquet")
df["delta"] = df["t_neutral"] - df["perf"]

# --- Clean missing or infinite values ---
df = df.replace([float("inf"), float("-inf")], pd.NA)
df = df.dropna(subset=["wind", "rho_air_abs", "altitude_m", "delta"])

print(f"✅ Data cleaned: {len(df)} rows remaining")

# Prepare variables
X = df[["wind", "rho_air_abs", "altitude_m"]]
X = sm.add_constant(X)
y = df["delta"]

# Fit model
model = sm.OLS(y, X).fit()
print(model.summary())

# --- Visualization: feature correlations ---
sns.pairplot(df[["wind", "rho_air_abs", "altitude_m", "delta"]], diag_kind="kde")
plt.suptitle("Relationships Between Environmental Variables and ΔTime", y=1.02)
plt.show()

# --- Coefficients bar chart ---
coef_df = pd.DataFrame({
    "Variable": model.params.index,
    "Coefficient": model.params.values
}).set_index("Variable")

coef_df.drop("const", inplace=True)
coef_df.plot(kind="bar", legend=False)
plt.ylabel("ΔTime contribution (s per unit)")
plt.title("Estimated Effect of Each Environmental Factor")
plt.grid(True)
plt.show()

