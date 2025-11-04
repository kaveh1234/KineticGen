# notebooks/export_model_summary.py
"""
Exports key regression statistics and coefficients to a Parquet file.
"""

import pandas as pd
import statsmodels.api as sm

df = pd.read_parquet("data/processed/results_physics_refined.parquet")
df["delta"] = df["t_neutral"] - df["perf"]
df = df.dropna(subset=["wind", "rho_air_abs", "altitude_m", "delta"])

X = df[["wind", "rho_air_abs", "altitude_m"]]
X = sm.add_constant(X)
y = df["delta"]

model = sm.OLS(y, X).fit()

summary_df = pd.DataFrame({
    "Variable": model.params.index,
    "Coefficient": model.params.values,
    "P_value": model.pvalues.values,
    "Std_Error": model.bse.values
})
summary_df["R_squared"] = model.rsquared
summary_df.to_parquet("data/processed/model_summary.parquet")

print("✅ Exported model summary → data/processed/model_summary.parquet")
print(summary_df)

