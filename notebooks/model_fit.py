# notebooks/model_fit.py
"""
Fit multiple regression models to quantify physical sensitivities.
"""

import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt

df = pd.read_parquet("data/processed/results_physics.parquet")

# ensure numeric
for c in ["perf", "wind", "altitude_m", "rho_air"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# --- simple linear model ---
X = df[["wind", "rho_air", "altitude_m"]]
X = sm.add_constant(X)
y = df["perf"]
model = sm.OLS(y, X, missing="drop").fit()
print(model.summary())

# --- visualize residuals ---
plt.scatter(model.fittedvalues, model.resid)
plt.axhline(0, color="k", linestyle="--")
plt.xlabel("Fitted times")
plt.ylabel("Residuals (s)")
plt.title("Model residuals vs fitted values")
plt.grid(True)
plt.show()

