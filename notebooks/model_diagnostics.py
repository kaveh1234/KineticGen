# notebooks/model_diagnostics.py
"""
Model diagnostics: check multicollinearity and residual patterns.
"""

import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
import matplotlib.pyplot as plt
import seaborn as sns

# Load data
df = pd.read_parquet("data/processed/results_physics_refined.parquet")
df["delta"] = df["t_neutral"] - df["perf"]
df = df.dropna(subset=["wind", "rho_air_abs", "altitude_m", "delta"])

# Prepare variables
X = df[["wind", "rho_air_abs", "altitude_m"]]
X = sm.add_constant(X)
y = df["delta"]

# Fit model
model = sm.OLS(y, X).fit()
print(model.summary())

# --- Variance Inflation Factor (VIF) ---
vif_data = pd.DataFrame()
vif_data["feature"] = X.columns
vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]

print("\n🔍 Variance Inflation Factors:")
print(vif_data)

# --- Residual plot ---
sns.residplot(x=model.fittedvalues, y=model.resid, lowess=True, color="blue")
plt.xlabel("Fitted Values")
plt.ylabel("Residuals")
plt.title("Residuals vs Fitted Values")
plt.grid(True)
plt.show()

# --- Normality check ---
sm.qqplot(model.resid, line='s')
plt.title("Normal Q-Q Plot of Residuals")
plt.show()

