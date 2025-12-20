import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from statsmodels.stats.outliers_influence import variance_inflation_factor


def detect_feature_risks(df: pd.DataFrame, target_col: str | None = None):
    results = {}

    numeric_df = df.select_dtypes(include=[np.number]).copy()

    # ---------- VIF (Multicollinearity) ----------
    vif_scores = {}
    if numeric_df.shape[1] >= 2:
        vif_df = numeric_df.dropna()
        if vif_df.shape[0] > 0:
            for i, col in enumerate(vif_df.columns):
                try:
                    vif_scores[col] = variance_inflation_factor(
                        vif_df.values, i
                    )
                except Exception:
                    vif_scores[col] = np.nan

    # ---------- Correlation with target ----------
    target_corr = {}
    if target_col and target_col in df.columns:
        target_series = df[target_col]

        for col in numeric_df.columns:
            if col == target_col:
                continue
            corr = df[[col, target_col]].dropna().corr().iloc[0, 1]
            target_corr[col] = corr

    # ---------- Feature-level analysis ----------
    for col in df.columns:
        flags = []
        reason = []
        action = []

        # ID-like detection
        if df[col].nunique() / max(len(df), 1) > 0.95:
            flags.append("Leakage-Prone")
            reason.append("High cardinality (ID-like)")
            action.append("Drop")

        # Correlation leakage
        if col in target_corr and abs(target_corr[col]) > 0.9:
            flags.append("Leakage-Prone")
            reason.append("Highly correlated with target")
            action.append("Drop")

        # Multicollinearity
        if col in vif_scores and vif_scores[col] > 10:
            flags.append("High Risk")
            reason.append("High multicollinearity (VIF)")
            action.append("Drop or Transform")

        if not flags:
            flags.append("Safe")
            reason.append("No significant risk detected")
            action.append("Retain")

        results[col] = {
            "risk_label": flags,
            "reason": reason,
            "suggested_action": action
        }

    return results
