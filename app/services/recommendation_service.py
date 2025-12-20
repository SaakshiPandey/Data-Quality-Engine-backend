import numpy as np
import pandas as pd


def generate_recommendations(
    df: pd.DataFrame,
    feature_diagnostics: list,
    risk_analysis: dict,
    target_col: str | None = None
):
    recommendations = []

    # ---------- Feature-level recommendations ----------
    for feature_info in feature_diagnostics:
        feature = feature_info["feature"]
        missing_pct = feature_info["missing_percentage"]
        dtype = feature_info["dtype"]

        # Missing values
        if missing_pct > 0:
            if "float" in dtype or "int" in dtype:
                recommendations.append({
                    "type": "Preprocessing",
                    "scope": "Feature",
                    "target": feature,
                    "issue": "Missing Values",
                    "recommended_action": "Median Imputation",
                    "reason": f"{missing_pct}% missing values in numeric feature",
                    "impact": "High" if missing_pct > 20 else "Medium"
                })
            else:
                recommendations.append({
                    "type": "Preprocessing",
                    "scope": "Feature",
                    "target": feature,
                    "issue": "Missing Values",
                    "recommended_action": "Mode Imputation",
                    "reason": f"{missing_pct}% missing values in categorical feature",
                    "impact": "Medium"
                })

        # Risk & leakage based
        risk_info = risk_analysis.get(feature)
        if risk_info:
            if "Leakage-Prone" in risk_info["risk_label"]:
                recommendations.append({
                    "type": "Risk Mitigation",
                    "scope": "Feature",
                    "target": feature,
                    "issue": "Target Leakage",
                    "recommended_action": "Drop Feature",
                    "reason": ", ".join(risk_info["reason"]),
                    "impact": "High"
                })

            if "High Risk" in risk_info["risk_label"]:
                recommendations.append({
                    "type": "Risk Mitigation",
                    "scope": "Feature",
                    "target": feature,
                    "issue": "Multicollinearity",
                    "recommended_action": "Drop or Transform",
                    "reason": ", ".join(risk_info["reason"]),
                    "impact": "Medium"
                })

    # ---------- Dataset-level recommendations ----------
    if target_col and target_col in df.columns:
        target_counts = df[target_col].value_counts(normalize=True)
        if len(target_counts) == 2 and target_counts.min() < 0.2:
            recommendations.append({
                "type": "Preprocessing",
                "scope": "Dataset",
                "target": target_col,
                "issue": "Class Imbalance",
                "recommended_action": "Apply SMOTE",
                "reason": f"Minority class ratio is {round(target_counts.min(), 2)}",
                "impact": "High"
            })

    return recommendations
