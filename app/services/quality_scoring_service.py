import os
import pandas as pd
import numpy as np
from app.services.risk_leakage_service import detect_feature_risks
from app.services.recommendation_service import generate_recommendations

DATASET_STORAGE_PATH = "app/storage/datasets"


def compute_quality_score(
    dataset_id: str,
    target_col: str | None = None,
    version: str | None = None
) -> dict:
    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)

    if not os.path.exists(dataset_dir):
        raise FileNotFoundError("Dataset not found")

    if version:
        dataset_path = os.path.join(dataset_dir, version)
    else:
        dataset_path = os.path.join(dataset_dir, "v0_raw.csv")

    if not os.path.exists(dataset_path):
        raise FileNotFoundError("Dataset version not found")

    df = pd.read_csv(dataset_path)
    n_rows, n_cols = df.shape

    risk_analysis = detect_feature_risks(df, target_col)

    # ---------- Missing values ----------
    missing_ratio = df.isnull().sum().sum() / max(df.size, 1)

    # ---------- Duplicate rows ----------
    duplicate_ratio = df.duplicated().sum() / max(n_rows, 1)

    # ---------- Numeric columns ----------
    numeric_df = df.select_dtypes(include=[np.number])

    # ---------- Low variance ----------
    low_variance_cols = [
        col for col in numeric_df.columns
        if numeric_df[col].nunique(dropna=True) <= 1
    ]
    low_variance_ratio = len(low_variance_cols) / max(n_cols, 1)

    # ---------- Skewness (safe) ----------
    skewed_cols = []
    for col in numeric_df.columns:
        series = numeric_df[col].dropna()
        if len(series) < 3:
            continue
        skew_val = series.skew()
        if pd.notna(skew_val) and abs(skew_val) > 1:
            skewed_cols.append(col)

    skewness_ratio = len(skewed_cols) / max(len(numeric_df.columns), 1)

    # ---------- Scoring ----------
    score = 100.0
    score -= missing_ratio * 30
    score -= duplicate_ratio * 20
    score -= low_variance_ratio * 25
    score -= skewness_ratio * 15

    if pd.isna(score):
        score = 0

    final_score = max(int(round(score)), 0)

    # ---------- Feature diagnostics ----------
    feature_diagnostics = []

    for col in df.columns:
        missing_pct = df[col].isnull().mean() * 100

        flags = []
        if missing_pct > 20:
            flags.append("High Missingness")
        if col in low_variance_cols:
            flags.append("Low Variance")
        if col in skewed_cols:
            flags.append("High Skewness")
        if not flags:
            flags.append("Safe")

        feature_diagnostics.append({
            "feature": col,
            "missing_percentage": round(missing_pct, 2),
            "unique_values": df[col].nunique(dropna=True),
            "dtype": str(df[col].dtype),
            "quality_flags": flags,
            "risk_analysis": risk_analysis.get(col)
        })

    # ---------- Recommendations ----------
    recommendations = generate_recommendations(
        df=df,
        feature_diagnostics=feature_diagnostics,
        risk_analysis=risk_analysis,
        target_col=target_col
    )
    return {
        "dataset_id": dataset_id,
        "rows": n_rows,
        "columns": n_cols,
        "quality_score": final_score,
        "metrics": {
            "missing_ratio": round(missing_ratio, 4),
            "duplicate_ratio": round(duplicate_ratio, 4),
            "low_variance_ratio": round(low_variance_ratio, 4),
            "skewness_ratio": round(skewness_ratio, 4)
        },
        "feature_diagnostics": feature_diagnostics,
        "recommendations": recommendations
    }
