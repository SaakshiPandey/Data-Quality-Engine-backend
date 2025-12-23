import os
import json
import pandas as pd
import numpy as np 
from datetime import datetime

DATASET_STORAGE_PATH = "app/storage/datasets"


def extract_version_number(filename: str) -> int:
    name = filename.replace(".csv", "")
    number_part = name[1:].split("_")[0]
    return int(number_part)


def execute_step(dataset_id: str, action: str, params: dict) -> dict:
    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)
    if not os.path.exists(dataset_dir):
        raise FileNotFoundError("Dataset not found")

    versions = [
        f for f in os.listdir(dataset_dir)
        if f.endswith(".csv") and f.startswith("v")
    ]
    if not versions:
        raise FileNotFoundError("No dataset versions found")

    latest_version = sorted(
        versions, key=extract_version_number
    )[-1]

    df = pd.read_csv(os.path.join(dataset_dir, latest_version))

    feature = params.get("feature")
    if not feature:
        raise ValueError("Missing required parameter: feature")

    description = ""

    # ---------- SUPPORTED ACTIONS ----------
    if action == "drop_feature":
        if feature not in df.columns:
            raise ValueError(f"Feature '{feature}' not found")
        df.drop(columns=[feature], inplace=True)
        description = f"Dropped feature: {feature}"

    elif action == "median_impute":
        df[feature] = df[feature].fillna(df[feature].median())
        description = f"Median imputation on {feature}"

    elif action == "mean_impute":
        df[feature] = df[feature].fillna(df[feature].mean())
        description = f"Mean imputation on {feature}"

    elif action == "mode_impute":
        df[feature] = df[feature].fillna(df[feature].mode()[0])
        description = f"Mode imputation on {feature}"

    elif action == "log_transform":
        df[feature] = df[feature].apply(
            lambda x: np.log(x) if x > 0 else 0
        )
        description = f"Log transform applied on {feature}"


    elif action == "standard_scale":
        mean = df[feature].mean()
        std = df[feature].std()
        df[feature] = (df[feature] - mean) / std
        description = f"Standard scaling applied on {feature}"

    else:
        raise ValueError(f"Unsupported action: {action}")

    next_version_num = extract_version_number(latest_version) + 1
    safe_feature = feature.replace(" ", "_")
    new_version = f"v{next_version_num}_{action}_{safe_feature}.csv"


    df.to_csv(os.path.join(dataset_dir, new_version), index=False)

    # ---------- LOG ----------
    log_path = os.path.join(dataset_dir, "execution_log.json")
    logs = []
    if os.path.exists(log_path):
        logs = json.load(open(log_path))

    logs.append({
        "version": new_version,
        "action": action,
        "feature": feature,
        "description": description,
        "timestamp": datetime.utcnow().isoformat()
    })

    json.dump(logs, open(log_path, "w"), indent=2)

    return {
        "new_version": new_version,
        "description": description
    }
