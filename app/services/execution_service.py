import os
import json
import pandas as pd
from datetime import datetime

DATASET_STORAGE_PATH = "app/storage/datasets"


def get_latest_version(dataset_dir: str) -> str:
    versions = [
        f for f in os.listdir(dataset_dir)
        if f.endswith(".csv") and f.startswith("v")
    ]
    versions.sort()
    return versions[-1]


def execute_step(
    dataset_id: str,
    action: str,
    params: dict
) -> dict:
    """
    Executes ONE preprocessing step and creates a new dataset version.
    """

    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)
    if not os.path.exists(dataset_dir):
        raise FileNotFoundError("Dataset not found")

    # ---------- Load latest dataset ----------
    latest_version = get_latest_version(dataset_dir)
    latest_path = os.path.join(dataset_dir, latest_version)

    df = pd.read_csv(latest_path)

    # ---------- Apply preprocessing ----------
    description = ""
    if action == "drop_feature":
        feature = params.get("feature")
        if feature in df.columns:
            df = df.drop(columns=[feature])
            description = f"Dropped feature: {feature}"

    elif action == "median_impute":
        feature = params.get("feature")
        if feature in df.columns:
            median = df[feature].median()
            df[feature] = df[feature].fillna(median)
            description = f"Median imputation on {feature}"

    else:
        raise ValueError("Unsupported preprocessing action")

    # ---------- Create new version ----------
    version_number = int(latest_version[1]) + 1
    new_version_name = f"v{version_number}.csv"
    new_version_path = os.path.join(dataset_dir, new_version_name)

    df.to_csv(new_version_path, index=False)

    # ---------- Log execution ----------
    log_path = os.path.join(dataset_dir, "execution_log.json")
    log_entry = {
        "version": new_version_name,
        "action": action,
        "params": params,
        "description": description,
        "timestamp": datetime.utcnow().isoformat()
    }

    if os.path.exists(log_path):
        with open(log_path, "r") as f:
            logs = json.load(f)
    else:
        logs = []

    logs.append(log_entry)

    with open(log_path, "w") as f:
        json.dump(logs, f, indent=2)

    return {
        "dataset_id": dataset_id,
        "new_version": new_version_name,
        "description": description
    }
