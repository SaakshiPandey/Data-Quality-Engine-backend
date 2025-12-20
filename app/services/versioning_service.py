import os
import json
import shutil
from datetime import datetime

DATASET_STORAGE_PATH = "app/storage/datasets"


def rollback_to_version(dataset_id: str, target_version: str) -> dict:
    """
    Rollback dataset to a previous version by creating a new version copy.
    """

    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)
    if not os.path.exists(dataset_dir):
        raise FileNotFoundError("Dataset not found")

    target_path = os.path.join(dataset_dir, target_version)
    if not os.path.exists(target_path):
        raise FileNotFoundError("Target version does not exist")

    # ---------- Determine next version ----------
    versions = sorted(
        f for f in os.listdir(dataset_dir)
        if f.startswith("v") and f.endswith(".csv")
    )

    latest_version = versions[-1]
    next_version_number = int(latest_version[1]) + 1
    new_version_name = f"v{next_version_number}.csv"
    new_version_path = os.path.join(dataset_dir, new_version_name)

    # ---------- Create rollback version ----------
    shutil.copyfile(target_path, new_version_path)

    # ---------- Log rollback ----------
    log_path = os.path.join(dataset_dir, "execution_log.json")
    rollback_entry = {
        "version": new_version_name,
        "action": "rollback",
        "params": {
            "rollback_to": target_version
        },
        "description": f"Rolled back to {target_version}",
        "timestamp": datetime.utcnow().isoformat()
    }

    if os.path.exists(log_path):
        with open(log_path, "r") as f:
            logs = json.load(f)
    else:
        logs = []

    logs.append(rollback_entry)

    with open(log_path, "w") as f:
        json.dump(logs, f, indent=2)

    return {
        "dataset_id": dataset_id,
        "rolled_back_to": target_version,
        "new_version": new_version_name
    }
