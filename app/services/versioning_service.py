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
        if f.endswith(".csv") and f.split("_")[0].startswith("v")
    )

    latest_version = versions[-1]
    next_version_number = int(latest_version[1]) + 1
    new_version_name = f"v{next_version_number}_rollback_to_{target_version.replace('.csv','')}.csv"

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

def undo_last_execution(dataset_id: str) -> dict:
    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)
    log_path = os.path.join(dataset_dir, "execution_log.json")

    if not os.path.exists(log_path):
        raise ValueError("No execution history found")

    with open(log_path, "r") as f:
        logs = json.load(f)

    if not logs:
        raise ValueError("No execution to undo")

    last_step = logs.pop()

    # Remove dataset version file
    version_file = os.path.join(dataset_dir, last_step["version"])
    if os.path.exists(version_file):
        os.remove(version_file)

    # Save updated log
    with open(log_path, "w") as f:
        json.dump(logs, f, indent=2)

    return {
        "undone_version": last_step["version"],
        "message": f"Undone: {last_step['description']}"
    }
