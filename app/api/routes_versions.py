import os
import json
from fastapi import APIRouter, HTTPException

DATASET_STORAGE_PATH = "app/storage/datasets"

router = APIRouter(tags=["Dataset Versions"])


@router.get("/versions/{dataset_id}")
def list_dataset_versions(dataset_id: str):
    """
    List all dataset versions for a given dataset.
    """
    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)

    if not os.path.exists(dataset_dir):
        raise HTTPException(status_code=404, detail="Dataset not found")

    versions = sorted(
        [
            f for f in os.listdir(dataset_dir)
            if f.startswith("v") and f.endswith(".csv")
        ]
    )

    return {
        "dataset_id": dataset_id,
        "versions": versions,
        "latest": versions[-1] if versions else None
    }


@router.get("/execution-log/{dataset_id}")
def get_execution_log(dataset_id: str):
    """
    Retrieve execution history for a dataset.
    """
    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)
    log_path = os.path.join(dataset_dir, "execution_log.json")

    if not os.path.exists(dataset_dir):
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not os.path.exists(log_path):
        return {
            "dataset_id": dataset_id,
            "execution_log": []
        }

    with open(log_path, "r") as f:
        logs = json.load(f)

    return {
        "dataset_id": dataset_id,
        "execution_log": logs
    }
