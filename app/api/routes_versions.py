import os
from fastapi import APIRouter, HTTPException
from app.services.versioning_service import undo_last_execution
from pydantic import BaseModel

DATASET_STORAGE_PATH = "app/storage/datasets"

router = APIRouter(prefix="/versions", tags=["Dataset Versions"])


def extract_version_number(filename: str) -> int:
    name = filename.replace(".csv", "")
    if not name.startswith("v"):
        raise ValueError(f"Invalid version file: {filename}")

    number_part = name[1:].split("_")[0]
    return int(number_part)


@router.get("/{dataset_id}")
def list_dataset_versions(dataset_id: str):
    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)

    if not os.path.exists(dataset_dir):
        raise HTTPException(status_code=404, detail="Dataset not found")

    versions = [
        f for f in os.listdir(dataset_dir)
        if f.endswith(".csv") and f.startswith("v")
    ]

    try:
        versions_sorted = sorted(
            versions,
            key=lambda x: extract_version_number(x)
        )
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

    latest = versions_sorted[-1] if versions_sorted else None

    return {
        "versions": versions_sorted,
        "latest": latest
    }

@router.post("/undo/{dataset_id}")
def undo_execution(dataset_id: str):
    return undo_last_execution(dataset_id)


class RollbackRequest(BaseModel):
    version: str
@router.post("/rollback/{dataset_id}")
def rollback_dataset(dataset_id: str, payload: RollbackRequest):
    target_version = payload.version
    return perform_rollback(dataset_id, target_version)


