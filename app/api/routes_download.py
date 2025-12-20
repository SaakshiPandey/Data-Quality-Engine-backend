import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

DATASET_STORAGE_PATH = "app/storage/datasets"

router = APIRouter(prefix="/download", tags=["Dataset Download"])


@router.get("/{dataset_id}")
def download_latest_dataset(dataset_id: str):
    """
    Download the latest processed dataset (CSV only).
    """
    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)

    if not os.path.exists(dataset_dir):
        raise HTTPException(
            status_code=404,
            detail="Dataset not found"
        )

    # List all CSV versions
    versions = sorted(
        f for f in os.listdir(dataset_dir)
        if f.startswith("v") and f.endswith(".csv")
    )

    if not versions:
        raise HTTPException(
            status_code=404,
            detail="No dataset versions available"
        )

    latest_version = versions[-1]
    file_path = os.path.join(dataset_dir, latest_version)

    return FileResponse(
        path=file_path,
        media_type="text/csv",
        filename=f"{dataset_id}_{latest_version}"
    )
