import os
import uuid
import pandas as pd
from fastapi import UploadFile, HTTPException

DATASET_STORAGE_PATH = "app/storage/datasets"


def ingest_csv(file: UploadFile) -> dict:
    """
    Validates and ingests a CSV file.
    Saves raw dataset as version v0.
    """

    # 1. Enforce CSV-only upload
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=400,
            detail="Only CSV files are supported."
        )

    # 2. Generate dataset ID
    dataset_id = str(uuid.uuid4())

    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)
    os.makedirs(dataset_dir, exist_ok=True)

    raw_dataset_path = os.path.join(dataset_dir, "v0_raw.csv")

    # 3. Read CSV
    try:
        df = pd.read_csv(file.file)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to read CSV file: {str(e)}"
        )

    # 4. Basic validation
    if df.empty:
        raise HTTPException(
            status_code=400,
            detail="Uploaded CSV is empty."
        )

    if df.shape[1] == 0:
        raise HTTPException(
            status_code=400,
            detail="CSV must contain at least one column."
        )

    # 5. Save raw dataset
    df.to_csv(raw_dataset_path, index=False)

    # 6. Initial metadata
    metadata = {
        "dataset_id": dataset_id,
        "filename": file.filename,
        "rows": df.shape[0],
        "columns": df.shape[1],
        "column_names": df.columns.tolist(),
        "current_version": "v0_raw"
    }

    return metadata
