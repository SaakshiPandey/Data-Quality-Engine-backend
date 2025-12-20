from fastapi import APIRouter, UploadFile, File
from app.services.ingestion_service import ingest_csv

router = APIRouter(prefix="/upload", tags=["Dataset Upload"])


@router.post("/")
def upload_dataset(file: UploadFile = File(...)):
    """
    Upload CSV dataset and initialize dataset state.
    """
    metadata = ingest_csv(file)
    return {
        "message": "Dataset uploaded successfully",
        "dataset": metadata
    }
