import os
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from typing import Optional

from app.services.report_service import generate_report

REPORT_STORAGE_PATH = "app/storage/reports"

router = APIRouter(prefix="/report", tags=["Reports"])


@router.post("/{dataset_id}")
def generate_dataset_report(
    dataset_id: str,
    target_col: Optional[str] = Query(default=None)
):
    """
    Generate (or regenerate) dataset quality report.
    """
    try:
        result = generate_report(dataset_id, target_col)
        return {
            "status": "success",
            "report": result
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Report generation failed: {str(e)}"
        )


@router.get("/{dataset_id}/json")
def download_report_json(dataset_id: str):
    """
    Download JSON report.
    """
    json_path = os.path.join(
        REPORT_STORAGE_PATH, dataset_id, "report.json"
    )

    if not os.path.exists(json_path):
        raise HTTPException(
            status_code=404,
            detail="JSON report not found. Generate report first."
        )

    return FileResponse(
        path=json_path,
        media_type="application/json",
        filename="report.json"
    )


@router.get("/{dataset_id}/pdf")
def download_report_pdf(dataset_id: str):
    """
    Download PDF report.
    """
    pdf_path = os.path.join(
        REPORT_STORAGE_PATH, dataset_id, "report.pdf"
    )

    if not os.path.exists(pdf_path):
        raise HTTPException(
            status_code=404,
            detail="PDF report not found. Generate report first."
        )

    return FileResponse(
        path=pdf_path,
        media_type="application/pdf",
        filename="report.pdf"
    )
