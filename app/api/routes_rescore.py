from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from app.services.rescoring_service import rescore_dataset

router = APIRouter(prefix="/rescore", tags=["Post-Execution Scoring"])


@router.get("/{dataset_id}")
def rescore(
    dataset_id: str,
    target_col: Optional[str] = Query(default=None)
):
    """
    Compare dataset quality before and after preprocessing.
    """
    try:
        return rescore_dataset(dataset_id, target_col)
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Rescoring failed: {str(e)}"
        )
