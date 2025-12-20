from fastapi import APIRouter, HTTPException
from app.services.quality_scoring_service import compute_quality_score
from typing import Optional
from fastapi import Query

router = APIRouter(prefix="/analyze", tags=["Dataset Analysis"])


@router.get("/{dataset_id}")
def analyze_dataset(
    dataset_id: str,
    target_col: Optional[str] = Query(default=None)
):
    try:
        analysis = compute_quality_score(dataset_id, target_col)
        return analysis
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="Dataset not found"
        )

