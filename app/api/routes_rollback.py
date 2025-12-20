from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.versioning_service import rollback_to_version

router = APIRouter(prefix="/rollback", tags=["Rollback / Undo"])


class RollbackRequest(BaseModel):
    target_version: str


@router.post("/{dataset_id}")
def rollback_dataset(
    dataset_id: str,
    request: RollbackRequest
):
    """
    Rollback dataset to a specified previous version.
    """
    try:
        result = rollback_to_version(
            dataset_id=dataset_id,
            target_version=request.target_version
        )
        return {
            "status": "success",
            "rollback": result
        }

    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=str(e)
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Rollback failed: {str(e)}"
        )
