from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any

from app.services.execution_service import execute_step

router = APIRouter(prefix="/execute", tags=["Execution Mode"])


class ExecutionRequest(BaseModel):
    action: str
    params: Dict[str, Any]


@router.post("/{dataset_id}")
def execute_preprocessing_step(
    dataset_id: str,
    request: ExecutionRequest
):
    """
    Execute a single preprocessing step on the dataset.
    """
    try:
        result = execute_step(
            dataset_id=dataset_id,
            action=request.action,
            params=request.params
        )
        return {
            "status": "success",
            "execution": result
        }

    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="Dataset not found"
        )

    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Execution failed: {str(e)}"
        )
