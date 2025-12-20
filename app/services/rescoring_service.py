import os
from app.services.quality_scoring_service import compute_quality_score

DATASET_STORAGE_PATH = "app/storage/datasets"


def rescore_dataset(dataset_id: str, target_col: str | None = None) -> dict:
    """
    Computes before vs after quality scores.
    """

    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)
    if not os.path.exists(dataset_dir):
        raise FileNotFoundError("Dataset not found")

    # ---------- Initial score (v0) ----------
    initial_result = compute_quality_score(
        dataset_id=dataset_id,
        target_col=target_col,
        version="v0_raw.csv"
    )

    # ---------- Latest version ----------
    versions = sorted(
        f for f in os.listdir(dataset_dir)
        if f.startswith("v") and f.endswith(".csv")
    )

    latest_version = versions[-1]

    final_result = compute_quality_score(
        dataset_id=dataset_id,
        target_col=target_col,
        version=latest_version
    )

    improvement = final_result["quality_score"] - initial_result["quality_score"]

    return {
        "dataset_id": dataset_id,
        "initial_version": "v0_raw.csv",
        "final_version": latest_version,
        "initial_score": initial_result["quality_score"],
        "final_score": final_result["quality_score"],
        "improvement": improvement,
        "initial_metrics": initial_result["metrics"],
        "final_metrics": final_result["metrics"]
    }
