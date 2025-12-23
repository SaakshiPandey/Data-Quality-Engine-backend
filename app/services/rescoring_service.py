import os
from app.services.quality_scoring_service import compute_quality_score

DATASET_STORAGE_PATH = "app/storage/datasets"


def extract_version_number(filename: str) -> int:
    """
    Safely extract version number from:
    v1.csv
    v0_raw.csv
    v10_drop_Name.csv
    """
    name = filename.replace(".csv", "")
    if not name.startswith("v"):
        raise ValueError(f"Invalid version filename: {filename}")

    number_part = name[1:].split("_")[0]
    return int(number_part)


def rescore_dataset(dataset_id: str, target_col: str | None = None) -> dict:
    """
    Computes before vs after quality scores.
    """

    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)
    if not os.path.exists(dataset_dir):
        raise FileNotFoundError("Dataset not found")

    # ---------- Initial score (ALWAYS raw) ----------
    initial_version = "v0_raw.csv"

    initial_result = compute_quality_score(
        dataset_id=dataset_id,
        target_col=target_col,
        version=initial_version
    )

    # ---------- Find latest version safely ----------
    versions = [
        f for f in os.listdir(dataset_dir)
        if f.endswith(".csv") and f.startswith("v")
    ]

    if not versions:
        raise FileNotFoundError("No dataset versions found")

    versions_sorted = sorted(
        versions,
        key=extract_version_number
    )

    latest_version = versions_sorted[-1]

    final_result = compute_quality_score(
        dataset_id=dataset_id,
        target_col=target_col,
        version=latest_version
    )

    improvement = final_result["quality_score"] - initial_result["quality_score"]

    return {
        "dataset_id": dataset_id,
        "initial_version": initial_version,
        "final_version": latest_version,
        "initial_score": initial_result["quality_score"],
        "final_score": final_result["quality_score"],
        "improvement": improvement,
        "initial_metrics": initial_result["metrics"],
        "final_metrics": final_result["metrics"]
    }
