import os
import json
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from app.services.rescoring_service import rescore_dataset
from app.services.quality_scoring_service import compute_quality_score

DATASET_STORAGE_PATH = "app/storage/datasets"
REPORT_STORAGE_PATH = "app/storage/reports"


def generate_report(dataset_id: str, target_col: str | None = None) -> dict:
    """
    Generates dataset quality report in JSON and PDF formats.
    """

    dataset_dir = os.path.join(DATASET_STORAGE_PATH, dataset_id)
    report_dir = os.path.join(REPORT_STORAGE_PATH, dataset_id)

    if not os.path.exists(dataset_dir):
        raise FileNotFoundError("Dataset not found")

    os.makedirs(report_dir, exist_ok=True)

    # ---------- Collect analysis ----------
    rescore_result = rescore_dataset(dataset_id, target_col)

    final_analysis = compute_quality_score(
        dataset_id=dataset_id,
        target_col=target_col,
        version=rescore_result["final_version"]
    )

    # ---------- Load execution history ----------
    log_path = os.path.join(dataset_dir, "execution_log.json")
    execution_log = []
    if os.path.exists(log_path):
        with open(log_path, "r") as f:
            execution_log = json.load(f)

    # ---------- Assemble report ----------
    report_data = {
        "dataset_id": dataset_id,
        "generated_at": datetime.utcnow().isoformat(),
        "initial_score": rescore_result["initial_score"],
        "final_score": rescore_result["final_score"],
        "improvement": rescore_result["improvement"],
        "initial_metrics": rescore_result["initial_metrics"],
        "final_metrics": rescore_result["final_metrics"],
        "feature_diagnostics": final_analysis["feature_diagnostics"],
        "recommendations": final_analysis["recommendations"],
        "execution_log": execution_log
    }

    # ---------- Save JSON ----------
    json_path = os.path.join(report_dir, "report.json")
    with open(json_path, "w") as f:
        json.dump(report_data, f, indent=2)

    # ---------- Generate PDF ----------
    pdf_path = os.path.join(report_dir, "report.pdf")
    _generate_pdf(report_data, pdf_path)

    return {
        "dataset_id": dataset_id,
        "json_report": json_path,
        "pdf_report": pdf_path
    }


def _generate_pdf(report_data: dict, output_path: str):
    """
    Internal helper to generate PDF report.
    """
    c = canvas.Canvas(output_path, pagesize=A4)
    width, height = A4

    y = height - 40

    def draw_line(text):
        nonlocal y
        c.drawString(40, y, text)
        y -= 18
        if y < 40:
            c.showPage()
            y = height - 40

    draw_line("Dataset Quality Report")
    draw_line("-" * 60)

    draw_line(f"Dataset ID: {report_data['dataset_id']}")
    draw_line(f"Generated At: {report_data['generated_at']}")

    draw_line("")
    draw_line(f"Initial Quality Score: {report_data['initial_score']}")
    draw_line(f"Final Quality Score: {report_data['final_score']}")
    draw_line(f"Improvement: {report_data['improvement']}")

    draw_line("")
    draw_line("Executed Preprocessing Steps:")
    if report_data["execution_log"]:
        for step in report_data["execution_log"]:
            draw_line(f"- {step['description']}")
    else:
        draw_line("No preprocessing executed.")

    draw_line("")
    draw_line("Top Recommendations:")
    for rec in report_data["recommendations"][:5]:
        draw_line(
            f"- {rec['target']}: {rec['recommended_action']} ({rec['impact']})"
        )

    c.save()
