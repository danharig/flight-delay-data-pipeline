from pathlib import Path
import ast


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_required_files_exist():
    required_files = [
        "dags/flight_delay_pipeline.py",
        "databricks/01_bronze_ingestion.py",
        "databricks/02_silver_transform.py",
        "databricks/03_gold_aggregations.py",
        "databricks/04_data_quality_checks.py",
        "requirements.txt",
        "Dockerfile",
        "docker-compose.yml",
    ]

    for file in required_files:
        assert (PROJECT_ROOT / file).exists(), f"Missing: {file}"


def test_python_files_have_valid_syntax():
    python_files = [
        "dags/flight_delay_pipeline.py",
        "databricks/01_bronze_ingestion.py",
        "databricks/02_silver_transform.py",
        "databricks/03_gold_aggregations.py",
	"databricks/04_data_quality_checks.py",
    ]

    for file in python_files:
        path = PROJECT_ROOT / file

        with open(path, "r", encoding="utf-8") as f:
            ast.parse(f.read(), filename=str(path))