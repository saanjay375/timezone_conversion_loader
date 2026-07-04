"""
paths.py

Directory and file path management.
"""

from pathlib import Path
from datetime import datetime


def get_project_root() -> Path:
    """
    timezone_conversion_loader directory
    """

    return Path(__file__).resolve().parent


def get_logs_root() -> Path:
    """
    Logs directory.
    """

    logs_dir = get_project_root() / "Logs"

    logs_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    return logs_dir


def get_table_log_directory(
    schema: str,
    table_name: str,
) -> Path:
    """
    Logs/schema.table
    """

    table_dir = get_logs_root() / f"{schema}.{table_name}"

    table_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    return table_dir


def get_execution_log_file(
    schema: str,
    table_name: str,
) -> Path:
    """
    Execution log for this run.
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    return (
        get_table_log_directory(
            schema,
            table_name,
        )
        / f"execution_{timestamp}.log"
    )


def get_checkpoint_file(
    schema: str,
    table_name: str,
) -> Path:

    return (
        get_table_log_directory(
            schema,
            table_name,
        )
        / "checkpoint.json"
    )


def get_summary_file(
    schema: str,
    table_name: str,
) -> Path:

    return (
        get_table_log_directory(
            schema,
            table_name,
        )
        / "summary.json"
    )
