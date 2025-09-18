from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.config import RenderConfig, ExecutionConfig
from cosmos.constants import LoadMode, ExecutionMode

DBT_ROOT = Path("/opt/airflow/dags/dbt")

project_cfg = ProjectConfig(dbt_project_path=DBT_ROOT / "ecom_marts")
profile_cfg = ProfileConfig(
    profile_name="ecom_marts",
    target_name="dev",
    profiles_yml_filepath=DBT_ROOT / "profiles.yml",
)

exec_cfg = ExecutionConfig(
    execution_mode=ExecutionMode.VIRTUALENV,
    virtualenv_dir=Path("/tmp/cosmos_venv_ecom_marts"),
)
render_cfg = RenderConfig(load_method=LoadMode.DBT_LS)

with DAG(
    dag_id="dbt_marts",
    start_date=datetime(2024, 1, 1),
    concurrency=1,
    max_active_runs=1,
    catchup=False,
) as dag:
    dbt_marts = DbtTaskGroup(
        group_id="marts",
        project_config=project_cfg,         
        profile_config=profile_cfg,
        render_config=render_cfg,
        execution_config=exec_cfg,
        operator_args={
            "py_requirements": ["dbt-clickhouse"],
            "install_deps": True,
        },
    )
