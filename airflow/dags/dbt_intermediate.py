from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.config import RenderConfig, ExecutionConfig
from cosmos.constants import LoadMode, ExecutionMode

DBT_ROOT = Path("/opt/airflow/dags/dbt")

project_cfg = ProjectConfig(dbt_project_path=DBT_ROOT / "ecom_intermediate")
profile_cfg = ProfileConfig(
    profile_name="ecom_intermediate",
    target_name="dev",
    profiles_yml_filepath=DBT_ROOT / "profiles.yml",
)

exec_cfg = ExecutionConfig(
    execution_mode=ExecutionMode.VIRTUALENV,
    virtualenv_dir=Path("/opt/airflow/cosmos_venvs/ecom_intermediate"),
)

render_cfg = RenderConfig(load_method=LoadMode.DBT_LS)

with DAG(
    dag_id="dbt_intermediate",
    start_date=datetime(2024, 1, 1),
    concurrency=60,             # cho phép tối đa 60 task RUNNING cùng lúc trong DAG này
    max_active_runs=1,          # chỉ 1 run đang hoạt động để dồn tài nguyên cho run hiện tại
    catchup=False,
) as dag:
    dbt_intermediate = DbtTaskGroup(
        group_id="intermediate",
        project_config=project_cfg,
        profile_config=profile_cfg,
        render_config=render_cfg,
        execution_config=exec_cfg,
        operator_args={
            "py_requirements": ["dbt-clickhouse"],
            "install_deps": True,
        },
    )
