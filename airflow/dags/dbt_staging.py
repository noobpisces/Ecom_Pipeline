from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.config import RenderConfig, ExecutionConfig
from cosmos.constants import LoadMode, ExecutionMode

DBT_ROOT_staging = Path("/opt/airflow/dags/dbt")  # trỏ tới dbt_project.yml
project = "ecom_staging"
project_cfg_staging = ProjectConfig(dbt_project_path=DBT_ROOT_staging/project)

# Dùng profiles.yml cho ClickHouse
profile_cfg_staging = ProfileConfig(
    profile_name="ecom_staging",            # khớp với profiles.yml
    target_name="dev",
    profiles_yml_filepath=DBT_ROOT_staging / "profiles.yml",
)

# Khuyến nghị: chạy ở chế độ virtualenv để tách dbt khỏi Airflow
exec_cfg_staging = ExecutionConfig(
    execution_mode=ExecutionMode.VIRTUALENV,
    # Tạo/giữ venv để chạy nhanh hơn các lần sau (đường dẫn local trong worker)
    virtualenv_dir=Path("/tmp/cosmos_venv_ecom"),
)

render_cfg_staging = RenderConfig(
    # Mặc định Cosmos dùng DBT_LS để parse project.
    # Vì bạn đã cài dbt qua extras, DBT_LS sẽ hoạt động OK.
    load_method=LoadMode.DBT_LS,
    # Ví dụ chỉ chạy các model có tag=marts:
    # select=["tag:marts"],
)

with DAG(
    dag_id="dbt_staging",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    concurrency=1,
    max_active_runs=1,
    catchup=False,
) as dag:
    dbt_models = DbtTaskGroup(
        group_id="dbt_ecom",
        project_config=project_cfg_staging,
        profile_config=profile_cfg_staging,
        render_config=render_cfg_staging,
        execution_config=exec_cfg_staging,
        # Cài adapter trong venv do Cosmos quản lý
        operator_args={
            "py_requirements": ["dbt-clickhouse"],
            "install_deps": True,    # chạy `dbt deps` trước
            # "full_refresh": True,  # bật khi cần
        },
    )
