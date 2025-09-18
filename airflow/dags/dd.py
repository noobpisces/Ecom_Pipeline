# from datetime import datetime
# from pathlib import Path

# from airflow import DAG
# from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
# from cosmos.config import RenderConfig, ExecutionConfig
# from cosmos.constants import LoadMode, ExecutionMode

# DBT_ROOT = Path("/opt/airflow/dags/dbt")

# # --- Staging ---
# project_cfg_staging = ProjectConfig(dbt_project_path=DBT_ROOT / "ecom_staging")
# profile_cfg_staging = ProfileConfig(
#     profile_name="ecom_staging",
#     target_name="dev",
#     profiles_yml_filepath=DBT_ROOT / "profiles.yml",
# )
# exec_cfg = ExecutionConfig(
#     execution_mode=ExecutionMode.VIRTUALENV,
#     virtualenv_dir=Path("/tmp/cosmos_venv_ecom"),
# )
# render_cfg = RenderConfig(load_method=LoadMode.DBT_LS)

# # --- Intermediate ---
# project_cfg_intermediate = ProjectConfig(dbt_project_path=DBT_ROOT / "ecom_intermediate")
# profile_cfg_intermediate = ProfileConfig(
#     profile_name="ecom_intermediate",
#     target_name="dev",
#     profiles_yml_filepath=DBT_ROOT / "profiles.yml",
# )

# # --- Marts ---
# project_cfg_marts = ProjectConfig(dbt_project_path=DBT_ROOT / "ecom_marts")
# profile_cfg_marts = ProfileConfig(
#     profile_name="ecom_marts",
#     target_name="dev",
#     profiles_yml_filepath=DBT_ROOT / "profiles.yml",
# )

# with DAG(
#     dag_id="dbt_pipeline",
#     start_date=datetime(2024, 1, 1),
#     schedule="@daily",
#     catchup=False,
#     default_args={"retries": 0},
# ) as dag:

#     # Staging
#     dbt_staging = DbtTaskGroup(
#         group_id="staging",
#         project_config=project_cfg_staging,
#         profile_config=profile_cfg_staging,
#         render_config=render_cfg,
#         execution_config=exec_cfg,
#         operator_args={
#             "py_requirements": ["dbt-clickhouse"],
#             "install_deps": True,
#         },
#     )

#     # Intermediate
#     dbt_intermediate = DbtTaskGroup(
#         group_id="intermediate",
#         project_config=project_cfg_intermediate,
#         profile_config=profile_cfg_intermediate,
#         render_config=render_cfg,
#         execution_config=exec_cfg,
#         operator_args={
#             "py_requirements": ["dbt-clickhouse"],
#             "install_deps": True,
#         },
#     )

#     # Marts
#     dbt_marts = DbtTaskGroup(
#         group_id="marts",
#         project_config=project_cfg_marts,
#         profile_config=profile_cfg_marts,
#         render_config=render_cfg,
#         execution_config=exec_cfg,
#         operator_args={
#             "py_requirements": ["dbt-clickhouse"],
#             "install_deps": True,
#         },
#     )

#     # Thứ tự chạy: staging -> intermediate -> marts
#     dbt_staging >> dbt_intermediate >> dbt_marts
