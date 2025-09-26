from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DBT_PROJECT_DIR = "/usr/app"          # mount vào container
DBT_PROFILES_DIR = "/usr/app/.dbt"    # mount profiles.yml ở đây
COMPOSE_NETWORK = "E-COMERCER_bridge"  # trùng với name ở docker-compose.yml

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="dbt_clickhouse_run",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # Image đã build từ ./dbt/Dockerfile
    common_kwargs = dict(
        image="dbt:latest",  # nếu bạn build, tên sẽ là <compose_project>_dbt:latest; để chắc cú, đổi thành "dbt" nếu compose đặt tag như thế.
        api_version="auto",
        auto_remove=True,
        tty=True,
        mounts=[
            # Mount project dbt từ host vào /usr/app (phải chứa dbt_project.yml, models/, packages.yaml, .dbt/profiles.yml,...)
            Mount(source="/opt/airflow/dags/../dbt", target=DBT_PROJECT_DIR, type="bind"),
        ],
        network_mode=COMPOSE_NETWORK,  # cho container thấy "clickhouse" bằng DNS service name
        environment={
            "DBT_PROFILES_DIR": DBT_PROFILES_DIR
        },
        working_dir=DBT_PROJECT_DIR,
        docker_url="unix://var/run/docker.sock",
    )

    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        command="bash -lc 'dbt deps'",
        **common_kwargs,
    )

    # seed (nếu dùng)
    dbt_seed = DockerOperator(
        task_id="dbt_seed",
        command="bash -lc 'dbt seed --profiles-dir {} --target dev'".format(DBT_PROFILES_DIR),
        **common_kwargs,
    )

    # run models (ví dụ chỉ chạy layer staging+intermediate+marts)
    dbt_run = DockerOperator(
        task_id="dbt_run",
        # ví dụ lọc theo tags; tùy bạn chỉnh --select
        command=(
            "bash -lc 'dbt run --profiles-dir {p} --target dev "
            "--select tag:staging tag:intermediate tag:marts'"
        ).format(p=DBT_PROFILES_DIR),
        **common_kwargs,
    )

    # test (nếu muốn)
    dbt_test = DockerOperator(
        task_id="dbt_test",
        command="bash -lc 'dbt test --profiles-dir {} --target dev'".format(DBT_PROFILES_DIR),
        **common_kwargs,
    )

    dbt_deps >> dbt_seed >> dbt_run >> dbt_test
