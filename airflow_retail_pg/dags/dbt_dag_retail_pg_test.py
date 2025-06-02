import os
from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Cấu hình kết nối Postgres qua Airflow connection ID
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_conn",  # <-- Đây là Connection ID trong Airflow UI
        profile_args={
            "schema": "dbt_postgres"
        },
    )
)

# Khởi tạo DAG DBT
dbt_postgres_dag = DbtDag(
    dag_id="dbt_postgres_pipeline",
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/retail_pg"),
    operator_args={
        "install_deps": True
        },
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/venv/bin/dbt", 
    ),
    schedule="@daily",
    # schedule=None,  # Không tự động chạy
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
