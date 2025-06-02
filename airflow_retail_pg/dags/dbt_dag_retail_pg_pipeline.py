import os
from datetime import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ExecutionConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Define the DBT project path and executable path
dbt_project_path = "/usr/local/airflow/dags/dbt/retail_pg"
dbt_executable_path = f"{os.environ['AIRFLOW_HOME']}/venv/bin/dbt"

project_config = ProjectConfig(dbt_project_path)
execution_config = ExecutionConfig(dbt_executable_path=dbt_executable_path)
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_conn",
        profile_args={"schema": "dbt_postgres"}
    )
)

# Define the DAG because the `dbt_dag_retail_pg_test.py` dag does not meet the requirements, all dags in the same level. So I will create a new DAG for the retail_pg pipeline.
# This DAG will run the DBT tasks in a specific order using DbtTaskGroup by using `RenderConfig` to select specific tags for each step.
with DAG(
    dag_id="retail_pg_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "retail_pg"],
) as dag:

    # Step 1: SEEDS
    seeds = DbtTaskGroup(
        group_id="seeds",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["path:seeds"],
        ),
    )

    # Step 2: STAGING
    staging = DbtTaskGroup(
        group_id="staging",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:staging"],
        ),
    )

    # Step 3: INTERMEDIATE
    intermediate = DbtTaskGroup(
        group_id="intermediate",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:intermediate"],
        ),
    )

    # Step 4: MARTS
    marts = DbtTaskGroup(
        group_id="marts",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["tag:marts"],
        ),
    )

    # Pipeline dependencies
    seeds >> staging >> intermediate >> marts
