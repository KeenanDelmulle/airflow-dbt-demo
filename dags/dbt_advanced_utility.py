from pendulum import datetime
import dbt_dag_parser

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from dbt_dag_parser import DbtDagParser #include.

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "/home/keenan/code/jaffle_shop"
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = "dev"
DBT_TAG = "tag_staging"
PIPENV_BASH_ENV = "/home/keenan/.local/share/virtualenvs/airflow_tutorial-1roRshV0/bin/dbt"

with DAG(
    "dbt_advanced_dag_utility",
    start_date=datetime(2020, 12, 23),
    description="A dbt wrapper for Airflow using a utility class to map the dbt DAG to Airflow tasks",
    schedule_interval=None,
    catchup=False,
) as dag:

    start_dummy = DummyOperator(task_id="start")
    # We're using the dbt seed command here to populate the database for the purpose of this demo
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"/home/keenan/.local/share/virtualenvs/airflow_tutorial-1roRshV0/bin/dbt {DBT_GLOBAL_CLI_FLAGS} seed "
            f"--profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
        ),
        env={
            "DBT_USER": "{{ conn.postgres_default.login }}",
            "DBT_ENV_SECRET_PASSWORD": "{{ conn.postgres_default.password }}",
            "DBT_HOST": "{{ conn.postgres_default.host }}",
            "DBT_SCHEMA": "{{ conn.postgres_default.schema }}",
            "DBT_PORT": "{{ conn.postgres_default.port }}",
        },
    )
    end_dummy = DummyOperator(task_id="end")

    # The parser parses out a dbt manifest.json file and dynamically creates tasks for "dbt run" and "dbt test"
    # commands for each individual model. It groups them into task groups which we can retrieve and use in the DAG.
    dag_parser = DbtDagParser(
        #dbt_global_cli_flags=DBT_GLOBAL_CLI_FLAGS,
        dbt_project_dir=DBT_PROJECT_DIR,
        dbt_profiles_dir=DBT_PROJECT_DIR,
        dbt_target=DBT_TARGET,
    )
    dbt_run_group = dag_parser.get_dbt_run_group()
    dbt_test_group = dag_parser.get_dbt_test_group()

    start_dummy >> dbt_seed >> dbt_run_group >> dbt_test_group >> end_dummy
