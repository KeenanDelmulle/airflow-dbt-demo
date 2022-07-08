import json
import pendulum
from pendulum import datetime
import dbt_dag_parser
from dbt_dag_parser import DbtDagParser #include.

from airflow import DAG
from airflow.operators.bash import BashOperator

# We're hardcoding this value here for the purpose of the demo, but in a production environment this
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "/home/keenan/code/jaffle_shop" #this is the directory to which we will be working with where our dbt project is located and the 
                                            # associated profiles.yml , maifest.json and dbt_profiles.yml files are located
PIPENV_BASH_ENV = "/home/keenan/.local/share/virtualenvs/airflow_tutorial-1roRshV0/bin/dbt"


DBT_ENV = {  # this serves as a connection to the database where our DBT DAG comes from
    "DBT_USER": "{{conn.postgres_default.login}}", 
    "DBT_ENV_SECRET_PASSWORD": "{{conn.postgres_default.password}}", 
    "DBT_HOST": "{{ conn.postgres_default.host }}", 
    "DBT_SCHEMA": "{{ conn.postgres_default.schema }}", 
    "DBT_PORT": "{{ conn.postgres_default.port }}",  
}

with DAG(
    "dbt_advanced_dag", # initializing a new airflow DAG
    start_date=datetime(2020, 12, 23),
    description="A dbt wrapper for Airflow.",
    schedule_interval=None,
    catchup=False,
) as dag:

    def load_manifest(): # loading the structure of our DBT DAG with all tasks, dependencies etc
        local_filepath = f"{DBT_PROJECT_DIR}/target/manifest.json"
        with open(local_filepath) as f:
            data = json.load(f)
        return data

    def make_dbt_task(node, dbt_verb):  # this is where we create our "run" and "test" airflow tasks from our 1 original dbt DAG node
        """Returns an Airflow operator either run and test an individual model"""
        GLOBAL_CLI_FLAGS = "--no-write-json"
        model = node.split(".")[-1]
        if dbt_verb == "run":
            dbt_task = BashOperator(
                task_id=node,
                bash_command=(
                    f"{PIPENV_BASH_ENV} {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model} " # these are bash commands which run each individual task
                    f"--profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}" # it is constructed this way to allow easier error checking if a task should fail
                ),                                                                      # this way we do not need to run the entire dbt project again to fix an error
                env=DBT_ENV, # maintains connection to database while generating our new airflow tasks
            )
        elif dbt_verb == "test":
            node_test = node.replace("model", "test")
            dbt_task = BashOperator(
                task_id=node_test,
                bash_command=(
                    f"{PIPENV_BASH_ENV} {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model} " # they still used run
                    f"--profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
                ),
                env=DBT_ENV,
            )
        return dbt_task

    # This task loads the CSV files from dbt/data into the local postgres database for the purpose of this demo.
    # In practice, we'd usually expect the data to have already been loaded to the database.
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{PIPENV_BASH_ENV} seed  --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}", # need to import the data   
        env=DBT_ENV,
    )

    data = load_manifest()
    dbt_tasks = {}

    for node in data["nodes"].keys(): # Generates the tasks in the form of an airflow DAG without dependencies yet...
        if node.split(".")[0] == "model":
            node_test = node.replace("model", "test")
            dbt_tasks[node] = make_dbt_task(node, "run")
            dbt_tasks[node_test] = make_dbt_task(node, "test")

    for node in data["nodes"].keys():
        if node.split(".")[0] == "model":
            # Set dependency to run tests on a model after model runs finishes
            node_test = node.replace("model", "test")
            dbt_tasks[node] >> dbt_tasks[node_test]
            # Set all model -> model dependencies so all the models becomes linearly dependant on their previous dbt upstream nodes
            for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
                upstream_node_type = upstream_node.split(".")[0]
                if upstream_node_type == "model":
                    dbt_seed >> dbt_tasks[upstream_node] >> dbt_tasks[node]

    