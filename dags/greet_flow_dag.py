import logging
import pendulum
from airflow.decorators import dag, task


# Define a simple DAG that logs a greeting message
@dag(
    dag_id="simple_greeting_workflow",
    start_date=pendulum.now(),
    schedule_interval=None,
    catchup=False,
    tags=["example", "logging"]
)
def greeting_pipeline():

    @task(task_id="print_greeting")
    def display_greeting():
        logging.info("Greetings from Airflow DAG!")

    # Trigger the task execution
    display_greeting()


# Instantiate the DAG
greeting_dag = greeting_pipeline()