import logging
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook


# DAG definition: Lists objects from S3 using Airflow connection and variables
@dag(
    dag_id="s3_key_listing_workflow",
    start_date=pendulum.now(),
    schedule_interval=None,
    catchup=False,
    tags=["aws", "s3", "utility"]
)
def s3_key_extractor():

    @task(task_id="fetch_s3_keys")
    def fetch_keys_from_s3():
        """
        Retrieve and log object keys from two S3 prefixes defined in Airflow Variables.
        """

        # Initialize the S3 connection using Airflow's predefined connection
        s3_hook = S3Hook(aws_conn_id="aws_credentials")

        # Read custom variables for S3 configuration
        s3_bucket_name = Variable.get("s3_bucket")
        prefix_logs = Variable.get("s3_prefix_log_data")
        prefix_songs = Variable.get("s3_prefix_song_data")

        # ---- List log data keys ----
        logging.info(f"Fetching S3 keys under: {s3_bucket_name}/{prefix_logs}")
        log_keys = s3_hook.list_keys(bucket_name=s3_bucket_name, prefix=prefix_logs)
        if log_keys:
            for key in log_keys:
                logging.info(f"Found file: s3://{s3_bucket_name}/{key}")
        else:
            logging.warning("No log data found under the given prefix.")
        logging.info("--------------------------------------------------")

        # ---- List song data keys ----
        logging.info(f"Fetching S3 keys under: {s3_bucket_name}/{prefix_songs}")
        song_keys = s3_hook.list_keys(bucket_name=s3_bucket_name, prefix=prefix_songs)
        if song_keys:
            for key in song_keys:
                logging.info(f"Found file: s3://{s3_bucket_name}/{key}")
        else:
            logging.warning("No song data found under the given prefix.")
        logging.info("--------------------------------------------------")

    # Execute the S3 listing task
    fetch_keys_from_s3()


# Instantiate the DAG
s3_key_listing_dag = s3_key_extractor()