from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries


# ---------------- Default Configuration ----------------
default_args = {
    "owner": "DataOpsTeam",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 31),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}


# ---------------- DAG Definition ----------------
@dag(
    dag_id="redshift_etl_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    max_active_runs=1,
    catchup=False,
    tags=["redshift", "etl", "s3"]
)
def redshift_etl_workflow():

    # ---------- Load Airflow Variables ----------
    s3_bucket_name = Variable.get("s3_bucket")
    prefix_log_data = Variable.get("s3_prefix_log_data")
    prefix_song_data = Variable.get("s3_prefix_song_data")
    log_json_path = Variable.get("s3_prefix_log_json_path")
    aws_region = Variable.get("region")

    # ---------- Start & End Tasks ----------
    start_pipeline = EmptyOperator(task_id="start_etl_process")
    finish_pipeline = EmptyOperator(task_id="end_etl_process")

    # ---------- Stage Data to Redshift ----------
    stage_logs = StageToRedshiftOperator(
        task_id="stage_events_from_s3",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket=s3_bucket_name,
        s3_key=prefix_log_data,
        region=aws_region,
        file_format="JSON",
        s3_json_paths_format=log_json_path
    )

    stage_songs = StageToRedshiftOperator(
        task_id="stage_songs_from_s3",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket=s3_bucket_name,
        s3_key=prefix_song_data,
        region=aws_region,
        file_format="JSON"
    )

    # ---------- Load Fact Table ----------
    populate_songplays = LoadFactOperator(
        task_id="load_songplays_fact",
        redshift_conn_id="redshift",
        table="songplays",
        sql_query=SqlQueries.songplay_table_insert
    )

    # ---------- Load Dimension Tables ----------
    populate_songs = LoadDimensionOperator(
        task_id="load_songs_dimension",
        redshift_conn_id="redshift",
        table="songs",
        sql_query=SqlQueries.song_table_insert
    )

    populate_users = LoadDimensionOperator(
        task_id="load_users_dimension",
        redshift_conn_id="redshift",
        table="users",
        sql_query=SqlQueries.user_table_insert
    )

    populate_artists = LoadDimensionOperator(
        task_id="load_artists_dimension",
        redshift_conn_id="redshift",
        table="artists",
        sql_query=SqlQueries.artist_table_insert
    )

    populate_time = LoadDimensionOperator(
        task_id="load_time_dimension",
        redshift_conn_id="redshift",
        table="time",
        sql_query=SqlQueries.time_table_insert
    )

    # ---------- Run Data Quality Checks ----------
    verify_data = DataQualityOperator(
        task_id="run_data_quality_tests",
        redshift_conn_id="redshift",
        data_quality_count_checks=SqlQueries.all_data_quality_count_checks,
        data_quality_null_checks=SqlQueries.all_data_quality_null_checks
    )

    # ---------- Task Dependency Graph ----------
    start_pipeline >> [stage_logs, stage_songs] >> populate_songplays
    populate_songplays >> [populate_songs, populate_users, populate_artists, populate_time]
    [populate_songs, populate_users, populate_artists, populate_time] >> verify_data
    verify_data >> finish_pipeline


# Instantiate the DAG
etl_redshift_dag = redshift_etl_workflow()