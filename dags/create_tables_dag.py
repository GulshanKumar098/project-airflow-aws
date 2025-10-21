import pendulum
from airflow.decorators import dag
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.empty import EmptyOperator
import sql_statements


@dag(
    dag_id="initialize_redshift_tables",
    start_date=pendulum.now(),
    schedule=None,
    max_active_runs=1,
    catchup=False,
    tags=["redshift", "setup"]
)
def initialize_tables():

    # --- Define start marker ---
    start_process = EmptyOperator(task_id="start_table_creation")

    # --- Create all tables in Redshift ---

    artists_table = PostgresOperator(
        task_id="create_artists_tbl",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_ARTISTS_TABLE_SQL
    )

    songplays_table = PostgresOperator(
        task_id="create_songplays_tbl",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_SONGPLAYS_TABLE_SQL
    )

    songs_table = PostgresOperator(
        task_id="create_songs_tbl",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_SONGS_TABLE_SQL
    )

    time_table = PostgresOperator(
        task_id="create_time_tbl",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TIME_TABLE_SQL
    )

    users_table = PostgresOperator(
        task_id="create_users_tbl",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_USERS_TABLE_SQL
    )

    staging_events = PostgresOperator(
        task_id="create_staging_events_tbl",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_STAGING_EVENTS_TABLE_SQL
    )

    staging_songs = PostgresOperator(
        task_id="create_staging_songs_tbl",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_STAGING_SONGS_TABLE_SQL
    )

    # --- Define DAG structure ---
    start_process >> [
        artists_table,
        songplays_table,
        songs_table,
        time_table,
        users_table,
        staging_events,
        staging_songs
    ]


# Instantiate the DAG
setup_redshift_tables_dag = initialize_tables()