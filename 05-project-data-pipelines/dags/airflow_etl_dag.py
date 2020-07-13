from datetime import datetime, timedelta
import os
from airflow import DAG
from helpers import SqlQueries
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)


# start airflow
# /opt/airflow/start.sh

default_args = {
    'owner': 'ajn',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('airflow_etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

#create_tables = PostgresOperator(
#    task_id='create_tables',
#    postgres_conn_id="redshift",
#    sql="create_tables.sql"
#)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    source_location="s3://udacity-dend/log_data",
    load_table="staging_events",
    file_type="json",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    source_location="s3://udacity-dend/song_data",
    load_table="staging_songs",
    file_type="json"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stat=SqlQueries.songplay_table_insert,
    load_table="songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stat=SqlQueries.user_table_insert,
    load_table="users",
    should_truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stat=SqlQueries.song_table_insert,
    load_table="songs",
    should_truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stat=SqlQueries.artist_table_insert,
    load_table="artists",
    should_truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',    
    dag=dag,
    redshift_conn_id="redshift",
    sql_stat=SqlQueries.time_table_insert,
    load_table="time",
    should_truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    sql_tests = [
        (SqlQueries.nulls_in_songs_table, 0),
        (SqlQueries.nulls_in_users_table, 0),
        (SqlQueries.nulls_in_artists_table, 0),
        (SqlQueries.nulls_in_time_table, 0),
        (SqlQueries.nulls_in_songplays_table, 0),
    ]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table]

[load_user_dimension_table, load_song_dimension_table, 
 load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
