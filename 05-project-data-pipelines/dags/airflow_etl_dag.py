from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# DAG default_args dict
default_args = {
    'owner': 'ajn',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 10),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('airflow_etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
          )

# Start Operator: start_execution task
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Task: stage data from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    s3_path="s3://udacity-dend/log_data",
    #json_path="s3://udacity-dend/log_json_path.json",
    file_type="JSON"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",=
    s3_key="song_data",
    s3_path="s3://udacity-dend/song_data",
    #json_path="auto",
    file_type="JSON"
)

# Task: load data to fact & dim tables
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.time_table_insert
)

# Data Quality Requirments:
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    test_query='select count(*) from songs where songid is null;',
    expected_result=0,
    dag=dag
)

# End Operator: end_execution task
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Pipeline Data Flow: Task Dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
