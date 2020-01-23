from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Sparkify',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 11),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('etl_pipeline_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="s3://udacity-dend/log_data",
    data_format="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'",
    create_stmt=SqlQueries.staging_events_table_create
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="s3://udacity-dend/song_data",
    data_format="JSON 'auto'",
    create_stmt=SqlQueries.staging_songs_table_create
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    append=False,
    table="songplays",
    create_stmt=SqlQueries.songplay_table_create,
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    append=False,
    table="users",
    aws_credentials_id="aws_credentials",
    create_stmt=SqlQueries.user_table_create,
    sql_query=SqlQueries.user_table_insert

)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    append=False,
    table="songs",
    aws_credentials_id="aws_credentials",
    create_stmt=SqlQueries.song_table_create,
    sql_query=SqlQueries.song_table_insert

)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    append=False,
    table="artists",
    aws_credentials_id="aws_credentials",
    create_stmt=SqlQueries.artist_table_create,
    sql_query=SqlQueries.artist_table_insert

)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    append=False,
    table="times",
    aws_credentials_id="aws_credentials",
    create_stmt=SqlQueries.time_table_create,
    sql_query=SqlQueries.time_table_insert

)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    tables=["users","songs","artists","times"],
    test=SqlQueries.data_quality_count_test,
    test_error_msg="The table {} returned zero results"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator              >> stage_events_to_redshift
start_operator              >> stage_songs_to_redshift
stage_events_to_redshift    >> load_songplays_table
stage_songs_to_redshift     >> load_songplays_table
load_songplays_table        >> load_user_dimension_table
load_songplays_table        >> load_song_dimension_table
load_songplays_table        >> load_artist_dimension_table
load_songplays_table        >> load_time_dimension_table
load_user_dimension_table   >> run_quality_checks
load_song_dimension_table   >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table   >> run_quality_checks
run_quality_checks          >> end_operator