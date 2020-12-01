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
    'owner': 'asher_cornelius',
    'start_date': datetime(2020, 12, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}


# set DAG schedule_interval to None for development, or '@hourly' submittal
with DAG("sparkify_ETL_dag", default_args=default_args, description="Load and transform data in Redshift with Airflow", schedule_interval=None) as dag: 
    
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        create="staging_events_table_create",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        json_type=f"s3://{s3_bucket}/log_json_path.json",
        region="us-west-2",
        append=False
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        create="staging_songs_table_create",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="song_data/A/A",
        json_type="auto",
        region="us-west-2",
        append=False
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="songplays",
        sql=SqlQueries.songplay_table_insert,
        append=False
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="users",
        sql=SqlQueries.user_table_insert,
        append=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="songs",
        sql=SqlQueries.song_table_insert,
        append=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="artists",
        sql=SqlQueries.artist_table_insert,
        append=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="time",
        sql=SqlQueries.time_table_insert,
        append=False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        quality_checks=[
            {'sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0},
            {'sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
            {'sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
            {'sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},
            {'sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0},

            {'sql': "SELECT COUNT(*) FROM songplays WHERE playid is NOT null", 'expected_result': -1},
            {'sql': "SELECT COUNT(*) FROM users WHERE userid is NOT null", 'expected_result': -1},
            {'sql': "SELECT COUNT(*) FROM songs WHERE songid is NOT null", 'expected_result': -1},
            {'sql': "SELECT COUNT(*) FROM artists WHERE artistid is NOT null", 'expected_result': -1},
            {'sql': "SELECT COUNT(*) FROM time WHERE start_time is NOT null", 'expected_result': -1}

        ]
    )

    end_operator = DummyOperator(task_id='Stop_execution')


# task dependencies

start_operator >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
