from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements


default_args = {
    'owner': 'Sparkify',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 31),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


@dag(default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
     schedule_interval='@hourly')
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        target_table='staging_events',
        s3_bucket='omfc-sparkify',
        s3_folder='log-data'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        target_table='staging_songs',
        s3_bucket='omfc-sparkify',
        s3_folder='song-data'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        target_table='songplays_fact',
        select_data_sql='songplay_table_insert'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        target_table='songplays_fact',
        select_data_sql='songplay_table_insert'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        target_table='songs_dim',
        select_data_sql='song_table_insert'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        target_table='artists_dim',
        select_data_sql='artist_table_insert'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        target_table='time_dim',
        select_data_sql='time_table_insert'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        check_records_exist_on_tables=['songs_dim', 'artists_dim', 'time_dim']
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, 
    load_artist_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks  
    run_quality_checks >> end_operator
    
final_project_dag = final_project()