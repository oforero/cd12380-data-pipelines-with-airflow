import pendulum
from airflow.decorators import dag

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from udacity.common.final_project_ddl_statements import CreateTables

@dag(start_date=pendulum.now(),
     max_active_runs=1)
def create_tables():
    
    ddl = CreateTables()
    start_operator = DummyOperator(task_id='Begin_execution')

    create_staging_events_table = PostgresOperator(
        task_id='create_staging_events_table',
        postgres_conn_id='redshift',
        sql=ddl.create_staging_events_table
    )

    create_staging_songs_table = PostgresOperator(
        task_id='create_staging_songs_table',
        postgres_conn_id='redshift',
        sql=ddl.create_staging_songs_table
    )

    create_songplays_fact_table = PostgresOperator(
        task_id='create_songplays_fact_table',
        postgres_conn_id='redshift',
        sql=ddl.create_songplays_fact_table
    )

    create_artists_dim_table = PostgresOperator(
        task_id='create_artists_dim_table',
        postgres_conn_id='redshift',
        sql=ddl.create_artists_dim_table
    )

    create_songs_dim_table = PostgresOperator(
        task_id='create_songs_dim_table',
        postgres_conn_id='redshift',
        sql=ddl.create_songs_dim_table
    )

    create_time_dim_table = PostgresOperator(
        task_id='create_time_dim_table',
        postgres_conn_id='redshift',
        sql=ddl.create_time_dim_table
    )

    create_users_dim_table = PostgresOperator(
        task_id='create_users_dim_table',
        postgres_conn_id='redshift',
        sql=ddl.create_users_dim_table
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> [create_staging_events_table, create_staging_songs_table] >> create_songplays_fact_table
    create_songplays_fact_table >> [create_artists_dim_table,
        create_songs_dim_table, create_time_dim_table, 
        create_users_dim_table] >> end_operator

create_tables_dag = create_tables()