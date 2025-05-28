import trace
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from posix import truncate

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 target_table,
                 select_data_sql,
                 redshift_conn_id = "redshift",
                 truncate_table = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.select_data_sql = select_data_sql
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f'Truncating dimension table: {self.target_table}')
            truncate_sql = f"TRUNCATE TABLE {self.target_table}"
            redshift.run(truncate_sql)
            self.log.info(f'Truncated dimension table: {self.target_table}')

        self.log.info(f'Populating the dimension table: {self.target_table}')
        insert_into_fact_table_sql = F"INSERT {self.target_table} {self.select_data_sql}"
        redshift.run(insert_into_fact_table_sql)
        self.log.info('Populated the fact table')
