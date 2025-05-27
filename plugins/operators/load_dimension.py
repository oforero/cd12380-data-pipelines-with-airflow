from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 target_table,
                 select_data_sql,
                 redshift_conn_id = "redshift",  
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.select_data_sql = select_data_sql

    def execute(self, context):
        self.log.info(f'Populating the fact table: {self.target_table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_into_fact_table_sql = F"INSERT {self.target_table} {self.select_data_sql}"
        redshift.run(insert_into_fact_table_sql)
        self.log.info('Populated the fact table')
