from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",  
                 check_records_exist_on_tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_records_exist_on_tables = check_records_exist_on_tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('\n\n############ DATA QUALITY COUNT CHECKS ############')
        for table in self.check_records_exist_on_tables:
            query = f"SELECT COUNT(*) FROM {table}"
            records = redshift.get_records(query)
            
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data quality check failed. {query} returned no results")
                raise ValueError(f"Data quality check failed. {query} returned no results")
            
            self.log.info(f"Data quality on query {query} check passed with {records[0][0]} records\n\n")