from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 target_table,
                 s3_bucket,
                 s3_folder,
                 aws_region = 'us-east-1',
                 redshift_conn_id = "redshift",
                 s3_format = 'json',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_folder = s3_folder
        self.s3_format = s3_format
        self.aws_region = aws_region
        self.redshift_conn_id = redshift_conn_id
        self.s3_format = s3_format
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_credentials=metastoreBackend.get_connection("aws_credentials")
        aws_login = aws_credentials.login
        aws_password = aws_credentials.password
        self.log.info('Fetched AWS credentials')

        s3_path = f"s3://{self.s3_bucket}/{self.s3_folder}"

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        input_format = "FORMAT AS JSON 'auto'" if self.s3_format else ""

        self.log.info(f"Staging S3 data to {self.target_table}")
        copy_from_s3 = (
            f"COPY {self.target_table} "
            f"FROM '{s3_path}' "
            f"ACCESS_KEY_ID '{aws_login}' "
            f"SECRET_ACCESS_KEY '{aws_password}' "
            f"REGION '{self.aws_region}' "
            f"TIMEFORMAT as 'epochmillisecs' "
            f"{input_format};"
        )
        self.log.info(copy_from_s3)
        redshift.run(copy_from_s3)
        self.log.info(f"Data staged to {self.target_table}")
