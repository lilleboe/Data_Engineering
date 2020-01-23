from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql_date = """
        COPY {} 
        FROM '{}/{}/{}/'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {};
    """
    copy_sql = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {};
    """
    
    @apply_defaults
    def __init__(self,
                 # Define the operators params 
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 execution_date="",
                 data_format="",
                 create_stmt="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map the params
        self.redshift_conn_id   = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table              = table
        self.s3_bucket          = s3_bucket
        self.data_format        = data_format
        self.create_stmt        = create_stmt
        self.execution_date     = kwargs.get('execution_date')
        self.log.info("Attributes assigned...")

    def execute(self, context):
        
        self.log.info("Connecting to AWS...")
        aws = AwsHook(self.aws_credentials_id)
        self.log.info("AWS hook completed")
        credentials = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Dropping table and recreating...")
        redshift.run(self.create_stmt)
        self.log.info("Copying data from S3 to Redshift...")
        # Backfill a specific date
        if self.execution_date:
            formatted_sql = StageToRedshiftOperator.copy_sql_time.format(
                self.table, 
                self.s3_bucket, 
                self.execution_date.strftime("%Y"),
                self.execution_date.strftime("%d"),
                credentials.access_key,
                credentials.secret_key, 
                self.data_format,
                self.execution_date
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table, 
                self.s3_bucket, 
                credentials.access_key,
                credentials.secret_key, 
                self.data_format,
                self.execution_date
            )
                
        self.log.info("Executing Redshift copy...")
        redshift.run(formatted_sql)
