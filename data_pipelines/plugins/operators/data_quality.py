from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define the operators params (with defaults) 
                 redshift_conn_id="",
                 tables=[],
                 test="",
                 test_error_msg="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map the params
        self.redshift_conn_id = redshift_conn_id
        self.tables           = tables
        self.test             = test
        self.test_error_msg   = test_error_msg
        
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            records = redshift.get_records(self.test.format(table))     
            if records[0][0] == 0:
                self.log.error(self.test_error_msg.format(table))
                raise ValueError(self.test_error_msg.format(table))
            self.log.info("The data quality check on table {} was successful...".format(table, records[0][0]))
            