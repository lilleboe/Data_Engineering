from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",    
                 create_stmt = "",
                 sql_query = "", 
                 append = False,
                 *args, **kwargs):
                    
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table            = table
        self.create_stmt      = create_stmt
        self.sql_query        = sql_query
        self.append           = append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Creating the {} fact table if it doesn't exist...".format(self.table))
        redshift.run(self.create_stmt)
        if not self.append:
            self.log.info("Delete the {} fact table data".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))         
        self.log.info("Inserting data into the {} fact table".format(self.table))
        redshift.run(self.sql_query)
        