from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_query = """INSERT INTO {} {}"""
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 sql="",
                 append=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.sql = sql
        self.append = append
        
    def execute(self, context):
        self.log.info(f"LoadFactOperator for table '{self.table}' starting...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append:
            self.log.info(f"Clearing data from fact table '{self.table}'...")
            redshift.run(f"DELETE FROM {self.table}")

        redshift.run(LoadFactOperator.insert_query.format(self.table, self.sql))
        
        self.log.info(f"LoadFactOperator for table '{self.table}' complete.")
