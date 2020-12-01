from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowSkipException

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.quality_checks = quality_checks

    def execute(self, context):
        self.log.info('DataQualityOperator starting....')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # quality check borrowed from https://knowledge.udacity.com/questions/54406
        for check in self.quality_checks:
            sql = check.get('sql')
            exp_result = check.get('expected_result')
 
            records = redshift.get_records(sql)[0]
 
            # when exp_result is -1 this simply means we want at least one record, but we don't know how many exactly
            if (exp_result != -1 and exp_result != records[0]) or (exp_result == -1 and records[0] == 0):
                self.log.warn('**********************')
                self.log.warn('**** Tests failed ****')
                self.log.warn('**********************')
#                 raise ValueError(f"Data quality check failed for SQL {sql}, expected {exp_result} but got {records[0]}")
                raise AirflowFailException(f"Data quality check failed for SQL {sql}, expected {exp_result} but got {records[0]}")
            self.log.info(f"---  :) Quality checked passed :)  ---")
                