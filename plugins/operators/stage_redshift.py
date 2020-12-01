from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION '{}';
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 table="",
                 create="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 json_type="",
                 region="",
                 append=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create = create
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.json_type = json_type
        self.region = region
        self.append = append

    def execute(self, context):
        self.log.info('StageToRedshiftOperator starting...')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
       
        if not self.append:
            self.log.info(f"Clearing data from staging table '{self.table}'...")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Copying data from S3 to Redshift to table '{self.table}'")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_type,
            self.region
        )

        redshift.run(formatted_sql)
        self.log.info('StageToRedshiftOperator complete')
