from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

import logging
log = logging.getLogger(__name__)

class StageToRedshiftOperator(BaseOperator):
    '''
    Copy files from s3 to redshift.
    '''
    ui_color = '#358140'
    @apply_defaults
    
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_conn_id="aws_credentials",
                 source_location="",
                 load_table="",
                 file_type="json",
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)        
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.source_location = source_location
        self.load_table = load_table
        self.file_type = file_type
        self.json_path = json_path
        
    def execute(self, context):        
        if self.file_type in ["json", "csv"]:            
            redshift_hook = PostgresHook(self.redshift_conn_id)
            aws_hook = AwsHook(self.aws_conn_id)
            credentials = aws_hook.get_credentials()
            
            self.log.info(f'StageToRedshiftOperator loading data to staging table.')
            redshift_hook.run(SqlQueries.truncate_table.format(self.load_table))
            if self.file_type == "json":
                if self.json_path != "":
                    redshift_hook.run (
                        SqlQueries.copy_json_with_json_path_to_redshift.format (
                            self.load_table,
                            self.source_location,
                            credentials.access_key, 
                            credentials.secret_key,
                            self.json_path
                        )
                    )
                else:
                    redshift_hook.run (
                        SqlQueries.copy_json_to_redshift.format (
                            self.load_table,
                            self.source_location,
                            credentials.access_key, 
                            credentials.secret_key
                        )
                    )               
            elif self.file_type == "csv":
                redshift_hook.run (
                    SqlQueries.copy_csv_to_redshift.format (
                    self.load_table,
                    self.source_location,
                    credentials.access_key, 
                    credentials.secret_key)
                )

            self.log.info(f'StageToRedshiftOperator completed data load to staging table.')
        else:
            raise ValueError("file_type must be json or csv")
