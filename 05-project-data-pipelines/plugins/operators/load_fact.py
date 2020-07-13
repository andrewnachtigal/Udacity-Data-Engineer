from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

import logging
log = logging.getLogger(__name__)


class LoadFactOperator(BaseOperator):
    """
    Loads fact table in Redshift from data in staging table(s)
    """
    ui_color = '#F98866'
    @apply_defaults
    
    def __init__(self,
                 redshift_conn_id = "redshift",
                 sql_stat = "",
                 load_table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stat = sql_stat
        self.load_table = load_table
        
    def execute(self, context):
        self.log.info("Starting data load to fact table.")
        redshift_hook = PostgresHook(self.redshift_conn_id)        
        redshift_hook.run(self.sql_stat)
        self.log.info("Complete data insert to fact table: {}".format(self.load_table))