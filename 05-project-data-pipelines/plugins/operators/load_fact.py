from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

import logging
log = logging.getLogger(__name__)


class LoadFactOperator(BaseOperator):
    """
    Loads fact table in Redshift from data in staging table(s)
        
    Parameters:
        Redshift connection ID
        Target table in Redshift to load
        SQL query for getting data to load into target table
    """
    
    ui_color = '#F98866'
    
    @apply_defaults
    
    def __init__(self,
                 redshift_conn_id = "redshift",
                 targ_table = "",
                 sql_stat = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.targ_table = targ_table
        self.sql_stat = sql_stat
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading into fact table...")
        
        sql_stmt = f"""
            INSERT INTO {self.targ_table}
            {self.sql_stat}
        """
        
        #sql_stmt = f"INSERT INTO {self.targ_table} ({self.sql_stat})"
        #sql_stmt = 'INSERT INTO %s %s' % (self.targ_table, self.sql_stat)
        
        redshift.run(sql_stmt)
        self.log.info("Completed data load to fact table.")