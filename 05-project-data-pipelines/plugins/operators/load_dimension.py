from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

import logging
log = logging.getLogger(__name__)


class LoadDimensionOperator(BaseOperator):
    """
    Loads dimension table in Redshift from staging table data.
    
    Parameters:
        Redshift connection ID
        Target table in Redshift to load
        SQL query for getting data to load into target table
    """
    
    ui_color = '#80BD9E'
    @apply_defaults
    
    def __init__(self,
                 redshift_conn_id = "redshift",
                 should_truncate = True,
                 targ_table = "",
                 sql_stat = "",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.should_truncate = should_truncate
        self.sql_stat = sql_stat
        self.targ_table = targ_table
        self.append_only = append_only

        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading data to dimension table.")
        self.log.info("Append only mode.")
        if self.append_only:
            sql_stmt = 'INSERT INTO %s %s' % (self.targ_table, self.sql_stat)
            redshift.run(sql_stmt)
        else:
            sql_del_stmt = 'DELETE FROM %s' % (self.targ_table)
            redshift.run(sql_del_stmt)
            sql_stmt = 'INSERT INTO %s %s' % (self.targ_table, self.sql_stat)
            redshift.run(sql_stmt)
        self.log.info("Completed data load to dimension table.")
        