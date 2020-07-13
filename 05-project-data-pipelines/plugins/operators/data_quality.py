from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging
log = logging.getLogger(__name__)

class DataQualityOperator(BaseOperator):
    """
    Runs data quality check by passing test SQL query and expected result.
    """
    ui_color = '#89DA59'
    @apply_defaults
    
    def __init__(self,
                 redshift_conn_id = "redshift",
                 sql_tests = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_tests = sql_tests
        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for (sql_stat, expected_result) in self.sql_tests:
            row = redshift_hook.get_first(sql_stat)
            if row is not None:
                if row[0] == expected_result:
                    self.log.info("Test Passed: {}\nResult == {}\n=".format(sql_stat, expected_result))
                else:
                    raise ValueError("Test Failed: {}\nResult != {}\n=".format(sql_stat, expected_result))