import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class DataQualityOperator(BaseOperator):
    """
    Runs data quality check by passing test SQL query and expected result

    :param redshift_conn_id: Redshift connection ID
    :param test_query: SQL query to run on Redshift data warehouse
    :param expected_result: Expected result to match against result of
        test_query
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 test_query="",
                 expected_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.test_query = test_query
        self.expected_result = expected_result

    def execute(self, context):

        self.log.info("Connected with " + self.redshift_conn_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("testing...")
        records = redshift_hook.get_records(self.test_query)
        if records[0][0] != self.expected_result:
            raise ValueError(f"Data quality check error.")
        else:
            self.log.info("Data quality check passed")
