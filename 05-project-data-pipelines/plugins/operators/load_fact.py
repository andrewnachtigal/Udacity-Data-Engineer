import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class LoadFactOperator(BaseOperator):
    """
    Loads fact table in Redshift from data in staging table(s)
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_table_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_table_sql = load_table_sql

    def execute(self, context):

        self.log.info("Connected with " + self.redshift_conn_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Loading data into fact table in Redshift")
        table_insert_sql = f"""
            INSERT INTO {self.table}
            {self.load_table_sql}
        """
        redshift_hook.run(table_insert_sql)
