import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class LoadDimensionOperator(BaseOperator):
    """
    Loads dimension table in Redshift from staging table data.
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_table_sql="",
                 append_insert=False,
                 primary_key="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_table_sql = load_table_sql
        self.append_insert = append_insert
        self.primary_key = primary_key

    def execute(self, context):

        self.log.info("Connected with " + self.redshift_conn_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_insert:
            table_insert_sql = f"""
                create temp table stage_{self.table} (like {self.table});

                insert into stage_{self.table}
                {self.load_table_sql};

                delete from {self.table}
                using stage_{self.table}
                where {self.table}.{self.primary_key} = stage_{self.table}.{self.primary_key};

                insert into {self.table}
                select * from stage_{self.table};
            """
        else:
            table_insert_sql = f"""
                insert into {self.table}
                {self.load_table_sql}
            """

            redshift_hook.run(f"TRUNCATE TABLE {self.table};")

        self.log.info("Loading data into dimension table in Redshift")
        redshift_hook.run(table_insert_sql)
