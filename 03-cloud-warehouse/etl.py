import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loop over copy_table_queries. copies data from S3 to staging tables."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Loop over insert_table_queries. insert data into Fact and Dimnsion tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """MAIN function creates connection to redshift cluster and creates cursor.
    Calls functions to load staging tables.
    Inserts records in fact and dimension tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()