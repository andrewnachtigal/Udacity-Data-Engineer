
import configparser
import psycopg2
from sql_tables import drop_table_queries, create_table_queries


def drop_tables(cur, conn):
    """Function drops existing tables listed in 'drop_table_queries' list from 'sql_tables.py'."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Function creates tables listed in 'create_table_queries' list from 'sql_tables.py'."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


    
def main():
    """Function connects to AWS Redshift; runs 'drop_tables' and 'create_tables' functions."""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Dropping Tables...')
    drop_tables(cur, conn)
    print('Dropped Tables, Creating Tables')
    create_tables(cur, conn)
    print('Created Tables')

    conn.close()

if __name__ == "__main__":
    main()