import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads JSON input data from Udacity's S3 buckets and inserts into the staging_songs
    and staging_events tables.
    """
    print('Loading stage tables...')
    for query in copy_table_queries:
        print('Running query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('-' * 25)


def insert_tables(cur, conn):
    """Inserts data from the staging_songs and staging_events tables into
    the analytics tables.
    """
    print('Inserting into analytics tables...')
    for query in insert_table_queries:
        print('Running query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('-' * 25)


def main():
    """Makes a connection to the specified database, loads the
    staging tables then inserts from staging tables into the
    analytic tables.
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