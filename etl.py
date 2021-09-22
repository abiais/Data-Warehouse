import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ This function executes the load_staging_tables queries imported from the sql_queries.py file in our session in the redshift database

    Args:
        cur : is the cursor that allows python to execute the redshift commands in the database session 
        conn : is our connection to the redshift database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ This function executes the insert_tables queries imported from the sql_queries.py file in our session in the redshift database

    Args:
        cur : is the cursor that allows python to execute the redshift commands in the database session 
        conn : is our connection to the redshift database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()