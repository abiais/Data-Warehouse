import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ This function executes the drop_tables queries imported from the sql_queries.py file in our session in the redshift database

    Args:
        cur : is the cursor that allows python to execute the redshift commands in the database session 
        conn : is our connection to the redshift database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ This function executes the def create_tables queries imported from the sql_queries.py file in our session in the redshift database

    Args:
        cur : is the cursor that allows python to execute the redshift commands in the database session 
        conn : is our connection to the redshift database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()