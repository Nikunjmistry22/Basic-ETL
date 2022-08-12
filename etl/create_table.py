import psycopg2
from sql_queries import create_table_queries,drop_table_queries

def create_database():
    try:
        conn=psycopg2.connect(host="localhost",dbname="postgres",user="postgres",password="nikunj22")
    except psycopg2.Error as e:
        print("Error: Could not connect the postgresql database")
        print(e)

    try:
        curr=conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not connect the postgresql database")
        print(e)
    conn.set_session(autocommit=True)

    # create sparkify database with UTF8 encoding
    try:
        curr.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        print("Error: Could not drop Database")
        print(e)

    try:
        curr.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print("Error: Could not create Database")
        print(e)

        # close connection to default database
    conn.close()

    # connect to sparkify database
    try:
        conn = psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=nikunj22")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        curr = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    return curr, conn


def drop_tables(curr, conn):
    """Drops all tables defined in `sql_queries.drop_table_queries`.

    Args:
        cur (psycopg2.cursor): A database cursor
        conn (psycopg2.connection): A database connection
    """
    for query in drop_table_queries:
        try:
            curr.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Could not drop table from query: {}".format(query))
            print(e)


def create_tables(curr, conn):
    """Creates all tables defined in `sql_queries.create_table_queries`.

    Args:
        cur (psycopg2.cursor): A database cursor
        conn (psycopg2.connection): A database connection
    """
    for query in create_table_queries:
        try:
            curr.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Could not create table from query: {}".format(query))
            print(e)


def main():
    curr, conn = create_database()

    drop_tables(curr, conn)
    create_tables(curr, conn)

    curr.close();
    conn.close()

if __name__ == "__main__":
    main()