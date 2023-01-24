import psycopg2
from unidecode import unidecode
import os


def connect_to_database():
    try:
        # Connect to the database
        connection = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            port=5432,
        )
        return connection
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: ", error)
        return None


def execute_query(query, params=None, return_result=False):
    # Connect to the database
    connection = connect_to_database()
    # Execute the query and commit the changes
    with connection:
        with connection.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if return_result:
                result = cursor.fetchall()
        connection.commit()

    if return_result:
        return result


def create_database(database_name):
    connection = connect_to_database()
    # Check if the database already exists
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'"
            )
            exists = cursor.fetchone()

    # If the database does not exist, create it
    if not exists:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE {database_name}")


def remove_accents(s):
    return unidecode(s)
