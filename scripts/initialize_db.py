from db.db import connect_to_database


def check_connection():
    # Connect to the database
    connection = connect_to_database()
    if connection is None:
        return False
    # Check if the connection is successful
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            if result[0] == 1:
                return True
            else:
                return False
