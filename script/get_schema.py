from db import con
import psycopg2


def fetch_schema():
    """Fetch the database schema from PostgreSQL"""
    schema = {}
    try:
        cursor = con.cursor()
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            cursor.execute(
                f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
            )
            columns = cursor.fetchall()
            schema[table_name] = {column[0]: column[1] for column in columns}
        cursor.close()
    except psycopg2.Error as e:
        print("Error fetching schema:", e)
    return schema
