import psycopg2
import logging
import clickhouse_connect
from datetime import datetime, timedelta
from config import clickhouse_config, postgres_config, select_query, insert_query

log = logging.getLogger(__name__)

def clickhouse_command(client, query, query_params):
    response = client.command(query, params=query_params)
    return response.result_rows

def insert_data_to_postgres(connection, insert_query, data):
    cursor = connection.cursor()
    for row in data:
        cursor.execute(insert_query, row)
    connection.commit()
    cursor.close()

def main():

    try:
        # Clickhouse connection
        ch_client = clickhouse_connect.get_client(
            host=clickhouse_config['host'],
            port=clickhouse_config['port'],
            username=clickhouse_config['user'],
            password=clickhouse_config['password']
        )

        # Postgres connection
        pg_conn = psycopg2.connect(
            host=postgres_config['host'],
            port=postgres_config['port'],
            dbname=postgres_config['dbname'],
            user=postgres_config['user'],
            password=postgres_config['password']
        )

        # Get the date for the query (yesterday's date)
        query_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

        # SELECT data from ClickHouse
        ch_data = clickhouse_command(ch_client, select_query, {'date': query_date})

        # Insert data into PostgreSQL
        insert_data_to_postgres(pg_conn, insert_query, ch_data)

        # Close connections
        pg_conn.close()
        ch_client.disconnect()

    except Exception as e:
        print(f" Exception: {e}")

if __name__ == "__main__":
    main()