clickhouse_config = {
    "host": "localhost",
    "port": 8123,
    "user": "default",
    "password": ""
}

postgres_config = {
    "host": "localhost",
    "port": 5432,
    "dbname": "your_database",
    "user": "your_user",
    "password": "your_password",
}


select_query = """
    SELECT toStartOfDay(pickup_datetime) as day,
           payment_type,
           count(trip_id) as rides,
           floor(sum(total_amount), 2) as amount
    FROM default.trips
    WHERE toStartOfDay(pickup_datetime) = toDate(%(date)s)
    GROUP BY toStartOfDay(pickup_datetime), payment_type;
"""

insert_query = """
    INSERT INTO your_table (day, payment_type, rides, amount)
    VALUES (%(day)s, %(payment_type)s, %(rides)s, %(amount)s);
  """