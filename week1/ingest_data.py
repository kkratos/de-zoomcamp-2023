#!/usr/bin/env python
# coding: utf-8

import os
from sqlalchemy import create_engine
from time import time
import pandas as pd
import argparse


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name,
                          iterator=True, chunksize=100000, low_memory=False)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("End of data.")
            break  # Break the loop when StopIteration is raised

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk..., took %.3f second' % (t_end - t_start))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description="Ingest CSV data into Postgres")

    # User, Password, host, port, databasename, table name, url for the csv

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True,
                        help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True,
                        help='database name for postgres')
    parser.add_argument('--table_name', required=True,
                        help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()
    main(args)

# query = """
# SELECT 1 as number;
# """

# pd.read_sql(query, con=engine)

# query = """
# SELECT *
# FROM pg_catalog.pg_tables
# WHERE schemaname != 'pg_catalog' AND
#     schemaname != 'information_schema';
# """

# pd.read_sql(query, con=engine)


# query = """
# SELECT * FROM yellow_taxi_data LIMIT 10
# """

# pd.read_sql(query, con=engine)


# ```sql
# SELECT *
# FROM pg_catalog.pg_tables
# WHERE schemaname != 'pg_catalog' AND
#     schemaname != 'information_schema';
# ```
# Source: https://www.postgresqltutorial.com/postgresql-show-tables/
#
#
