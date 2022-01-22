import argparse
import os
import time
import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta


def ingest_data(user, password, host, port, db, csv_name, table_name):
    engine = create_engine(f"postgresql+pg8000://{user}:{password}@{host}/{db}", client_encoding='utf8')

    print("writing to database")
    with pd.read_csv(csv_name, parse_dates=["tpep_pickup_datetime","tpep_dropoff_datetime"], chunksize=100000) as reader:
        for idx,df in enumerate(reader):
            start_time = time.monotonic()
            df.to_sql(name=table_name,con=engine, if_exists="append", index=False)
            print(f"Wrote Chunk {idx+1} in {timedelta(seconds=time.monotonic() - start_time)} seconds ..")


def main(args):
    user = args.user
    password = args.password
    host = args.host 
    port = args.port 
    db = args.db
    table_name = args.table_name
    url = args.url
    csv_name = 'output.csv'

    # Fetching data
    os.system(f"wget {url} -O {csv_name}")

    # Ingesting Data
    ingest_data(user, password, host, port, db, csv_name, table_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', help='user name for postgres', required=True)
    parser.add_argument('--password', help='password for postgres', required=True)
    parser.add_argument('--host', help='host for postgres', required=True)
    parser.add_argument('--port', help='port for postgres', required=True)
    parser.add_argument('--db', help='database name for postgres', required=True)
    parser.add_argument('--table_name', help='name of the table where we will write the results to', required=True)
    parser.add_argument('--url', help='url of the csv file', required=True)
    args = parser.parse_args()
    main(args)