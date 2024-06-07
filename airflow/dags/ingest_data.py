import sys
import argparse
import subprocess
from time import time

import pandas as pd
from sqlalchemy import create_engine
import logging
from pydantic import BaseModel, validator, PostgresDsn

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('ingestion_logger')


class Params(BaseModel):
    user: str
    password: str
    host: str
    port: int
    db: str
    table_name: str
    url: str

    @validator('port')
    def valid_port(cls, v):
        if not (1 <= v <= 65535):
            raise ValueError('port outside the range')
        return v

    @property
    def postgres_dsn(self) -> PostgresDsn:
        return PostgresDsn.build(
            scheme='postgresql',
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            path=f"/{self.db}"
        )


def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
    
    logger.info(execution_date)

    # Create database engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read CSV in chunks
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000, compression='gzip')
    df = next(df_iter)

    # Convert datetime columns
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Create table with the schema of the DataFrame
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Insert the first chunk
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()
            df = next(df_iter)

            # Convert datetime columns
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            # Insert chunk into the database
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()
            logger.info(f'Inserted another chunk, took {t_end - t_start:.3f} seconds')

        except StopIteration:
            logger.info("Finished ingesting data into the PostgreSQL database")
            break


def main(params: Params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # Determine the output CSV filename
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    logger.info('Starting file download...')
    result = subprocess.run(['wget', url, '-O', csv_name], capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error('File download unsuccessful')
        sys.exit(1)
    
    logger.info('File downloaded successfully')

    ingest_callable(user=user, password=password, host=host, port=port, db=db, table_name=table_name, csv_file=csv_name, execution_date=time())

if __name__ == "__main__":
    print('works')
    parser = argparse.ArgumentParser(description='Ingest CSV data into PostgreSQL')
    parser.add_argument('--user', required=True, help='PostgreSQL user')
    parser.add_argument('--password', required=True, help='PostgreSQL password')
    parser.add_argument('--host', required=True, help='PostgreSQL host')
    parser.add_argument('--port', required=True, help='PostgreSQL port')
    parser.add_argument('--db', required=True, help='PostgreSQL database name')
    parser.add_argument('--table_name', required=True, help='Target table name')
    parser.add_argument('--url', required=True, help='URL of the CSV file')

    args = parser.parse_args()

    params = Params(
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
        db=args.db,
        table_name=args.table_name,
        url=args.url
    )

    main(params=params)
