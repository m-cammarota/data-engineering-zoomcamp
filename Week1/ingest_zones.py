import pandas as pd
import argparse
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    csv_file = params.csv_file

    # Read the file
    print(f'Reading data from {csv_file}')
    df = pd.read_csv(csv_file)

    print(f'Ingesting data from {csv_file} into {table_name} table in {db} database')
    # Connect to PostgreSQL database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Insert the data into the database
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--csv_file', help='name of the csv file')


    args = parser.parse_args()

    main(args) 
    
    """
    python ingest_zones.py `
    --user=postgres `
    --password=postgres `
    --host=localhost `
    --port=5433 `
    --db=ny_taxi `
    --table_name=zone  `
    --csv_file=./data/taxi_zone_lookup.csv
    
    """