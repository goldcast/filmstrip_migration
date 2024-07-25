import argparse
import concurrent
import functools
import json

import pandas as pd
from sqlalchemy import create_engine
import boto3

from utils.cleanup import cleanup_directory
from utils.filmstrip import generate_filmstrip, upload_filmstrip_to_s3, check_file_in_s3, STATIC_ASSETS_BUCKET, \
    S3_KEY_BASE_PATH, FILMSTRIP_INDEX_FILE
from utils.recordings import download_m3u8_and_ts_files


def process_row(env, simulive_server, row):
    event_id = row['event_id']
    broadcast_id = row['broadcast_id']
    if not check_file_in_s3(STATIC_ASSETS_BUCKET,
                            S3_KEY_BASE_PATH.format(event_id, broadcast_id, FILMSTRIP_INDEX_FILE)):
        try:
            downloaded_file_name = download_m3u8_and_ts_files(
                event_id, broadcast_id, env, simulive_server
            )
            generate_filmstrip(downloaded_file_name, event_id, broadcast_id)
            upload_filmstrip_to_s3(event_id, broadcast_id)
        except Exception as ex:
            print(f"Exception: {ex}")
        finally:
            cleanup_directory(event_id, broadcast_id)


if __name__ == "__main__":
    secrets_manager = boto3.client("secretsmanager")

    VES_TOKEN = secrets_manager.get_secret_value(
        SecretId="prod/content-lab-credentials"
    )["SecretString"]
    mediastore_endpoint = (
        "https://uago73t2my3lb2.data.mediastore.us-east-1.amazonaws.com"
    )
    simulive_server = "https://stream.goldcast.io"

    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=str, default="prod")
    parser.add_argument("--days", type=str, default="7")
    args = parser.parse_args()
    env = args.env
    days = args.days
    db_username = None
    db_password = None
    db_host = None
    db_port = None
    db_name = None

    if env == "prod":
        simulive_server = "https://stream.goldcast.io"
        prod_creds = secrets_manager.get_secret_value(
            SecretId="prod/content-lab-db/readonly"
        )["SecretString"]
        db = json.loads(prod_creds)
        db_username = db["USER"]
        db_password = db["PASSWORD"]
        db_host = db["HOST"]
        db_port = db["PORT"]
        db_name = db["NAME"]

    else:
        simulive_server = "https://stream.alpha.goldcast.io"
        alpha_creds = secrets_manager.get_secret_value(
            SecretId="alpha-content-lab-db/readonly"
        )["SecretString"]
        db = json.loads(alpha_creds)
        db_username = db["USER"]
        db_password = db["PASSWORD"]
        db_host = db["HOST"]
        db_port = db["PORT"]
        db_name = db["NAME"]

    row_processor = functools.partial(process_row, env, simulive_server)
    # Ensure that all necessary environment variables are set
    if not all([db_username, db_password, db_host, db_port, db_name]):
        raise ValueError("One or more environment variables are missing. Please set DB_USERNAME, DB_PASSWORD, DB_HOST, "
                         "DB_PORT, and DB_NAME.")

    # Create a connection string for PostgreSQL
    connection_string = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

    # Create a database engine
    engine = create_engine(connection_string)

    # Define the SQL query to read data from the content_upload table
    query = ("select id as broadcast_id, project_id as event_id from media_content WHERE end_time >= NOW() - INTERVAL "
             "'{}"
             "days' and batch_status='DONE' and type='RECORDING' and media_type='VIDEO'")
    # Read the data into a pandas DataFrame
    df = pd.read_sql(query.format(days), engine)

    # Display the first few rows of the DataFrame
    print("Data from content_upload table:")
    print(df.head())

    # Apply the function to each row
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(row_processor, [row for _, row in df.iterrows()])

    # Close the connection
    engine.dispose()
