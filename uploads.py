import argparse
import concurrent
import json

import pandas as pd
from sqlalchemy import create_engine
import boto3

from utils.cleanup import cleanup_directory
from utils.filmstrip import generate_filmstrip, upload_filmstrip_to_s3, check_file_in_s3, STATIC_ASSETS_BUCKET, \
    S3_KEY_BASE_PATH, FILMSTRIP_INDEX_FILE
from utils.media_processor import MediaProcessor


def process_row(row):
    project_id = row['project_id']
    content_id = row['content_id']
    s3_index_key = S3_KEY_BASE_PATH.format(project_id, content_id, FILMSTRIP_INDEX_FILE)
    if not check_file_in_s3(STATIC_ASSETS_BUCKET, s3_index_key):
        try:
            processor = MediaProcessor(
                project_id=project_id,
                content_id=content_id,
                media_type='VIDEO',
                mediastore_endpoint=mediastore_endpoint,
                ves_token=VES_TOKEN,
            )
            input_file = processor.process_media()
            generate_filmstrip(input_file, project_id, content_id)
            upload_filmstrip_to_s3(project_id, content_id)
        except Exception as ex:
            print(f"Exception: {ex}")
        finally:
            cleanup_directory(project_id, content_id)


if __name__ == "__main__":
    secrets_manager = boto3.client("secretsmanager")

    VES_TOKEN = secrets_manager.get_secret_value(
        SecretId="prod/content-lab-credentials"
    )["SecretString"]
    mediastore_endpoint = (
        "https://uago73t2my3lb2.data.mediastore.us-east-1.amazonaws.com"
    )

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
        alpha_creds = secrets_manager.get_secret_value(
            SecretId="alpha-content-lab-db/readonly"
    )["SecretString"]
        db = json.loads(alpha_creds)
        db_username = db["USER"]
        db_password = db["PASSWORD"]
        db_host = db["HOST"]
        db_port = db["PORT"]
        db_name = db["NAME"]

    # Ensure that all necessary environment variables are set
    if not all([db_username, db_password, db_host, db_port, db_name]):
        raise ValueError("One or more environment variables are missing. Please set DB_USERNAME, DB_PASSWORD, DB_HOST, "
                         "DB_PORT, and DB_NAME.")

    # Create a connection string for PostgreSQL
    connection_string = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

    # Create a database engine
    engine = create_engine(connection_string)

    # Define the SQL query to read data from the content_upload table
    query = ("select id as content_id, project_id from content_upload WHERE created_at >= NOW() - INTERVAL '{} "
             "days' and deleted is null and av_type='VIDEO' and import_source_type is null and import_url is null")

    # Read the data into a pandas DataFrame
    df = pd.read_sql(query.format(days), engine)

    # Display the first few rows of the DataFrame
    print("Data from content_upload table:")
    print(df.head())

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_row, [row for _, row in df.iterrows()])

    # Close the connection
    engine.dispose()
