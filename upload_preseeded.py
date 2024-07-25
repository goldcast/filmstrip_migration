import argparse
import concurrent
import json
import os
import pandas as pd
from sqlalchemy import create_engine
import boto3

from utils.filmstrip import upload_files_to_s3, check_file_in_s3, FILMSTRIP_INDEX_FILE

secrets_manager = boto3.client("secretsmanager")
STATIC_ASSETS_BUCKET = "staticassets.goldcast.com"
S3_KEY_PRESEEDED_BASE_PATH = "content-lab/filmstrip/pre-seeded/filmstrip_index.json"
S3_KEY_BASE_PATH = "content-lab/filmstrip/{}/{}/filmstrip_index.json"

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
         "days' and deleted is null and av_type='VIDEO' and is_sample_upload='true'")
# Read the data into a pandas DataFrame
df = pd.read_sql(query.format(days), engine)


def download_file_from_s3(local_file_name):
    s3 = boto3.client('s3')
    s3.download_file(STATIC_ASSETS_BUCKET, S3_KEY_PRESEEDED_BASE_PATH, local_file_name)
    print(f"{local_file_name} has size: {os.path.getsize(local_file_name)}")
    return local_file_name


# Function to apply
def process_row(row):
    project_id = row['project_id']
    content_id = row['content_id']
    if not check_file_in_s3(STATIC_ASSETS_BUCKET,
                            S3_KEY_BASE_PATH.format(project_id, content_id, FILMSTRIP_INDEX_FILE)):
        upload_files_to_s3(STATIC_ASSETS_BUCKET, S3_KEY_BASE_PATH.format(project_id, content_id), "index.json")


download_file_from_s3("index.json")
# Apply the function to each row
with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(process_row, [row for _, row in df.iterrows()])

os.remove("index.json")
# Close the connection
engine.dispose()
