import argparse
import concurrent
import json
import os
import pandas as pd
from sqlalchemy import create_engine
import boto3

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


def copy_s3_file(source_bucket, source_key, destination_bucket, destination_key):
    try:
        s3 = boto3.client('s3')
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
        print(f'Successfully copied {source_key} from {source_bucket} to {destination_bucket}/{destination_key}')
    except Exception as e:
        print(f"Error: {e}")


# Function to apply
def process_row(row):
    project_id = row['project_id']
    content_id = row['content_id']
    copy_s3_file(STATIC_ASSETS_BUCKET, S3_KEY_PRESEEDED_BASE_PATH, STATIC_ASSETS_BUCKET, S3_KEY_BASE_PATH.format(project_id, content_id))


# Apply the function to each row
with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(process_row, [row for _, row in df.iterrows()])
# Close the connection
engine.dispose()
