import json
import logging
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

logger = logging.getLogger(__name__)
FILMSTRIP_FPS = 2
FILMSTRIP_INDEX_FILE = "filmstrip_index.json"
FILMSTRIP_INDEX_KEY = "filmstrip_file_names"
S3_KEY_BASE_PATH = "content-lab/filmstrip/{}/{}/{}"
OUTPUT_DIRECTORY = "downloads/{}/{}"
STATIC_ASSETS_BUCKET = "staticassets.goldcast.com"
NUM_PARALLEL_UPLOADS = 5


def generate_filmstrip(input_file, project_id, content_id):
    output_directory = OUTPUT_DIRECTORY.format(project_id, content_id)
    os.makedirs(output_directory, exist_ok=True)
    # Generate multiple images with filmstrip of the complete video file where each image contains 30 frames
    cmd = [
        "ffmpeg",
        "-i",
        input_file,
        "-vf",
        f"fps={FILMSTRIP_FPS},scale=200:-1,tile=5x6",
        f"{output_directory}/filmstrip_%04d.png",
        "-y",
    ]
    logger.info(f"command : {cmd}")
    try:
        subprocess.run(cmd, check=True)
        convert_png_to_webp(output_directory)

    except Exception as err:
        logger.exception(f"An error occurred while generating the film strip: {err}")


def convert_png_to_webp(output_directory):
    for root, _, files in os.walk(output_directory):
        for file in files:
            if file.endswith(".png"):
                file_path = os.path.join(root, file)
                cmd = [
                    "ffmpeg",
                    "-i",
                    file_path,
                    "-c:v",
                    "libwebp",
                    f"{output_directory}/{os.path.splitext(os.path.basename(file_path))[0]}.webp",
                    "-y",
                ]
                subprocess.run(cmd, check=True)


def upload_filmstrip_to_s3(project_id, content_id):
    try:
        output_directory = OUTPUT_DIRECTORY.format(project_id, content_id)
        with ThreadPoolExecutor(max_workers=NUM_PARALLEL_UPLOADS) as executor:
            filmstrip_file_paths = []
            for root, _, files in os.walk(output_directory):
                for file in files:
                    if file.endswith(".webp"):
                        file_path = os.path.join(root, file)
                        s3_file_key = S3_KEY_BASE_PATH.format(project_id, content_id, file)
                        filmstrip_file_paths.append(s3_file_key)
                        executor.submit(upload_files_to_s3, STATIC_ASSETS_BUCKET, s3_file_key, file_path)

            filmstrip_index_filepath = os.path.join(output_directory, FILMSTRIP_INDEX_FILE)
            s3_index_file_key = store_filmstrip_index_in_json(project_id, content_id, filmstrip_index_filepath,
                                                              filmstrip_file_paths)
            executor.submit(upload_files_to_s3, STATIC_ASSETS_BUCKET, s3_index_file_key, filmstrip_index_filepath)

    except Exception as err:
        logger.exception(f"An error occurred while uploading the film strip to S3 bucket: {err}")


def store_filmstrip_index_in_json(project_id, content_id, filmstrip_index_filepath, filmstrip_file_paths):
    # Creating dictionary to store the index files
    filmstrip_file_paths.sort()
    data = {FILMSTRIP_INDEX_KEY: filmstrip_file_paths}

    with open(filmstrip_index_filepath, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    s3_file_key = S3_KEY_BASE_PATH.format(project_id, content_id, FILMSTRIP_INDEX_FILE)
    return s3_file_key


def upload_files_to_s3(bucket_name, s3_file_key, local_file_name):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_file_name, bucket_name, s3_file_key)
        print(f"File uploaded successfully to {bucket_name}/{s3_file_key}")
    except FileNotFoundError:
        print(f"The file {local_file_name} was not found.")
    except NoCredentialsError:
        print("Credentials not available or incorrect.")


def check_file_in_s3(bucket_name, s3_file_key):
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=s3_file_key)
        file_content = response['Body'].read().decode('utf-8')
        print(f"Successfully read JSON file from {bucket_name}/{s3_file_key}")
        return len(json.loads(file_content)['filmstrip_file_names']) > 0
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"File not found: {s3_file_key}")
        else:
            print(f"Error occurred: {e}")
        return False
