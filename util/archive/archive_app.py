# archive_app.py
#
# Archive Free user data
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import requests
import sys
import time
import os
import shutil
from botocore.exceptions import ClientError, BotoCoreError
from flask import Flask, request, jsonify

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

app = Flask(__name__)
app.url_map.strict_slashes = False

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation
config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("../util_config.ini")
accounts_database = config.get('gas', 'AccountsDatabase')

# Get configuration and add to Flask app object
environment = "archive_app_config.Config"
app.config.from_object(environment)


aws_region = app.config['AWS_REGION_NAME']
s3_result_bucket_name = app.config['AWS_S3_BUCKET_NAME']
vault_name = app.config['AWS_VAULT_NAME']
annotations_table = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']

def download_from_s3(bucket_name, file_key):
    # Download the result file from s3 result bucket into instance
    local_directory = os.path.dirname(file_key)
    try:
        if not os.path.exists(local_directory):
            os.makedirs(local_directory)
    except OSError as e:
        print(f"Failed to create directory {local_directory}: {e}")
        return False

    s3 = boto3.client('s3')

    try:
        s3.download_file(bucket_name, file_key, file_key)
        print(f"In archive_app: Downloaded {file_key} to {file_key}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"File {file_key} not found in bucket {bucket_name}.")
            return False
        else:
            print(f"Failed to download {file_key}: {e}")
            return False
    except NotADirectoryError as e:
        print(f"Target is not a directory: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False 


def upload_to_glacier_vault(file_path):
    # Upload file to glacier vault
    glacier = boto3.client('glacier', region_name=aws_region)
    try:
        with open(file_path, 'rb') as file:
            # Reference: glacier upload_archive
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/upload_archive.html
            response = glacier.upload_archive(vaultName=vault_name, body=file)
        print(f"File uploaded to Glacier vault {vault_name}. Archive ID: {response['archiveId']}")
    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
        return None
    except IOError as e:
        print(f"Error: Unable to read the file at {file_path}. IOError: {e}")
        return None
    except ClientError as e:
        print(f"Error: AWS client encountered an issue: {e}")
        return None
    return response['archiveId']



def delete_from_s3(bucket_name, file_key):
    # Delete file from S3 bucket
    try:
        s3 = boto3.client('s3')
        s3.delete_object(Bucket=bucket_name, Key=file_key)
        print(f"Deleted {file_key} from S3 bucket {bucket_name}")
        return True
    except ClientError as e:
        # This will catch client-side issues such as incorrect access permissions, non-existent bucket, etc.
        print(f"Client error when trying to delete from S3: {e}")
        return False
    except Exception as e:
        # This is a catch-all for any other exceptions that might be raised.
        print(f"Unexpected error: {e}")
        return False

def delete_local_file(file_key):
    # Delete file from local instance
    try:
        local_directory = os.path.dirname(file_key)
        shutil.rmtree(local_directory)
        print(f"Deleted local directory {local_directory}")
        return True
    except (OSError, shutil.Error) as e:
        print(f"Failed to delete local directory: {e}")
        return False


def update_dynamodb(job_id, archive_id):
    # Update the database to include the archive_id
    dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    table = dynamodb.Table(annotations_table)
    # Reference: Update item
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
    try:
        # Update item in DynamoDB table
        response = table.update_item(
            Key={
                'job_id': job_id  # Make sure 'job_id' is the correct partition key
            },
            UpdateExpression="SET results_file_archive_id = :archive_id",
            ExpressionAttributeValues={
                ':archive_id': archive_id  # The key should start with ':'
            },
            ReturnValues="UPDATED_NEW"
        )
        return True
    except ClientError as e:
        # Handle specific client errors as needed
        print("DynamoDB Client Error:", e.response['Error']['Message'])
        return False
    except Exception as e:
        # General exception handling
        print("An unexpected error occurred:", str(e))
        return False



@app.route("/", methods=["GET"])
def home():
    return f"This is the Archive utility: POST requests to /archive."


@app.route("/archive", methods=["POST"])
def archive_free_user_data():
    data = json.loads(request.data)
    if data['Type'] == 'SubscriptionConfirmation':
        # Confirm the subscription by visiting the SubscribeURL
        response = requests.get(data['SubscribeURL'])
        print('Processing SubscriptionConfirmation....')
        if response.status_code == 200:
            return jsonify({"message": "Subscription confirmed"}), 200
        else:
            return jsonify({"error": "Failed to confirm subscription"}), 400
    elif data['Type'] == 'Notification':
        
        archive_details = json.loads(data['Message'])

        s3_key_result_file = archive_details.get('s3_key_result_file')
        job_id = archive_details.get('job_id')
        user_id = archive_details.get('user_id')

        profile = helpers.get_user_profile(user_id, accounts_database)
        print('profile:', profile)

        if profile[4] != 'free_user':
            print("Premium user, do not need archive")
            return jsonify({"message": "Premium user, do not need archive"}), 201
        
        print("Free_user, archiving....")
        # MOVE FROM S3 To GLACIER
        
        # Down load file from s3
        if not download_from_s3(s3_result_bucket_name, s3_key_result_file):
            return jsonify({"error": "In archive_app.py failed to down load file from s3"}), 400
        
        # Upload the file to glacier
        archive_id = upload_to_glacier_vault(s3_key_result_file)
        if archive_id is None:
            return jsonify({"error": "In archive_app.py failed to upload the file to glacier"}), 400
        
        # remove file from s3
        if not delete_from_s3(s3_result_bucket_name, s3_key_result_file):
            return jsonify({"error": "In archive_app.py failed to delete file from s3"}), 400

        # remove file from local instance
        if not delete_local_file(s3_key_result_file):
            return jsonify({"error": "In archive_app.py failed to delete file from local instance"}), 400

        # Update database
        if not update_dynamodb(job_id, archive_id):
            return jsonify({"error": "In archive_app.py failed to update database"}), 400

        return jsonify({"message": "Archive request processed successfully"}), 201

### EOF








