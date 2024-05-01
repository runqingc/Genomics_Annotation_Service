# annotator.py
#
# NOTE: This file lives on the AnnTools instance
#
# Copyright (C) 2013-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import os
import sys
import time
from subprocess import Popen, PIPE
from botocore.exceptions import ClientError, BotoCoreError

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("annotator_config.ini")

aws_region = config.get('aws', 'AwsRegionName')
queue_url = config.get('sqs', 'QueueUrl')
CNetID = config.get('DEFAULT', 'CnetId')
annotations_table = config.get('gas', 'AnnotationsTable')
"""Reads request messages from SQS and runs AnnTools as a subprocess.

Move existing annotator code here
"""


def receive_sqs_messages(sqs, max_number=10, wait_time=20):
    """
    Attempt to read a specified maximum number of messages from the queue using long polling.

    :param sqs: the queue to get message from
    :param max_number: int
        The maximum number of messages to retrieve in one call.
    :param wait_time: int
        The duration (in seconds) the call waits for a message to appear in the queue before returning.

    :return: list A list of messages, each represented as a dictionary. Returns an empty list if no messages are
    available or an error occurs.
    """
    try:

        # Attempt to receive the maximum number of messages
        # Reference: Boto 3 documentation receive_message
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time
        )
        return response.get('Messages', [])
    except ClientError as e:
        # Handle specific AWS client errors, such as access issues or resource not found
        print(f"An AWS ClientError occurred: {e.response['Error']['Code']} - {e.response['Error']['Message']}")
        return []
    except BotoCoreError as e:
        # Handle errors in the boto3 library itself
        print(f"A BotoCoreError occurred: {str(e)}")
        return []
    except Exception as e:
        # Optional: Catch any other unexpected errors
        print(f"An unexpected error occurred: {str(e)}")
        return []


def copy_file_from_s3(s3_bucket_name, s3_file_name, local_user_id, local_uuid, local_prefix):
    """
    Copies a specified file from an AWS S3 bucket to a local directory, creating the directory if it does not exist.

    :param s3_bucket_name: str
        The name of the S3 bucket from which to download the file.
    :param s3_file_name: str
        The name of the file to download from the S3 bucket.
    :param local_user_id: str
        The user identifier used to create a specific subdirectory path under the local basis directory.
    :param local_uuid: str
        The UUID associated with the specific job or session, used to create a unique subdirectory for storing the file.
    :param local_prefix: str
        The prefix within the S3 bucket under which to search for the file. This helps in filtering the objects list.
    :return: bool
        Returns True if the file was successfully downloaded and False otherwise. This includes cases where the file
        does not exist or other errors occur during the download process.
    """
    s3 = boto3.client('s3')
    local_directory_base = f'./anntools/data/{local_user_id}'
    # Create unique local directory to preserve the input file
    local_directory = os.path.join(local_directory_base, local_uuid)
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)
    # List objects in the S3 bucket at the specified prefix
    # Reference: list_objects_v2
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
    s3_response = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=local_prefix)

    if 'Contents' in s3_response and len(s3_response['Contents']) > 0:
        # There's only one file matches, and it starts with the uuid
        local_file_path = os.path.join(local_directory, s3_file_name.split('~')[-1])
        print(s3_bucket_name, s3_file_name, local_file_path)
        try:
            # Download the file
            s3.download_file(s3_bucket_name, s3_file_name, local_file_path)
            print(f"Downloaded {s3_file_name} to {local_file_path}")
            return True
        except ClientError as e:
            # Check if the error was due to the file not being found
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"File {s3_file_name} not found in bucket {s3_bucket_name}.")
                return False
            else:
                # Other S3 related errors
                print(f"Failed to download {s3_file_name}: {e}")
                return False
    else:
        print("No files found at the specified prefix.")
        return False


def spawn_annotation_process(local_user_name, local_uuid, local_file_name):
    """
    Launches an annotation process as a background subprocess using a specified Python script.

    :param local_user_name: str
        The identifier for the user under whose directory the file is located.
    :param local_uuid: str
        The unique identifier for the job.
    :param local_file_name: str
        The name of the file to be processed by the annotation job.
    :return: bool
        Returns True if the subprocess was successfully started, False if an error occurred which could be due
        to file not being found, permission errors, or other unexpected issues.

    """
    try:
        # Construct the full path to the local file
        run_file_path = os.path.join(f'./anntools/data/{local_user_name}', local_uuid, local_file_name.split('~')[-1])
        # Start the annotation job as a background process
        Popen(['python', './run.py', run_file_path, local_uuid])
        print(
            f"Annotation process started for ./anntools/data/{local_user_name} {run_file_path} {local_uuid}, running run.py.")
        return True
    except FileNotFoundError:
        # Handle the case where the file to execute is not found (e.g., './anntools/run.py' does not exist)
        print(
            f"Error: The file './run.py' was not found. Unable to start annotation process for {local_uuid}.")
        return False
    except PermissionError:
        # Handle the case where the Python script cannot be executed due to insufficient permissions
        print(f"Error: Permission denied when trying to execute './run.py'. Check file permissions.")
        return False
    except Exception as e:
        # Catch all other exceptions that could be raised by subprocess.Popen
        print(f"An unexpected error occurred while trying to start the annotation process for {local_uuid}: {str(e)}")
        return False


def update_job_status(local_uuid):
    """
    Update the job status from pending to running

    :param local_uuid: str
        The unique identifier for the job.
    :return: bool
        True if the database was updated correctly, False otherwise
    """
    try:
        dynamodb = boto3.resource('dynamodb', region_name=aws_region)
        table = dynamodb.Table(annotations_table)
        table.update_item(
            Key={
                'job_id': local_uuid
            },
            UpdateExpression="SET job_status = :new_status",
            ConditionExpression="job_status = :current_status",
            ExpressionAttributeValues={
                ':new_status': 'RUNNING',
                ':current_status': 'PENDING'
            },
            ReturnValues="UPDATED_NEW"
        )
        return True
    except ClientError as e:
        # Handle common DynamoDB client errors, such as ConditionalCheckFailedException
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print(f"Condition check failed: Job {local_uuid} is not in PENDING state or doesn't exist.")
        else:
            print(f"Failed to update job status for {local_uuid}: {e.response['Error']['Message']}")
        return False  # Return False to indicate failure, and no response

    except Exception as e:
        # This catches any other exceptions that might occur
        print(f"An unexpected error occurred while updating job status for {local_uuid}: {str(e)}")
        return False


def delete_message(sqs, local_receipt_handle):
    """
    Delete the message after the process spawned successfully

    :param sqs: the queue to delete message from
    :param local_receipt_handle: the receipt handle extracted from the message
    :return: True if the message was deleted correctly, False otherwise
    """
    # Delete the message from the queue
    # Reference: Boto 3 documentation receive_message
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
    try:
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=local_receipt_handle
        )
        return True
    except ClientError as e:
        # ClientError caught from boto3 call
        print(f"Failed to delete the message: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        # General exception catch, if unexpected error occurs
        print(f"An unexpected error occurred while deleting the message: {str(e)}")
        return False


def handle_requests_queue(sqs=None):
    # Read messages from the queue
    messages = receive_sqs_messages(sqs)
    if messages:
        # Process messages
        for message in messages:
            # Double parse JSON as per SNS-SQS integration format
            data = json.loads(json.loads(message['Body'])['Message'])

            # Extract user_name, and uuid ect. from the received data
            user_name = data.get('user_id')
            uuid = data.get('job_id')
            bucket_name = data.get('s3_inputs_bucket')
            prefix_file = data.get('s3_key_input_file')
            file_name = data.get('input_file_name')
            prefix = f"{CNetID}/{user_name}/{uuid}"
            receipt_handle = message['ReceiptHandle']

            # Copy file from s3 bucket to instance
            if not copy_file_from_s3(bucket_name, prefix_file, user_name, uuid, prefix):
                continue

            # Spawn an Annotate process
            if not spawn_annotation_process(user_name, uuid, file_name):
                continue

            # Update job status in the dynamodb
            if not update_job_status(uuid):
                continue

            # Delete the message after spawning the subprocess
            if not delete_message(sqs, receipt_handle):
                continue


def main():
    # Get handles to queue
    sqs = boto3.client('sqs', region_name=aws_region)

    # Poll queue for new results and process them
    while True:
        handle_requests_queue(sqs)


if __name__ == "__main__":
    main()

### EOF
