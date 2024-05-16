# thaw_app.py
#
# Thaws upgraded (Premium) user data
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import requests
import sys
import time

from botocore.exceptions import ClientError, BotoCoreError, ParamValidationError
from flask import Flask, request, jsonify

app = Flask(__name__)
app.url_map.strict_slashes = False

# Get configuration and add to Flask app object
environment = "thaw_app_config.Config"
app.config.from_object(environment)
aws_region = app.config['AWS_REGION_NAME']
vault_name = app.config['AWS_GLACIER_VAULT']
annotations_table = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
user_index_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE_USER_INDEX"]
restore_request_sns = app.config["AWS_RESTORE_REQUEST_SNS"]



def update_restore_status(job_id, status):
    # find the entry with job_id, and update its restoring status
    dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    table = dynamodb.Table(annotations_table)
    # Reference: Update item
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
    try:
        # Update item in DynamoDB table
        response = table.update_item(
            Key={
                'job_id': job_id  
            },
            UpdateExpression="SET file_restore_status = :status",
            ExpressionAttributeValues={
                ':status': status  
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

    

def initiate_glacier_retrieval(job_id, archive_id):
    """
        Given an archive_id, initiate the glacier retrieval
    """
    print("Verify pass in parameters in initiate_glacier_retrieval in thaw, job_id, archive_id:", job_id, archive_id)

    # Create a Glacier client
    glacier_client = boto3.client('glacier', region_name=aws_region)
    thaw_job_id = ''
    # Try Expedited retrieval first
    # Reference: initiate_job
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
    try:
        response = glacier_client.initiate_job(
            vaultName=vault_name,
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'Tier': 'Expedited',
                'SNSTopic': restore_request_sns,
                'Description': job_id
            }
        )
        print("Expedited retrieval initiated successfully.")
        print("Job ID:", response['jobId'])
        update_restore_status(job_id, 'Expedited')
        return thaw_job_id
    except ClientError as e:
        # Check if the error is because of capacity constraints
        if e.response['Error']['Code'] == 'InsufficientCapacityException':
            print("Expedited retrieval failed due to insufficient capacity. Trying standard retrieval...")
            # Fall back to Standard retrieval
            # Reference: initiate_job
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
            try:
                response = glacier_client.initiate_job(
                    vaultName=vault_name,
                    jobParameters={
                        'Type': 'archive-retrieval',
                        'ArchiveId': archive_id,
                        'Tier': 'Standard',
                        'SNSTopic': restore_request_sns,
                        'Description': job_id
                    }
                )
                print("Standard retrieval initiated successfully.")
                print("Job ID:", response['jobId'])
                update_restore_status(job_id, 'Standard')
                return thaw_job_id
            except ClientError as e:
                print("Failed to initiate standard retrieval:", e)
                return thaw_job_id
        else:
            print("Error initiating expedited retrieval:", e)
            return thaw_job_id



@app.route("/", methods=["GET"])
def home():
    return f"This is the Thaw utility: POST requests to /thaw."


@app.route("/thaw", methods=["POST"])
def thaw_premium_user_data():
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
        print('Received Thaw request')
        thaw_details = json.loads(data['Message'])
        job_id = thaw_details.get('job_id')
        archive_id = thaw_details.get('archive_id')
        # initiate the glacier retrieval
        thaw_job_id = initiate_glacier_retrieval(job_id, archive_id) 
        if thaw_job_id=='':
            return jsonify({"error": "Failed to initiate thawing process"}), 400

        return jsonify({"message": "Notification received"}), 200


### EOF
