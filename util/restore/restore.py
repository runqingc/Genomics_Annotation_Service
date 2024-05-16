# restore.py
#
# Restores thawed data, saving objects to S3 results bucket
# NOTE: This code is for an AWS Lambda function
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##

import boto3
import json
from botocore.exceptions import ClientError

# Define constants here; no config file is used for Lambdas
AWS_REGION_NAME = "us-east-1"
DYNAMODB_TABLE = "runqingc_annotations"
AWS_S3_RESULTS_BUCKET = "gas-results"
AWS_GLACIER_VAULT = "ucmpcs"
RESTORE_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/127134666975/runqingc_a16_restore_requests"

s3_client = boto3.client('s3', region_name=AWS_REGION_NAME)
glacier_client = boto3.client('glacier', region_name=AWS_REGION_NAME)



def find_s3_result_key(dynamodb, job_id):
    # Find the s3_result_key based on job_id
    primary_key = {'job_id': {'S': job_id}}
    # Reference: get_item
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/get_item.html
    try:
        response = dynamodb.get_item(TableName=DYNAMODB_TABLE, Key=primary_key)
        item = response.get('Item')
        if item:
            s3_key_result_file = item.get('s3_key_result_file', {}).get('S') 
            print("s3_key_result_file:", s3_key_result_file)
            return s3_key_result_file 
        else:
            print("Item not found.")
            return ''
    except ClientError as e:
        print("An error occurred:", e.response['Error']['Message'])
        return ''
    except Exception as e:
        print("In restore.py, lambda, Other error happened when I try to retrieve the item with job_id")  
        return '' 
 

def delete_message(sqs_client, receipt_handle):
    # delete a message
    # Reference: delete_message
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
    try:
        sqs_client.delete_message(
                QueueUrl=RESTORE_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
        return True
    except ClientError as e:
        print(f"An error occurred: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        print(f"Unexpected error occurred: {str(e)}")
        return False


def copy_to_s3(thaw_job_id, s3_key_result_file):
    # copy the s3 file to its original position
    # Reference: get_job_output
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
    try:
        # Initiate the job to get the output
        response = glacier_client.get_job_output(
            vaultName=AWS_GLACIER_VAULT,
            jobId=thaw_job_id
        )
        
        # Get the output stream
        body = response['body'].read()
        
        # Upload the data to S3
        s3_client.put_object(
            Bucket=AWS_S3_RESULTS_BUCKET,
            Key=s3_key_result_file,
            Body=body
        )
        print(f"Data from Glacier archive {thaw_job_id} has been copied to S3 key {s3_key_result_file}")
        return True
    except ClientError as e:
        print(f"An error occurred: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        print(f"Unexpected error occurred: {str(e)}")
        return False


def delete_glacier_archive(archive_id):
    # delete the archived file from glacier, given the archive_id
    # Reference: delete_archive
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/delete_archive.html
    try:
        response = glacier_client.delete_archive(
            vaultName=AWS_GLACIER_VAULT,
            archiveId=archive_id
        )
        print(f"Archive {archive_id} successfully deleted from Glacier.")
        return True
    except ClientError as e:
        print(f"An error occurred while deleting archive {archive_id}: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        print(f"Unexpected error occurred while deleting archive {archive_id}: {str(e)}")
        return False


def delete_dynamodb_fields(dynamodb_client, job_id):
    # delete the results_file_archive_id and file_restore_status fields to indicate the job complete
    # Reference: update_item
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
    try:
        response = dynamodb_client.update_item(
            TableName=DYNAMODB_TABLE,
            Key={
                'job_id': {'S': job_id}
            },
            UpdateExpression="REMOVE results_file_archive_id, file_restore_status",
            ReturnValues="UPDATED_NEW"
        )
        print(f"Fields 'results_file_archive_id' and 'file_restore_status' successfully removed from item with job_id {job_id}.")
        return True
    except ClientError as e:
        print(f"An error occurred while updating item with job_id {job_id}: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        print(f"Unexpected error occurred while updating item with job_id {job_id}: {str(e)}")
        return False


def lambda_handler(event, context):
    sqs_client = boto3.client('sqs')
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION_NAME)
    queue_url = RESTORE_QUEUE_URL

    for record in event['Records']:
        # Parse the message body 
        sns_message = json.loads(record['body'])
        message = json.loads(sns_message['Message'])

        archive_id = message['ArchiveId']
        job_id = message['JobDescription']
        thaw_job_id = message['JobId']
        print('thaw_job_id: ', thaw_job_id)

        print(f'Archive ID: {archive_id}')
        print(f'Job Description: {job_id}')

        # Find the s3_key_result_file of given job_id in dynamodb
        s3_key_result_file = find_s3_result_key(dynamodb, job_id)
        if s3_key_result_file == '':
            continue

        # Copy the restored data to S3 bucket    
        if copy_to_s3(thaw_job_id, s3_key_result_file) == False:
            continue   

        # Delete the message from the queue
        receipt_handle = record['receiptHandle']
        if delete_message(sqs_client, receipt_handle) == False:
            continue
        
        # delete the glacier archive file
        if delete_glacier_archive(archive_id) == False:
            continue
        
        # update the dynamodb to indicate the job finish
        if delete_dynamodb_fields(dynamodb, job_id) == False:
            continue    

    return {
        'statusCode': 200,
        'body': json.dumps('Messages processed successfully')
    }

    


### EOF
