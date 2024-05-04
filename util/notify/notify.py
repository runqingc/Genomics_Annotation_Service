# notify.py
#
# Notify user of job completion via email
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import os
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo


from botocore.exceptions import ClientError, BotoCoreError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("../util_config.ini")
config.read("notify_config.ini")

aws_region = config.get('aws', 'AwsRegionName')
queue_url = config.get('sqs', 'ResultQueueUrl')
max_number = int(config.get('sqs', 'MaxMessages'))
wait_time = int(config.get('sqs', 'WaitTime'))
annotations_table = config.get('gas', 'AnnotationsTable')
mail_default_sender = config.get('gas', 'MailDefaultSender')
accounts_database = config.get('gas', 'AccountsDatabase')
job_detail_url_base = config.get('web', 'JobDetailUrlBase')
aws_time_zone = config.get('aws', 'AwsTimeZone')

def format_time(time_utc):
    # Helper function to display the date/time in the instance timezone
    # Reference : the usage of ZoneInfo
    # https://docs.python.org/3/library/zoneinfo.html
    dt_utc = datetime.strptime(time_utc, "%Y-%m-%dT%H:%M:%S.%fZ")
    timezone = ZoneInfo(aws_time_zone)
    dt_local = dt_utc.replace(tzinfo=ZoneInfo('UTC')).astimezone(timezone)
    formatted_time = dt_local.strftime("%Y-%m-%d @ %H:%M:%S")
    return formatted_time

"""A12
Reads result messages from SQS and sends notification emails.
"""


def read_message_from_queue(sqs):
    """
        Attempt to read a specified maximum number of messages from the queue using long polling.

        :param sqs: the queue to get message from

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


def send_email_to_user(user_id, job_id, complete_time):
    profile = helpers.get_user_profile(user_id, accounts_database)
    print('profile: ', profile)
    subject = f"Subject: Results available for job {job_id}"
    link_to_details_page_for_job_id = job_detail_url_base + job_id
    print(link_to_details_page_for_job_id)
    body = f"Your annotation job completed at {complete_time}. Click here to view job details and results: {link_to_details_page_for_job_id}."
    helpers.send_email_ses(recipients=profile[2],
                   sender=mail_default_sender,
                   subject=subject, body=body)


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


def handle_results_queue(sqs=None):
    # Read messages from the queue
    messages = read_message_from_queue(sqs)
    if messages:
        # Process messages
        for message in messages:
            # Double parse JSON as per SNS-SQS integration format
            data = json.loads(json.loads(message['Body'])['Message'])
            user_id = data.get('user_id')
            job_id = data.get('job_id')
            complete_time = format_time(data.get('complete_time'))
            receipt_handle = message['ReceiptHandle']
            # Process messages --> send email to user
            send_email_to_user(user_id, job_id, complete_time)
           # Delete the message after spawning the subprocess
            if not delete_message(sqs, receipt_handle):
                return 

    



def main():
    # Get handles to SQS
    sqs = boto3.client('sqs', region_name=aws_region)

    # Poll queue for new results and process them
    while True:
        handle_results_queue(sqs)


if __name__ == "__main__":
    main()

### EOF
