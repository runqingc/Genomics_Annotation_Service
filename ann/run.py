# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import time
import driver
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import shutil
from datetime import datetime


bucket_name = config.get('s3', 'ResultsBucketName') # Result bucket name
aws_region = config.get('aws', 'AwsRegionName')
queue_url = config.get('sqs', 'QueueUrl')
CNetID = config.get('DEFAULT', 'CnetId')
annotations_table = config.get('gas', 'AnnotationsTable')
max_number = int(config.get('sqs', 'MaxMessages'))
wait_time = int(config.get('sqs', 'WaitTime'))
result_topic_arn = config.get('sns', 'ResultTopicArn')

"""A rudimentary timer for coarse-grained profiling
"""


class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


def remove_directory(path):
    """
    Remove a given directory.
    :param path:
    :return: True if success, False otherwise
    """
    try:
        # Reference: shutil — High-level file operations
        # https://docs.python.org/3/library/shutil.html
        shutil.rmtree(path)
        print(f"Directory {path} has been removed successfully.")
        return True
    except FileNotFoundError:
        print(f"Directory {path} does not exist.")
        return False
    except PermissionError:
        print(f"Permission denied: cannot remove directory {path}.")
        return False
    except Exception as e:
        print(f"Failed to remove directory {path}: {str(e)}")
        return False


def upload_file(local_file_path, s3_file_path):
    """
    Upload file to S3 bucket.
    :param local_file_path:
    :param s3_file_path:
    :return: True if success, False otherwise
    """
    # Create an S3 client
    s3 = boto3.client('s3')
    try:
        # Upload the file
        with open(local_file_path, 'rb') as data:
            # Boto3 documentation: upload_fileobj
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_fileobj.html
            s3.upload_fileobj(data, bucket_name, s3_file_path)
        print(f"File successfully uploaded to {bucket_name}/{s3_file_path}")
        return True
    # Track all error that might occur when upload file
    except ClientError as error:
        print(f"Failed to upload {local_file_path} to {s3_file_path}: {error}")
        return False
    except FileNotFoundError:
        print(f"File {local_file_path} not found")
        return False
    except Exception as error:
        print(f"An error occurred: {error}")
        return False


if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')
        user_name = sys.argv[1].split('/')[3]
        uuid = sys.argv[2]
        original_file_name = sys.argv[1].split('/')[5]
        # generate file_name to upload
        annot_file_name = original_file_name.split('.')[0] + '.annot.vcf'
        log_file_name = original_file_name + '.count.log'
        # find the path of file to be uploaded
        annot_file_path = os.path.join(f'./anntools/data/{user_name}', uuid, annot_file_name)
        log_file_path = sys.argv[1] + '.count.log'
        prefix = f"{CNetID}/{user_name}/{uuid}/"
        # try to upload file, and catch exception in the upload_file function
        if not upload_file(annot_file_path, prefix + annot_file_name):
            print(f"Error uploading annotation file to S3.")
        if not upload_file(log_file_path, prefix + log_file_name):
            print(f"Error uploading log file to S3.")
        if not remove_directory(os.path.join(f'./anntools/data/{user_name}', uuid)):
            print(f"Error removing the directory")

        dynamodb = boto3.resource('dynamodb', region_name=aws_region)
        table = dynamodb.Table(annotations_table)

        current_time = datetime.utcnow().isoformat() + 'Z'

        # Reference: Update item
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
        try:
            response = table.update_item(
                Key={
                    'job_id': uuid
                },
                UpdateExpression="""
                            SET s3_results_bucket = :res_bucket,
                                s3_key_result_file = :res_key,
                                s3_key_log_file = :log_key,
                                complete_time = :comp_time,
                                job_status = :status
                        """,
                ExpressionAttributeValues={
                    ':res_bucket': bucket_name,
                    ':res_key': prefix + annot_file_name,
                    ':log_key': prefix + log_file_name,
                    ':comp_time': current_time,
                    ':status': 'COMPLETED'
                },
                ReturnValues="UPDATED_NEW"
            )
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ConditionalCheckFailedException':
                print("Conditional check failed:", e.response['Error']['Message'])
            else:
                print("DynamoDB Client Error:", e.response['Error']['Message'])
            sys.exit()
        except Exception as e:
            # Catch any other exceptions that may occur
            print("An unexpected error occurred:", str(e))
            sys.exit()
        
        # sqs notification
        result_notification_data = {
            "job_id": uuid
        }

        # Initialize SNS client
        sns = boto3.client('sns')
        try:
            response = sns.publish(
                TopicArn=result_topic_arn,
                Message=json.dumps(result_notification_data)
            )
            print("Message published successfully:", response)
        except ClientError as e:
            # Handle client errors, such as issues with the network or incorrect AWS credentials
            print("AWS client error occurred:", e)
            sys.exit()
        except BotoCoreError as e:
            # Handle low-level exceptions, such as errors from the underlying HTTP library
            print("BotoCore error occurred:", e)
            sys.exit()
        except Exception as e:
            # Handle other possible exceptions
            print("An error occurred:", e)
            sys.exit()

            
    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF