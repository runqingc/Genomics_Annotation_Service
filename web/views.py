# views.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import uuid
import time
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from flask import abort, flash, redirect, render_template, request, session, url_for, jsonify

from app import app, db
from decorators import authenticated, is_premium
from auth import get_profile


def format_time(time_utc):
    # Helper function to display the date/time in the instance timezone
    # Reference : the usage of ZoneInfo
    # https://docs.python.org/3/library/zoneinfo.html
    dt_utc = datetime.strptime(time_utc, "%Y-%m-%dT%H:%M:%S.%fZ")
    timezone = ZoneInfo(app.config["AWS_TIMEZONE"])
    dt_local = dt_utc.replace(tzinfo=ZoneInfo('UTC')).astimezone(timezone)
    formatted_time = dt_local.strftime("%Y-%m-%d @ %H:%M:%S")
    return formatted_time


def generate_download_url(s3_client, bucket_name, object_name, expiration=3600):
    # Helper function to generate a pre signed URL to download a file from S3 bucket.
    # Reference : Presigned URLs
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except ClientError as e:
        # Log the error code and error message for more specificity
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        app.logger.error(f"ClientError generating presigned URL: {error_code}, {error_message}")
        return None
    except ParamValidationError as e:
        # Handle cases where the parameters are incorrect
        app.logger.error(f"Parameter validation error: {str(e)}")
        return None
    except Exception as e:
        # Generic exception handling to catch unforeseen errors
        app.logger.error(f"Unexpected error: {str(e)}")
        return None
    return response



"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    # Open a connection to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
    user_id = session["primary_identity"]

    # Generate unique ID to be used as S3 key (name)
    key_name = (
            app.config["AWS_S3_KEY_PREFIX"]
            + user_id
            + "/"
            + str(uuid.uuid4())
            + "~${filename}"
    )

    # Create the redirect URL
    redirect_url = str(request.url) + "/job"

    # Define policy conditions
    encryption = app.config["AWS_S3_ENCRYPTION"]
    acl = app.config["AWS_S3_ACL"]
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl,
        "csrf_token": app.config["SECRET_KEY"],
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl},
        ["starts-with", "$csrf_token", ""],
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template(
        "annotate.html", s3_post=presigned_post, role=session["role"]
    )


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route("/annotate/job", methods=["GET"])
@authenticated
def create_annotation_job_request():
    region = app.config["AWS_REGION_NAME"]

    # Parse redirect URL query parameters for S3 object info
    bucket_name = request.args.get("bucket")
    s3_key = request.args.get("key")

    # Extract the job ID from the S3 key
    job_id = s3_key.split('/')[2].split('~')[0]
    user_name = s3_key.split('/')[1]
    file_name = s3_key.split('/')[2].split('~')[1]
    submit_time = datetime.utcnow().isoformat() + 'Z'
    # Persist job to database
    data = {"job_id": job_id,
            "user_id": user_name,
            "input_file_name": file_name,
            "s3_inputs_bucket": bucket_name,
            "s3_key_input_file": s3_key,
            "submit_time": submit_time,
            "job_status": "PENDING",
            "user_name": session['name'],
            "user_email": session['email'],
            "user_role": session['role']
            }

    # Initialize a boto3 client
    # Reference: Table: dynamodb.Table operation
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    try:
        table.put_item(Item=data)
    except ClientError as e:
        # Handle client-side errors (e.g., missing table, bad request format)
        app.logger.error(f"ClientError in DynamoDB operation: {e}")
        return abort(500)
    except BotoCoreError as e:
        # Handle errors that are less straightforward, could be due to issues on boto3's end
        app.logger.error(f"BotoCoreError in DynamoDB operation: {e}")
        return abort(500)
    except Exception as e:
        # General exception catch block, just in case
        app.logger.error(f"Unexpected error: {e}")
        return abort(500)

    # Send message to request queue
    # Create an SNS client
    # Reference: SNS Client
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html
    sns = boto3.client('sns', region_name=app.config["AWS_REGION_NAME"])
    topic_arn = app.config["AWS_SNS_JOB_REQUEST_TOPIC"]
    print(topic_arn)
    # Reference: sns publish
    # From AWS boto3 documentation - publish
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
    try:
        response = sns.publish(
            TopicArn=topic_arn,
            Message=json.dumps(data)
        )
        print(response)
    except ClientError as e:
        # Handle client-side or server-side error from AWS
        app.logger.error(f"ClientError in SNS operation: {e.response['Error']['Message']}")
        return abort(500)  # Use abort to trigger the 500 error handler
    except ParamValidationError as e:
        # Handle parameter validation errors
        app.logger.error(f"Parameter Validation Error: {str(e)}")
        return abort(400)  # Use abort to trigger the 400 error handler
    except EndpointConnectionError as e:
        # Handle connection errors
        app.logger.error(f"Endpoint Connection Error: {str(e)}")
        return abort(500)  # Use abort to trigger the 500 error handler
    except BotoCoreError as e:
        # Handle errors from the core Boto3 library
        app.logger.error(f"BotoCore Error: {str(e)}")
        return abort(500)  # Use abort for 500 errors
    except Exception as e:
        # Generic handler for any other exceptions
        app.logger.error(f"Unknown Error: {str(e)}")
        return abort(500)  # Use abort for 500 errors

    return render_template("annotate_confirm.html", job_id=job_id)


"""List all annotations for the user
"""


@app.route("/annotations", methods=["GET"])
@authenticated
def annotations_list():
    region = app.config["AWS_REGION_NAME"]
    # Get list of annotations to display
    user_id = session.get('primary_identity')
    # handle unauthorized access
    if user_id is None:
        return abort(403)
    # Query the dynamodb to retrieve information
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

    # Reference: How to query to dynamodb using index
    # From AWS boto3 documentation - query
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
    try:
        response = table.query(
            IndexName=app.config["AWS_S3_SECONDARY_INDEX"],
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
    except ClientError as e:
        # Handle client-side or server-side error from AWS
        app.logger.error(f"ClientError in DynamoDB operation: {e.response['Error']['Message']}")
        return abort(500)  # Use abort to trigger the 500 error handler
    except ParamValidationError as e:
        # Handle parameter validation errors
        app.logger.error(f"Parameter Validation Error: {str(e)}")
        return abort(400)  # Use abort to trigger the 400 error handler
    except EndpointConnectionError as e:
        # Handle connection errors to AWS services
        app.logger.error(f"Endpoint Connection Error: {str(e)}")
        return abort(500)  # Use abort to trigger the 500 error handler
    except BotoCoreError as e:
        # Handle errors from the core Boto3 library
        app.logger.error(f"BotoCore Error: {str(e)}")
        return abort(500)  # Use abort for 500 errors
    except Exception as e:
        # Generic handler for any other exceptions
        app.logger.error(f"Unknown Error: {str(e)}")
        return abort(500)  # Use abort for 500 errors

    annotations = []
    for item in response['Items']:
        annotations.append(
            {"id": item['job_id'], "request_time": format_time(item['submit_time']), "file_name": item['input_file_name'],
             "status": item['job_status']})

    return render_template("annotations.html", annotations=annotations)


"""Display details of a specific annotation job
"""


@app.route("/annotations/<id>", methods=["GET"])
@authenticated
def annotation_details(id):

    dynamodb = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    key = {'job_id': id}
    try:
        response = table.get_item(Key=key)
        # Check if item was found
        if 'Item' not in response:
            app.logger.info("No item found with ID: {}".format(id))
            return abort(404)  # Not found
    except ClientError as e:
        app.logger.error(f"ClientError in DynamoDB operation: {e.response['Error']['Message']}")
        return abort(500)  # Internal server error
    except ParamValidationError as e:
        app.logger.error(f"Parameter Validation Error: {str(e)}")
        return abort(400)  # Bad request
    except EndpointConnectionError as e:
        app.logger.error(f"Endpoint Connection Error: {str(e)}")
        return abort(500)  # Internal server error
    except BotoCoreError as e:
        app.logger.error(f"BotoCore Error: {str(e)}")
        return abort(500)  # Internal server error
    except Exception as e:
        # Generic handler for any other exceptions
        app.logger.error(f"Unknown Error: {str(e)}")
        return abort(500)  # Internal server error

    job_detail = response.get('Item')
    # handle error when user typed in an invalid job detail
    if job_detail is None:
        return abort(404)

    user_id = session.get('primary_identity')
    # check the requested job ID belongs to the user that is currently authenticated
    if user_id != job_detail['user_id']:
        return abort(403)
    
    # Retrieve information
    # Open a connection to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )
    request_id = job_detail['job_id']
    request_time = format_time(job_detail['submit_time'])
    vcf_input_file = job_detail['input_file_name']
    status = job_detail['job_status']
    complete_time = 'N/A'
    result_file_download_url = ''
    log_file_view_url = ''
    # Generate unique ID to be used as S3 key (name)
    input_file_key_name = (
            app.config["AWS_S3_KEY_PREFIX"]
            + user_id
            + "/"
            + request_id
            + "~"
            + vcf_input_file
    )
    input_file_download_url = generate_download_url(s3, app.config["AWS_S3_INPUTS_BUCKET"], input_file_key_name)
    if status == 'COMPLETED':
        complete_time = format_time(job_detail['complete_time'])
        # print("input_file_key_name: ", input_file_key_name)
        result_file_key_name = (
            app.config["AWS_S3_KEY_PREFIX"]
            + user_id
            + "/"
            + request_id
            + "/"
            + vcf_input_file.split('.')[0]
            + ".annot.vcf"
        )
        result_file_download_url = generate_download_url(s3, app.config["AWS_S3_RESULTS_BUCKET"], result_file_key_name)
        log_file_view_url = url_for('annotation_log', id=id)
        print("log_file_view_url:", log_file_view_url)
    return render_template("annotation.html", request_id=request_id,
                           request_time=request_time,
                           vcf_input_file=vcf_input_file,
                           status=status,
                           complete_time=complete_time,
                           input_file_download_url=input_file_download_url,
                           result_file_download_url=result_file_download_url,
                           log_file_view_url=log_file_view_url
                           )


"""Display the log file contents for an annotation job
"""


@app.route("/annotations/<id>/log", methods=["GET"])
@authenticated
def annotation_log(id):
    # check the requested job ID belongs to the user that is currently authenticated
    dynamodb = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
    table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    key = {'job_id': id}
    try:
        response = table.get_item(Key=key)
        # Check if item was found
        if 'Item' not in response:
            app.logger.info("No item found with ID: {}".format(id))
            return abort(404)  # Not found
    except ClientError as e:
        app.logger.error(f"ClientError in DynamoDB operation: {e.response['Error']['Message']}")
        return abort(500)  # Internal server error
    except ParamValidationError as e:
        app.logger.error(f"Parameter Validation Error: {str(e)}")
        return abort(400)  # Bad request
    except EndpointConnectionError as e:
        app.logger.error(f"Endpoint Connection Error: {str(e)}")
        return abort(500)  # Internal server error
    except BotoCoreError as e:
        app.logger.error(f"BotoCore Error: {str(e)}")
        return abort(500)  # Internal server error
    except Exception as e:
        # Generic handler for any other exceptions
        app.logger.error(f"Unknown Error: {str(e)}")
        return abort(500)  # Internal server error

    job_detail = response.get('Item')
    # handle error when user typed in an invalid job detail
    if job_detail is None:
        return abort(404)

    user_id = session.get('primary_identity')
    # check the requested job ID belongs to the user that is currently authenticated
    if user_id != job_detail['user_id']:
        return abort(403)

    # Retrieve information
    request_id = job_detail['job_id']
    vcf_input_file = job_detail['input_file_name']

    # Open a connection to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    log_file_key_name = (
            app.config["AWS_S3_KEY_PREFIX"]
            + user_id
            + "/"
            + request_id
            + "/"
            + vcf_input_file
            + ".count.log"
        )
    print("log_file_key_name: ", log_file_key_name)

    # retrieve the file content as a string
    # show
    try:
        # Retrieve the log file content
        log_obj = s3.get_object(Bucket=app.config["AWS_S3_RESULTS_BUCKET"], Key=log_file_key_name)
        log_contents = log_obj['Body'].read().decode('utf-8')
    except ClientError as e:
        app.logger.error(f"Error retrieving log file from S3: {e.response['Error']['Message']}")
        return abort(500)  # Internal server error
    except ParamValidationError as e:
        app.logger.error(f"Parameter Validation Error: {str(e)}")
        return abort(400)  # Bad request
    except EndpointConnectionError as e:
        app.logger.error(f"Endpoint Connection Error: {str(e)}")
        return abort(500)  # Internal server error
    except NoCredentialsError as e:
        app.logger.error("No credentials to access AWS services.")
        return abort(500)  # Internal server error
    except BotoCoreError as e:
        app.logger.error(f"BotoCore Error: {str(e)}")
        return abort(500)  # Internal server error
    except HTTPError as e:
        app.logger.error(f"HTTP Error: {str(e)}")
        return abort(500)  # Internal server error
    except Exception as e:
        app.logger.error(f"Unknown Error: {str(e)}")
        return abort(500)  # Internal server error

    return render_template("view_log.html", log_contents=log_contents, job_id=request_id)


"""Subscription management handler
"""
import stripe
from auth import update_profile


@app.route("/subscribe", methods=["GET", "POST"])
@authenticated
def subscribe():
    if request.method == "GET":
        # Display form to get subscriber credit card info

        # If A15 not completed, force-upgrade user role and initiate restoration
        pass

    elif request.method == "POST":
        # Process the subscription request

        # Create a customer on Stripe

        # Subscribe customer to pricing plan

        # Update user role in accounts database

        # Update role in the session

        # Request restoration of the user's data from Glacier
        # ...add code here to initiate restoration of archived user data
        # ...and make sure you handle files pending archive!

        # Display confirmation page
        pass


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Set premium_user role
"""


@app.route("/make-me-premium", methods=["GET"])
@authenticated
def make_me_premium():
    # Hacky way to set the user's role to a premium user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="premium_user")
    return redirect(url_for("profile"))


"""Reset subscription
"""


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


"""Home page
"""


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html"), 200


"""Login page; send user to Globus Auth
"""


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


"""400 Bad Request error handler
"""


@app.errorhandler(400)
def bad_request(e):
    return (
        render_template(
            "error.html",
            title="Bad Request",
            alert_level="warning",
            message="The request could not be understood by the server due to malformed syntax. \
      Please check your input and try again.",
        ),
        400,
    )


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist. \
      Please check the URL and try again.",
        ),
        404,
    )


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party.",
        ),
        403,
    )


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed; \
      get your act together, hacker!",
        ),
        405,
    )


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could \
      not process your request.",
        ),
        500,
    )


"""CSRF error handler
"""

from flask_wtf.csrf import CSRFError


@app.errorhandler(CSRFError)
def csrf_error(error):
    return (
        render_template(
            "error.html",
            title="CSRF error",
            alert_level="danger",
            message=f"Cross-Site Request Forgery error detected: {error.description}",
        ),
        400,
    )

### EOF
