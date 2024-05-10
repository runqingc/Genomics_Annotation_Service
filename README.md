# GAS Framework
An enhanced web framework (based on [Flask](https://flask.palletsprojects.com/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](https://getbootstrap.com/docs/3.3/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts/apps for notifications, archival, and restoration
* `/aws` - AWS user data files


### Approach

The data flow now looks like this:

​	User log in and submit the annotation file. In views.py I send a notification to the job_request sns topic.

​	The "/process-job-request" in annotator_webhook subscribs to the sns topic, so once the message was published to that topic, the "/process-job-request" will receive a post request and triggers the annotation process (spawn a process calling run.py).

​	Inside run.py, I include a state machine. One the annotation process is done, it will starts executing, waiting for 3 minutes and push a message to archive sns topic. The message include user_id, job_id and s3_key_result_file.

​	Inside archive_app.py I have an end point "/archive" that subscribs to the archive sns topic, so once the message was published to that topic, the "/archive" will be triggered. 

​	The "/archive" endpoint first extract user information from the account database, returning the user profile. (This step is done with the helper function "get_user_profile" defined in /util/helpers )  It then extract the user_role to decide if the user is a free user. If it does, it will remove the file from s3 and upload it to glacier vault. By this way I will be able to handle the situation when a user becomes a premuim on after he/she submit the annotaiton job, the result file will now not be put the glacier once the process completes. 



### Rationale

**Scalability**

​	By using AWS SNS to facilitate communication between different components of the system, each component operates independently. This means that scaling any part of the system (e.g., increasing the number of workers in `run.py` for processing or handling more requests at the `/archive` endpoint) can be done without impacting other components. This is crucial for handling varying loads, ensuring that the system can handle growth in user numbers or data volume efficiently.

​	Utilizing AWS Step Functions to manage the annotation and archiving processes ensures that the workflow is clearly defined and each step is decoupled from others. Step Functions scale seamlessly with increasing demand, managing state transitions and retries automatically, which offloads complexity from the application, making it easier to scale.

​	The asynchronous nature of SNS and the state machine ensures that the system can handle high throughput and latency variability effectively. Long-running processes like annotation and archival do not block user interactions or other system processes.

**Consistency**

​	Using AWS Step Functions ensures that the application's state transitions are consistent and error-handling is straightforward. Each execution of a state machine is tracked and logged, providing clear visibility into the processing state and history, which aids in maintaining consistency across the system.

​	The design includes mechanisms to ensure that operations such as archiving are idempotent. This means that repeated processing of the same message (due to message redelivery, for example) will not lead to inconsistent states, such as multiple archives of the same file.

​	By checking the user's role before moving files to Glacier, the system ensures that changes in user status are respected in real-time, preventing premium users from losing access to their files. This approach guarantees that user data is handled correctly according to their subscription level, maintaining system integrity and user trust.