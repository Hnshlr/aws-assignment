# Cloud Computing (Amazon Web Services) Assignment 2022-2023

Implementation of an application processing large data sets in parallel on a distributed Cloud environment (ie. AWS).

© Copyright 2023, All rights reserved to Hans Haller, CSTE-CIDA Student at Cranfield Uni. SATM, Cranfield, UK.

https://www.github.com/Hnshlr

### Solution setup - Pre-requisites:
1. Make sure the aws credentials taken from the Learner Lab are updated in the ~/.aws/credentials file (Test connection locally using aws sts get-caller-identity)
2. Specify the "labsuser.pem" perm-key's (taken from the Learner Lab) path, needed by paramiko to connect to the EC2 instances and execute ssh commands.

### Solution setup steps (Using the environment setup function):
1. Make sure the AWS environment of the account whose credentials were imported is empty.
2. If the AWS environment is not empty, execute the kill_all() function following the imports.
3. Verify that the backend path in the second cell is correct.
4. Execute the environment_setup() function.
5. Verify that the AWS environment is fully setup using the view_all() function.
6. Execute the solution_execution() function to run the solution, and view the results.

### Python version

The implementation is developed under Python (version 3.10.6). In order to communicate with AWS and run API calls, it uses Boto3 (version 1.26.37), a Python SDK capable of initiating and managing its services such as EC2 instances, SQS queues or S3 buckets. The implementation uses several other minor packages, presented in the documentation. [3]

### Implementation

The development was achieved using JetBrains’ DataSpell IDE. The core of the implementation is conducted in a Jupyter Notebook, whose functions are called out in different cells. This allows the user to select at the end of the notebook the tasks they wish to perform, measure the computation time and draw the graphs he wishes to.