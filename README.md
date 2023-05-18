# Cloud Computing (Amazon Web Services) Assignment 2022-2023

Implementation of an application processing large data sets in parallel on a distributed Cloud environment (ie. AWS).

© Copyright 2022, All rights reserved to Hans Haller, CSTE-CIDA Student at Cranfield Uni. SATM, Cranfield, UK.

https://www.github.com/Hnshlr

## Implementation

### Python version

The implementation is developed under Python (version 3.10.6). In order to communicate with AWS and run API calls, it uses Boto3 (version 1.26.37), a Python SDK capable of initiating and managing its services such as EC2 instances, SQS queues or S3 buckets. The implementation uses several other minor packages, presented in the documentation. [3]

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

## Results

### Validity of the Matrix Product

In order to verify the results, the master EC2 instance responsible for the final gathering of all the block results and reconstruction of the result matrix, saves the result in a .csv file and saves it in an allocated place on the previously created S3 bucket. To allow comparison of the result with the one numpy computes on a single node, a save of the initial matrix in a .csv file is also done prior to splitting the matrix in blocks and sending tasks to the jobs SQS queue.

![alt text](https://user-images.githubusercontent.com/74055973/284330054-4d46b37c-427d-47d7-aaae-97a4ec223d52.png)

The verification of the results can be done either from the master, the workers, or the user’s local machine, as they share the same .aws/credentials (ie. they all have access to the previously created services, including the S3 bucket), using the verify_result_matrix() which takes the bucket_name, the operation_id and the operation_type (add or mx).

The function can be given an operation id to verify a specific operation that occurred in the environment, or simply verify all results of a specific operation type

### Progress Monitoring

To monitor the progress of the solution, the scripts progressively print information regarding its evolution. Some of the scripts are sent from one instance to another, either using AWS’s SSM or a package called paramiko.

![alt text](https://user-images.githubusercontent.com/74055973/284331008-4fb25a0c-464f-4bd0-ba4b-431ef2684d1d.png)

Using paramiko, the instance submits one or multiple scripts to another instance and sleeps while progressively printing the output. Using SSM, the instance submits one or multiple scripts to another instance, but instantly gets returned an HTTP header that informs them if the scripts were correctly submitted to the instance or not. Once the scripts are submitted, they’re being executed in the background and the “sender” can resume its execution.

![alt text](https://user-images.githubusercontent.com/74055973/284331271-4178ce0f-7182-4eb4-9878-f3003af872dd.png)

Some of the scripts such as the installation of packages during the environment setup (figure 10) are preferably executed in the background/simultaneously, thus they’re sent using SSM. However, for the computation of matrix operations (figure 10) which requires a response such as a value or a boolean, paramiko is preferred. For huge matrix operations, a huge amount of lines could be outputted, thus the user can decide to semi-monitor the progress by specifying “False” in the solution_execution() function.

![alt text](https://user-images.githubusercontent.com/74055973/284331422-50265618-5123-4ca4-9375-b2563a6568ba.png)

### Performance

The measurement of the computation time is done locally on the user computer, and allows the user to decide which steps they want to take into account for the measurement, among the AWS environment setup, the solution backend synchronization and the matrix operation.

Depending on the task, a timer is run either on an EC2 instance or the local machine. For testing purposes, environment setups and terminations, as well as matrix operations were run and measured. Here are some of the results I was able to gather: