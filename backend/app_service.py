from backend.controllers.app_controller import *
from backend.controllers.ec2_controller import *
from backend.controllers.s3_controller import *
from backend.controllers.sqs_controller import *


# SERVICE: VERIFY JOBS:
def verify_result_matrix(bucket_name, instance_name, op_id, op_type):
    # JOB: VERIFY THE RESULTS COMPUTED ON AWS DISTRIBUTED CLOUD ENVIRONMENT, USING NUMPY'S MATRIX/ADD FUNCTIONS:
    command = 'sudo python3 backend/work_service.py verify ' + bucket_name + ' ' + op_id + ' ' + op_type
    stdout, stderr = exec_SSH_on_instance(instance_name, command)
    outprint(stdout)

# SERVICE: VERIFY MULTIPLE JOBS:
def verify_multiple_jobs(bucket_name, instance_name, op_ids, op_type):
    for i in range(len(op_ids)):
        command = 'sudo python3 backend/work_service.py verify ' + bucket_name + ' ' + op_ids[i] + ' ' + op_type
        stdout, stderr = exec_SSH_on_instance(instance_name, command)
        outprint(stdout)


# SERVICE: STOP THE EC2 INSTANCES, PURGE THE SQS QUEUES AND THE S3 BUCKET:
def clean_and_stop(sqss, s3s, ec2s, delete_all):
    for queue_name in sqss:
        purge_queue(queue_name)
        if delete_all:
            delete_sqs_queue(queue_name)
    for bucket_name in s3s:
        purge_s3_bucket(bucket_name)
        if delete_all:
            delete_s3_bucket(bucket_name)
    for instances_name in ec2s:
        stop_instance_by_name(instances_name)
        if delete_all:
            terminate_instance_by_name(instances_name)

# SERVICE: KILL ALL:
def kill_all():
    terminate_all_instances()
    delete_all_sqs_queues()
    delete_all_s3_buckets()