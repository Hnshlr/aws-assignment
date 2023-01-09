from backend.controllers.s3_controller import *
from backend.controllers.sqs_controller import *
from backend.controllers.ssm_controller import *

import numpy as np
import threading


# SOLUTION'S AWS ENVIROMENT SETUP:
def environment_setup(queues_names, bucket_names, instances_names, backend_path):
    create_queues_thread = threading.Thread(target=create_sqs_queues, args=(queues_names,))
    create_buckets_thread = threading.Thread(target=create_s3_buckets, args=(bucket_names,))
    create_instances_thread = threading.Thread(target=create_instances_and_wait_for_running, args=(instances_names,))
    create_queues_thread.start()
    create_buckets_thread.start()
    create_instances_thread.start()


    # SEND BACKEND FOLDER -> S3 BUCKET -> EC2 INSTANCES:
    create_buckets_thread.join()
    upload_to_s3_thread = threading.Thread(target=upload_dir_to_s3(backend_path, bucket_names[0], 'backend'))
    upload_to_s3_thread.start()

    # INSTALL SSM AGENT ON INSTANCES:
    create_instances_thread.join()
    install_SSM_agent_thread = threading.Thread(target=verify_SSM_on_instances, args=(get_instance_ids_by_names(instances_names),))
    install_SSM_agent_thread.start()

    # INSTALL PACKAGES ON EC2 INSTANCES (SEND COMMANDS & RUNS IN THE BACKGROUND)
    install_SSM_agent_thread.join()
    exec_commands_on_instances_using_SSM_thread = threading.Thread(target=exec_SSHs_on_instances_using_SSM, args=(
        get_instance_ids_by_names(instances_names),
        [
            'aws s3 sync s3://' + bucket_names[0] + '/backend /home/ec2-user/backend',
            'cd /home/ec2-user/backend && sudo mkdir data && cd data && sudo mkdir input && sudo mkdir output && cd output && sudo mkdir mx && sudo mkdir add',
            'sudo yum install tree -y',
            'pip3 install boto3',
            'pip3 install numpy',
            'pip3 install pandas'
        ]
    ))
    exec_commands_on_instances_using_SSM_thread.start()

    # WAIT FOR ALL THE LAST THREADS TO FINISH:
    exec_commands_on_instances_using_SSM_thread.join()
    create_queues_thread.join()
    upload_to_s3_thread.join()
    print('Environment setup completed.')
    print('Warning: Waiting ~1min for the instances to install the packages is recommended.')

# SOLUTION'S AWS ENVIROMENT STATUS:
def view_all():
    print('EC2 INSTANCES:')
    print(view_all_instances(False))
    print('S3 BUCKETS:')
    print(view_all_s3_buckets())
    print('SQS QUEUES:')
    print(view_all_sqs_queues())

# UPDATE BACKEND FOLDER ON S3, AND EC2 INSTANCES:
def update_backend(instances_ids, bucket_name, backend_path):
    purge_s3_bucket(bucket_name)
    upload_dir_to_s3(backend_path, bucket_name, 'backend')
    exec_SSHs_on_instances_using_SSM(instances_ids, [
        'rm -rf /home/ec2-user/backend',
        'aws s3 sync s3://' + bucket_name + '/backend /home/ec2-user/backend',
        'cd /home/ec2-user/backend && sudo mkdir data && cd data && sudo mkdir input && sudo mkdir output && cd output && sudo mkdir mx && sudo mkdir add'])

# SOLUTION EXECUTION:
def solution_execution(matrix_shape, used_workers, worker_amount, instances_names, queues_names, bucket_names, monitor):
    # START JOB TIMER:
    sol_timer = time.time()

    # WORKERS'S LOOP: READY UP THE WORKERS, HAVING THEM WAIT FOR JOBS:
    command = 'cd /home/ec2-user && sudo python3 backend/work_service.py worker ' + queues_names[0] + ' ' + queues_names[1]
    work_resp = exec_SSHs_on_instances_using_SSM(get_instance_ids_by_names(instances_names)[1:min(used_workers, worker_amount)+1], [command], bucket_names[1])

    # MASTER'S LOOP: CREATE A MATRIX, SPLIT IT INTO SLICES, AND SEND THE SLICES TO THE JOBS QUEUE:
    command = 'cd /home/ec2-user && sudo python3 backend/work_service.py master_threaded ' + str(matrix_shape) + ' ' + queues_names[0] + ' ' + queues_names[1] + ' ' + bucket_names[0]
    if monitor:
        exec_SSH_on_instance_live(instances_names[0], command)
    else:
        exec_SSH_on_instance(instances_names[0], command, False)

    # STOP THE WORKERS LOOPS:
    stop_SSHs_on_instances_using_SSM(get_instance_ids_by_names(instances_names)[1:min(used_workers, worker_amount)+1], work_resp['Command']['CommandId'])

    # STOP THE TIMER:
    print('The computation took: ' + str(np.round(time.time() - sol_timer, 2)) + ' seconds.')
    return np.round(time.time() - sol_timer, 2)

# SERVICE: VERIFY JOBS:
def verify_result_matrix(bucket_name, instance_name, op_id, op_type):
    # JOB: VERIFY THE RESULTS COMPUTED ON AWS DISTRIBUTED CLOUD ENVIRONMENT, USING NUMPY'S MATRIX/ADD FUNCTIONS:
    command = 'sudo python3 backend/work_service.py verify ' + bucket_name + ' ' + op_id + ' ' + op_type
    exec_SSH_on_instance(instance_name, command)

# SERVICE: VERIFY MULTIPLE JOBS:
def verify_multiple_jobs(bucket_name, instance_name, op_ids, op_type):
    for i in range(len(op_ids)):
        command = 'sudo python3 backend/work_service.py verify ' + bucket_name + ' ' + op_ids[i] + ' ' + op_type
        exec_SSH_on_instance(instance_name, command)


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
    print('Bulk killing in progress...')
    for instance in ec2.instances.all():
        if instance.state['Name'] != 'terminated':
            instance.terminate()
    queues = get_all_sqs_queues()
    for queue in queues:
        queue.delete()
    for bucket in s3.buckets.all():
        bucket.objects.all().delete()
        bucket.delete()
    print('Killed all instances, queues and buckets.')
    time.sleep(60)   # Wait one minute for safety measures.