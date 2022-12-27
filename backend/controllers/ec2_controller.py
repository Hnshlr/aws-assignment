from backend.controllers.boto3_controller import *

import time
import paramiko

# Permission key:
pem_key = 'learner-lab-cfg/labsuser.pem'
# Create an EC2 resource (higher level abstraction than a client):
ec2 = boto3.resource('ec2')

# START ALL EC2 INSTANCES:
def start_all_instances():
    # Start all instances:
    for instance in ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['stopped']}]):
        instance.start()
        print('Starting instance: ', instance.id, instance.tags[0]['Value'])

# STOP EC2 INSTANCE - BY NAME:
def stop_instance_by_name(instance_name):
    # Stop instance by name:
    for instance in ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [instance_name], 'Name': 'instance-state-name', 'Values': ['running']}]):
        instance.stop()
        print('Stopping instance: ', instance.id, instance.tags[0]['Value'])

# STOP ALL EC2 INSTANCES:
def stop_all_instances():
    # Stop all instances:
    for instance in ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]):
        instance.stop()
        print('Stopping instance: ', instance.id, instance.tags[0]['Value'])

# PRINT EC2 INSTANCE DETAILS:
def print_instance_details(instance):
    print("{id=%s, name=%s, state=%s, type=%s}" % (instance.id, instance.tags[0]['Value'], instance.state['Name'], instance.instance_type))

# VIEW ALL EC2 INSTANCES:
def view_all_instances(include_terminated):
    # Print instance ID, name, state, and type:
    for instance in ec2.instances.all():
        if include_terminated or instance.state['Name'] != 'terminated':
            print_instance_details(instance)

# VIEW ALL EC2 INSTANCES - BY STATE FILTER:
def view_instances_by_state(state):
    # Print instance ID, name, state, and type; who are running
    for instance in ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': [state]}]):
        print_instance_details(instance)

# TERMINATE ALL EC2 INSTANCES:
def terminate_all_instances():
    # Terminate all instances:
    for instance in ec2.instances.all():
        if instance.state['Name'] != 'terminated':
            instance.terminate()
            print('Terminated instance: ', instance.id, instance.tags[0]['Value'])

# TERMINATE EC2 INSTANCE - BY NAME:
def terminate_instance_by_name(name):
    for instance in ec2.instances.all():
        if instance.tags[0]['Value'] == name and instance.state['Name'] != 'terminated':
            instance.terminate()
            print('Terminated instance: ', instance.id, instance.tags[0]['Value'])

# CREATE INSTANCE - BY NAME, USING "amazon linux 2" AMI, "t2.micro" INSTANCE TYPE, "vockey" KEY PAIR, AND "default" SECURITY GROUP:
def create_instance(name):
    ec2.create_instances(
        ImageId='ami-0b0dcb5067f052a63',
        MinCount=1,
        MaxCount=1,
        InstanceType='t2.micro',
        KeyName='vockey',
        SecurityGroupIds=['sg-0f52fa9fe5477133b'],
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': name
                    },
                ]
            },
        ],
        IamInstanceProfile={
            'Arn': 'arn:aws:iam::868429207081:instance-profile/LabInstanceProfile'
        },
    )

# CREATE MULTIPLES INSTANCES - BY NAME, USING "amazon linux 2" AMI, "t2.micro" INSTANCE TYPE, "vockey" KEY PAIR, AND "default" SECURITY GROUP:
def create_instances_and_wait_for_running(names):
    for name in names:
        create_instance(name)
    print('All instances created. Waiting for them to be running...')
    # Filter for instances with the given names:
    instances = ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': names}, {'Name': 'instance-state-name', 'Values': ['pending']}])
    amount = 0
    while len(list(instances)) > 0:
        if amount != len(list(instances)):
            print('Remaining instances: ', len(list(instances)))
            amount = len(list(instances))
        time.sleep(1)
        instances = ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': names}, {'Name': 'instance-state-name', 'Values': ['pending']}])
    print('All instances are running.')

# UPDATE INSTANCE AWS CREDENTIALS:
def update_instance_credentials_using_boto3_session_credentials(instance_name):
    exec_SSH_on_instance(instance_name, 'aws configure set aws_access_key_id '+get_boto3_session_credentials().access_key)
    exec_SSH_on_instance(instance_name, 'aws configure set aws_secret_access_key '+get_boto3_session_credentials().secret_key)
    exec_SSH_on_instance(instance_name, 'aws configure set aws_session_token '+get_boto3_session_credentials().token)
    exec_SSH_on_instance(instance_name, 'aws configure set region us-east-1')

# GET INSTANCE ID - BY NAME:
def get_instance_id_by_name(name):
    for instance in ec2.instances.all():
        if instance.tags[0]['Value'] == name and instance.state['Name'] != 'terminated':
            return instance.id

# GET INSTANCE PUBLIC IP - BY NAME:
def get_instance_public_dns_by_name(name):
    for instance in ec2.instances.all():
        if instance.tags[0]['Value'] == name and instance.state['Name'] != 'terminated':
            return instance.public_dns_name

# GET INSTANCE PUBLIC IP - BY NAME:
def get_instance_public_ip_by_name(name):
    for instance in ec2.instances.all():
        if instance.tags[0]['Value'] == name and instance.state['Name'] != 'terminated':
            return instance.public_ip_address

# GET INSTANCE PUBLIC IP - BY ID:
def get_instance_public_dns_by_id(instance_id):
    for instance in ec2.instances.all():
        if instance.id == instance_id:
            return instance.public_dns_name

# EXECUTE SSH COMMAND ON INSTANCE - BY NAME:
def exec_SSH_on_instance(instance_name, command):
    paramiko_key = paramiko.RSAKey.from_private_key_file(pem_key)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(hostname=get_instance_public_dns_by_id(get_instance_id_by_name(instance_name)), username='ec2-user', pkey=paramiko_key)
        stdin, stdout, stderr = client.exec_command(command)
        return stdout.read(), stderr.read()
    except Exception as e:
        return e

# DOWNLOAD DIRECTORY ON INSTANCE FROM S3 BUCKET - BY NAME:
def download_directory_on_instance_from_s3_bucket(instance_name, s3_bucket, s3_directory, local_directory):
    stdout, stderr = exec_SSH_on_instance(instance_name, 'aws s3 sync s3://'+s3_bucket+'/'+s3_directory+' '+local_directory)
    return stdout, stderr