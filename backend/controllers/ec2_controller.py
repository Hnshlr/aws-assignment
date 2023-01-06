from backend.controllers.boto3_controller import *

import time
import paramiko

# Permission key:
pem_key = 'learner-lab-cfg/labsuser.pem'
# Create an EC2 resource (higher level abstraction than a client):
ec2 = boto3.resource('ec2', region_name='us-east-1')

# START ALL EC2 INSTANCES:
def start_all_instances():
    # Start all instances:
    for instance in ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['stopped']}]):
        instance.start()
        print('Starting instance: ', instance.id, instance.tags[0]['Value'])

# START ALL EC2 INSTANCES - AND WAIT FOR THEM TO BE RUNNING:
def start_all_instances_and_wait_for_running():
    # Start all instances:
    for instance in ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['stopped']}]):
        instance.start()
        print('Starting instance: ', instance.id, instance.tags[0]['Value'])
    print('All instances started. Waiting for them to be running...')
    # Filter for instances with the given names:
    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['pending']}])
    amount = 0
    while len(list(instances)) > 0:
        if amount != len(list(instances)):
            print('Remaining instances: ', len(list(instances)))
            amount = len(list(instances))
        time.sleep(1)
        instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['pending']}])
    print('All instances are running.')

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

# STOP ALL EC2 INSTANCES - AND WAIT FOR THEM TO BE STOPPED:
def stop_all_instances_and_wait_for_stopped():
    # Stop all instances:
    for instance in ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]):
        instance.stop()
        print('Stopping instance: ', instance.id, instance.tags[0]['Value'])
    print('All instances stopped. Waiting for them to be stopped...')
    # Filter for instances with the given names:
    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['stopping']}])
    amount = 0
    while len(list(instances)) > 0:
        if amount != len(list(instances)):
            print('Remaining instances: ', len(list(instances)))
            amount = len(list(instances))
        time.sleep(1)
        instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['stopping']}])
    print('All instances are stopped.')

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
    print('Instances public IPs:')
    publicips = []
    for instance in ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': names}, {'Name': 'instance-state-name', 'Values': ['running']}]):
        publicips.append(instance.public_dns_name)
    print(publicips)

# UPDATE INSTANCE AWS CREDENTIALS:
def update_instances_credentials_using_boto3_session_credentials(instance_ids):
    boto3_credentials = get_boto3_session_credentials()
    commands = [
        'aws configure set aws_access_key_id '+boto3_credentials.access_key,
        'aws configure set aws_secret_access_key '+boto3_credentials.secret_key,
        'aws configure set aws_session_token '+boto3_credentials.token,
        'aws configure set region us-east-1'
    ]
    paramiko_key = paramiko.RSAKey.from_private_key_file(pem_key)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    for instance_id in instance_ids:
        client.connect(hostname=get_instance_public_dns_by_id(instance_id), username='ec2-user', pkey=paramiko_key)
        for command in commands:
            stdin, stdout, stderr = client.exec_command(command)
            if stderr.read().decode('utf-8') != '':
                print('Error: ', stderr.read().decode('utf-8'))
        client.close()
    print('All instances credentials updated.')

# GET INSTANCE ID - BY NAME:
def get_instance_id_by_name(name):
    for instance in ec2.instances.all():
        if instance.tags[0]['Value'] == name and instance.state['Name'] != 'terminated':
            return instance.id

# GET INSTANCE IDS - BY NAMES:
def get_instance_ids_by_names(names):
    instance_ids = []
    for instance in ec2.instances.all():
        if instance.tags[0]['Value'] in names and instance.state['Name'] == 'running':
            instance_ids.append(instance.id)
    return instance_ids

# GET INSTANCE PUBLIC IP - BY NAME:
def get_instance_public_dns_by_name(name):
    for instance in ec2.instances.all():
        if instance.tags[0]['Value'] == name and instance.state['Name'] != 'terminated':
            return instance.public_dns_name

# GET INSTANCES PUBLIC IPS - BY NAMES:
def get_instances_public_dns_by_names(names):
    instance_public_dns = []
    for instance in ec2.instances.all():
        if instance.tags[0]['Value'] in names and instance.state['Name'] != 'terminated':
            instance_public_dns.append(instance.public_dns_name)
    return instance_public_dns

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
    try:
        exec_SSH_on_instance(instance_name, 'rm -rf '+local_directory)
    except:
        pass
    stdout, stderr = exec_SSH_on_instance(instance_name, 'aws s3 sync s3://'+s3_bucket+'/'+s3_directory+' '+local_directory)
    return stdout, stderr

# UPLOAD LOCAL FILE TO EC2 INSTANCE - BY NAME:
def upload_local_file_to_ec2_instance(instance_name, local_file, remote_file):
    paramiko_key = paramiko.RSAKey.from_private_key_file(pem_key)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(hostname=get_instance_public_dns_by_id(get_instance_id_by_name(instance_name)), username='ec2-user', pkey=paramiko_key)
        sftp = client.open_sftp()
        sftp.put(local_file, remote_file)
        sftp.close()
        return 'File uploaded successfully.'
    except Exception as e:
        return e