import boto3

ssm_client = boto3.client('ssm', region_name='us-east-1')

# EXECUTE SSH COMMAND ON INSTANCE USING SSM - BY INSTANCE ID:
def exec_SSH_on_instance_using_SSM(instance_id, command):
    response = ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName='AWS-RunShellScript',
        Parameters={'commands': [command]}
    )
    return response

# EXECUTE SSH COMMANDS ON INSTANCE USING SSM - BY INSTANCE ID:
def exec_SSHs_on_instance_using_SSM(instance_id, commands):
    response = ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName='AWS-RunShellScript',
        Parameters={'commands': commands}
    )
    return response

# EXECUTE SSH COMMANDS ON INSTANCES USING SSM - BY INSTANCE IDS:
def exec_SSHs_on_instances_using_SSM(instance_ids, commands):
    response = ssm_client.send_command(
        InstanceIds=instance_ids,
        DocumentName='AWS-RunShellScript',
        Parameters={'commands': commands}
    )
    print('Sent ', len(commands), ' commands to ', len(instance_ids), ' instances.')
    print('Commands are now being executed in the background.')
    return response

# EXECUTE SSH COMMAND ON INSTANCES USING SSM - BY INSTANCE IDS:
def exec_SSH_on_instances_using_SSM(instance_ids, command):
    response = ssm_client.send_command(
        InstanceIds=instance_ids,
        DocumentName='AWS-RunShellScript',
        Parameters={'commands': [command]}
    )
    return response
