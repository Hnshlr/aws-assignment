import boto3

ssm_client = boto3.client('ssm')

# EXECUTE SSH COMMAND ON INSTANCE USING SSM - BY INSTANCE ID:
def exec_SSH_on_instance_using_SSM(instance_id, command):
    response = ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName='AWS-RunShellScript',
        Parameters={'commands': [command]}
    )
    return response