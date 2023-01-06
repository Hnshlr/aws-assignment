import boto3

ssm_client = boto3.client('ssm', region_name='us-east-1')

# EXECUTE SSH COMMANDS ON INSTANCES USING SSM - BY INSTANCE IDS - OPTIONAL OUPUT BUCKET:
def exec_SSHs_on_instances_using_SSM(instance_ids, commands, output_bucket_name=None):
    if output_bucket_name is not None:
        response = ssm_client.send_command(
            InstanceIds=instance_ids,
            DocumentName='AWS-RunShellScript',
            Parameters={'commands': commands},
            OutputS3BucketName=output_bucket_name
        )
    else:
        response = ssm_client.send_command(
            InstanceIds=instance_ids,
            DocumentName='AWS-RunShellScript',
            Parameters={'commands': commands}
        )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print('Successfully sent ', len(commands), ' commands to ', len(instance_ids), ' instances.')
        print('Commands are now being executed in the background.')
        if output_bucket_name != None:
            print('Saving output to the S3 bucket: ', output_bucket_name)
            return response

# STOP SSH COMMANDS ON INSTANCES USING SSM - BY INSTANCE IDS AND COMMAND ID:
def stop_SSHs_on_instances_using_SSM(instance_ids, command_id):
    response = ssm_client.cancel_command(
        InstanceIds=instance_ids,
        CommandId=command_id
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print('Successfully stopped command ', command_id, ' on ', len(instance_ids), ' instances.')
        return response