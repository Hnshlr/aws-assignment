import boto3

sqs = boto3.resource('sqs', region_name='us-east-1')

# CREATE SQS QUEUE:
def create_sqs_queue(queue_name):
    try:
        queue = sqs.create_queue(
            QueueName=queue_name,
            Attributes={
                'FifoQueue': 'true',
                'MessageRetentionPeriod': '86400',
                'ContentBasedDeduplication': 'true'
            }
        )
        return queue
    except Exception as e:
        print(e)
        return None

# CREATE MULTIPLE SQS QUEUES - AND WAIT FOR ALL TO BE CREATED:
def create_sqs_queues(queue_names):
    queues = []
    for i in range(len(queue_names)):
        try:
            queue = create_sqs_queue(queue_names[i])
            queues.append(queue)
            print(i+1,'/',len(queue_names),': Created queue: ', queue_names[i])
        except Exception as e:
            print(e)
            return None

# DELETE SQS QUEUE:
def delete_sqs_queue(queue_name):
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        queue.delete()
        print('Deleted queue: ', queue_name)
        return True
    except Exception as e:
        print(e)
        return False

# DELETE ALL SQS QUEUES:
def delete_all_sqs_queues():
    queues = get_all_sqs_queues()
    for queue in queues:
        queue.delete()
        print('Deleted queue: ', queue.url)

# GET ALL SQS QUEUES:
def get_all_sqs_queues():
    queues = []
    for queue in sqs.queues.all():
        queues.append(queue)
    return queues

# VIEW ALL SQS QUEUES:
def view_all_sqs_queues():
    queues = get_all_sqs_queues()
    for queue in queues:
        print(queue.url)

# GET QUEUE STATUS:
def get_queue_attributes(queue_name):
    queue_attributes = sqs.get_queue_by_name(QueueName=queue_name).attributes
    return queue_attributes

# SEND MESSAGE TO SQS QUEUE:
def send_message_to_sqs_queue(queue_name, message, group_id):
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        response = queue.send_message(
            MessageBody=message,
            MessageGroupId=group_id
        )
        print('Successfully sent msg to: ', queue_name)
        return response
    except Exception as e:
        print(e)
        return None

# BULK SEND MESSAGES TO SQS QUEUE, AND CONTROL RESPONSE:
def send_bulk_messages_to_sqs_queue(queue_name, messages, group_id):
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
    except Exception as e:
        print(e)
        return None
    for i in range(0, len(messages)):
        response = queue.send_message(
            MessageBody=messages[i],
            MessageGroupId=group_id
        )
        # print(response)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200 or 'connection' in response['ResponseMetadata']['HTTPHeaders']:
            # print('ERROR: Failed to send message to SQS queue! Retrying now...')
            while response['ResponseMetadata']['HTTPStatusCode'] != 200 or 'connection' in response['ResponseMetadata']['HTTPHeaders']:
                response = queue.send_message(
                    MessageBody=messages[i],
                    MessageGroupId=group_id
                )
                # print(response)
                if response['ResponseMetadata']['HTTPStatusCode'] == 200 and 'connection' not in response['ResponseMetadata']['HTTPHeaders']:
                    break
        print('Step ', i+1, '/', len(messages), ' successful.')
    print('Successfully sent ', len(messages), ' messages to: ', queue_name)

# GET LAST 10 MESSAGES FROM SQS QUEUE:
def get_last_ten_messages_from_sqs_queue(queue_name, flight_time):
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        messages = queue.receive_messages(
            MaxNumberOfMessages=10,
            VisibilityTimeout=flight_time
        )
        return messages
    except Exception as e:
        print(e)
        return None

# GET LAST MESSAGE FROM SQS QUEUE:
def get_last_message_from_sqs_queue(queue_name, flight_time):
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        messages = queue.receive_messages(
            MaxNumberOfMessages=1,
            VisibilityTimeout=flight_time
        )
        return messages
    except Exception as e:
        print(e)
        return None

# GET THE AMOUNT OF AVAILABLE MESSAGES IN SQS QUEUE:
def get_amount_of_available_messages_in_sqs_queue(queue_name):
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        amount = queue.attributes['ApproximateNumberOfMessages']
        return int(amount)
    except Exception as e:
        print(e)
        return None

# GET THE AMOUNT OF IN-FLIGHT MESSAGES IN SQS QUEUE:
def get_amount_of_in_flight_messages_in_sqs_queue(queue_name):
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        amount = queue.attributes['ApproximateNumberOfMessagesNotVisible']
        return int(amount)
    except Exception as e:
        print(e)
        return None

# DELETE MESSAGE FROM SQS QUEUE - BY RECEIPT HANDLE:
def delete_message_from_sqs_queue(queue_name, receipt_handle):
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        response = queue.delete_messages(
            Entries=[
                {
                    'Id': '1',
                    'ReceiptHandle': receipt_handle
                }
            ]
        )
        return response
    except Exception as e:
        print(e)
        return None

# GET MAX MESSAGE SIZE FROM SQS QUEUE - IN BYTES:
def get_max_message_size_from_sqs_queue(queue_name):
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        attributes = queue.attributes
        return int(attributes['MaximumMessageSize'])
    except Exception as e:
        print(e)
        return None

# PURGE SQS QUEUE:
def purge_queue(queue_name):
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        queue.purge()
        print('Purged queue: ', queue_name)
        return True
    except Exception as e:
        print(e)
        return False