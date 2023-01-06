import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join('')))
from backend.controllers.sqs_controller import *
from backend.controllers.matrix_controller import *
from backend.controllers.s3_controller import *

# MASTER'S JOB: SEND A MATRIX MULTIPLICATION JOB TO SQS, WHILE COLLECTING RESULTS:
def master_loop(matrix_shape, jobs_queue_name, results_queue_name, s3_bucket_name):
    jobs_messages = create_matrix_and_messages(matrix_shape, jobs_queue_name, s3_bucket_name)
    results_messages = []
    blocks_amount = len(jobs_messages)
    result_size = int(np.sqrt(blocks_amount))
    op_id = json.loads(jobs_messages[0])['op-id']
    blocks_matrix = np.empty((result_size, result_size), dtype=np.ndarray)
    try:
        jobs_queue = sqs.get_queue_by_name(QueueName=jobs_queue_name)
        jobs_queue.purge()
        jobs_groupid = 'work'
        results_queue = sqs.get_queue_by_name(QueueName=results_queue_name)
        results_queue.purge()
        result_groupid = 'result'
        print('Jobs and results queues ready.')
    except Exception as e:
        print(e)
        return None
    send_jobs, receive_results = True, True
    print('Starting master loop...')
    while send_jobs or receive_results:
        # Send jobs to the jobs SQS queue:
        if len(jobs_messages) == 0 and send_jobs:
            print('All jobs have been sent.')
            send_jobs = False
        if send_jobs:
            response = jobs_queue.send_message(
                MessageBody=jobs_messages[0],
                MessageGroupId=jobs_groupid
            )
            verify_response_and_resend_if_needed(response, jobs_messages[0], jobs_groupid, jobs_queue)
            jobs_messages.pop(0)
            print('Sent job n° ', blocks_amount - len(jobs_messages), '/', blocks_amount, ' successfully.')
        # Gather results from the results SQS queue:
        if len(results_messages) == blocks_amount and receive_results:
            print('All computed results have been received.')
            receive_results = False
            # Concatenate the blocks array of arrays of arrays into a single array of arrays:
            result_matrix = np.concatenate([np.concatenate(blocks_matrix[i], axis=1) for i in range(blocks_matrix.shape[0])], axis=0)
            pd.DataFrame(result_matrix).to_csv('backend/data/output/mx/' + op_id + '.csv', index=False, header=False)
            upload_local_file_to_s3('backend/data/output/mx/' + op_id + '.csv', s3_bucket_name, 'backend/data/output/mx/' + op_id + '.csv')
            print('Result matrix id n°', op_id, ' successfully computed and saved to: ', s3_bucket_name)
        if receive_results:
            messages = results_queue.receive_messages(MaxNumberOfMessages=10, VisibilityTimeout=5)
            for message in messages:
                results_queue.delete_messages(Entries=[{'Id': '1', 'ReceiptHandle': message.receipt_handle}])
                results_messages.append(message)
                message_body = json.loads(message.body)
                blocks_matrix[message_body['i']][message_body['j']] = np.array(message_body['result'])
                print('Received block result n° [', message_body['i'], ',', message_body['j'], '] from operation id n° ', message_body['op-id'], ' successfully.')
    print('Shutting down the master loop...')

# WORKER'S JOB: KEEP LISTENING TO THE SQS QUEUE, COLLECT JOBS, COMPUTE RESULTS AND SEND TO SQS QUEUE:
def worker_loop(jobs_queue_name, results_queue_name):
    try:
        jobs_queue = sqs.get_queue_by_name(QueueName=jobs_queue_name)
        results_queue = sqs.get_queue_by_name(QueueName=results_queue_name)
        result_groupid = 'result'
        print('Jobs and results queues ready.')
    except Exception as e:
        print(e)
        return None
    print('Starting worker loop...')
    while True:
        try:
            messages = jobs_queue.receive_messages(MaxNumberOfMessages=10, VisibilityTimeout=5)
            # If the message has a specific group id, break the loop:
            if len(messages) != 0:
                print('Received ', len(messages), ' jobs. Processing...')
            for message in messages:
                jobs_queue.delete_messages(Entries=[{'Id': '1', 'ReceiptHandle': message.receipt_handle}])
                message_body = json.loads(message.body)
                result = np.dot(np.array(message_body['left-slice']), np.array(message_body['right-slice']))
                result_message = {
                    'message-id': message_body['message-id'],
                    'op-id': message_body['op-id'],
                    'op-type': message_body['op-type'],
                    'matrix-shape': message_body['matrix-shape'],
                    'block-shape': message_body['block-shape'],
                    'i': message_body['i'],
                    'j': message_body['j'],
                    'result': result.tolist()
                }
                response = results_queue.send_message(
                    MessageBody=json.dumps(result_message),
                    MessageGroupId=result_groupid
                )
                verify_response_and_resend_if_needed(response, json.dumps(result_message), result_groupid, results_queue)
                print('Sent block result n° [', message_body['i'], ',', message_body['j'], '] from operation id n° ', message_body['op-id'], ' successfully.')
        except Exception as e:
            print(e)
            return None


# SERVICES:
def verify_response_and_resend_if_needed(response, message_body, groupd_id, queue_name):
    if response['ResponseMetadata']['HTTPStatusCode'] != 200 or 'connection' in response['ResponseMetadata']['HTTPHeaders']:
        while response['ResponseMetadata']['HTTPStatusCode'] != 200 or 'connection' in response['ResponseMetadata']['HTTPHeaders']:
            response = queue_name.send_message(
                MessageBody=message_body,
                MessageGroupId=groupd_id
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200 and 'connection' not in response['ResponseMetadata']['HTTPHeaders']:
                break

def create_matrix_and_messages(matrix_shape, jobs_queue_name, s3_bucket_name):
    # CREATE A MATRIX - GIVING THE SIDE SIZE:
    matrix = create_random_square_matrix(matrix_shape)
    op_id = str(np.random.randint(1000000000))
    print('Created a random ' + str(matrix_shape) + 'x' + str(matrix_shape) + ' matrix.')
    pd.DataFrame(matrix).to_csv('backend/data/input/' + op_id + '.csv', index=False, header=False)
    upload_local_file_to_s3('backend/data/input/' + op_id + '.csv', s3_bucket_name, 'backend/data/input/' + op_id + '.csv')
    print('Uploaded the matrix to: ' + s3_bucket_name)

    # SPLIT MATRIX INTO BLOCKS - USING OPTIMAL BLOCKS AMOUNT:
    max_SQS_msg_size = get_max_message_size_from_sqs_queue(jobs_queue_name)
    blocks = split_matrix_in_blocks(matrix, find_optimal_blocks_amount(matrix, max_SQS_msg_size))
    print('Split the matrix into ' + str(blocks.shape[0]) + '*' + str(blocks.shape[1]) + '=' + str(len(blocks)) + ' blocks.')

    # STORE IN AN ARRAY THE SLICES REQUIRED TO COMPUTE EACH BLOCK OF THE RESULT MATRIX:
    slices = np.empty((blocks.shape[0], blocks.shape[1], 2), dtype=np.ndarray)
    for i in range(0, blocks.shape[0]):
        for j in range(0, blocks.shape[1]):
            slices[i][j][0] = np.concatenate([blocks[i][k] for k in range(blocks.shape[0])], axis=1)
            slices[i][j][1] = np.concatenate([blocks[k][j] for k in range(blocks.shape[0])], axis=0)

    # CREATE A LIST OF MESSAGES TO SEND TO SQS QUEUE:
    messages = []
    for i in range(0, blocks.shape[0]):
        for j in range(0, blocks.shape[1]):
            message_body = {
                'message-id': i * blocks.shape[0] + j + 1,
                'op-id': op_id,
                'op-type': 'mx',
                'matrix-shape': matrix_shape,
                'block-shape': blocks.shape[0],
                'i': i,
                'j': j,
                'left-slice': slices[i][j][0].tolist(),
                'right-slice': slices[i][j][1].tolist()
            }
            json_message_body = json.dumps(message_body)
            messages.append(json_message_body)
    return messages
def verify_result_matrix(results_s3_bucket_name, id, op_type):
    if op_type == 'mx':
        download_file_from_s3(results_s3_bucket_name, 'backend/data/output/mx/', id + '.csv', 'backend/data/output/mx/')
        result_matrix = pd.read_csv('backend/data/output/mx/' + id + '.csv', header=None).values
        download_file_from_s3(results_s3_bucket_name, 'backend/data/input/', id + '.csv', 'backend/data/input/')
        input_matrix = pd.read_csv('backend/data/input/' + id + '.csv', header=None).values
        bool = np.array_equal(result_matrix, np.dot(input_matrix, input_matrix))
        print('Results are correct for multiplication n°' + id + ": " + str(bool))
        return bool
    elif op_type == 'add':
        download_file_from_s3(results_s3_bucket_name, 'backend/data/output/add/', id + '.csv', 'backend/data/output/add/')
        result_matrix = pd.read_csv('backend/data/output/add/' + id + '.csv', header=None).values
        download_file_from_s3(results_s3_bucket_name, 'backend/data/input/', id + '.csv', 'backend/data/input/')
        input_matrix = pd.read_csv('backend/data/input/' + id + '.csv', header=None).values
        bool = np.array_equal(result_matrix, np.add(input_matrix, input_matrix))
        print("Results are correct for addition n°" + id + ": " + str(bool))
        return bool


def main():
    if sys.argv[1] == 'master':
        master_loop(int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5])
    elif sys.argv[1] == 'worker':
        worker_loop(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == 'verify':
        verify_result_matrix(sys.argv[2], sys.argv[3], sys.argv[4])

if __name__ == '__main__':
    main()
