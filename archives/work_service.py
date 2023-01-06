import sys
import os
import numpy as np
import pandas as pd
import json

sys.path.append(os.path.abspath(os.path.join('')))
from backend.controllers.sqs_controller import *
from backend.controllers.matrix_controller import *
from backend.controllers.s3_controller import *

# JOB: CREATE A MATRIX, SPLIT IT INTO SLICES, AND SEND THE SLICES TO THE JOBS QUEUE:
def create_matrix_then_split_and_send_jobs(matrix_shape, jobs_queue_name, s3_bucket_name):
    # CREATE A MATRIX - GIVING THE SIDE SIZE:
    matrix = create_random_square_matrix(matrix_shape)
    mx_id = str(np.random.randint(1000000000))
    print('Created a random ' + str(matrix_shape) + 'x' + str(matrix_shape) + ' matrix.')
    pd.DataFrame(matrix).to_csv('backend/data/input/' + mx_id + '.csv', index=False, header=False)
    upload_local_file_to_s3('backend/data/input/' + mx_id + '.csv', s3_bucket_name, 'backend/data/input/' + mx_id + '.csv')
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
                'mx-id': mx_id,
                'mx-shape': matrix_shape,
                'block-shape': blocks.shape[0],
                'i': i,
                'j': j,
                'left-slice': slices[i][j][0].tolist(),
                'right-slice': slices[i][j][1].tolist()
            }
            json_message_body = json.dumps(message_body)
            messages.append(json_message_body)

    # BULK SEND MESSAGES TO THE "JOBS" QUEUE:
    print('Starting to send ' + str(len(messages)) + ' messages to: ' + jobs_queue_name)
    send_bulk_messages_to_sqs_queue(jobs_queue_name, messages, 'work')

# JOB: GATHER JOBS FROM THE JOBS QUEUE, COMPUTE THE RESULTS, AND SEND THE RESULTS TO THE RESULTS QUEUE:
def gather_jobs_then_compute_and_send_results(jobs_queue_name, results_queue_name):
    print("Beginning matrix multiplication.")
    print("Gathering jobs from the jobs queue, and sending results to the results queue.")
    while get_amount_of_available_messages_in_sqs_queue(jobs_queue_name) > 0:
        messages = get_last_ten_messages_from_sqs_queue(jobs_queue_name, 5)
        result_messages = []
        print('...')
        for message in messages:
            message_body = json.loads(message.body)
            left_slice = np.array(message_body['left-slice'])
            right_slice = np.array(message_body['right-slice'])
            result = np.dot(left_slice, right_slice)
            result_message_body = {
                'mx-id': message_body['mx-id'],
                'mx-shape': message_body['mx-shape'],
                'block-shape': message_body['block-shape'],
                'i': message_body['i'],
                'j': message_body['j'],
                'result': result.tolist()
            }
            json_result_message_body = json.dumps(result_message_body)
            result_messages.append(json_result_message_body)
        try:
            send_bulk_messages_to_sqs_queue(results_queue_name, result_messages, 'result')
            for message in messages:
                delete_message_from_sqs_queue(jobs_queue_name, message.receipt_handle)
        except:
            print('Error occured while sending messages to the results queue. Continuing.')
            break
    print('The jobs queue is empty. All jobs have been gathered and sent to the results queue.')

# JOB: GATHER RESULTS FROM THE RESULTS QUEUE, AND RECONSTRUCT THE RESULT MATRIX:
def gather_results_and_reconstruct_matrix(results_queue_name, results_s3_bucket_name):
    print("Beginning matrix reconstruction.")
    results = []
    print("Gathering results from the results queue.")
    while get_amount_of_available_messages_in_sqs_queue(results_queue_name) > 0 or get_amount_of_in_flight_messages_in_sqs_queue(results_queue_name) > 0:
        messages = get_last_ten_messages_from_sqs_queue(results_queue_name, 5)
        print('...')
        for message in messages:
            message_body = json.loads(message.body)
            results.append(message_body)
            delete_message_from_sqs_queue(results_queue_name, message.receipt_handle)
    print('The results queue is empty. All results have been gathered.')
    print('Reconstructing the result matrix.')
    blocks_matrix = np.empty((results[0]['block-shape'], results[0]['block-shape']), dtype=np.ndarray)
    for result in results:
        blocks_matrix[result['i']][result['j']] = np.array(result['result'])
    result_matrix = np.concatenate([np.concatenate(blocks_matrix[i], axis=1) for i in range(blocks_matrix.shape[0])], axis=0)
    print('Finished reconstructing the result matrix.')
    print('Sending result matrix in a .csv file to: ' + results_s3_bucket_name)
    pd.DataFrame(result_matrix).to_csv('backend/data/output/mx/' + results[0]['mx-id'] + '.csv', index=False, header=False)
    upload_local_file_to_s3('backend/data/output/mx/' + results[0]['mx-id'] + '.csv', results_s3_bucket_name, 'backend/data/output/mx/' + results[0]['mx-id'] + '.csv')
    print('Finished sending result matrix n°' + results[0]['mx-id'] + ' in a .csv file to: ' + results_s3_bucket_name)

# def download_file_from_s3(bucketname, path, filename, destination_path):
#     s3.Bucket(bucketname).download_file(path+filename, destination_path+filename)

# JOB: VERIFY A RESULT MATRIX:
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
    if sys.argv[1] == 'create':
        create_matrix_then_split_and_send_jobs(int(sys.argv[2]), sys.argv[3], sys.argv[4])
    elif sys.argv[1] == 'getjobs':
        gather_jobs_then_compute_and_send_results(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == 'getresults':
        gather_results_and_reconstruct_matrix(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == 'verify':
        verify_result_matrix(sys.argv[2], sys.argv[3], sys.argv[4])

if __name__ == '__main__':
    main()
