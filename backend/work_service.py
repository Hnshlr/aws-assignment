import sys
import os
import numpy as np
import json

sys.path.append(os.path.abspath(os.path.join('')))
from backend.controllers.sqs_controller import *
from backend.controllers.matrix_controller import *

# JOB: CREATE A MATRIX, SPLIT IT INTO SLICES, AND SEND THE SLICES TO THE JOBS QUEUE:
def create_matrix_then_split_and_send_jobs(matrix_shape, jobs_queue_name):
    # CREATE A MATRIX - GIVING THE SIDE SIZE:
    matrix = create_random_square_matrix(matrix_shape)

    # SPLIT MATRIX INTO BLOCKS - USING OPTIMAL BLOCKS AMOUNT:
    max_SQS_msg_size = get_max_message_size_from_sqs_queue(jobs_queue_name)
    blocks = split_matrix_in_blocks(matrix, find_optimal_blocks_amount(matrix, max_SQS_msg_size))

    # STORE IN AN ARRAY THE SLICES REQUIRED TO COMPUTE EACH BLOCK OF THE RESULT MATRIX:
    slices = np.empty((blocks.shape[0], blocks.shape[1]), dtype=np.ndarray)
    for i in range(0, blocks.shape[0]):
        for j in range(0, blocks.shape[1]):
            slices[i][j][0] = np.concatenate([blocks[i][k] for k in range(blocks.shape[0])], axis=1)
            slices[i][j][1] = np.concatenate([blocks[k][j] for k in range(blocks.shape[0])], axis=0)

    # CREATE A LIST OF MESSAGES TO SEND TO SQS QUEUE:
    messages = []
    for i in range(0, blocks.shape[0]):
        for j in range(0, blocks.shape[1]):
            message_body = {
                'i': i,
                'j': j,
                'left-slice': slices[i][j][0].tolist(),
                'right-slice': slices[i][j][1].tolist()
            }
            json_message_body = json.dumps(message_body)
            messages.append(json_message_body)

    # BULK SEND MESSAGES TO THE "JOBS" QUEUE:
    send_bulk_messages_to_sqs_queue(jobs_queue_name, messages, 'work')

# JOB: GATHER JOBS FROM THE JOBS QUEUE, COMPUTE THE RESULTS, AND SEND THE RESULTS TO THE RESULTS QUEUE:
def gather_jobs_then_compute_and_send_results(jobs_queue_name, results_queue_name):
    print("Beginning matrix multiplication.")
    print("Gathering jobs from the jobs queue, and sending results to the results queue.")
    while get_amount_of_in_flight_messages_in_sqs_queue(jobs_queue_name) > 0 or get_amount_of_available_messages_in_sqs_queue(jobs_queue_name) > 0:
        messages = get_last_ten_messages_from_sqs_queue(jobs_queue_name, 5)
        result_messages = []
        for message in messages:
            print('...')
            message_body = json.loads(message.body)
            left_slice = np.array(message_body['left-slice'])
            right_slice = np.array(message_body['right-slice'])
            result = np.dot(left_slice, right_slice)
            result_message_body = {
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

def gather_results_and_reconstruct_matrix(results_queue_name, result_matrix_shape):
    print("Beginning matrix reconstruction.")
    result_matrix = np.empty((result_matrix_shape, result_matrix_shape), dtype=np.ndarray)
    print("Gathering results from the results queue.")
    while get_amount_of_available_messages_in_sqs_queue(results_queue_name) > 0 or get_amount_of_in_flight_messages_in_sqs_queue(results_queue_name) > 0:
        messages = get_last_ten_messages_from_sqs_queue(results_queue_name, 5)
        print('...')
        for message in messages:
            message_body = json.loads(message.body)
            result_matrix[message_body['i']][message_body['j']] = np.array(message_body['result'])
            delete_message_from_sqs_queue(results_queue_name, message.receipt_handle)
    result_matrix = np.concatenate([np.concatenate([result_matrix[i][j] for j in range(result_matrix_shape)], axis=1) for i in range(result_matrix_shape)], axis=0)
    print('The results queue is empty. All results have been gathered and the result matrix has been reconstructed.')
    return result_matrix

def main():
    if sys.argv[1] == 'create':

    elif sys.argv[1] == 'getjobs':
        gather_jobs_then_compute_and_send_results(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == 'getresults':
        gather_results_and_reconstruct_matrix(sys.argv[2], int(sys.argv[3]))

if __name__ == '__main__':
    main()
