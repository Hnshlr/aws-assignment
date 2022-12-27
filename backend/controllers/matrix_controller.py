import numpy as np
import json

# Create a matrix of nxn size:
def create_random_square_matrix(n):
    return np.random.randint(0, 10, size=(n, n))

# Split the matrix in blocks until the size of combined blocks is less than max_message_size:
def find_optimal_blocks_amount(matrix, max_message_size):
    matrix_body = {
        'i': 1,
        'j': 1,
        'temp-slice': matrix.tolist()
    }
    matrix_size = len(json.dumps(matrix_body))
    min_blocks_amount = 1
    for i in range(2, 10000):
        block_size = matrix_size / i**2
        amount_of_blocks_per_message = 2*i
        message_size = block_size * amount_of_blocks_per_message
        if message_size < max_message_size and matrix.shape[0] % i < np.round(matrix.shape[0] / i):
            min_blocks_amount = i**2
            break
    return min_blocks_amount

# Split matrix into blocks:
def split_matrix_in_blocks(matrix, amount_of_blocks):
    side_size = np.sqrt(amount_of_blocks)
    if not side_size.is_integer():
        raise ValueError('ERROR: Amount of blocks must be a square number!')
    else:
        if matrix.shape[0] % side_size != 0:
            pad = np.floor(matrix.shape[0] / side_size)
            sub_matrices = np.empty((int(side_size)+1, int(side_size)+1), dtype=np.ndarray)
            for i in range(0, sub_matrices.shape[0]-1):
                for j in range(0, sub_matrices.shape[0]-1):
                    sub_matrices[i][j] = matrix[int(i * pad):int((i + 1) * pad), int(j * pad):int((j + 1) * pad)]
            for i in range(0, sub_matrices.shape[0]-1):
                sub_matrices[i][sub_matrices.shape[0]-1] = matrix[int(i * pad):int((i + 1) * pad), int((sub_matrices.shape[0]-1) * pad):]
            for j in range(0, sub_matrices.shape[0]-1):
                sub_matrices[sub_matrices.shape[0]-1][j] = matrix[int((sub_matrices.shape[0]-1) * pad):, int(j * pad):int((j + 1) * pad)]
            sub_matrices[sub_matrices.shape[0]-1][sub_matrices.shape[0]-1] = matrix[int((sub_matrices.shape[0]-1) * pad):, int((sub_matrices.shape[0]-1) * pad):]
        else:
            sub_matrices = np.empty((int(side_size), int(side_size)), dtype=np.ndarray)
            pad = matrix.shape[0] / side_size
            for i in range(0, sub_matrices.shape[0]):
                for j in range(0, sub_matrices.shape[0]):
                    sub_matrices[i][j] = matrix[int(i * pad):int((i + 1) * pad), int(j * pad):int((j + 1) * pad)]
    return sub_matrices

# Compute a single block:
def compute_single_block(A, B, i, j, size):
    block = np.dot(
        np.concatenate([A[i][k] for k in range(size)], axis=1),
        np.concatenate([B[k][j] for k in range(size)], axis=0)
    )
    return block

# Multiply two matrices:
def multiply_matrices(matrix1, matrix2, amount_of_blocks):
    # Split matrices into blocks:
    A = split_matrix_in_blocks(matrix1, amount_of_blocks)
    B = split_matrix_in_blocks(matrix2, amount_of_blocks)
    result = np.empty((A.shape[0], B.shape[1]), dtype=np.ndarray)
    size = A.shape[0]
    for i in range(0, result.shape[0]):
        for j in range(0, result.shape[1]):
            result[i][j] = compute_single_block(A, B, i, j, size)
    result = np.concatenate([np.concatenate([result[i][j] for j in range(size)], axis=1) for i in range(size)], axis=0)
    return result