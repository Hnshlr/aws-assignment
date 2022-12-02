import numpy as np

def compute_single_block(A, B, i, j, size):
    block = np.dot(
        np.concatenate([A[i][k] for k in range(size)], axis=1),
        np.concatenate([B[k][j] for k in range(size)], axis=0)
    )
    return block

if __name__ == '__main__':
    pass