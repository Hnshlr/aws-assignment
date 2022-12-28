import sys, os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join('')))
from backend.controllers.matrix_controller import *
from backend.controllers.s3_controller import *

def outprint(stdout, stderr):
    if stdout.decode('utf-8') != '':
        print(stdout.decode('utf-8'))
    if stderr.decode('utf-8') != '':
        print('Error:\n', stderr.decode('utf-8'))
