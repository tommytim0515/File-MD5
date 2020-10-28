import os
import csv
import json
import pandas
import hashlib
import multiprocessing
from multiprocessing import Lock, Process, Queue


start_file_counter = 0
procs = []

def get_paths(file_index):
    with open('paths/find_path{}.json'.format(file_index)) as f:
        paths = json.load(f)
        return paths

if __name__ == '__main__':
    if not os.path.exists('paths'):
        print('Cannot find directory: paths')
    else:        
        num_cpu = multiprocessing.cpu_count()
        print('Number of CPU: {}'.format(num_cpu))
    
    paths = get_paths(start_file_counter)
    # while paths is not None:
    for key, value in paths.items():
        print(value)