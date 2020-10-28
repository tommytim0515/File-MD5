import os
import csv
import json
import pandas
import queue
import hashlib
import multiprocessing
from multiprocessing import Lock, Process, Queue

DISK_NAME = 'MRI05'
CSV_DIR = './Miley_201028_MRI05_paths.csv'
MAX_CSV_Q_SIZE = 2000
file_counter = 0
procs = []
path_queue = Queue()
csv_queue = Queue()
path_queue_lock = Lock()
csv_queue_lock = Lock()
path_lock = Lock()


def get_paths(file_index):
    if not os.path.exists('paths/find_path{}.json'.format(file_index)):
        return
    with open('paths/find_path{}.json'.format(file_index)) as f:
        paths = json.load(f)
        return paths


def calculate_md5(path_queue, path_queue_lock, csv_queue, csv_queue_lock):
    while not path_queue.empty():

        path_queue_lock.acquire()
        path = path_queue.get()
        path_queue_lock.release()

        size = os.path.getsize(path)
        md5_hash = hashlib.md5()
        md5_value = None

        with open(path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
            md5_value = md5_hash.hexdigest()

        if md5_value is None:
            print('File handling error!')
        else:
            csv_queue_lock.acquire()
            csv_queue.put([DISK_NAME, path, size, md5_value])
            csv_queue_lock.release()


def save_to_csv(csv_queue):
    if not csv_queue.empty():
        csv_results = [[] for i in range(4)]
        while not csv_queue.empty():
            buffer = csv_queue.get()
            for i in range(4):
                csv_results[i].append(buffer[i])

        df = pandas.DataFrame(data={"Disk": csv_results[0], "Path": csv_results[1],
                                    "FileSize": csv_results[2], "md5": csv_results[3]})

        if not os.path.exists(CSV_DIR):
            df.to_csv(CSV_DIR, sep=',', index=False)
        else:
            with open(CSV_DIR, 'a', newline='') as f:
                df.to_csv(f, sep=',', header=False, index=False)
        print('Saved to csv successfully')
        for item in csv_results[1]:
            paths[item] = '1'


def save_to_csv_process(csv_queue, path_queue, csv_queue_lock, paths, path_lock):
    while not path_queue.empty():
        if csv_queue.qsize() > MAX_CSV_Q_SIZE:
            csv_results = [[] for i in range(4)]

            csv_queue_lock.acquire()
            while not csv_queue.empty():
                buffer = csv_queue.get()
                for i in range(4):
                    csv_results[i].append(buffer[i])
            csv_queue_lock.release()

            df = pandas.DataFrame(data={"Disk": csv_results[0], "Path": csv_results[1],
                                        "FileSize": csv_results[2], "md5": csv_results[3]})

            if not os.path.exists(CSV_DIR):
                df.to_csv(CSV_DIR, sep=',', index=False)
            else:
                with open(CSV_DIR, 'a', newline='') as f:
                    df.to_csv(f, sep=',', header=False, index=False)
            print('Saved to csv successfully')
            path_lock.acquire()
            for item in csv_results[1]:
                paths[item] = '1'
            path_lock.release()


if __name__ == '__main__':
    if not os.path.exists('paths'):
        print('Cannot find directory: paths')
    else:
        num_cpu = multiprocessing.cpu_count()
        print('Number of CPU: {}'.format(num_cpu))

    paths = get_paths(file_counter)
    while paths is not None:
        for key, value in paths.items():
            if value != '1':
                path_queue.put(key)
        print('{} Loading paths successful.'.format(file_counter))

        save_to_csv_flag = True

        for i in range(num_cpu - 1):
            proc = Process(target=calculate_md5, args=(
                path_queue, path_queue_lock, csv_queue, csv_queue_lock, ))
            procs.append(proc)
            proc.start()

        proc = Process(target=save_to_csv_process,
                       args=(csv_queue, path_queue, csv_queue_lock, paths, path_lock,))
        procs.append(proc)
        proc.start()

        for proc in procs:
            proc.join()

        save_to_csv(csv_queue)

        with open('paths/find_path{}.json'.format(file_counter), 'w') as f:
            json.dump(paths, f, indent=4)
            print('paths/find_path{}.json modified'.format(file_counter))

        file_counter += 1
        paths = get_paths(file_counter)
        procs = []
