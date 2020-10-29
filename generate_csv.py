import os
import csv
import json
import pandas
import queue
import hashlib
from multiprocessing import Process, Pool, cpu_count

DISK_NAME = 'MRI05'
CSV_DIR = './Miley_201028_MRI05_paths.csv'
file_counter = 0


def get_paths(file_index):
    if not os.path.exists('paths/find_path{}.json'.format(file_index)):
        return
    with open('paths/find_path{}.json'.format(file_index)) as f:
        paths = json.load(f)
        return paths


def calculate_md5_pool(path):
    if path is not None:
        size = os.path.getsize(path)
        md5_hash = hashlib.md5()
        md5_value = None
        with open(path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
            md5_value = md5_hash.hexdigest()
        if md5_value is None:
            print('File handling error!')
            return
        else:
            return [DISK_NAME, path, size, md5_value]


def save_to_csv_pool(results):
    df = pandas.DataFrame(results, columns=['Disk', 'Path', 'FileSize', 'md5'])
    if not os.path.exists(CSV_DIR):
        df.to_csv(CSV_DIR, sep=',', index=False)
    else:
        with open(CSV_DIR, 'a', newline='') as f:
            df.to_csv(f, sep=',', header=False, index=False)
    print('Saved to csv successfully')


if __name__ == '__main__':
    if not os.path.exists('paths'):
        print('Cannot find directory: paths')
    else:
        num_cpu = cpu_count()
        print('Number of CPU: {}'.format(num_cpu))

        paths = get_paths(file_counter)
        while paths is not None:
            if paths['finished'] == '1':
                print('{} already done.'.format(file_counter))
            else:
                print('{} Loading paths successful.'.format(file_counter))

                pool = Pool(processes=num_cpu)
                results = pool.map(calculate_md5_pool, paths['content'])
                pool.close()
                pool.join()

                save_to_csv_pool(results)

                paths['finished'] = '1'
                with open('paths/find_path{}.json'.format(file_counter), 'w') as f:
                    json.dump(paths, f, indent=4)
                    print(
                        'paths/find_path{}.json modified'.format(file_counter), end='\n\n')

            file_counter += 1
            paths = get_paths(file_counter)

        print('All done')
