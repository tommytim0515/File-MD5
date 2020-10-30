import os
import json

target_dir = '' + os.sep
paths = {'finished': '0', 'content': []}
file_counter = 0
path_counter = 0
MAX_PATH_SIZE = 5000


if __name__ == '__main__':
    if not os.path.exists('paths'):
        os.mkdir('paths')

    for dirpath, _, filenames in os.walk(target_dir):
        for f in filenames:
            paths['content'].append(os.path.join(dirpath, f))
            path_counter += 1
            if path_counter >= MAX_PATH_SIZE:
                with open('paths/find_path{}.json'.format(file_counter), 'w') as f:
                    json.dump(paths, f, indent=4)
                    print('paths/find_path{}.json generated'.format(file_counter))
                paths['content'].clear()
                path_counter = 0
                file_counter += 1

    if len(paths['content']) > 0:
        with open('paths/find_path{}.json'.format(file_counter), 'w') as f:
            json.dump(paths, f, indent=4)
            print('paths/find_path{}.json generated'.format(file_counter))