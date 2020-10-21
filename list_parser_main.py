# coding=UTF-8

import json
import os.path
import tarfile
import threading
import time
import traceback
from queue import Queue, PriorityQueue
import yadisk
from vk_parsing import list_parser as parser
from vk_parsing.tokens import Session, groups

y = yadisk.YaDisk(token="PUT YOUR TOKEN HERE")

s_tkns_n = 5  # Number of service tokens in each group
search_time = 60  # Time spent on one user. Default = 20

row_q = Queue()
users_and_logs_q = PriorityQueue()

last_num, df = parser.read_sample()

for i, row in df.iterrows():  # Put the users ids to the queue
    row_q.put(row)

del df  # Clear the memory

service_groups, users_groups = groups(s_tkns_n)

thread_list = []
for idx in range(len(service_groups)):
    thread = threading.Thread(target=parser.collect_users_data, args=(
        Session(service_groups[idx], users_groups[idx]), row_q, users_and_logs_q, search_time))
    thread_list.append(thread)

for thread in thread_list:
    thread.start()
    time.sleep(1)

# Read the queues and write them to files
time1 = time.time()
time2 = time.time()
dir = 'Vk_parser/data/'  # Yandex disk dir
while True:
    # While pairs are coming strictly after each other (in sorted queue), write them to the file.
    # Otherwise, wait for the next pair number to come
    try:
        with open('data/users.txt', 'a') as users_f, open('data/logs.txt', 'a') as logs_f:
            while not users_and_logs_q.empty():
                user_and_log = users_and_logs_q.get()  # Get from the smallest to the highest pair_num

                pair_num = int(user_and_log[0])

                # Does the current element follow the previous?
                if (pair_num - last_num) == 1:

                    logs = user_and_log[1][1]
                    # Write down user and his pair
                    if len(user_and_log[1][0]) > 0:  # If user info is returned
                        user_infos = user_and_log[1][0]
                        for info in user_infos:
                            users_f.write('{}\n'.format(json.dumps(info)))
                        users_f.flush()

                    # Write down user's and pair's logs
                    for user_log in logs:
                        logs_f.write(user_log)
                    logs_f.flush()

                    last_num = pair_num
                else:
                    users_and_logs_q.put(user_and_log)  # Put it back to the queue
                    time.sleep(10)  # Wait for next item to come
            print('Queue is empty!')

            # Check the file size and dump it to the yandex disk
            time2 = (time.time() - time1) / 3600 / 24  # Days
            if time2 > 3:  # 3 # If more than 3 days passed since the last check
                time1 = time.time()  # Update last check time

                # if filesize > than 5 gb:
                if os.path.getsize('data/users.txt') > 5e9:  # 5e9

                    # Check if token is valid
                    if not y.check_token():
                        parser.parser.send_message(
                            'Yandex Disk API token is invalid. Update the token and load it to the code. Before '
                            'that, the files will not be uploaded to yandex disk.')
                        continue

                    # Get the last file name from ya disk
                    nums = []
                    for file in y.listdir(dir):
                        if 'data' in file['name']:
                            dot = file['name'].find('.')
                            nums.append(int(file['name'][4:dot]))
                    arc_name = 'data{}.tar.xz'.format(max(nums) + 1)

                    # Compress users and logs
                    with tarfile.open('data/' + arc_name, "w:xz") as tar:
                        tar.add('data/users.txt', arcname=os.path.basename('data/users.txt'))
                        tar.add('data/logs.txt', arcname=os.path.basename('data/logs.txt'))
                    print('Compressed')

                    # Load the archive to Yandex disk
                    y.upload('data/' + arc_name, dir + arc_name, timeout=120)
                    print('Uploaded')

                    os.remove('data/' + arc_name)  # Remove the archive
                    os.remove('data/users.txt')  # Remove the 'users' file


    except Exception:
        tb = traceback.format_exc()
        print(tb)
        parser.parser.send_message(tb)
        os._exit(1)
    time.sleep(20)
