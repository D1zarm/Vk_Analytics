# coding=UTF-8
import os
import queue
import threading
import time
import traceback

import pandas as pd
from vk.exceptions import VkAPIError

from sample_preparing.user_info_verification import verify
from vk_parsing import parser


def _trim_list(text):
    ch = len(text) - 1
    while text[ch] == '':
        del text[ch]
        ch -= 1
        if ch is -1: break  # ch is -1 when the users list is empty

    return text


def read_sample():
    # Extract the last pair number from data
    global one_member_left
    global pair_num

    with open('data/logs.txt', 'r') as logs:
        text = _trim_list(logs.read().split('\n'))

        if len(text) is 0:  # If file is empty
            prev_num = pair_num = 0  # Initialise
        elif len(text) is 1:  # If only 1 member has been checked
            pair_num = 1
            prev_num = 0
        else:
            last_line = text[-1]
            pair_num = int(last_line[:last_line.find('.')])

            prev_line = text[-2]  # To see whether only 1 member or the whole pair have been analyzed
            prev_num = int(prev_line[:prev_line.find('.')])

        del text  # Clear memory from data

    if prev_num != pair_num:  # if prev_num != pair_num then only 1 member has been analyzed
        one_member_left = True
        pair_num -= 1  # To go back to the pair in which one member has not been checked yet

    users_df = pd.read_csv('New_Sample/final_sorted_sample.csv', index_col=0)[pair_num:]

    if len(users_df) is 0:  # Check if the file is finished
        print('All users have been processed. Exiting')
        parser.send_message('All users have been processed. Exiting')
        os._exit(1)

    return pair_num, users_df


pair_num = 0
one_member_left = False
_lock = threading.Lock()  # For atomic incrementing


# Takes one_member_left, pair_num - global atomic variables (2nd is needed to write down data correctly), users_df as a queue!


def collect_users_data(session, row_q, users_and_logs_q, search_time=20):
    global pair_num
    global one_member_left
    time_period = 4 * 365
    try:
        print(
            'Thread started\nUser key: {}\nPublic key: {}'.format(session.get_user_token(),
                                                                  session.get_service_token()))

        # for i, row in users_df.iterrows():  # Iterate through dataframe
        while not row_q.empty():

            with _lock:
                pair_num += 1  # Increment pair number because we are now working with next pair
            loc_pair_num = pair_num

            row = row_q.get()
            male_id = row['male_id']
            female_id = row['female_id']

            check_df = pd.DataFrame.from_dict({0: row},
                                              orient="index")  # Create a df from 1 row to pass it to 'verify' method
            if len(verify(session,
                          check_df)) == 0:  # If verification didn't return the pair back - it means, the pair didn't pass
                log = '{}. The pair (https://vk.com/id{} and https://vk.com/id{}) didn\'t pass verification\n'.format(
                    loc_pair_num, male_id, female_id)
                users_and_logs_q.put((loc_pair_num, (
                [], [log, log])))  # Write down the same log twice due to specifics of one_member_left check
                continue  # Take the next pair

            if one_member_left:
                pair = [female_id]
                with _lock:
                    one_member_left = False
            else:
                pair = [male_id, female_id]

            users = []
            logs = []
            for idx, user_id in enumerate(pair):  # Check the pair
                try:  # Catch vk exceptions.


                    print('\n{}. Analyzing user {}'.format(loc_pair_num, user_id))

                    user_info, log = parser.parse_user(session, pair_num = loc_pair_num, user_id=user_id,
                                                          running_mins=search_time,
                                                          outburst_time=search_time / 2,
                                                          outburst_likes_count=20,
                                                          post_likes_threshold=20000,
                                                          time_period=time_period,
                                                          skipping_amount=600,
                                                          group_posts_count=8000,
                                                          liked_posts_threshold=20)

                    # Remember the result
                    users.append(user_info)

                    # Remember log
                    logs.append(log)
                    print(log)

                except VkAPIError as e_vk:  # If one user closes his profile while being checked
                    tb = traceback.format_exc()
                    if e_vk.code is 30 or e_vk.code is 18:  # 30 - User's profile is private; 18 - user is deleted or blocked
                        log = '{}. User\'s is private, deleted or blocked. Skipping the pair\n'.format(loc_pair_num)

                        users_and_logs_q.put((loc_pair_num, ([], [log, log])))
                        continue
                    elif e_vk.code is 28:  # Access token has expired
                        parser.send_message(
                            '{} Access token has expired: {}\n{}'.format(loc_pair_num, session.get_user_token(), tb))
                        print('{}. Access token has expired.\n{}'.format(loc_pair_num, tb))
                        os._exit(1)
                    else:
                        print('{}. Unknown vk exception.\n{}'.format(loc_pair_num, tb))
                        parser.send_message(
                            '{}. Unhandled exception in VkParsing! Terminating the program\n{}'.format(loc_pair_num,
                                                                                                       tb))
                        os._exit(1)

            # Send the pair and their log to the users_q
            try:
                users_and_logs_q.put((loc_pair_num, (users, logs)), timeout=15)
            except queue.Full:
                print(list(users_and_logs_q.queue))
                raise
        print('Queue is empty. All of the users have been processed')
        parser.send_message('Queue is empty. All of the users have been processed. Update the users list '
                            '(final_sorted_sample.csv).')

    except Exception:  # Any exception (except of KeyboardInterrupt)
        tb = traceback.format_exc()
        parser.send_message('Unhandled exception in VkParsing! Terminating the program\n{}'.format(tb))
        print(tb)
        os._exit(1)
