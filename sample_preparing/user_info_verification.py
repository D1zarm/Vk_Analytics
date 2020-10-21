# coding=UTF-8

import collections
import time
import traceback
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import vk
from matplotlib import pyplot as plp

from sample_preparing.gender_verification import has_correct_gender
from vk_parsing.parser import send_message


def read_sample_files(males_path, females_path, ages_path=None):
    df = pd.DataFrame(columns=['male_id', 'male_bdate', 'female_id'])

    with open(males_path, 'r') as file:
        df['male_id'] = _trim_list(file.read().split('\n'))
    with open(females_path, 'r') as file:
        df['female_id'] = _trim_list(file.read().split('\n'))
    if ages_path is not None:
        with open(ages_path, 'r') as file:
            df['male_bdate'] = [datetime.now().year - int(year) for year in _trim_list(file.read().split('\n'))]
    df = remove_duplicates(df)
    return df


def read_vk_csv(csv_path):
    df = pd.read_csv(csv_path, delimiter=';')
    df.drop(['Unnamed: 4'], axis='columns', inplace=True)
    if df.iloc[0]['VK ID'] == 'VK ID':  # If row #0 contains columns names (barkov parser specifics)
        df.drop([0], axis=0, inplace=True)  # Drop this row
    df.dropna(subset=['ССЫЛКА НА ПАРТНЁРА'], inplace=True)
    df.dropna(subset=['ДАТА РОЖДЕНИЯ'], inplace=True)
    df['ССЫЛКА НА ПАРТНЁРА'] = df['ССЫЛКА НА ПАРТНЁРА'].apply(lambda s: s[str(s).find('id') + 2:])
    df['ДАТА РОЖДЕНИЯ'] = df['ДАТА РОЖДЕНИЯ'].apply(lambda s: s[-4:])
    df = df[~df['ДАТА РОЖДЕНИЯ'].str.contains('\.')]

    if df.iloc[1]['ПОЛ'] == 'М':  # Manage different inputs (male of female list)
        df.drop(['ПОЛ'], axis='columns', inplace=True)
        df.columns = ['male_id', 'male_bdate', 'female_id']
    elif df.iloc[1]['ПОЛ'] == 'Ж':
        df.drop(['ПОЛ'], axis='columns', inplace=True)
        df.columns = ['female_id', 'female_bdate', 'male_id']
    else:
        print('Data is not recognized, check the data. Exiting the program')
        exit()

    df.reset_index(drop=True, inplace=True)
    df = remove_duplicates(df)
    return df


def remove_duplicates(df):
    df.drop_duplicates(subset='male_id', keep='first', inplace=True)
    df = df.drop_duplicates(subset='female_id', keep='first').reset_index(drop=True)
    return df


def merge_data(df1, df2):
    return remove_duplicates(pd.concat([df1, df2], ignore_index=True, sort=False))



def verify(session, df, retrying_time=60*5):
    """
    Verifies age by friends' ages distribution
    (Also checks on closed profile and whether a pair is really a pair at the moment)
    age has to be mentioned in user's profile

    :return: a list of users who passed verification
    """
    def _is_a_pair(user_info, pair_info):
        if 'relation_partner' in user_info and user_info['relation_partner']['id'] == pair_info['id'] or \
                'relation_partner' in pair_info and pair_info['relation_partner']['id'] == user_info['id']:
            return True
        else:
            return False

    def age_matches(age, friends_mode, birth_year=None):
        matches = False
        if birth_year is None:
            birth_year = datetime.now().year - age
        if 0 < age < 20:
            if friends_mode - 2 < int(birth_year) < friends_mode + 2:  # Allow 1 year of variance
                matches = True
        elif 19 < age < 25:
            if friends_mode - 3 < int(birth_year) < friends_mode + 3:  # Allow 2 years of variance
                matches = True
        elif 24 < age < 41:
            if friends_mode - 5 < int(birth_year) < friends_mode + 5:  # Allow 4 year of variance
                matches = True
        elif 40 < age < 51:
            if friends_mode - 9 < int(birth_year) < friends_mode + 9:  # 8 years of variance
                matches = True

        elif 50 < age:  # From 51 years
            if friends_mode - 16 < int(birth_year) < friends_mode + 16:  # Allow 15 years of variance
                matches = True
        return matches

    user_vk = session.get_user_vk()
    new_dict = {}  # Add objects to dict instead of df, because it is much faster
    o = 0  # number of users in new_dict
    users_info = None
    pairs_info = None

    try:
        while len(df) > 0:
            exc_n = 0
            timed_out = False
            int_serv = False
            while True:
                try:
                    # Friends count is returned only with user token
                    time.sleep(0.34)

                    # Take 650 users each time, because if more are taken vk API may return some junk

                    users_info = user_vk.users.get(user_ids=df['male_id'][:650], name_case='nom',
                                                   fields='bdate, lists, relation, sex',
                                                   v='5.89')
                    time.sleep(0.34)
                    pairs_info = user_vk.users.get(user_ids=df['female_id'][:650], name_case='nom',
                                                   fields='bdate, lists, relation, sex',
                                                   v='5.89')
                    time.sleep(0.34)
                except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError,
                        requests.exceptions.HTTPError, requests.exceptions.ChunkedEncodingError):
                    print("Exception! Getting user users_info timed out. Retrying...")
                    timed_out = True
                    time.sleep(1)
                    continue


                except vk.exceptions.VkAPIError as e:
                    if e.code in (28, 29):  # Rate limit is reached / access token has expired
                        if exc_n is 0:  # If the exception happens the 1st time in this loop
                            # write_to_file(df=pd.DataFrame.from_dict(new_dict, "index"),
                            #               path='Sample/tmp/7-excepted-users.csv')
                            send_message(
                                'Sample preparing users.get() exception. The results have been saved to Sample/tmp/7-...')

                        print('RATE LIMIT REACHED. Updating the user key')
                        user_vk = session.update_user_key()

                        if exc_n is session.get_user_tokens_n():  # If checked all of the keys
                            print('All of the keys have been checked. Exiting the program')
                            send_message('All of the keys have been checked. Exiting the program')
                            exit()

                        exc_n += 1
                        time.sleep(1)
                        continue

                    elif e.code is 6:
                        print('user_key: {}'.format(session.get_user_token()))
                        raise
                    elif e.code is 10:
                        tr = traceback.format_exc()
                        msg = 'Internal server error:\n{}\n\nRetrying in {} mins\nUser_key: {}'.format(tr,
                                                                                         '%.0f' % (retrying_time / 60),session.get_user_token())
                        print(msg)
                        if not int_serv:
                            send_message(msg)
                            int_serv = True

                        time.sleep(retrying_time)
                        user_vk = session.update_user_key()
                        continue

                    print("Unexpected exception!")
                    raise

                if timed_out or int_serv:  # If after successful try block
                    print('Got the user users_info! Continuing running\n')
                break

            for i, user_info in enumerate(users_info):
                pair_info = pairs_info[i]

                # Check whether returned user/pair ids are present in df. If no, vk api returned a mess
                if len(df.loc[df['male_id'] == user_info['id']]) == 0:  # If user id is not present in male_id column
                    message = 'User https://vk.com/id{} is not presented in df. Vk API returned a mess. Exiting the program'.format(
                        user_info['id'])
                    print(message)
                    send_message(message)
                    # write_to_file(df=pd.DataFrame.from_dict(new_dict, "index"),
                    #               path='Sample/tmp/7-excepted-users.csv')
                    exit()

                if len(df.loc[
                           df['female_id'] == pair_info['id']]) == 0:  # If pair id is not present in female_id column
                    message = 'User https://vk.com/id{} is not presented in df. Vk API returned a mess. Exiting the program'.format(
                        pair_info['id'])
                    print(message)
                    send_message(message)
                    # write_to_file(df=pd.DataFrame.from_dict(new_dict, "index"),
                    #               path='Sample/tmp/7-excepted-users.csv')
                    exit()

                # If profile is closed
                if 'deactivated' in users_info or 'is_closed' in user_info and user_info['is_closed'] \
                        or 'deactivated' in pair_info or 'is_closed' in pair_info and pair_info['is_closed']:
                    print(
                        'User https://vk.com/id{} or his pair https://vk.com/id{} is deactivated or has a closed profile. Skipping the pair\n'.format(
                            user_info['id'], pair_info['id']))
                    continue

                # If bdate is not mentioned
                if 'bdate' not in user_info \
                        or 'bdate' not in pair_info:
                    print(
                        'User https://vk.com/id{} or his pair https://vk.com/id{} didn\'t mention the bdate. Skipping the pair'.format(
                            user_info['id'], pair_info['id']))
                    continue

                if not _is_a_pair(user_info, pair_info):  # If a pair is no longer a pair
                    print('A pair (https://vk.com/id{} and https://vk.com/id{}) is no longer a pair. Skipping'.format(
                        user_info['id'], pair_info['id']))
                    continue  # Skip the pair

                # If user and pair have the same sex, consider their relationship status as fake. It's Russia, baby
                if user_info['sex'] == pair_info['sex']:
                    print('User https://vk.com/id{} and pair https://vk.com/id{} have the same sex. Skipping'.format(
                        user_info['id'], pair_info['id']))
                    continue

                # If mentioned a wrong gender
                if not (has_correct_gender(user_info['sex'], user_info['first_name'], user_info['last_name'])
                        and has_correct_gender(pair_info['sex'], pair_info['first_name'], pair_info['last_name'])):
                    print(
                        'User https://vk.com/id{} or his pair https://vk.com/id{} mentioned a wrong gender. Skipping the pair'.format(
                            user_info['id'], pair_info['id']))
                    continue  # Skip the pair

                user_birth_year = user_info['bdate'][-4:]
                pair_birth_year = pair_info['bdate'][-4:]

                # If birth year is not mentioned
                if '.' in user_birth_year or '.' in pair_birth_year:
                    print(
                        'User\'s https://vk.com/id{} or his pair\'s https://vk.com/id{} birth year is not mentioned. Skipping the pair'.format(
                            user_info['id'], pair_info['id']))
                    continue

                user_age = datetime.now().year - int(user_birth_year)
                pair_age = datetime.now().year - int(pair_birth_year)

                # If schoolers are married.
                # As practice show, this users are in relation, so don't skip them.
                # if (user_age < 17 or pair_age < 17) and (('relation' in user_info and user_info['relation'] in (4, 8))
                #                                          or ('relation' in pair_info and pair_info[
                #             'relation'] in (4, 8))):
                #     print('Schoolers https://vk.com/id{} and https://vk.com/id{} are married. Skipping them'.format(user_info['id'], pair_info['id']))
                #     continue

                exc_n = 0
                int_serv = False  # Internal server problem exception flag.
                user_friends = {'items': [], 'count': 0}
                pair_friends = {'items': [], 'count': 0}
                while True:
                    try:
                        user_friends = user_vk.friends.get(user_id=user_info['id'],
                                                           fields='bdate',
                                                           v='5.89')
                        time.sleep(0.34)  # Because only 3 requests per sec are allowed

                        pair_friends = user_vk.friends.get(user_id=user_info['id'],
                                                           fields='bdate',
                                                           v='5.89')
                        time.sleep(0.34)
                    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError,
                            requests.exceptions.HTTPError, requests.exceptions.ChunkedEncodingError):
                        print('RequestTimeout, ConnectionError or HTTPError. Retrying')
                        time.sleep(1)
                        continue
                    except vk.exceptions.VkAPIError as e:
                        if e.code is 6:
                            print('Too many requests error. Retrying')  # 3 requests per second are allowed
                            time.sleep(1)
                            continue
                        elif e.code in (28,29):  # Rate limit reached / access token has expired
                            if exc_n is 0:  # If the exception happens the 1st time in this loop
                                # print('Saving the results')
                                # write_to_file(df=pd.DataFrame.from_dict(new_dict, "index"),
                                #               path='Sample/tmp/7-excepted-users.csv')
                                send_message(
                                    'Sample preparing friends.get() exception. The results have (not) been saved to Sample/tmp/7-...')
                            exc_n += 1

                            print('RATE LIMIT REACHED. Updating the user key')
                            user_vk = session.update_user_key()

                            if exc_n is session.get_user_tokens_n():  # If checked all the keys
                                print('All of the keys have been checked. Exiting the program')
                                send_message('All of the keys have been checked. Exiting the program')
                                exit()
                            time.sleep(1)
                            continue
                        elif e.code is 30:  # This profile is private
                            print('User\'s profile is private. Skipping him')
                            break  # return an empty list
                        elif e.code is 10:  # Internal server problem
                            tr = traceback.format_exc()
                            msg = 'Internal server error:\n{}\n\nRetrying in {} mins'.format(tr,
                                                                                             '%.0f' % (
                                                                                                         retrying_time / 60), session.get_user_token())
                            print(msg)
                            if not int_serv:
                                send_message(msg)
                                int_serv = True

                            time.sleep(retrying_time)
                            user_vk = session.update_user_key()
                            continue
                        elif e.code is 5:  # Invalid access_token
                            print('Invalid access token. Exiting the program')
                            send_message('Invalid access token:\n{}'.format(session.get_user_token()))
                            raise

                        else:
                            print('Unknown friends.get() exception. Saving the results')
                            # write_to_file(df=pd.DataFrame.from_dict(new_dict, "index"),
                            #               path='Sample/tmp/7-excepted-users.csv')
                            send_message('An unknown friends.get() exception. '
                                         'The results have been saved to Sample/tmp/7-...'
                                         '\n{}'.format(e))
                            raise
                    except:
                        print('Friends.get exception')
                        raise
                    break

                k = 0
                l = 0
                user_friends_birth_years_sums = {}
                pair_friends_birth_years_sums = {}

                for friend in user_friends['items']:
                    if 'bdate' not in friend:
                        # print('Friend\'s bdate is hidden. Skipping him')
                        continue
                    friends_birth_year = friend['bdate'][-4:]
                    if '.' in friends_birth_year:
                        continue

                    # For mode calculation
                    if friends_birth_year in user_friends_birth_years_sums:
                        user_friends_birth_years_sums[friends_birth_year] += 1
                    else:
                        user_friends_birth_years_sums[friends_birth_year] = 1

                    k += 1
                if k == 0:  # If user doesn't have friends
                    continue  # Don't even consider the pair

                # Same for pairs
                for friend in pair_friends['items']:
                    if 'bdate' not in friend:
                        continue
                    friends_birth_year = friend['bdate'][-4:]
                    if '.' in friends_birth_year:
                        continue
                    # For mode calculation
                    if friends_birth_year in pair_friends_birth_years_sums:
                        pair_friends_birth_years_sums[friends_birth_year] += 1
                    else:
                        pair_friends_birth_years_sums[friends_birth_year] = 1
                    l += 1

                if l == 0:  # If user doesn't have friends
                    continue  # Don't even consider the pair

                # friends_birth_year_mean = birth_years_sum / k
                user_friends_mode = int(max(user_friends_birth_years_sums, key=user_friends_birth_years_sums.get))
                pair_friends_mode = int(
                    max(pair_friends_birth_years_sums, key=pair_friends_birth_years_sums.get))

                # If user's age is wrong
                if not (age_matches(user_age, user_friends_mode, birth_year=user_birth_year) and age_matches(pair_age,
                                                                                                             pair_friends_mode,
                                                                                                             birth_year=pair_birth_year)):
                    print(
                        'Someone\'s age is wrong. Skipping the pair\n\tUser: https://vk.com/id{} Birth: {}, Mode = {}, Age: {}, Difference = {} years. Skipping him'.format(
                            user_info['id'], user_birth_year, "%.2f" % user_friends_mode, user_age,
                                                              "%.2f" % (user_friends_mode - int(user_birth_year))))
                    print(
                        '\tPair: https://vk.com/id{} Birth: {}, Mode = {}, Age: {}, Difference = {} years. Skipping her\n'.format(
                            pair_info['id'], pair_birth_year, "%.2f" % pair_friends_mode, pair_age,
                                                              "%.2f" % (pair_friends_mode - int(pair_birth_year))))
                    continue  # Skip the pair

                # If the code has reached this point, it means that a user has been successfully verified
                print('Verified:\n\tUser https://vk.com/id{}, Birth: {}, Mode: {}, Age: {}, Difference: {}'.format(
                    user_info['id'],
                    user_birth_year,
                    "%.2f" % user_friends_mode,
                    user_age, "%.2f" % (
                            user_friends_mode - int(
                        user_birth_year))))
                print('\tPair https://vk.com/id{}, Birth: {}, Mode: {}, Age: {}, Difference: {}\n'.format(
                    pair_info['id'],
                    pair_birth_year,
                    "%.2f" % pair_friends_mode,
                    pair_age, "%.2f" % (
                            pair_friends_mode - int(
                        pair_birth_year))))

                new_dict[o] = {'male_id': user_info['id'], 'male_bdate': user_birth_year,
                               'female_id': pair_info['id'], 'female_bdate': pair_birth_year}
                o += 1

            df = df.iloc[650:]

    except Exception:
        # write_to_file(df=pd.DataFrame.from_dict(new_dict, "index"), path='Sample/tmp/7-excepted-users.csv')
        send_message('Sample preparing unknown exception. Result is saved to Sample/tmp/7-...')
        raise
    # send_message('All users have been checked.')
    return pd.DataFrame.from_dict(new_dict, "index")


# count - how much users to include in the final set
def group_sort(df, count=None):
    n = len(df)  # Remember the users number
    if count is not None:
        if count < n:
            n = count

    # Manage different input (males/females)
    bdate = 'male_bdate' if 'male_bdate' in df.columns and df.iloc[0]['male_bdate'] != np.nan else 'female_bdate'

    # Group the values
    map = {}
    for index, row in df.iterrows():
        if row[bdate] in map:
            map[row[bdate]].append(row)
        else:
            map[row[bdate]] = [row]

    plp.figure(figsize=(16, 8))
    plp.bar(map.keys(), [len(map[key]) for key in map.keys()], color='g')
    plp.show()

    # Sort the map
    map = collections.OrderedDict(sorted(map.items(), reverse=True))

    new_dict = {}
    k = 0

    while k < n:
        for age in map:
            if len(map[age]) is not 0:
                user = map[age].pop()
                new_dict[k] = user
                k += 1

    return pd.DataFrame.from_dict(new_dict, orient="index")


# Removes '' elements from the end of the list
def _trim_list(text):
    ch = len(text) - 1
    while text[ch] == '':
        del text[ch]
        ch -= 1
        if ch is -1: break  # ch is -1 when the users list is empty

    return text


def contains_duplicates(df):
    return len(df) != len(df['male_id'].unique())


def write_to_file(df, path):
    df.to_csv(r'{}'.format(path), index=True, header=True)
