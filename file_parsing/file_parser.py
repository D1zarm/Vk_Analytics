import json
import os
from csv import writer

import numpy as np

text_header = ['user_id', 'post_id', 'text', 'type', 'date']
photo_header = ['user_id', 'post_id', 'photo', 'type', 'date']
info_header = ['user_id', 'sex', 'bdate', 'status', 'flwrs', 'videos', 'audios', 'photos', 'friends', 'subs',
               'pages', 'posts_n', 'pst_frq', 'wall_likes_mean', 'avatar_frq', 'avatar_mean', 'smoking', 'alcohol',
               'parth_id',
               'country', 'city']


def parse_photos(attach, user_id):
    # Post might have a photo as attachment or be a photo itself
    # id -6 - user's avatar. Don't consider.

    if attach['type'] == 'photo':
        if attach['photo']['owner_id'] != user_id:
            w = z = y = x = m = s = None

            for size in attach['photo']['sizes']:
                if size['type'] == 'w':
                    w = size['url']
                if size['type'] == 'z':
                    z = size['url']
                if size['type'] == 'y':
                    y = size['url']
                if size['type'] == 'x':
                    x = size['url']
                if size['type'] == 'm':
                    m = size['url']
                if size['type'] == 's':
                    s = size['url']

            if w is not None:
                return w
            elif z is not None:
                return z
            elif y is not None:
                return y
            elif x is not None:
                return x
            elif m is not None:
                return m
            elif s is not None:
                return s
            else:
                # If didn't find 'w', 'z' or 'y' in photo sizes
                print('Photo size has not been found. Modify the code')
                exit(1)

    return None  # If nothing has been returned


def parse_audio(post):
    pass


def parse_videos(post):
    pass


def parse_docs(post):
    pass


def parse_polls(post):
    pass


def parse_page(post):
    pass


def parse_market(post):
    pass


def parse_events(post):
    pass


def parse_geo(post):
    pass


# Extracts text from user's post
# if eng - translate to russian
def parse_text(post):
    if 'text' in post:
        return post['text'].replace('\t', '. ').replace('\n', ', ')
    else:
        return ''


def parse_user_info(user):
    user_info = [user['id'], user['sex'], user['bdate'], user['status'],
                 user['followers_count'], user['counters']['videos'], user['counters']['audios'],
                 user['counters']['photos'],
                 user['counters']['friends'], user['counters']['subscriptions'], user['counters']['pages'],
                 len(user['user_posts']['items']), user['posting_frequency'],
                 user['wall_likes_mean'],
                 user['avatars_posting_frequency']]

    # Replace 0 or absence with Nan
    user_info.append(user['avatar_likes_mean']) if 'avatar_likes_mean' in user else user_info.append(np.nan)
    user_info.append(user['personal']['smoking']) if 'personal' in user and 'smoking' in user['personal'] and \
                                                     user['personal']['smoking'] != 0 else user_info.append(np.nan)
    user_info.append(user['personal']['alcohol']) if 'personal' in user and 'alcohol' in user['personal'] and \
                                                     user['personal']['alcohol'] != 0 else user_info.append(np.nan)
    user_info.append(user['relation_partner']['id']) if 'relation_partner' in user else user_info.append(np.nan)
    user_info.append(user['country']['id']) if 'country' in user else user_info.append(np.nan)
    user_info.append(user['city']['id']) if 'city' in user else user_info.append(np.nan)

    # Replace /t on '. ' for delimiter to work correctly in Dask framework
    user_info[3] = user_info[3].replace('\t', '. ').replace('\n', ', ')
    if user_info[3] is '':  # If status is empty
        user_info[3] = np.nan  # Add nan

    # user_info.append(parse_groups_info(user['groups']))

    return user_info


def parse_post_info(post):
    # parse likes count and geo

    pass


def parse_groups_info(groups):
    # create a vector of likes to vk themes (musician/music - 34, humour - 56, movies - 12)
    # First month we were not obtaining groups theme info due to bug.
    pass


# Parse posts and post's reposts' data
def parse_post(post, user_id):
    parse_post_info(post)  # likes and geo
    # For each repost
    # For each attachment
    text = ''
    photos = []
    audios = []  # A list of audio vectors
    # ...
    if 'copy_history' in post:  # if post contains reposts
        reposts = [post] + post['copy_history']  # add reposts to posts list
    else:
        reposts = [post]  # else analyze only 1 post

    for idx, repost in enumerate(reposts):  # Run through the reposts and the post

        # https://vk.com/dev/objects/attachments_w
        text += ' ' + parse_text(repost)
        text = text.strip()

        if 'attachments' in repost:
            for attach in repost['attachments']:

                photo = parse_photos(attach, user_id)
                if photo is not None:
                    photos.append(photo)

                audio = parse_audio(attach)
                if audio is not None:
                    audios.append(audio)

                parse_videos(attach)  # Later or never.

                # Not necessary
                parse_docs(attach)
                parse_polls(attach)
                parse_page(attach)
                parse_market(attach)
                parse_events(attach)
    date = post['date']
    return (text, photos, date)


def update_dataset(users_path, texts_path, photos_path, infos_path):
    with open(users_path, 'r') as file, open(texts_path, 'a+', newline='') as write_obj, \
            open(photos_path, 'a+', newline='') as write_obj2, open(infos_path, 'a+') as info_f:

        text_writer = writer(write_obj)
        photo_writer = writer(write_obj2)
        info_writer = writer(info_f)

        # Create the file headers if files are empty
        if os.stat(texts_path).st_size == 0:
            text_writer.writerow(text_header)
            photo_writer.writerow(photo_header)
            info_writer.writerow(info_header)

        while True:
            user = file.readline()

            if not user:
                print('End of file')
                break
            user = json.loads(user)

            user_info = parse_user_info(user)
            info_writer.writerow(user_info)

            post_idx = -1

            # For each liked post, parse its content
            likes = [like for group in user['groups'] for like in group['liked_posts']]
            for post in likes:

                post_idx += 1
                post_content = parse_post(post, user['id'])

                date = post_content[2]

                text = post_content[0]
                if text != '':
                    text_writer.writerow([user['id'], post_idx, text, 'like', date])

                photos = post_content[1]
                if len(photos) > 0:
                    # Write photos to the photos file
                    for photo in photos:
                        photo_writer.writerow([user['id'], post_idx, photo, 'like', date])

                # Manage other media here
                # Audio and groups are left
                #

            # For each re(post), parse its content
            for post in user['user_posts']['items']:
                post_idx += 1
                post_content = parse_post(post, user['id'])

                date = post_content[2]

                text = post_content[0]
                if text != '':
                    text_writer.writerow([user['id'], post_idx, text, 'post', date])

                photos = post_content[1]
                if len(photos) > 0:
                    for photo in photos:
                        photo_writer.writerow([user['id'], post_idx, photo, 'post', date])

                # Manage other media here
                # Audio and groups are left
                #

def get_pairs(users_path):
    with open(users_path, 'r') as file:
        k = 0
        while True:
            user = file.readline()
            pair = file.readline()

            if not user or not pair:
                print('End of file')
                break
            user = json.loads(user)
            pair = json.loads(pair)

            if user['likes_count'] > 99 and pair['likes_count'] > 99:
                print('{}. {}: {} likes'.format(k, user['id'], user['likes_count']))
                print('{}. {}: {} likes'.format(k, pair['id'], pair['likes_count']))
                k += 1

            # if user['likes_count'] > 99:
            #     print('{}. {}: {} likes'.format(k, user['id'], user['likes_count']))
            #     k+=1
            # if pair['likes_count'] > 99:
            #     print('{}. {}: {} likes'.format(k, pair['id'], pair['likes_count']))
            #     k+=1
