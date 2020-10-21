# coding=UTF-8

import random
import time
import traceback

import numpy as np
import requests
import vk


# Handle closed profiles!
# The text is also mostly on pictures in MDK
# 300 posts in MemeBlog group is approximately equal to 7 days. Set the post date break!

# put groups_count and group_posts_count as max as possible. The vk_parsing thing is done timer and skipping_amount
def get_liked_posts(session, pair_num=1, running_mins=60, user_id=None, groups_count=500, group_posts_count=8000,
                    user_domain=None,
                    skipping_point=0,
                    skipping_amount=600, post_likes_threshold=20000, liked_posts_threshold=20, retrying_secs=1 * 60,
                    add_time_for_groups=[], outburst_time=None, outburst_likes_count=None,
                    time_period=4 * 365):
    """
        Collect likes from user's groups and pages

        :param session: An object of Session class, in which user and public tokens are encapsulated
        :param pair_num: A number of pair/user, currently being processed
        :param running_mins: Overall search time of likes in minutes
        :param user_id: id of user being processed
        :param groups_count: Number of user's groups requested from vk
        :param group_posts_count: Number of posts analyzed in each group
        :param user_domain: User's domain, if user's id is not mentioned
        :param skipping_point: Threshold of minimum posts analyzed
        :param skipping_amount: Skip the group if no likes found in 'skipping_amount' of group's posts
        :param post_likes_threshold: Skip posts having more likes than 'post_likes_threshold'. For time economy.
        :param liked_posts_threshold: Skip the group after 'liked_posts_threshold' likes found there.
        :param outburst_time: Finish the search if less than 'outburst_likes_count' likes found in 'outburst_time' minutes
        :param outburst_likes_count: Finish the search if less than 'outburst_likes_count' likes found in 'outburst_time' minutes
        :param retrying_secs: Seconds of retrying in case a connection error occured.
        :param add_time_for_groups: List of groups, the time spent on which, is to add to the overall search time.
        :param time_period: a post's age deadline in days.

    """
    if outburst_time is None:
        outburst_time = running_mins
        outburst_likes_count = 20  # May set any number
    else:
        outburst_time *= 60  # We then measure it in seconds
    print('\n{}. Getting user\'s likes'.format(pair_num))
    service_vk = session.get_service_vk()
    if user_id is None:
        user_id = service_vk.utils.resolveScreenName(screen_name=user_domain, v='5.92')['object_id']

    time1 = time.time()
    time_is_over = False

    # At first we pull user's groups
    add_mins, groups = _get_groups(session, user_id, groups_count, retrying_time=retrying_secs)
    running_mins += add_mins
    print('{}. Got {} groups\n'.format(pair_num, groups['count']))

    # Now we collect posts from each group (better to implement time range restriction)
    groups_likes = []  # Initialize a list of 'group to likes' dicts
    likes_n = 0  # Overall likes count for this user
    analyzed_group_n_to_likes = {}  # Number of analysed group to likes number obtained from this group. For statistics
    for analyzed_groups_n, group in enumerate(groups['items'], start=1):

        print('{}. Analyzing group https://vk.com/{}'.format(pair_num, group['screen_name']))

        # If group is in banned_groups list
        banned_time = 0
        is_banned = False
        for banned_group in add_time_for_groups:
            if banned_group.lower() in group['name'].lower():
                print('{}. This group is in the banned_groups list!'.format(pair_num))
                is_banned = True
                banned_time = time.time()  # Measure the time we spent on analyzing it, and then add that time to the vk_parsing timer

        group_liked_posts = []  # Initialize likes obtained from this particular group
        tmp_wall = {'items': [],
                    'count': 0}  # Init to make the block of code return at least empty objects, not nothing
        posts_left = group_posts_count
        skipped_posts_count = 0  # For groups skipping
        one_like_within_skipping_amount = False
        k = 0
        this_group_likes = 0

        while posts_left > 0:

            # Define the offset.
            if k is 0:  # For the first query
                offset = 0
            else:  # For any other query
                offset = k * 100 + 1  # 0, 101, 201...

            timed_out = False
            int_serv = False
            exc_time = time.time()
            key_n = 0  # Counter of how many times function _update_public_key() has been called
            while True:
                try:
                    tmp_wall = service_vk.wall.get(owner_id='-{}'.format(group['id']), offset=offset, count=100,
                                                   v='5.92')  # count = 100 is Vk maximum by 24.01.2020. Don't change or many things will behave incorrectly
                except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError,
                        requests.exceptions.HTTPError, requests.exceptions.ChunkedEncodingError):
                    exc_time2 = time.time() - exc_time
                    if exc_time2 < retrying_secs:  # If the retrying timer is not over
                        print(
                            "{}. Exception! Getting posts timed out. Retrying ({}/{} secs)...".format(pair_num,
                                                                                                      '%.0f' % (
                                                                                                          exc_time2),
                                                                                                      retrying_secs))
                        timed_out = True
                        time.sleep(1)
                        continue  # Retry
                    else:
                        print("{}. Retrying for too long. Throwing an exception".format(pair_num))
                        send_message('{}. wall.get() retrying for too long'.format(pair_num))
                        raise

                except vk.exceptions.VkAPIError as e:
                    if e.code is 15:  # Access denied: group is blocked.
                        print("{}. Exception! Access denied: group is blocked. Skipping the group\n".format(pair_num))
                        tmp_wall = {'items': [], 'count': 0}
                        break
                    elif e.code in (28, 29):  # Rate limit reached / access_token has expired
                        print("\n{}. Exception! Rate limit on wall.get() reached. Updating the service key".format(
                            pair_num))

                        service_vk = session.update_public_key()

                        if key_n is session.get_service_tokens_n():
                            print('{}. All of the public keys in vk parser are invalid. Exiting the program'.format(
                                pair_num))
                            send_message(
                                '{}. All of the public keys in vk parser are invalid. Exiting the program'.format(
                                    pair_num))
                            raise
                        key_n += 1

                        time.sleep(1)
                        continue
                    elif e.code is 6:
                        print('{}. Too many requests for groups.get per second. Retrying...'.format(pair_num))
                        time.sleep(2)
                        continue
                    elif e.code is 10:
                        tr = traceback.format_exc()
                        msg = '{}. Internal server error:\n{}\n\nRetrying in {} mins\nService_key: {}'.format(pair_num,
                                                                                                              tr,
                                                                                                              '%.0f' % (
                                                                                                                      retrying_secs / 60),
                                                                                                              session.get_service_token())
                        print(msg)
                        if not int_serv:
                            # send_message(msg)
                            int_serv = True

                        time.sleep(retrying_secs)
                        service_vk = session.update_public_key()
                        continue
                    else:
                        tb = traceback.format_exc()
                        print('{}. Unknown vk exception.\n{}'.format(pair_num, tb))
                        raise

                if timed_out or int_serv:  # If after successful try block
                    print('{}. Got the posts! Continuing running\n'.format(pair_num))
                    # Add time, spent on exceptions to the search time
                    running_mins += (time.time() - time1) / 60
                break

            # Get all the posts that are posted within the time_period
            post_idx = 0
            for post_idx, post in enumerate(reversed(tmp_wall['items'])):
                passed = (time.time() - post['date']) / 86400  # Days had passed since the post was published
                if passed < time_period:  # If the last post is not outdated then no post in this 100 is outdated
                    break
            tmp_wall['items'] = tmp_wall['items'][0:100 - post_idx]
            tmp_wall['count'] = len(tmp_wall['items'])  # Override the posts count by the returned items count

            if tmp_wall['count'] < 1:  # if got 0 posts from the wall than we have analyzed the whole group
                print('{}. The whole group has been analyzed'.format(pair_num))
                break

            # Collecting user's likes on the group wall

            add_mins, tmp_liked = _liked_posts(session, pair_num, user_id, tmp_wall, retrying_time=retrying_secs,
                                               likes_threshold=post_likes_threshold)
            running_mins += add_mins

            # After analyzing another 100 posts from the group:

            this_group_likes += len(tmp_liked)  # incrementing likes got from this particular group

            # extending posts liked in this group list
            group_liked_posts.extend(tmp_liked)

            posts_left -= tmp_wall['count']

            k += 1  # k*100 - analyzed posts amount

            if post_idx is not 0:  # If met an outdated post
                print('{}. Some posts are outdated. Finishing vk_parsing the group wall'.format(pair_num))
                break  # Finish vk_parsing user's wall

            # If after 'skipping_point' posts analyzed we didn't find any like on a 'skipping_amount' of posts
            # skip this group
            if k * 100 > skipping_point - 1:  # If checked more than 'skipping_point' amount of posts from the group
                skipped_posts_count += tmp_wall['count']
                if skipped_posts_count < skipping_amount:  # If still haven't process the skipping_amount of posts
                    if len(tmp_liked) > 0:  # and also got at least 1 like in that skipping_amount range
                        one_like_within_skipping_amount = True
                elif not one_like_within_skipping_amount:  # If checked the 'skipping_amount' of posts and didn't get any like
                    print('\n{}. No likes found in {} posts\n'.format(pair_num, skipping_amount))
                    break  # Skip this group
                else:
                    skipped_posts_count = 0
                    one_like_within_skipping_amount = False

            if this_group_likes > liked_posts_threshold - 1:  # If user liked more than 'liked_posts_threshold' posts from this group
                print('{}. More than {} likes got from a group ({}). Moving on to the next group'.format(pair_num,
                                                                                                         liked_posts_threshold,
                                                                                                         this_group_likes))
                break  # skip this group

            # Check the timer and exit if it is over
            time2 = time.time()
            if time2 - time1 > running_mins * 60:
                print('{}. Time is over\n'.format(pair_num))
                time_is_over = True
                break

            # If didn't get 'outburst_like_count' likes in 'outburst time' secs, user in an 'outburst'. Skip him
            if time2 - time1 > outburst_time and likes_n + this_group_likes < outburst_likes_count:
                print('{}. Less than {} likes in {} mins were obtained for the user. Skipping the user'.format(pair_num,
                                                                                                               outburst_likes_count,
                                                                                                               outburst_time / 60))
                time_is_over = True
                break
        likes_n += len(group_liked_posts)  # Add likes obtained from this group to overall likes number
        # if group['type'] in ('group', 'event'):
        #     group['activity'] = 'Unknown'
        if 'activity' not in group:
            group['activity'] = np.nan
        groups_likes.append({'name': group['name'], 'theme': group['activity'], 'liked_posts': group_liked_posts})

        analyzed_group_n_to_likes[
            analyzed_groups_n] = this_group_likes  # Statistical info for choosing algorithm coefficients

        # Add time for the group that might be excluded from the final dataset
        if is_banned:
            banned_time2 = time.time() - banned_time
            print(
                '{}. The group https://vk.com/{} was in the banned_groups list. Adding {} secs to the search\n'.format(
                    pair_num,
                    group['screen_name'], banned_time2))
            running_mins += banned_time2 / 60  # Running time is counted in minutes
            time_is_over = False
        print('{}. Finished analyzing group. {} posts analyzed\n{} groups analyzed\n'.format(pair_num, k * 100,
                                                                                             analyzed_groups_n))
        if time_is_over: break

    # Gathering some statistics
    liked_groups_n = 0
    for gr in groups_likes:
        if len(gr['liked_posts']) > 0:
            liked_groups_n += 1  # Number of groups where we found at least 1 like

    return {'likes_n': likes_n, 'liked_groups_n': liked_groups_n,
            'analyzed_group_n_to_likes': analyzed_group_n_to_likes}, groups_likes


def _get_groups(session, user_id, groups_count, retrying_time=5 * 60):
    groups = {'items': [], 'count': 0}
    timed_out = False
    int_serv = False
    user_vk = session.get_user_vk()
    exc_time = time.time()
    add_mins = 0
    key_n = 0  # Counter of how many times function _update_user_key() has been called
    while True:
        try:
            groups = user_vk.groups.get(user_id=user_id, count=groups_count, extended=1, fields='activity', v='5.92')
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError, requests.exceptions.ChunkedEncodingError):
            exc_time2 = time.time() - exc_time
            if exc_time2 < retrying_time:  # Если время переборов ещё не вышло
                print("Exception! Getting groups timed out. Retrying ({}/{} secs)...".format('%.0f' % (exc_time2),
                                                                                             retrying_time))
                timed_out = True
                time.sleep(1)
                continue
            else:
                print("Retrying for too long. Throwing an exception")
                send_message('groups.get() retrying for too long')
                raise

        except vk.exceptions.VkAPIError as e:
            if e.code is 30:  # User's profile is private
                print('User\'s profile is private')
                raise
            elif e.code in (29, 28):  # Rate limit reached / access_token has expired
                user_vk = session.update_user_key()

                if key_n is session.get_user_tokens_n():
                    print('All of the user keys in vk parser are invalid. Exiting the program')
                    send_message('Exception! All of the user keys in vk parser are invalid.')
                    raise
                key_n += 1
                time.sleep(2)
                continue
            elif e.code is 10:
                tr = traceback.format_exc()
                msg = 'Internal server error:\n{}\n\nRetrying in {} mins\nuser_key: {}'.format(tr,
                                                                                               '%.0f' % (
                                                                                                       retrying_time / 60),
                                                                                               session.get_user_token())
                print(msg)
                if not int_serv:
                    # send_message(msg)
                    int_serv = True

                time.sleep(retrying_time)
                user_vk = session.update_user_key()
                continue

            else:
                print("Unexpected exception!")
                raise

        if timed_out or int_serv:  # If after successful try block
            print('Got the groups! Continuing running\n')
            add_mins += (time.time() - exc_time) / 60
        break

    # Removing banned_groups from the groups list
    # Commented because we don't need to remove that groups. We need to add time for them instead
    # if banned_groups_names is not None:
    #     groups2 = {'items': [], 'count': 0}
    #     for group in groups['items']:
    #         banned = False
    #         for banned_group in banned_groups_names:
    #             if banned_group.lower() in group['name'].lower():
    #                 banned = True
    #                 print('Group https://vk.com/{} is excluded from the group list\n'.format(group['screen_name']))
    #         if not banned:
    #             groups2['items'].append(group)
    #     groups2['count'] = len(groups2['items'])
    # else: groups2 = groups

    return add_mins, groups


# For each post figure out if user's like is present
def _liked_posts(session, pair_num, user_id, wall, retrying_time=5 * 60, likes_threshold=1000):
    service_vk = session.get_service_vk()
    add_mins = 0
    liked_posts = []
    likes = {'items': [], 'count': -1}
    for post in wall['items']:
        if 'likes' not in post:  # Vk Api may return an empty post
            continue  # just skip it
        k = 0
        # likes_processed = 0  # For testing
        # likes_time1 = time.time() # For timing test
        while True:  # To process the whole amount of likes, not only 1000
            # Define the offset.
            if k is 0:  # For the first query
                offset = 0
            else:  # For any other query
                offset = k * 1000 + 1  # 0, 1001, 2001...

            timed_out = False
            exc_time = time.time()
            key_n = 0
            int_serv = False
            while True:
                try:
                    likes = service_vk.likes.getList(type='post', owner_id=post['owner_id'], item_id=post['id'],
                                                     count=1000, offset=offset,
                                                     v='5.92')
                except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError,
                        requests.exceptions.HTTPError, requests.exceptions.ChunkedEncodingError):
                    exc_time2 = time.time() - exc_time
                    if exc_time2 < retrying_time:  # Если время переборов ещё не вышло
                        print(
                            "{}. Exception! Getting likes timed out. Retrying ({}/{} secs)...".format(pair_num,
                                                                                                      '%.0f' % (
                                                                                                          exc_time2),
                                                                                                      retrying_time))
                        timed_out = True
                        time.sleep(1)
                        continue
                    else:
                        print("{}. Retrying for too long. Throwing an exception".format(pair_num))
                        send_message('{}Retrying for too long. Throwing an exception'.format(pair_num))
                        raise

                except vk.exceptions.VkAPIError as vk_e:
                    if vk_e.code is 10:
                        tr = traceback.format_exc()
                        msg = 'Internal server error:\n{}\n\nRetrying in {} mins\nservice_key: {}'.format(tr,
                                                                                                          '%.0f' % (
                                                                                                                  retrying_time / 60),
                                                                                                          session.get_service_token())
                        print(msg)
                        if not int_serv:
                            # send_message(msg)
                            int_serv = True

                        time.sleep(retrying_time)

                        service_vk = session.update_public_key()
                        continue
                    elif vk_e.code in (29, 28):  # Rate limit reached / access_token has expired
                        service_vk = session.update_public_key()

                        if key_n is session.get_service_tokens_n():
                            print('All of the service keys in vk parser are invalid. Exiting the program')
                            send_message('Exception! All of the user keys in vk parser are invalid.')
                            raise
                        key_n += 1
                        time.sleep(2)
                        continue
                    else:
                        print("{}. Unexpected exception!".format(pair_num))
                        raise

                if timed_out or int_serv:  # If after successful try block
                    print('{}. Got the likes! Continuing running\n'.format(pair_num))
                    add_mins += (time.time() - exc_time) / 60  # Add time, spent on these exceptions
                break

            # look if user's like is present in collected ones
            if int(user_id) in likes['items']:
                liked_posts.append(post)
                print(
                    '{}. Like for user https://vk.com/id{} found! Post: https://vk.com/id{}?w=wall{}_{}'.format(
                        pair_num, user_id,
                        user_id,
                        post[
                            'from_id'],
                        post[
                            'id']))

            # likes_processed += len(likes['items']) # For timing test
            if len(likes['items']) < 1000:  # if got < than 1000 likes it means the whole post have been analyzed
                break
            if likes[
                'count'] > likes_threshold - 1:  # If a post contains more than 'likes_threshold' (default - 1000) likes, it's too expensive to check it
                break  # 6 secs for 147k likes comparing to 0.1 secs for 2k likes. We might bypass 1 pinned post per a huge group with this break
            # We will still check each post at least ones, because we need to get the 'likes count'
            k += 1

    return add_mins, liked_posts


def get_posts(session, user_id, count=300, time_period=3 * 365, retrying_time=5 * 60):
    def is_avatar(post):
        return ('attachments' in post) and (len(post['attachments']) > 0 and 'photo' in post['attachments'][0]) and \
               post['attachments'][0]['photo'][
                   'album_id'] == -6

    print('\nGetting user\'s posts and reposts')
    user_vk = session.get_user_vk()
    posts_left = count
    k = 0
    posts = tmp_posts = {'count': 0, 'items': []}
    while posts_left > 0:

        # Define the offset.
        if k is 0:  # For the first query
            offset = 0
        else:  # For any other query
            offset = k * 100 + 1  # 0, 101, 201...

        timed_out = False
        int_serv = False
        exc_time = time.time()
        key_n = 0  # Counter of how many times function _update_public_key has been called
        while True:
            try:
                # print('wall.get!! {}'.format(session.get_user_token())) # testing
                tmp_posts = user_vk.wall.get(owner_id=user_id, offset=offset, filter='owner', count=100,
                                             v='5.92')  # count = 100 is Vk maximum by 24.01.2020. Don't change or many things will behave incorrectly
                time.sleep(0.34)
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError,
                    requests.exceptions.HTTPError, requests.exceptions.ChunkedEncodingError):
                exc_time2 = time.time() - exc_time
                if exc_time2 < retrying_time:  # If the retrying timer is not over
                    print(
                        "Exception! Getting user posts timed out. Retrying ({}/{} secs)...".format('%.0f' % (exc_time2),
                                                                                                   retrying_time))
                    timed_out = True
                    time.sleep(1)
                    continue  # Retry
                else:
                    print("Retrying for too long. Throwing an exception")
                    send_message('wall.get() retrying for too long')
                    raise

            except vk.exceptions.VkAPIError as e:
                if e.code is 15:  # Access denied
                    print("Exception! Access denied. Could not get user posts\n")
                    return posts
                elif e.code in (29, 28):  # Rate limit reached / access_token has expired
                    print("\nException! Rate limit on user_vk.wall.get() reached. Updating the user key")
                    user_vk = session.update_user_key()

                    if key_n is session.get_user_tokens_n():
                        msg = 'All of the user keys in vk parser are invalid. Waiting for 1 hour\n' \
                              'user_token = {}'.format(session.get_user_token())
                        print(msg)
                        send_message(msg)

                        time.sleep(60 * 60)  # Wait for user key to refresh. Сheck every hour
                        key_n = 0
                        continue
                    key_n += 1

                    time.sleep(5)
                    continue
                elif e.code is 10:  # Internal server error
                    tr = traceback.format_exc()
                    msg = 'Internal server error:\n{}\nRetrying in {} mins\nuser_key: '.format(tr, '%.0f' % (
                            retrying_time / 60), session.get_user_token())
                    print(msg)
                    if not int_serv:
                        # send_message(msg)
                        int_serv = True

                    time.sleep(retrying_time)
                    user_vk = session.update_user_key()
                    continue

                elif e.code is 30:
                    print('User\'s profile is private. Could not get any wall posts')
                    raise
                elif e.code is 6:
                    print('user_key: {}'.format(session.get_user_token()))
                    raise
                else:
                    print("Unexpected exception!")
                    raise

            if timed_out or int_serv:  # If after successful try block
                print('Got the user posts! Continuing running\n')
            break

        if len(tmp_posts['items']) < 1:  # if got 0 posts from the wall than we have analyzed the whole wall
            print('The whole wall has been analyzed')
            break

        # Remove outdated posts and exit if there are any.
        idx = 0
        for idx, post in enumerate(reversed(tmp_posts['items'])):
            passed = (time.time() - post['date']) / 86400  # Days had passed since the post was published
            if passed < time_period:  # If the last post is not outdated then no post in this 100 is outdated
                break

        if idx is not 0:
            tmp_posts['items'] = tmp_posts['items'][0:- idx]
            tmp_posts['count'] = len(tmp_posts['items'])

        posts['items'].extend(tmp_posts['items'])
        posts['count'] += len(tmp_posts['items'])

        if idx is not 0:  # If met an outdated post
            print('Some posts are outdated. Finishing analyzing user posts')
            break  # Finish vk_parsing user's wall

        posts_left -= len(tmp_posts['items'])

        k += 1

    # If there are no posts on user's wall:
    if len(posts['items']) == 0:
        print('There are no posts on user\'s wall')
        return 0, 0, posts

    # If there's only 1 post on user's wall and it's pinned:
    if len(posts['items']) == 1 and 'is_pinned' in posts['items'][0]:
        return (time.time() - posts['items'][0]['date']) / 86400, \
               posts['items'][0]['likes']['count'], posts

    # Now check the posting density and likes mean

    # If there is a pinned post, we don't consider it, because it might bypass the date break
    if 'is_pinned' in posts['items'][0]:
        posts['items'] = posts['items'][1:len(posts['items'])]
        posts['count'] = len(posts['items'])

    # Then, let's initialize the time passed from now until the user's first post (which is not avatar or pinned post)
    days_dif = 0
    for l in range(0, posts['count']):
        if not is_avatar(posts['items'][l]):  # If a post is not an avatar
            days_dif = (time.time() - posts['items'][0]['date']) / 86400
            break
    wall_likes_count = 0

    k = 0  # Number of posts, which are not avatars
    for i in range(0, posts['count'] - 1):
        if 'likes' not in posts['items'][i]:  # Vk API may return an empty post.
            continue  # Just skip it

        if not is_avatar(
                posts['items'][i]):  # If a post is not an avatar (I've noticed that album_id -6 means avatar picture)
            days_dif += (posts['items'][i]['date'] - posts['items'][i + 1]['date']) / 86400
            wall_likes_count += posts['items'][i]['likes']['count']
            k += 1  # increment posts count (which are not avatars)
    if not is_avatar(posts['items'][posts['count'] - 1]):  # if the last post is not an avatar
        wall_likes_count += posts['items'][posts['count'] - 1]['likes'][
            'count']  # Grab the last post (because is was skipped in the loop)
        k += 1

    if k is not 0:
        wall_likes_mean = wall_likes_count / k  # -k to not consider avatars
        posting_density = days_dif / k
    else:
        return 0, 0, posts

    # 1/posting_density is a velocity of posting.
    return 1 / posting_density, wall_likes_mean, posts


# Returns a full user info
def get_user_info(session, user_id, retrying_time=5 * 60):
    print('\nGetting user\'s info')
    user_vk = session.get_user_vk()
    exc_time = time.time()
    timed_out = False
    int_serv = False
    info = None
    key_n = 0  # Counter of _update_user_key()
    while True:
        try:
            # Friends count is returned only with user token
            info = user_vk.users.get(user_id=user_id, name_case='nom',
                                     fields='sex,bdate,career,about,activities,city,books,connections,contacts, '
                                            'counters, country,education,site,universities, schools, status, '
                                            'occupation, relation, personal, movies, music, quotes, tv, verified, '
                                            'followers_count,games,has_photo,home_town,interests,last_seen',
                                     v='5.89')
            time.sleep(0.34)
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError, requests.exceptions.ChunkedEncodingError):
            exc_time2 = time.time() - exc_time
            if exc_time2 < retrying_time:  # Если время переборов ещё не вышло
                print(
                    "Exception! Getting user info timed out. Retrying ({}/{} secs)...".format('%.0f' % (exc_time2),
                                                                                              retrying_time))
                timed_out = True
                time.sleep(1)
                continue
            else:
                print("Retrying for too long. Throwing an exception")
                send_message('users.get() retrying for too long.')
                raise

        except vk.exceptions.VkAPIError as e:
            if e.code in (28, 29):  # Rate limit reached / access_token has expired
                print('users.get() rate limit reached. Updating the key')
                user_vk = session.update_user_key()

                if key_n is session.get_user_tokens_n():
                    print('All of the user keys in vk parser are invalid. Exiting the program')
                    send_message('All of the user keys in vk parser are invalid.')
                    raise
                key_n += 1
                time.sleep(1)
                continue

            elif e.code is 10:
                tr = traceback.format_exc()
                msg = 'Internal server error:\n{}\n\nRetrying in {} mins\nuser_key: {}'.format(tr,
                                                                                               '%.0f' % (
                                                                                                       retrying_time / 60),
                                                                                               session.get_user_token())
                print(msg)
                if not int_serv:
                    # send_message(msg)
                    int_serv = True

                time.sleep(retrying_time)
                user_vk = session.update_user_key()
                continue

            else:
                print("Unexpected exception!")
                raise

        if timed_out or int_serv:  # If after successful try block
            print('Got the user info! Continuing running\n')
        break

    if info is None:
        print('Could get any info about user https://vk.com/id{}'.format(user_id))

    return info[0]


def avatars_counters(session, user_id, time_period, retrying_time=5 * 60):
    print('\nGetting the mean of profile picture likes')
    photos = {'count': 0, 'items': []}
    service_vk = session.get_service_vk()
    exc_time = time.time()
    timed_out = False
    int_serv = False
    key_n = 0
    while True:
        try:
            # Friends count is returned only with user token
            photos = service_vk.photos.get(owner_id=user_id, album_id='profile', extended='1', rev=1,
                                           v='5.89')  # rev = 1 means from the earliest to the latest
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError, requests.exceptions.ChunkedEncodingError):
            exc_time2 = time.time() - exc_time
            if exc_time2 < retrying_time:  # Если время переборов ещё не вышло
                print(
                    "Exception! Getting user photos timed out. Retrying ({}/{} secs)...".format('%.0f' % (exc_time2),
                                                                                                retrying_time))
                timed_out = True
                time.sleep(1)
                continue
            else:
                print("Retrying for too long. Throwing an exception")
                send_message('photos.get() retrying for too long.')
                raise

        except vk.exceptions.VkAPIError as e:
            tb = traceback.format_exc()
            if e.code is 5:  # Invalid access token
                print('Invalid access token: {}\nUser: {}'.format(session.get_service_token(), user_id))
                send_message('Invalid access token: {}'.format(session.get_service_token()))
            elif e.code is 10:  # Internal server error
                tr = traceback.format_exc()
                msg = 'Internal server error:\n{}\n\nRetrying in {} mins\nservice_key: {}'.format(tr, '%.0f' % (
                        retrying_time / 60), session.get_service_token())
                print(msg)
                if not int_serv:
                    # send_message(msg)
                    int_serv = True

                time.sleep(retrying_time)
                service_vk = session.update_public_key()
                continue
            elif e.code in (29, 28):  # Rate limit reached / access_token has expired
                service_vk = session.update_public_key()

                if key_n is session.get_service_tokens_n():
                    print('All of the service keys in vk parser are invalid. Exiting the program')
                    send_message('Exception! All of the user keys in vk parser are invalid.')
                    raise
                key_n += 1
                time.sleep(2)
                continue

            else:
                print('Unknown vk exception.\n{}'.format(tb))

            raise

        if timed_out or int_serv:  # If after successful try block
            print('Got the user photos! Continuing running\n')
        break

    # Remove outdated photos and exit if there are any.
    idx = 0
    for idx, photo in enumerate(photos['items']):
        passed = (time.time() - photo['date']) / 86400  # Days had passed since the photo was published
        if passed < time_period:  # If the last post is not outdated then no photo is outdated
            break

    if idx is not 0:  # if there were any outdated photos
        photos['items'] = photos['items'][idx:]
    photos['count'] = len(photos['items'])

    # Now calculate the velocity of avatars posting and likes mean
    if len(photos['items']) == 0:  # If there is no photos
        return 0, 0  # return 0
    photo_likes_count = 0
    days_dif = (time.time() - photos['items'][0]['date']) / 86400
    for i in range(0, photos['count'] - 1):
        days_dif += (photos['items'][i]['date'] - photos['items'][i + 1]['date']) / 86400
        photo_likes_count += photos['items'][i]['likes']['count']
    photo_likes_count += photos['items'][photos['count'] - 1]['likes'][
        'count']  # Grab the last post (which was skipped in the loop)

    avatars_likes_mean = photo_likes_count / photos['count']

    avatars_density = days_dif / (photos['count'])

    # 1 / avatars_density is a velocity of avatars posting
    return avatars_likes_mean, 1 / avatars_density


def send_message(text):
    """
    A method to send some texts (i.e., warnings, code errors) to you in vk social network
    :param text: a text to send
    """
    id = 'put your vk id here'
    user_vk = vk.API(vk.Session(
        access_token='put a user token here'))
    user_vk.messages.send(user_id=id, message=text, random_id=random.getrandbits(64), v='5.90')


def parse_user(session, user_id, running_mins,
               outburst_time,
               outburst_likes_count,
               post_likes_threshold,
               time_period,
               skipping_amount,
               group_posts_count,
               liked_posts_threshold,
               pair_num=1):
    time1 = time.time()

    user_info = get_user_info(session, user_id)  # If all is ok, than set the user_info

    avatar_likes_mean, avatars_posting_frequency = avatars_counters(session, user_id,
                                                                    time_period=time_period)

    posting_frequency, wall_likes_mean, user_posts = get_posts(session, user_id, count=250,
                                                               time_period=time_period)
    stats, groups = get_liked_posts(session, pair_num=pair_num, user_id=user_id,
                                    running_mins=running_mins,
                                    outburst_time=outburst_time,
                                    outburst_likes_count=outburst_likes_count,
                                    post_likes_threshold=post_likes_threshold,
                                    time_period=time_period,
                                    skipping_amount=skipping_amount,
                                    group_posts_count=group_posts_count,
                                    liked_posts_threshold=liked_posts_threshold)

    user_info['user_posts'] = user_posts
    user_info['likes_count'] = stats['likes_n']
    user_info['groups'] = groups
    user_info['posting_frequency'] = posting_frequency
    user_info['wall_likes_mean'] = wall_likes_mean
    user_info['avatar_likes_mean'] = avatar_likes_mean
    user_info['avatars_posting_frequency'] = avatars_posting_frequency

    time2 = time.time() - time1

    log = '{}. For {}: {} likes in {} groups has been obtained. {} groups has been analyzed in {} minutes\n'.format(
        pair_num, user_id,
        stats['likes_n'],
        stats[
            'liked_groups_n'],
        len(
            groups),
        '%.1f' % (
                time2 / 60))

    return user_info, log
