import json

import pandas as pd
from gensim.models import KeyedVectors
from toolbox.nlp import preprocessing_rus as pr
from toolbox.nlp.ml import kmeans_tools as kt
from file_parsing.file_parser import parse_user_info, parse_post, text_header, info_header
from joblib import load
from vk_parsing import tokens, parser
import time
from datetime import datetime
from pymystem3 import Mystem
import numpy as np


def predict(user_id, wv_path, kmeans_path, regr_path):
    # Collect user's info from vk.com
    session = tokens.Session()

    user_data, log = parser.parse_user(session, user_id=user_id,
                      running_mins=15,
                      outburst_time=30,
                      outburst_likes_count=10,
                      post_likes_threshold=1000,
                      time_period=4 * 365,
                      skipping_amount=400,
                      group_posts_count=3000,
                      liked_posts_threshold=20)
    print(log)

    user_info = parse_user_info(user_data)  # Merge it with clusters vector

    text_posts = []
    post_idx = -1

    for post in user_data['user_posts']['items'] + [like for group in user_data['groups'] for like in group['liked_posts']]:

        post_idx += 1
        post_content = parse_post(post, user_data['id'])

        text = post_content[0]
        if text != '':
            text_posts.append([user_data['id'], post_idx, text])

    # Add user data to dataframes
    df = pd.DataFrame.from_records(text_posts)
    df.columns = text_header[:3] # [:3]
    df.dropna()
    df = df.drop(columns=['post_id'])

    # Preprocess posts
    mystem = Mystem()
    df['text'] = df['text'].apply(lambda x: pr.preprocess_post(x, False, mystem))
    mystem.close()
    text_n = len(df['text'])  # Save text posts number
    if text_n < 100:
        print('Для пользователя собрано не достаточно много информации. Предсказание может быть неточным')
        print('Кол-во текстовых постов: {}'.format(text_n))

    # Turn posts to vectors and predict vectors' clusters
    wv = KeyedVectors.load(wv_path, mmap='r')

    df['text'] = df['text'].apply(lambda x: pr.to_vector_tfidf(x, wv, one_vector=True))
    df = df.dropna()

    kmeans = load(kmeans_path)
    li = df.groupby('user_id')['text'].apply(list).to_numpy()
    df['cluster'] = kmeans.predict(li[0])

    df = kt.clusters_to_vector(df, kmeans.n_clusters).set_index('user_id')

    # Create info df and merge it with clusters df
    df_info = pd.DataFrame.from_records([user_info])
    df_info.columns = info_header
    df_info.set_index('user_id', inplace=True)
    df_info.drop(columns=['sex', 'status', 'smoking', 'alcohol', 'parth_id', 'country', 'city'],
                 inplace=True)

    df = df_info.merge(df, on='user_id', how='inner')  # Merging user's info and clusters

    df.drop(columns=['bdate', 'posts_n'], inplace=True)

    # Predicting age
    regr = load(regr_path)
    age = regr.predict(df)

    print('\n\nПользователю {} лет'.format(datetime.today().year - datetime.fromtimestamp(age).year))

regr1 = 'Dataset/Regression/linear.joblib' # Для всех. Неровная выборка.
regr2 = 'Dataset/Regression/no_filter_80+.joblib' # Для пользователей, родившихся после 1990 года. Хорошо предсказывает, т.к. дата лайков близка
regr3 = 'Dataset/Regression/balanced_all.joblib'

predict(57556862,
        wv_path='Dataset/Word2Vec/keyed_vectors/word2vec',
        kmeans_path='Dataset/Clustering/KMeans/rep-n200posts.joblib',
        regr_path = regr2)

