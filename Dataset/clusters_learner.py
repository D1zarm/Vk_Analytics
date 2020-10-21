#!/usr/bin/env python
# coding: utf-8
import json
import time
from datetime import datetime

from dask import array as da
from dask import dataframe as dd
from dask.distributed import Client
from dask_ml.cluster import KMeans
from gensim.models import KeyedVectors
from joblib import load, dump
from sklearn.cluster import KMeans as SKMeans
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error as mae, mean_squared_error as mse
from sklearn.utils._openmp_helpers import _openmp_effective_n_threads
from toolbox.nlp import preprocessing_rus as my_nlp
from toolbox.nlp.ml import kmeans_tools as kt
from numpy import nan
import numpy as np
import pandas as pd
from sklearn.model_selection import KFold


def timestamp_to_year(Y):
    for idx, y in enumerate(Y):
        Y[idx] = datetime.fromtimestamp(y).year
    return Y


def train(df, regr):
    y = df['bdate']
    X = df.drop(columns=['bdate'])

    print(X.head())
    print(X.columns[:16])

    kf = KFold(n_splits=5, shuffle=True, random_state=21)

    R2_train_l = []
    R2_test_l = []
    mae_er_l = []
    mse_er_l = []

    for train_index, test_index in kf.split(X):
        X_train, X_test = X.loc[train_index], X.loc[test_index]
        y_train, y_test = y.loc[train_index], y.loc[test_index]

        regr.fit(X_train, y_train)

        R2_train_l.append(regr.score(X_train, y_train))
        R2_test_l.append(regr.score(X_test, y_test))

        y_pred = regr.predict(X_test)

        y_pred = timestamp_to_year(y_pred)
        y_test = timestamp_to_year(y_test.to_numpy())

        mae_er_l.append(mae(y_test, y_pred))
        mse_er_l.append(mse(y_test, y_pred, squared=False))

    R2_test = np.array(R2_test_l).mean()
    R2_train = np.array(R2_train_l).mean()
    mae_er = np.array(mae_er_l).mean()
    mse_er = np.array(mse_er_l).mean()

    print('R2_test = {}'.format('%.3f' % R2_test))
    print('R2_train = {}'.format('%.3f' % R2_train))
    print('mae = {}'.format(mae_er))
    print('rmse = {}'.format('%.3f' % mse_er))
    print('Top 10 outliers: {}'.format(sorted(mae_er_l, reverse=True)[:10]))

    return R2_test


def df_to_vector_predict(df, kmeans_path):

    wv = KeyedVectors.load(wv_path, mmap='r')
    kmeans = load(kmeans_path)

    # Much faster than concating to np matrix and then applying
    df['cluster'] = df['text'].apply(lambda x: int(kmeans.predict([my_nlp.to_vector_tfidf(x, wv, True)])[0]))

    return df


def learn_clusters(n_clust):
    client = Client(n_workers=4, processes=True)

    # 1. Learn clusters

    # Full set
    kmeans_path = 'Clustering/KMeans/n{}posts.joblib'.format(n_clust)

    array = da.from_npy_stack(npy_stack_path)
    kmeans = KMeans(n_clusters=n_clust)

    # Learn on a part of set
    # array = np.load('Clustering/npy_post_vecs_part/0.npy')
    # kmeans = SKMeans(n_clusters=n_clust)

    print('Fitting')
    kmeans.fit(array)

    del array
    # Dump centroids to the disk

    # Dump as a sklearn object, for (maybe) faster prediction and less problems
    skmeans = SKMeans(n_clusters=n_clust)
    skmeans.cluster_centers_ = kmeans.cluster_centers_
    skmeans._n_threads = _openmp_effective_n_threads()
    dump(skmeans, kmeans_path)
    del kmeans, skmeans

    # dump(kmeans, kmeans_path) # For learning on a part of set
    # del kmeans
    print('Fitted')

    # 3. Turn posts into clusters
    kmeans_path = 'Clustering/KMeans/n{}posts.joblib'.format(n_clust)

    df = dd.read_parquet('preprocessed.parquet')
    df = df.map_partitions(df_to_vector_predict, kmeans_path,
                           meta={'user_id': int, 'post_id':int,'text': object, 'type':str, 'date':str, 'cluster': int})
    df.to_parquet('Clustering/KMeans/n{}posts.parquet'.format(n_clust))
    print('Clustered')

    # 2.5. Filter outdated posts out. (The next time write date of parsing to user_info)
    # For each user find his last like and filter out likes that are older than the last + half a year
    df = dd.read_parquet('Clustering/KMeans/n{}posts.parquet'.format(n_clust))
    print('Original df len: {}'.format(len(df)))

    year = 31536000  # One year in timestamp
    kyear=20
    break_time = kyear*year  # 0.75*year - A quarter to year
    last_like = df['date'].max().compute() # Set has been fully collected on 8 of June 2020

    df = df[df['date'] > last_like - break_time] # Pass only a quarter-to-year recent likes
    print('max_date: {} '.format(df['date'].max().compute()))
    print('min date: {}'.format(df['date'].min().compute()))
    print('Filtered df len: {}'.format(len(df)))
    print('Likes has been filtered out by date')

    # 3. Group clusters by user_id and turn them into a single vector for each user

    # df = dd.read_parquet('Clustering/KMeans/n{}posts.parquet'.format(n_clust)) # INSTEAD OF FILTER!

    # - Count text_likes number for each user (and later merge with user_info)
    count = df.drop(columns=['post_id', 'type', 'date', 'cluster']).groupby('user_id')['text'].count().compute()
    count.rename('text_likes', inplace = True)

    # Generate meta
    meta = {'user_id': int}
    for i in range(n_clust):
        meta[i] = float

    df = df.map_partitions(lambda df_part: kt.clusters_to_vector(df_part, n_clust), meta=meta)

    df.to_parquet('Clustering/KMeans/n{}posts-cluster_vecs.parquet'.format(n_clust))

    # 5. Merge clusters and user_info dataframes. (Working with pandas frames)
    df_info = pd.read_csv('users_info.csv')

    df_info = df_info.merge(count, on='user_id', how='inner')
    del count

    df = pd.read_parquet('Clustering/KMeans/n{}posts-cluster_vecs.parquet'.format(n_clust))

    df = df_info.merge(df, on='user_id', how='inner')  # Merging user's info and clusters. Maybe, mistake is here

    df.to_csv('Clustering/KMeans/n{}-final_dataset-{}year.csv'.format(n_clust, kyear))
    print('Final dataset has been saved')
    del df_info

    # Filter some users out
    # df = pd.read_csv('Clustering/KMeans/n{}-final_dataset.csv'.format(n_clust)).drop(columns=['Unnamed: 0']) # TESTING

    df = df.loc[(df['text_likes'] > 100) & (df['text_likes'] < 1000)]

    df['bdate'] = df['bdate'].apply(lambda bd: time.mktime(datetime.strptime(bd, "%d.%m.%Y").timetuple()))

    # Clean up the dataset
    df = df.drop(columns=['posts_n', 'text_likes', 'status', 'sex', 'smoking', 'alcohol', 'parth_id',
                          'country', 'city', 'user_id']).dropna().reset_index(drop=True)

    # 6. Supervise a Linear Regression model
    regr = LinearRegression()
    R2 = train(df, regr)

    client.close()
    return R2


def binar_search(range, n_iter=100):
    """
    Search for maximum
    :param range:
    :param n_iter:
    :return:
    """

    beg = range[0]
    end = range[1]

    r2_beg = learn_clusters(beg)
    print('n = {}, R2 = {}'.format(beg, r2_beg))
    r2_end = learn_clusters(end)
    print('n = {}, R2 = {}'.format(end, r2_end))

    for i in range(n_iter):
        cent = int((end - beg) / 2)
        print('{}. Center is: {}'.format(i, cent))

        r2_cent = learn_clusters(cent)
        print('n = {}, R2 = '.format(r2_cent))

        if r2_cent > r2_beg and r2_cent > r2_end:
            if r2_beg > r2_end:
                end = cent
            else:
                beg = cent
        else:
            print('R2 on center is lower than on edges. Choosing the random way - on the right')
            print('r2_beg = {}; r2_end = {}; cent = {}'.format(r2_beg, r2_cent, cent))
            beg = cent


if __name__ == '__main__':
    wv_path = 'Word2Vec/keyed_vectors/word2vec'
    npy_stack_path ='Clustering/npy_post_vecs'

    binar_search((40, 150), 100)
    #
    # exit()

    # 0. Turn preprocesesd.parquet to npy stack

    # 0.1 Turn parquet to csv
    # client = Client(n_workers=4)
    # df = dd.read_parquet('preprocessed.parquet')
    # df = df.repartition(npartitions=200)
    # def tojson(array):
    #     array = [ar.tolist() for ar in array]
    #     str = json.dumps(array)
    #     return str
    #
    # df['text'] = df['text'].apply(tojson, meta=str)
    # df.to_csv('preprocessed.csv', single_file=True)
    # client.close()
    #
    # 0.2 Turn preprocesed.csv to npy stack
    # wv = KeyedVectors.load(wv_path)
    # my_nlp.csv_to_tfidf_vectors_npy_stacks('preprocessed.csv', 'text', 'Clustering/npy_post_vecs_part',wv, True, unique=True, chunk_size=0.6)

    # Only supervise once
    learn_clusters(200)
