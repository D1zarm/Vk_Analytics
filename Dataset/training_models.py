from dask import dataframe as dd
from dask.distributed import Client
from gensim.models import KeyedVectors
from joblib import load
from pymystem3 import Mystem
from toolbox.nlp import preprocessing_rus as pr
from toolbox.nlp.ml import kmeans_tools as kt


# Preprocess
def preprocess_df(df):
    print(df.head())
    print(len(df))
    mystem = Mystem()  # Most important to set it here!

    df['text'] = df['text'].apply(lambda x: pr.preprocess_post(x, False, mystem))

    mystem.close()
    return df


def to_vector_predict(df):
    wv = KeyedVectors.load('Word2Vec/keyed_vectors/vectors', mmap='r')  # It is critical to init wv and kmeans here
    kmeans = load('Clustering/KMeans/sklearn/n200posts.joblib')

    df['text'] = df['text'].apply(lambda x: int(kmeans.predict([pr.to_vector_tfidf(x, wv, True)])[0]))

    print(df.head())
    return df


if __name__ == '__main__':
    # 1. Preprocess the text file

    client = Client(n_workers=4)
    # If memory issues
    # df = dd.read_csv('texts.csv', dtype={'user_id': int, 'post_id': int, 'text': str}) # Read the big file
    # df = df.repartition(npartitions=df.npartitions*5) # Convert the big file into .parquet parts
    df = dd.read_parquet('texts.parquet')

    print(df)

    df = df.map_partitions(preprocess_df, meta={'user_id': int, 'post_id': int, 'text': object})
    df = df.dropna()

    # df.to_csv('preprocessed-2', single_file=False, header_first_partition_only=True)
    df.to_parquet('preprocesed-2.parquet')
    # client.close()

    # exit()

    # 2. Turn posts to vectors and predict vectors' clusters

    # client = Client(n_workers=4)
    # df = dd.read_parquet('preprocesed-2.parquet')
    #
    df = df.map_partitions(to_vector_predict, meta = {'user_id': int, 'post_id': int, 'text': int})
    df.columns = ['user_id', 'post_id', 'cluster']

    df.to_parquet('clusters-2.parquet')
    # exit()

    # 3. Group clusters by user_id and turn them into a single vector for each user

    # client = Client(n_workers=4)
    # df = dd.read_parquet('clusters-2.parquet')

    # Generate meta:
    n_clusters = 200
    meta = {'user_id': int, 'clusters': str}
    for i in range(n_clusters):
        meta[i] = int

    df = df.map_partitions(lambda df_part: kt.clusters_to_vector(df_part, n_clusters), meta=meta)
    df = df.drop(columns=['clusters'])

    df.to_csv('user_clusters-2.csv', single_file=True)

    # 4. Go to notebook, merge clusters and user_info dataframes and supervise a regression model.
