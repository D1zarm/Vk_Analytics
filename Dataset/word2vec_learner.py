from toolbox.nlp import preprocessing_rus as pr
from dask import dataframe as dd
from dask.distributed import Client
from pymystem3 import Mystem
from gensim.models import Word2Vec
from gensim.models import KeyedVectors

if __name__ == '__main__':
    client = Client(n_workers=4)
    
    # 1. Preprocess data
    df = dd.read_csv('texts.csv', dtype={'user_id':int, 'post_id':int, 'text':str, 'type':str, 'date':float})

    def prep(df):
        mystem = Mystem()

        df['text'] = df['text'].apply(lambda post: pr.preprocess_post(post, to_upos=False, mystem=mystem))

        print(df.head())
        mystem.close()
        return df

    df = df.map_partitions(prep, meta = {'user_id':int, 'post_id':int, 'text':object, 'type':str, 'date':float})
    df = df.dropna()
    df.to_parquet('preprocessed.parquet')

    # Merge sentences
    df = dd.read_parquet('preprocessed.parquet')

    df['text'] = df['text'].apply(lambda sents: [word for sent in sents for word in sent], meta=object)

    df.to_csv('prep-merged.csv', single_file= True)

    # 2. Turn merged sentences to line-sentences
    pr.df_to_line_sentence('prep-merged.csv', 'text', 'Word2Vec/line-sents-2.cor')

    # 3. Supervise Word2Vec
    model = Word2Vec(corpus_file='Word2Vec/line-sents-2.cor', size=300, window=5, min_count=1, workers=4)

    model.train(corpus_file='Word2Vec/line-sents-2.cor', total_examples=model.corpus_count, total_words= model.corpus_total_words, epochs=5)

    model.save('Word2Vec/word2vec.model')
    model.wv.save('Word2Vec/vectors/word2vec')


    # model = KeyedVectors.load('Word2Vec/keyed_vectors/word2vec')
    # print(model.most_similar('кошка_S'))