# coding=UTF-8

from file_parsing import file_parser as fp

texts_path = 'Dataset/texts.csv'
photos_path = 'Dataset/photos.csv'
infos_path = 'Dataset/users_info.csv'

users_path = ['data/users1.txt', 'data/users2.txt', 'data/users3.txt'] #, 'data/users4.txt']

for users in users_path:
    fp.update_dataset(users, texts_path, photos_path, infos_path)
# fp.get_pairs(users_path)
