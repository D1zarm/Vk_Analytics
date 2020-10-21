# Vk_Analytics
Collects some users' data from vk.com (their 'likes' and basic profile information), builds a machine learning model and predicts users' age by their basic profile info and posts that they liked.

<i>All the privacy sensitive data has been removed.</i>

<h2>A brief usage instruction:</h2>

1) sample_preparing_main.py takes a list of user ids and filters it out by some criterias (basically, it verifies users' age by their friends' age distribution and users' sex by their names)
2) list_parser_main.py takes a filtered list of users and collects data on them from vk.com. The program is multithreaded and is designed to maximize the perfomance.
3) users_file_parser.py takes data collected by list_parser_main.py (a json file) and turns it into .csv files: 'photos.csv', 'texts.csv', 'users_info.csv'

Obtained data sets are then processed by scripts in the 'Dataset' folder.

4) word2vec_learner.py takes texts.csv file, which contains textual posts 'liked' by users, preprocesses the data and learns word representations for these texts.
5) clusters_learner.py takes the preprocessed textual data, clusterizes it and creates a linear regression model for age prediction. It also generates a 'final_dataset.cav' file which contains basic users' info as well as informaiton on their prefereces ('likes'), which is ready for passing to regression algoritms.
6) As 'final_dataset.csv' is ready, it's worth opening 'Regression_model.ipynb' to build some regression models. 'text_clustering.ipynb' is used to evaluate a clustering algorithm's perfomance. 
7) The 'Results' folder represents some of the liner regression model accuracy evaluation. More of the results are presented in 'Regression_model.ipynb' notebook, however, currently only in Russian language. 
8) 'predictor.py' is the final script which takes user's id from vk.com and predicts his or her age.   
