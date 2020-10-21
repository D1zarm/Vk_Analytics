# coding=UTF-8

from sample_preparing import user_info_verification as av
import pandas as pd
from vk_parsing.tokens import Session

sub_people = pd.read_csv('New_Sample/substracted_people.csv', index_col=0).reset_index(drop=True)
verified_sub_people = av.verify(Session(), sub_people)
av.write_to_file(verified_sub_people,'New_Sample/verified_subtracted_people.csv')
sorted_verified_subtracted = av.group_sort(verified_sub_people)
av.write_to_file(sorted_verified_subtracted,'New_Sample/sorted_verified_subtracted_people.csv')


