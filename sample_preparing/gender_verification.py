# coding=UTF-8

import pandas as pd
from yandex.Translater import Translater
import time
import requests

tr = Translater()
tr.set_key('put Yandex translator key here')
tr.set_from_lang('en')
tr.set_to_lang('ru')

dfru = pd.read_csv('Names_List/FakeNameGenerator_ru.csv')
dfen = pd.read_csv('Names_List/FakeNameGenerator_en.csv')
ru_names = pd.read_csv('Names_List/final_merged_ru_names_list.csv')
# ru_fam = pd.read_csv('Names_List/russian_surnames.csv', delimiter=';', index_col=0)

ru_male_names = set(
    s.replace('ё', 'е').replace(chr(769), '').lower() for s in ru_names[ru_names['sex'] == 'male']['names'])

ru_male_surnames_endings = set(
    s.replace(chr(769), '')[-2:].replace('ё', 'е').lower() for s in dfru[dfru['Gender'] == 'male']['Surname'])

ru_female_names = set(
    s.replace('ё', 'е').replace(chr(769), '').lower() for s in ru_names[ru_names['sex'] == 'female']['names'])
ru_female_surnames_endings = set(
    s.replace(chr(769), '')[-2:].replace('ё', 'е').lower() for s in dfru[dfru['Gender'] == 'female']['Surname'])
ru_female_surnames_endings.remove('ов')

en_male_names = set(
    s.replace('ё', 'е').replace(chr(769), '').lower() for s in dfen[dfen['Gender'] == 'male']['GivenName'])
en_male_surnames_endings = set(s[-2:].replace(chr(769), '').lower() for s in dfen[dfen['Gender'] == 'male']['Surname'])

en_female_names = set(s.replace(chr(769), '').lower() for s in dfen[dfen['Gender'] == 'female']['GivenName'])
en_female_surnames_endings = set(
    s[-2:].replace(chr(769), '').lower() for s in dfen[dfen['Gender'] == 'female']['Surname'])


def _translate(word):
    eng = True
    for ch in word:
        if not ('a' <= ch <= 'z' or 'A' <= ch <= 'Z'):
            eng = False
            break

    if eng:
        tr.set_text(word)
        while True:
            try:
                word = tr.translate()
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError):
                print("Exception! Translating timed out. Retrying...")
                time.sleep(1)
                continue
            break
    return word


def has_correct_gender(gender, name, surname):
    name = _translate(name)  # Translate if name/surname are written in English
    surname = _translate(surname)

    surname = surname.replace('ё', 'е').replace(chr(769), '').lower()  # Change ё on e and remove stresses
    name = name.replace('ё', 'е').replace(chr(769), '').lower()

    woman_score = 0

    if name in ru_female_names or name in en_female_names:
        woman_score += 1

    if surname[-2:] in ru_female_surnames_endings or surname[-2:] in en_female_surnames_endings:
        woman_score += 1

    if name in ru_male_names or name in en_male_names:
        woman_score -= 1

    if surname[-2:] in ru_male_surnames_endings or surname[-2:] in en_male_surnames_endings:
        woman_score -= 1

    # for vk api: 1 - female, 2 - male,
    return woman_score >= 0 and gender == 1 or woman_score <= 0 and gender == 2
