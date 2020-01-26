import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import argparse


class DataElem:

    def __init__(self, id: int):
        self.raiting = None
        self.text_content = ''
        self.tags = list()
        self.commented = None
        self.saved = None
        self.shared = None
        self.user = None
        self.time = None
        self.images = list()
        self.videos = list()
        self.error_mes = ''
        self.header = ''
        self.pluses_number = None
        self.minus_number = None
        self.id = id

    @property
    def as_tuple(self):
        return self.id, self.raiting, self.header, self.text_content, \
               self.images, self.videos, self.tags, self.commented, \
               self.saved, self.shared, self.user, self.time, \
               self.pluses_number, self.minus_number, self.error_mes

    @property
    def as_list(self):
        return list(self.as_tuple)


def get_article_description(id: int, error_list=None) -> DataElem:
    need_show_errors = False
    if error_list is None:
        error_list = list()
        need_show_errors = True

    res = requests.get('https://pikabu.ru/story/_' + str(id), headers={'user-agent':'Googlebot/2.1 (+http://www.google.com/bot.html)'})

    data = DataElem(id)

    if res.status_code != 200:
        data.error_mes += f"return {res.status_code} status code"
        error_list.append(f"{id} return {res.status_code} status code")
        return data

    soup = BeautifulSoup(res.content, "lxml")

    article = soup.find("article", {"data-story-id": id})
    if article is None:
        if soup.find('div', {'class': 'stories-feed'}):
            data.error_mes += "redirected to main page"
            error_list.append(f"{id} redirected to main page")
        else:
            data.error_mes += "unknown error"
            error_list.append(f"{id} article not found")
        return data

    left_elem = article.find("div", {"class": "story__left"})

    for elem in left_elem.find_all("div"):
        if 'class' not in elem.attrs or not elem['class']:
            continue
        has_type = elem['class'][0]
        if has_type in ["story__left", "story__scroll", "story__rating-block",
                        "story__rating-up", "story__rating-down", "collapse-button"]:
            continue
        elif has_type == "story__rating-count":
            try:
                data.raiting = int(article.find("div", {'class': 'story__rating-count'}).text)
            except ValueError:
                error_list.append(f"{id} can't decode rating")
        else:
            error_list.append(f"{id} unknown block")

    main_elem = article.find("div", {"class": "story__main"})
    for elem in main_elem.find_all("div"):
        if 'class' not in elem.attrs or not elem['class']:
            continue
        has_type = elem['class'][0]
        # story-block story-block_type_text
        if has_type in ['story__footer', 'story__views', 'story__user',
                        'user__info', 'avatar']:
            continue
        elif has_type in ['story__content', 'story__content-inner', 'story-block',
                          'player__stretch', 'player__logo', 'player__play',
                          'player__preview', 'player__state',
                          'story__additional-tools']:
            if has_type == 'story-block' and len(elem['class']) == 2 and \
                elem['class'][1] == 'story-block_type_text':
                data.text_content += elem.text.strip() + '\n'
            elif elem.text.strip() != '':
                error_list.append(f"{id} incorrect empty block {has_type}")
        elif has_type == 'story-image':
            img = elem.find('img')
            if 'data-src' in img.attrs:
                data.images.append(img['data-src'])
            else:
                error_list.append(f"{id} no link in photo")
        elif has_type == 'player':
            if 'data-source' in elem.attrs:
                data.videos.append(elem['data-source'])
            else:
                print(id, 'video without url')
        elif has_type in ["story__description"]:
            data.text_content += elem.text.strip() + '\n'
        elif has_type == "story__tags":
            for tag in elem.find_all("a", {"class": "tags__tag"}):
                data.tags.append(tag.text)
        elif has_type == 'story__tools':
            field = elem.find('a')
            try:
                data.commented = int(field.text.strip())
            except ValueError:
                error_list.append(f"{id} can't decode comment number")
        elif has_type == 'story__save':
            try:
                data.saved = int(elem.text.strip())
            except ValueError:
                error_list.append(f"{id} can't decode save number")
        elif has_type == 'story__share':
            try:
                data.shared = int(elem.text.strip())
            except ValueError:
                error_list.append(f"{id} can't decode share number")
        elif has_type == 'user__info-item':
            func_elem = elem.find('a', {'class': 'user__nick'})
            if func_elem:
                data.user = func_elem.text.strip()
                continue
            func_elem = elem.find('time', {'class': 'caption story__datetime hint'})
            if func_elem and 'datetime' in func_elem.attrs:
                data.time = func_elem['datetime']
        else:
            error_list.append(f"{id} unknown block {has_type}")

    data.header = main_elem.find("header", {'class': 'story__header'}).text.strip()

    data.text_content = data.text_content.strip()

    votes = soup.find("div", {'class': 'page-story__rating hint'})
    if votes is None:
        data.pluses_number = 0
        data.minus_number = 0
        data.raiting = 0
    else:
        try:
            data.pluses_number = int(votes['data-pluses'])
        except ValueError:
            error_list.append(f"{id} can't decode pluses number")
        try:
            data.minus_number = int(votes['data-minuses'])
        except ValueError:
            error_list.append(f"{id} can't decode minuses number")

    if need_show_errors:
        for mes in error_list:
            print(mes)

    return data


def get_article_range(start, end=None) -> (pd.DataFrame, list):
    if end is None:
        end = start[1]
        start = start[0]
    errors = list()
    res = pd.DataFrame(data={#'id': [],
                             'rating': [],
                             'header': [],
                             'text': [],
                             'imges': [],
                             'videos': [],
                             'tags': [],
                             'commented': [],
                             'saved': [],
                             'shared': [],
                             'user': [],
                             'time': [],
                             'pluses': [],
                             'minuses': [],
                             'error': []})
    for i in range(start, end):
        # print(i)
        tmp = get_article_description(i, error_list=errors).as_list
        if tmp:
            res.loc[tmp[0]] = tmp[1:]

    return res, errors


def may_be_main(func):
    data_path = os.path.dirname(os.path.abspath(__file__)) + '/data2'
    error_path = os.path.dirname(os.path.abspath(__file__)) + '/log_mes'

    parser = argparse.ArgumentParser(description='Crowle some posts from pikabu')
    parser.add_argument('start', type=int, nargs=1, default=50,
                        help='start id for crowling')
    parser.add_argument('end', type=int, nargs=1, default=100,
                        help='end if for crowling')

    args = parser.parse_args()

    start_id = args.start[0]
    end_id = args.end[0]

    start = time.time()

    data, erros = func(start_id, end_id)

    end = time.time()

    data.to_csv(data_path+f'/data_{start_id}_{end_id}.csv')

    with open(error_path+f'/log_{start_id}_{end_id}.txt', 'w+') as f_out:
        for mes in erros:
            f_out.write(mes+'\n')

    print(end-start)


if __name__ == '__main__':
    # 82 - one threaad from 50 to 100
    # 86 - 8 processes (10 on process) from 50 to 100
    # 84 - 8 process (1 on process) from 50 to 100
    # 1541 - 8 process (100 on process) from 0 to 1000


    # linear - 1554.46
    # threading - 1526.89
    # multiprocessing - 1508.43
    # joblib - 1485.86
    # subporceses -

    may_be_main(get_article_range)
