import requests
from bs4 import BeautifulSoup
import pandas as pd
from joblib import Parallel, delayed
import os


def get_article_description(id: int) -> [tuple, None]:
    res = requests.get('https://pikabu.ru/story/_' + str(id), headers={'user-agent':'Googlebot/2.1 (+http://www.google.com/bot.html)'})
    soup = BeautifulSoup(res.content, "lxml")
    article = soup.find("article", {"data-story-id": id})
    if article is None:
        return None

    try:
        raiting = int(article.find("div", {'class': 'story__rating-count'}).text)
    except ValueError:
        raiting = None

    header = article.find("header", {'class': 'story__header'}).text.strip()

    text_content = ''
    for block in article.find_all("div", {'class': 'story-block story-block_type_text'}):
        text_content += block.text.strip() + '\n'

    for block in article.find_all("div", {'class': 'story__description'}):
        text_content += block.text.strip() + '\n'
    text_content = text_content.strip()

    tags = list()
    try:
        for tag in article.find("div", {'class': 'story__tags tags'}).find_all("a", {"class": "tags__tag"}):
            tags.append(tag.text)
    except AttributeError:
        pass

    extra_data = article.find("div", {'class': 'story__footer'})

    commented = int(extra_data.find("span", {'class': 'story__comments-link-count'}).text.strip())

    saved = int(extra_data.find("div", {'class': 'story__save'}).text.strip())

    shared = int(extra_data.find("div", {'class': 'story__share'}).text.strip())

    user = article.find("a", {'class': 'user__nick'}).text

    time = article.find("div", {'class': 'story__user user'}).find('time')['datetime']

    votes = soup.find("div", {'class': 'page-story__rating hint'})
    pluses_number = int(votes['data-pluses'])
    minus_number = int(votes['data-minuses'])

    return id, raiting, header, text_content, tags, commented, saved, shared, user, time, pluses_number, minus_number


class Worker:
    def __init__(self):
        self.data = pd.DataFrame(data={#'id': [],
                             'rating': [],
                             'header': [],
                             'text': [],
                             'tags': [],
                             'commented': [],
                             'saved': [],
                             'shared': [],
                             'user': [],
                             'time': [],
                             'pluses': [],
                             'minuses': []})

    def process(self, i: int):
        tmp = get_article_description(i)
        if tmp is not None:
            if tmp[3] != '':
                self.data.loc[tmp[0]] = list(tmp[1:])
            else:
                print(i, 'no text')


if __name__ == '__main__':
    mypath = os.path.dirname(os.path.abspath(__file__)) + '/data'
    onlyfiles = [f for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath, f))]
    print(onlyfiles)
    id = 0
    for name in onlyfiles:
        if name[:4] == 'data' and name[-4:] == '.csv':
            id = max(id, int(name[:-4].split('_')[1]))
    print(id)
    while True:
        w = Worker()
        with Parallel(n_jobs=12, require='sharedmem') as parallel:
            parallel(delayed(w.process)(i) for i in range(id, id+1000))

        print(w.data)
        w.data.to_csv(f'data/data{id}_{id+1000}.csv')
        id += 1000
        break
