# -*- coding: utf-8 -*-

import os
import requests
from pymongo import MongoClient
from pyquery import PyQuery as pq
from multiprocessing import Pool
from config import *

client = MongoClient(MONGO_URI, connect=False)
db = client[MONGO_DB]


def get_one_page(url):
    try:
        response = requests.get(url)
        if response.status_code == requests.codes.ok:
            return response.text
        return None
    except Exception as e:
        print(e)


def parse_one_page(response):
    doc = pq(response)
    dd_list = doc('.board-wrapper > dd').items()
    result = dict()
    for dd in dd_list:
        result['name'] = dd('p.name > a').text()
        result['index'] = dd('i.board-index').text()
        result['image'] = dd('img.board-img').attr('data-src')
        result['actor'] = dd('p.star').text().replace('主演：', '')
        result['time'] = dd('p.releasetime').text().replace('上映时间：', '')
        result['score'] = dd('p.score').text().replace(' ', '')
        yield result


def save_to_mongodb(result):
    try:
        if db[MONGO_TABLE].update({'name': result['name']}, result, upsert=True):
            print('Successfully Saved!', result['name'])
    except Exception as e:
        print(e)


def download_image(url, index, name):
    print('Downloading...', url)
    try:
        response = requests.get(url)
        if response.status_code == requests.codes.ok:
            save_image(response.content, index, name)
        return None
    except Exception as e:
        print(e)


def save_image(content, index, name):
    file_path = '{0}/{1}-{2}.jpg'.format(os.getcwd(), index, name)
    if not os.path.exists(file_path):
        print(file_path)
        with open(file_path, 'wb') as f:
            f.write(content)
            f.close()


def main(offset):
    url = 'http://maoyan.com/board/4?offset=' + str(offset)
    html = get_one_page(url)
    items = parse_one_page(html)
    for item in items:
        save_to_mongodb(item)
        download_image(item['image'], item['index'], item['name'])


if __name__ == '__main__':
    pool = Pool()
    pool.map(main, [i * 10 for i in range(10)])
    pool.close()
    pool.join()
