#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 9/19/2017 3:49 PM
# @Author  : Ruiming_Ma
# @Site    : 
# @File    : zhihu_text.py
# @Software: PyCharm Community Edition

import requests
from pyquery import PyQuery as pq
import time
from pymongo import MongoClient

client = MongoClient()
db = client['zhihu1']
collection = db['zhihu1']


def getHTML(page):
    base_url = 'https://www.zhihu.com/node/ExploreAnswerListV2?'

    data = {'params': ['{"offset":' + str(page) + ', "type":"day"}']}

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2224.3 Safari/537.36',
        'Cookie': 'udid="ABAAlBTalwmPTjmFmNmJaMUno9RDWILM8mU=|1457589429"; d_c0="AHBA8g_voQmPTnOc3upez34tbpyC_qi5EnQ=|1460943889"; _zap=9595c7ee-3165-4cdb-84c3-cd05781649b5; q_c1=6b849d0d29e043928f0e8fdd1434b8ae|1503995706000|1465275492000; r_cap_id="NjZiYTBjYjNiNjlkNDlmMTg0YjAxNTE0NzkxMWQyZTg=|1505117306|b4c5eacb851cbb5a8f13020ca9899c8e9cdef811"; cap_id="NTI1MmEwZjMyMWM3NGViMzhkODQ1OGJkZmQyN2YxOWI=|1505117306|f0a1bdfd2f57f4e08b5b55b888254b5f35128720"; z_c0=Mi4xWFNVRUFBQUFBQUFBY0VEeUQtLWhDUmNBQUFCaEFsVk5rdEhkV1FENW9lUHZqWGZzQVBSMTA0NjMxRVNXRTAzZ01n|1505117330|025c9c27c4a3114c18ee2524db73ea2d18b68a37; q_c1=6b849d0d29e043928f0e8fdd1434b8ae|1505121866000|1465275492000; _xsrf=32f8515e6cdd52dad40dcf5b3bcf6422; __utma=51854390.1287005120.1478815128.1505833503.1505891241.5; __utmz=51854390.1505117314.2.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmv=51854390.100-1|2=registration_date=20120424=1^3=entry_date=20120424=1',
        'Host': 'www.zhihu.com',
        'Referer': 'https://www.zhihu.com/explore',
        'X-Requested-With': 'XMLHttpRequest'
    }
    try:
        html = requests.get(url=base_url, headers=headers, params=data)
        # print(html.url)
        if (html.status_code == 200):
            return html.text
    except requests.ConnectionError as e:
        print('Error', e.args)


def save_to_file(data):
    with open(r'C:\Users\ruim.NNITCORP\Desktop\Middleware\Python\zhihu\explore.txt', 'a', encoding='utf-8') as f:
        f.write('\n'.join([data['question'], data['author'], data['article']]))
        f.write('\n' + '=' * 80 + '\n')


def parseZhiHu(html):
    doc = pq(html)
    items = doc('.explore-feed.feed-item').items()
    for item in items:
        zhihu = {}
        question = item.find('h2').text()
        author = item.find('.author-link-line').text()
        # bio = item.find('.bio').attr('title')
        # print(bio)
        article = pq(item.find('.content').html()).text()
        zhihu['question'] = question
        zhihu['author'] = author
        zhihu['article'] = article
        # save_to_file(zhihu)
        dbInsert(zhihu)


def dbInsert(data):
    if collection.insert(data):
        print('saved to Mongo')


def main():
    for i in range(1, 10):
        parseZhiHu(getHTML(5 * i))
        time.sleep(1.5)

    print('Extracted 10 Pages of Zhi Hu complete')


main()
