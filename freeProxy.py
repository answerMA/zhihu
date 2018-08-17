#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 8/11/2018 10:51 PM
# @Author  : Ruiming_Ma
# @Site    : 
# @File    : freeProxy.py
# @Software: PyCharm Community Edition

import requests
from bs4 import BeautifulSoup
import redis
import time
import urllib.request
import socket
import threading


class redisDB():
    def __init__(self):
        self.rdb = redis.StrictRedis(host='localhost', port=6379, db=0)

    def getRedis(self):
        return self.rdb


class SpiderProxy(object):
    headers = {
        "Host": "www.xicidaili.com",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:47.0) Gecko/20100101 Firefox/47.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "http://www.xicidaili.com/wt/1",
    }

    def __init__(self, session_url):
        self.req = requests.session()
        self.req.get(session_url)

    def get_pagesource(self, url):
        html = self.req.get(url, headers=self.headers)
        return html.content

    def get_all_proxy(self, url, n):
        data = []
        for i in range(1, n):
            html = self.get_pagesource(url + str(i))
            soup = BeautifulSoup(html, "lxml")
            table = soup.find('table', id="ip_list")
            for row in table.findAll("tr"):
                cells = row.findAll("td")
                tmp = []
                for item in cells:
                    tmp.append(item.find(text=True))
                if len(tmp) == 0:
                    continue
                else:
                    ips = '{0}:{1}'.format(tmp[1], tmp[2])
                    data.append(ips)
            time.sleep(1)
        return data


class checkIP(threading.Thread):
    def __init__(self, data):
        threading.Thread.__init__(self)
        self.rdb = redisDB().getRedis()
        self.data = data
        socket.setdefaulttimeout(5)  #设置全局超时时间
        #self.url = "http://quote.stockstar.com/stock"  #打算抓取内容的网页
        self.url = 'http://www.baidu.com'

    def checkUse(self, data):
        for i in data:
            try:
                proxy_ip={'http': i}  #想验证的代理IP
                proxy_support = urllib.request.ProxyHandler(proxy_ip)
                opener = urllib.request.build_opener(proxy_support)
                opener.addheaders=[("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64)")]
                urllib.request.install_opener(opener)
                res = urllib.request.urlopen(self.url).read()
                if res:
                    self.rdb.lpush('pools', i)
                    print(i, 'is OK')

            except Exception as e:
                print(i, e)

    def run(self):
        threadLock.acquire()
        self.checkUse(data=self.data)
        threadLock.release()



if __name__ == '__main__':
    session_url = 'http://www.xicidaili.com/wt/1'
    url = 'http://www.xicidaili.com/wt/'
    threads = []
    threadLock = threading.Lock()


    p = SpiderProxy(session_url)
    proxy_ip = p.get_all_proxy(url, 10)
    for i in range(len(proxy_ip)):
        mythread = checkIP(proxy_ip)
        mythread.start()
        threads.append(mythread)

    for t in threads:
        t.join()

    '''
    for item in proxy_ip:
        if item:
            print(item)
    '''