#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 8/12/2018 12:35 PM
# @Author  : Ruiming_Ma
# @Site    : 
# @File    : MoGuProxy.py
# @Software: PyCharm Community Edition

import requests
import json

# 蘑菇代理的隧道订单
appKey = "cVJPVENucVNXbm5zUWd4MzpoczhIaXVPcHBxWnZuaEgy"

# 蘑菇隧道代理服务器地址
ip_port = 'transfer.mogumiao.com:9001'

# 准备去爬的 URL 链接
url = 'https://www.zhihu.com/api/v4/members/excited-vczh/followees?include=data%5B*%5D.answer_count%2Carticles_count%2Cgender%2Cfollower_count%2Cis_followed%2Cis_following%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset=0&limit=20'
#url = 'http://python-data.dr-chuck.net/comments_220996.json'
#url = 'https://www.zhihu.com/node/ExploreAnswerListV2?'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    "Proxy-Authorization": 'Basic '+ appKey,
}
proxy = {"http": "http://" + ip_port, "https": "https://" + ip_port}
r = requests.get(url, headers=headers, proxies=proxy, verify=False, allow_redirects=False)
print(r.status_code)
print(r.json()['data'])

if r.status_code == 302 or r.status_code == 301 :
    loc = r.headers['Location']
    url_f = url + loc
    print(loc)
    r = requests.get(url_f, headers=headers, proxies=proxy, verify=False, allow_redirects=False)
    print(r.status_code)
    print(r.text.encode('utf-8'))