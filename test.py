#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 8/12/2018 2:10 PM
# @Author  : Ruiming_Ma
# @Site    : 
# @File    : test.py
# @Software: PyCharm Community Edition

import requests
import pandas as pd
import time


# 蘑菇代理的隧道订单
appKey = "cVJPVENucVNXbm5zUWd4MzpoczhIaXVPcHBxWnZuaEgy"

# 蘑菇隧道代理服务器地址
ip_port = 'transfer.mogumiao.com:9001'


headers={
    #'authorization':'',#此处填写你自己的身份验证信息,
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Proxy-Authorization': 'Basic '+ appKey,

}
user_data = []
def get_user_data(page):
    for i in range(page):#翻页
        proxy = {"http": "http://" + ip_port, "https": "https://" + ip_port}
        url = 'https://www.zhihu.com/api/v4/members/excited-vczh/followees?include=data%5B*%5D.answer_count%2Carticles_count%2Cgender%2Cfollower_count%2Cis_followed%2Cis_following%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset={}&limit=20'.format(i*20)
        response = requests.get(url, headers=headers, proxies=proxy, verify=False, allow_redirects=False).json()['data']
        print(response)
        user_data.extend(response) #把response数据添加进user_data
        print('正在爬取第%s页' % str(i+1))
        time.sleep(1) #设置爬取网页的时间间隔为1秒

if __name__ == '__main__':
    get_user_data(10)
    df = pd.DataFrame.from_dict(user_data)#以字典保存数据
    df.to_csv('zhihu.csv',encoding='utf_8_sig')#保存到用户名为zhihu的csv文件中，encoding='utf_8_sig'参数是为了解决中文乱码的问题
    print(df)