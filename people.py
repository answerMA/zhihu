#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 8/10/2018 2:29 PM
# @Author  : Ruiming_Ma
# @Site    : 
# @File    : people.py
# @Software: PyCharm Community Edition

import requests
import pandas as pd
import time
from pymongo import MongoClient
import redis
import threading
import random
from logbook import FileHandler, Logger, TimedRotatingFileHandler
import logbook
import os

# 蘑菇代理的隧道订单
appKey = "cVJPVENucVNXbm5zUWd4MzpoczhIaXVPcHBxWnZuaEgy"

# 蘑菇隧道代理服务器地址
ip_port = 'transfer.mogumiao.com:9001'

'''
proxies = {
    'http': 'http://121.43.170.207:3128',
    #'https': 'http://101.132.122.230:3128',
}


my_headers = [
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
    'Opera/9.25 (Windows NT 5.1; U; en)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
    'Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)',
    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',
    'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9',
    "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7",
    "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0 ",
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
]
'''
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    "Proxy-Authorization": 'Basic ' + appKey,
    'Authorization': 'oauth c3cef7c66a1843f8b3a9e6a1e3160e20',
    '_xsrf':'xsrf',
    'referer': 'https://www.zhihu.com/question/29926060',

}

'''设置logbook'''
def log_type(record,handler):
    log = "[{date}] [{level}] [{filename}] [{func_name}] [{lineno}] {msg}".format(
        date = record.time,                              # 日志时间
        level = record.level_name,                       # 日志等级
        filename = os.path.split(record.filename)[-1],   # 文件名
        func_name = record.func_name,                    # 函数名
        lineno = record.lineno,                          # 行号
        msg = record.message                             # 日志内容
    )
    return log
LOG_DIR = os.path.join("Log")
logbook.set_datetime_format("local")
#handler = FileHandler('app.log')
handler = TimedRotatingFileHandler(
    os.path.join(LOG_DIR, '%s.log' % 'log'),date_format='%Y-%m-%d', bubble=True, encoding='utf-8')
handler.formatter = log_type
handler.push_application()
log = Logger('people')

''''''
user_data = []


class redisDB():
    '''
    waiting集合中是待读取的个人user-token
    pools列表是免费IP代理池
    topics列表中是待读取个人主题的user-token
    topic_success集合是成功读取个人主题的user-token
    answers_success 列表中是成功读取个人回答问题个数的url-token
    '''

    def __init__(self):
        self.rdb = redis.StrictRedis(host='localhost', port=6379, db=0)

    def getRedis(self):
        return self.rdb


class mongoDB():
    def __init__(self):
        self.client = MongoClient(maxPoolSize=50, waitQueueMultiple=10, waitQueueTimeoutMS=100)
        self.db = self.client['zhihu1']

    def getFollowers(self):
        collection = self.db['zhihu_followers']
        return collection

    def getTopic(self):
        collection = self.db['zhihu_topics']
        return collection

    def getAnswers(self):
        collection = self.db['zhihu_answers']
        return collection


class getProxy():
    def __init__(self):
        self.rdb = redisDB().getRedis()
        self.pool = {'http': ''}

    def getIPFromRedis(self):
        while True:
            if self.rdb.llen('pools') != 0:
                break
            time.sleep(2)
        ip = self.rdb.lpop('pools')
        ip = str(ip, encoding='utf-8')
        return ip

    def getIP(self):
        ip = self.getIPFromRedis()
        ip = 'http://' + ip
        proxies = {'http': ip}
        return proxies


class getUserFollower(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        # self.proxy = getProxy()
        self.collection = mongoDB().getFollowers()
        self.rdb = redisDB().getRedis()

    def checkRedis(self):
        """从redis的waiting中获取到url-token"""
        items = self.rdb.smembers('waiting')
        if len(items) == 0:
            self.rdb.sadd('waiting', 'excited-vczh')
            user_name = self.rdb.spop('waiting')
        else:
            user_name = self.rdb.spop('waiting')
        user_name = str(user_name, encoding='utf-8')  # 将字节类型的数据转化成字符串
        return user_name

    def insert2Topics(self, user_name):
        try:
            self.rdb.lpush('topics', user_name)
            #print(user_name + '插入 redis_topics 成功')
            log.info(user_name + '插入 redis_topics 成功')
        except Exception as e:
            log.warn('get-user-followers-----insert2Topics-----' + str(e))

    def insert2Redis(self, response):
        for i in response:
            time.sleep(0.5)
            try:
                result = self.rdb.sadd('waiting', i['url_token'])
                if result == 1:
                    #print(i['url_token'] + '插入 redis_waiting 成功')
                    log.info(i['url_token'] + '插入 redis_waiting 成功')
                else:
                    #print(i['url_token'] + '插入redis_waiting 失败')
                    log.info(i['url_token'] + '插入redis_waiting 失败')
            except Exception as e:
                log.warn('get_user_follower-----insert2Redis-----' + str(e))
                continue

    def get_user_follwers(self):
        while True:  # 翻页
            user_name = self.checkRedis()
            self.insert2Topics(user_name)
            while True:
                if self.get_user_page(user_name):
                    '''将成功读取其粉丝页面的用户加入到topics列表中，等待读取其关注主题'''
                    break
                else:
                    break
            if self.userAU == 1:
                break
        log.error('get_user_follwers 成功退出')

    def run(self):
        # threadLock.acquire()
        self.get_user_follwers()
        # threadLock.release()

    def get_user_page(self, user_name):
        self.page = 0
        self.userAU = 0 #判断是否进入401状态
        while True:  # 翻页
            if self.page == 400:
                log.info('{} 拥有超过400页的粉丝，放弃抓起更多', user_name)
                break
            proxies = {"http": "http://" + ip_port, "https": "https://" + ip_port, }
            # headers['User-Agent'] = random.choice(my_headers)
            url = 'https://www.zhihu.com/api/v4/members/{0}/followers?include=data%5B*%5D.answer_count%2Carticles_count%2Cgender%2Cfollower_count%2Cis_followed%2Cis_following%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset={1}&limit=20'.format(
                user_name, self.page * 20)
            try:
                status = requests.get(url, headers=headers, proxies=proxies, verify=False, allow_redirects=False, timeout=2)
                if status.status_code == 401:
                    self.userAU = 1
                    log.error('粉丝列表进入401状态')
                    break
                elif status.status_code != 200 and status.status_code != 401:
                    log.warn('网页返回码是：' + str(status.status_code) + '----------粉丝列表')
                    log.warn(status.content)
                    time.sleep(2)
                    continue
                '''网页返回码为200，则继续后续操作'''
                raw = status.json()
                response = raw['data']
                status = raw['paging']
            except Exception as e:
                #print(e)
                log.warn('get_user_page---' + str(e))
                continue

            print('正在爬取第{0}页----------{1}---粉丝列表'.format(str(self.page + 1), user_name))

            '''判断是否为最后一页'''
            if status['is_end'] is True:
                if len(response) != 0:
                    '''先将最后一页的数据插入mongoDB中'''
                    self.collection.insert_many(response)
                    '''将数据中的每一个url-token插入进redis的waiting集合中'''
                    self.insert2Redis(response)
                    time.sleep(2)
                break
            else:
                '''将粉丝全部信息数据插入进mongoDB中的zhihu_followers集合中'''
                self.collection.insert_many(response)
                ''' 将 url-token 插入进redis的waiting表中'''
                self.insert2Redis(response)
                # user_data.extend(response) #把response数据添加进user_data，用于写CSV文件用
                time.sleep(3)  # 设置爬取网页的时间间隔为3秒
                self.page += 1
        '''根据userAU标志位来判断是否出现401页面'''
        if self.userAU == 1:
            return False
        else:
            return True


class getUserTopics(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.collection = mongoDB().getTopic()
        self.rdb = redisDB().getRedis()

    def getUserfromRedis(self):
        while True:
            if self.rdb.llen('topics') != 0:
                break
            else:
                time.sleep(1)
                log.info('getUserTopics-----getUserfromRedis-----topics列表里面为空')
        user_name = self.rdb.rpop('topics')
        user_name = str(user_name, encoding='utf-8')  # 将字节类型的数据转化成字符串
        return user_name

    def insert2Redis(self, username):
        try:
            result = self.rdb.sadd('topic_success', username)
            if result  == 1:
                #print(username + '插入 redis_topic_success 成功')
                log.info(username + '插入 redis_topic_success 成功')
            else:
                #print(username + '插入 redis_topic_success 失败')
                log.info(username + '插入 redis_topic_success 失败')
        except Exception as e:
            log.warn('get_user_topics-----insert2Redis-----' + str(e))

    def insert2MongoDB(self, response, user_name):
        for i in response:
            new_doc = []
            topic = i['topic']
            topic['url_token'] = user_name
            new_doc.append(topic)
            try:
                self.collection.insert_many(new_doc)
            except Exception as e:
                print(e)
                continue

    def get_topics_page(self, user_name):
        self.page = 0
        self.userAU = 0 #判断是否进入401状态
        while True:  # 翻页
            proxies = {"http": "http://" + ip_port, "https": "https://" + ip_port, }
            url = 'https://www.zhihu.com/api/v4/members/{0}/following-topic-contributions?include=data%5B*%5D.topic.introduction&offset={1}&limit=20'.format(
                user_name, self.page * 20)

            try:
                status = requests.get(url, headers=headers, proxies=proxies, verify=False, allow_redirects=False, timeout=2)
                if status.status_code == 401:
                    self.userAU = 1
                    log.error('主题列表进入401状态')
                    break
                elif status.status_code != 200 and status.status_code != 401:
                    log.warn('网页返回码是：' + str(status.status_code) + '----------个人主题')
                    log.warn(status.content)
                    time.sleep(2)
                    continue
                '''网页返回码为200，则继续后续操作'''
                raw = status.json()
                response = raw['data']
                status = raw['paging']
            except Exception as e:
                #print(e)
                log.warn('get_topic_page---' + str(e))
                continue

            print('正在爬取第{0}页----------{1}---个人主题'.format(str(self.page + 1), user_name))

            '''判断是否为最后一页'''
            if status['is_end'] is True:
                if len(response) != 0:
                    '''先将最后一页的数据插入mongoDB中'''
                    self.insert2MongoDB(response, user_name)
                    time.sleep(2)
                break
            else:
                '''将粉丝全部信息数据插入进mongoDB中的zhihu_followers集合中'''
                self.insert2MongoDB(response, user_name)
                time.sleep(3)  # 设置爬取网页的时间间隔为3秒
                self.page += 1

        '''根据userAU标志位来判断是否出现401页面'''
        if self.userAU == 1:
            return False
        else:
            return True

    def get_user_topics(self):
        while True:  # 翻页
            user_name = self.getUserfromRedis()
            while True:
                if self.get_topics_page(user_name):
                    self.insert2Redis(user_name)
                    break
                else:
                    break
            if self.userAU == 1:
                break
        log.error('get_user_topics 成功退出')

    def old_get_topics(self, response):
        '''废弃不用'''
        for i in response:
            dic = i['topic']
            print(dic['name'])

        # user_data.extend(response) #把response数据添加进user_data，用于写CSV文件用
        print('正在爬取第%s页' % str(self.page + 1))
        time.sleep(1)  # 设置爬取网页的时间间隔为1秒
        self.page += 1

    def run(self):
        self.get_user_topics()


class getUserAnswer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.collection = mongoDB().getAnswers()
        self.rdb = redisDB().getRedis()

    def getFromRedis(self):
        """从redis的topic_success中获取到url-token"""
        items = self.rdb.smembers('topic_success')
        while True:
            if len(items) == 0:
                time.sleep(5)
                log.info('getUserAnswer-----checkRedis----{} 为空'.format('topic_success'))
            else:
                break
        user_name = self.rdb.spop('topic_success')
        user_name = str(user_name, encoding='utf-8')  # 将字节类型的数据转化成字符串
        return user_name

    def insert2Answers(self, user_name):
        try:
            self.rdb.lpush('answers_success', user_name)
            log.info(user_name + '插入 redis_answers 成功')
        except Exception as e:
            log.warn('get-user-answers-----insert2Answers-----' + str(e))

    def insert2MongoDB(self, response, user_name):
        for i in response:
            new_doc = []
            answer = i['question']
            answer['content'] = i['excerpt']
            answer['answer_url'] = i['url']
            answer['voteup_count'] = i['voteup_count']
            answer['url_token'] = user_name
            new_doc.append(answer)
            try:
                self.collection.insert_many(new_doc)
            except Exception as e:
                print(e)
                continue

    def get_user_answers(self):
        while True:  # 翻页
            user_name = self.getFromRedis()
            self.insert2Answers(user_name)
            while True:
                if self.get_answer_page(user_name):
                    '''将成功读取用户加入到answers列表中，等待读取其回答过问题'''
                    break
                else:
                    break
            if self.userAU == 1:
                break
        log.error('get_user_answers 成功退出')

    def run(self):
        # threadLock.acquire()
        self.get_user_answers()
        # threadLock.release()

    def get_answer_page(self, user_name):
        self.page = 0
        self.userAU = 0 #判断是否进入401状态
        while True:  # 翻页
            if self.page == 900:
                log.info('{} 回答超过900页的答案，放弃抓起更多', user_name)
                break
            proxies = {"http": "http://" + ip_port, "https": "https://" + ip_port, }
            url = 'https://www.zhihu.com/api/v4/members/{0}/answers?include=data%5B*%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2Creview_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cvoting%2Cis_author%2Cis_thanked%2Cis_nothelp%3Bdata%5B*%5D.author.badge%5B%3F(type%3Dbest_answerer)%5D.topics%3Bdci_info&offset={1}&limit=20&sort_by=created'.format(
                user_name, self.page * 20)
            try:
                status = requests.get(url, headers=headers, proxies=proxies, verify=False, allow_redirects=False, timeout=2)
                if status.status_code == 401:
                    self.userAU = 1
                    log.error('用户回答列表进入401状态')
                    break
                elif status.status_code != 200 and status.status_code != 401 and status.status_code != 410:
                    log.warn('网页返回码是：' + str(status.status_code) + '----------{} 用户回答列表'.format(user_name))
                    log.warn(status.content)
                    time.sleep(2)
                    continue
                elif status.status_code == 410:
                    log.warn('{} 用户回答列表进入401状态, 开始抓取下一个用户的回答列表'.format(user_name))
                    break
                '''网页返回码为200，则继续后续操作'''
                raw = status.json()
                response = raw['data']
                status = raw['paging']
            except Exception as e:
                #print(e)
                log.warn('get_answer_page---' + str(e))
                continue

            print('正在爬取第{0}页----------{1}---用户回答列表'.format(str(self.page + 1), user_name))

            '''判断是否为最后一页'''
            if status['is_end'] is True:
                if len(response) != 0:
                    '''先将最后一页的数据插入mongoDB的answers中'''
                    self.insert2MongoDB(response, user_name)
                    time.sleep(2)
                break
            else:
                '''将答案全部信息数据插入进mongoDB中的zhihu_answers列表中'''
                self.insert2MongoDB(response, user_name)
                time.sleep(3)  # 设置爬取网页的时间间隔为3秒
                self.page += 1
        '''根据userAU标志位来判断是否出现401页面'''
        if self.userAU == 1:
            return False
        else:
            return True


def checkThreads():
    log.info('checkThreads is running')
    checklist = ['userFollowers', 'userTopics', 'userAnswers',]
    while True:
        nowThreads = []
        now = threading.enumerate()
        for x in now:
            nowThreads.append(x.getName())
        log.debug(nowThreads)

        for j in checklist:
            if j not in nowThreads:
                if j == 'userFollowers':
                    log.warn('{} 线程已停止，现在重启'.format(j))
                    userFollowersThread = getUserFollower()
                    userFollowersThread.setName('userFollowers')
                    userFollowersThread.start()
                elif j == 'userTopics':
                    log.warn('{} 线程已停止，现在重启'.format(j))
                    #userTopicsThread = threading.Thread(target=getUserTopics)
                    userTopicsThread = getUserTopics()
                    userTopicsThread.setName('userTopics')
                    userTopicsThread.start()
                elif j == 'userAnswers':
                    log.warn('{} 线程已停止，现在重启'.format(j))
                    userAnswersThread = getUserAnswer()
                    userAnswersThread.setName('userAnswers')
                    userAnswersThread.start()

            else:
                log.info('{} 线程存在'.format(j))
        log.info('checkThead 进入30秒睡眠')
        time.sleep(5)
        log.info('checkThread 退出睡眠')



if __name__ == '__main__':
    threads = []
    initThreads = []
    threadLock = threading.Lock()

    userFollowersThread = getUserFollower()
    userTopicsThread = getUserTopics()
    userAnswersThread = getUserAnswer()

    userFollowersThread.setName('userFollowers')
    userTopicsThread.setName('userTopics')
    userAnswersThread.setName('userAnswers')

    userFollowersThread.start()
    userTopicsThread.start()
    userAnswersThread.start()

    threads.append(userFollowersThread)
    threads.append(userTopicsThread)
    threads.append(userAnswersThread)

    check = threading.Thread(target=checkThreads,args=())#用来检测是否有线程down并重启down线程
    check.setName('Thread:check')
    check.start()

    '''
    init = threading.enumerate()
    for i in init:
        initThreads.append(i.getName())
    log.info(initThreads)
    '''


    '''
    df = pd.DataFrame.from_dict(user_data)#以字典保存数据
    df.to_csv('zhihu.csv',encoding='utf_8_sig')#保存到用户名为zhihu的csv文件中，encoding='utf_8_sig'参数是为了解决中文乱码的问题
    print(df)
    '''
