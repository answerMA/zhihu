#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 8/13/2018 7:04 PM
# @Author  : Ruiming_Ma
# @Site    : 
# @File    : thread_test.py
# @Software: PyCharm Community Edition

import threading
import time

list_ip=['1.1.1.1','2.2.2.2','3.3.3.3'] #一组ip列表

def printIP(ip):
    time.sleep(2)
    print(ip)

#每180s获取当前线程名，并跟初始线程组比较，某一线程停止后自动运行
def checkThread(sleeptimes=5,initThreadsName=[]):
    for i in range(0,10080):#循环运行
        nowThreadsName=[]#用来保存当前线程名称
        now=threading.enumerate()#获取当前线程名
        for i in now:
            nowThreadsName.append(i.getName())#保存当前线程名称

        print(nowThreadsName)

        for ip in initThreadsName:
            if ip in nowThreadsName:
                pass #当前某线程名包含在初始化线程组中，可以认为线程仍在运行
            else:
                print('==='+ip,'stopped，now restart')
                t=threading.Thread(target=printIP,args=(ip,))#重启线程
                t.setName(ip)#重设name
                t.start()
        time.sleep(3)#隔一段时间重新运行，检测有没有线程down


if __name__ == '__main__':
    threads=[]
    initThreadsName=[]#保存初始化线程组名字
    for ip in list_ip:
        t=threading.Thread(target=printIP,args=(ip,))
        t.setName(ip)
        threads.append(t)

    for t in threads:
        t.start()

    init=threading.enumerate()#获取初始化的线程对象
    for i in init:
        initThreadsName.append(i.getName())#保存初始化线程组名字

    print(initThreadsName)

    check=threading.Thread(target=checkThread,args=(180,initThreadsName))#用来检测是否有线程down并重启down线程
    check.setName('Thread:check')
    check.start()
