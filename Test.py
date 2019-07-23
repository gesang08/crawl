#!/usr/bin/env python3
# encoding:utf-8


import threading
import time


def run():
    time.sleep(1)
    print('当前线程的名字是：%s' % threading.current_thread().name)
    # time.sleep(2)


if __name__ == '__main__':

    start_time = time.time()

    print('这是主线程：', threading.current_thread().name)
    thread_list = []
    for i in range(5):
        t = threading.Thread(target=run)
        thread_list.append(t)

    for t in thread_list:
        t.setDaemon(True)
        t.start()
    print(threading.enumerate())  # 以列表形式返回当前所有存活的 Thread 对象
    for t in thread_list:
        t.join()

    print('主线程结束！！！', threading.current_thread().name)
    print('一共用时：', time.time()-start_time)
