#!/usr/bin/env python3
# encoding:utf-8
# name:ThreadHelper.py

import threading
import time


class LoopTimer(threading.Thread):
    """
    通过线程的方式，在每隔interval的时间间隔里，执行一个函数target（方法）处理器的操作
    """
    def __init__(self, interval, target, args=(), kwargs={}):
        """
        继承threading.Thread类，重写构造函数
        :param interval: 时间间隔
        :param target: 函数或方法处理器的名称
        :param args: 以元组形式接收函数或方法的参数
        :param kwargs: 以字典的形式接收函数或方法的参数
        """
        threading.Thread.__init__(self)
        self.interval = interval
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.finished = threading.Event()

    def run(self):
        """
        重写threading.Thread下的run()方法，该方法在线程调用start()方法的时候执行
        :return:
        """
        while True:
            if not self.finished.is_set():
                # start = time.time()
                self.finished.wait(self.interval)  # 比interval的时间稍微大一点点，比如interval=1，等待时间可能为1.0008628368377686，每次等待时间都不一致
                # end = time.time()
                # print("等待时间：%s" % (end - start))
                self.target(*self.args, **self.kwargs)
            else:
                break


def target_test(name, age):
    print(name + ':' + age)


def target_test1(name, age):
    print(name + ':' + age[0] + 'or' + age[1])


if __name__ == '__main__':
    print('这是主线程：', threading.current_thread().name)
    # t = LoopTimer(interval=1, target=target_test, args=('gs', '24',))  # 元组参数测试
    # t.start()
    # print(t.getName())

    # t1 = LoopTimer(interval=1, target=target_test1, kwargs={'name': 'gs', 'age': '23'})  # 字典参数测试
    t1 = LoopTimer(interval=1, target=target_test1, kwargs={'name': 'gs', 'age': ['23', '24']})  # 传递字典参数时的key必须是target的形参名称，否则会报错
    t1.start()
    print(t1.getName())

