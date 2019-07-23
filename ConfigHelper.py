#!/usr/bin/env python3
# encoding:utf-8
# name:ConfigHelper.py
"""
功能：用于操作.ini、.conf、.cfg配置文件
"""
__info__ = {'function1': 'WriteConfig', 'function2': 'ReadConfig', 'author': 'gs', 'time': '20190719'}

import configparser as ConfigParser
import os

FileName = 'Config.ini'
Section = 'DB'


def WriteConfig(file_name=FileName, section=Section, attr=None, value=None):
    """
    向配置文件里面写数据
    :param file_name: 配置文件名称
    :param section: 表名
    :param attr: 字段
    :param value: 字段的值
    :return:None(success to write the data) or False
    """
    config = ConfigParser.ConfigParser()
    if file_name not in config.read(file_name, encoding='utf-8-sig'):  # config.read()以列表的形式返回配置文件名称
        return False
    if section not in config.sections():  # config.sections()以列表的形式返回所有section名称
        return False
    if attr not in config.options(section=section):  # config.options(section)以列表的形式返回section下所有字段
        return False
    if os.path.exists('./' + file_name):
        try:
            config.set(section, attr, str(value))  # config.set(section, option, value=None)更新指定section，option的值
            config.write(open(file_name, "w+", encoding='utf-8-sig'))  # 写回到配置文件
        except:
            return False
    else:
        return False


def ReadConfig(file_name=FileName, section=Section, attr=None):
    """
    从配置文件里面读数据
    :param file_name:配置文件名称
    :param section: 表名
    :param attr: 字段
    :return:value(read the data) or False
    """
    config = ConfigParser.ConfigParser()
    if file_name not in config.read(file_name, encoding='utf-8-sig'):  # config.read()以列表的形式返回配置文件名称
        return False
    if section not in config.sections():  # config.sections()以列表的形式返回所有section名称
        return False
    if attr not in config.options(section=section):  # config.options(section)以列表的形式返回section下所有字段
        return False
    if os.path.exists('./' + file_name):
        if config.has_option(section=section, option=attr):
            value = config.get(section=section, option=attr)  # 读取字段attr的值
            return value
        else:
            return False
    else:
        return False


if __name__ == '__main__':
    print(__info__)
    print(WriteConfig(file_name="Config.ini", section="DB", attr="dbname", value="crawler"))
    print(ReadConfig(file_name="Config.ini", section="DB", attr='dbname'))
