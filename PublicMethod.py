#!/usr/bin/env python3
# encoding:utf-8
# name:PublicMethod.py

import re
import sys
import time
from ConfigHelper import WriteConfig, ReadConfig


def RemoveSpecialCharacter(string):
    """
    去除特殊符号 只留汉字 数字 字母
    :param string: 待处理字符串
    :return: 处理后的字符串
    """
    cop = re.compile(r"[^\u4e00-\u9fa5^.^a-z^A-Z^0-9]")
    rev = cop.sub('', str(string))  # 将string1中匹配到的字符替换成空字符
    return rev


def InitDict():
    _dir = {'url': '', 'title': '', 'authors': '', 'unit': '', 'publication': '', 'keywords': '', 'abstract': '',
            'year': '', 'volume': '', 'issue': '', 'pagecode': '', 'doi': '', 'sponser': '', 'type': ''}
    return _dir

def CreatUrlBuffTable(db,TableName):
    CreatDBTableSql = '\
            CREATE TABLE IF NOT EXISTS `%s` (\
            `Index` int(11) unsigned NOT NULL AUTO_INCREMENT,\
            `Url` VARCHAR(255) DEFAULT NULL,\
            `State` INT(11) NULL DEFAULT \'0\'  COMMENT \'-5 日期不对 -10 出现错误 0 初始 10 处理中 20 处理结束\',\
            `Datetime` DATETIME NULL DEFAULT CURRENT_TIMESTAMP,\
            `Source` VARCHAR(200) NULL DEFAULT NULL,\
            UNIQUE INDEX `Url` (`Url`),\
            PRIMARY KEY (`Index`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8; ' % TableName
    dict_result = db.upda_sql(CreatDBTableSql)
    if not dict_result:
        print("创建%s表出现问题" % TableName)

def CreatResultDBTable(db,TableName):
    '''
    创建结构数据库表单，如果不存在就创建
    :return:
    '''
    CreatDBTableSql = '\
        CREATE TABLE IF NOT EXISTS `%s` (\
          `id` int(11) unsigned NOT NULL AUTO_INCREMENT,\
          `url` text DEFAULT NULL, \
          `title` varchar(200) DEFAULT NULL,\
          `authors` varchar(200) DEFAULT NULL,\
          `unit` text  DEFAULT NULL,\
          `publication` varchar(200) DEFAULT NULL,\
          `keywords` varchar(200) DEFAULT NULL,\
          `abstract` text DEFAULT NULL,\
          `year` varchar(200) DEFAULT NULL,\
          `volume` varchar(200) DEFAULT NULL,\
          `issue` varchar(200) DEFAULT NULL,\
          `pagecode` varchar(200) DEFAULT NULL,\
          `doi` varchar(200) DEFAULT NULL,\
          `sponser` text DEFAULT NULL,\
          `type` varchar(200) DEFAULT NULL,\
          `source` varchar(200) DEFAULT NULL,\
          PRIMARY KEY (`id`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8; ' % TableName
    dict_result = db.upda_sql(CreatDBTableSql)
    if not dict_result:
        print("创建出现问题")

if __name__ == '__main__':
    r = RemoveSpecialCharacter("我是##     _gg1h11111")
    print(r)
