#!/usr/bin/env python3
# encoding:utf-8
# name:MysqlHelper.py
"""
功能：1.通过普通方式和连接池方式操作MySQL数据库的类
      2.可以创建数据库和表
"""

__info__ = {'class1': 'Mysql', 'class2': 'MysqlPool', 'function1': 'CreateDB', 'function2': 'CreateTable',
            'author': 'gs', 'time': '20190716'}

import pymysql as MySQLdb
from DBUtils.PooledDB import PooledDB
from ConfigHelper import ReadConfig

DBNAME = ReadConfig(file_name="Config.ini", section="DB", attr="dbname")
DBHOST = ReadConfig(file_name="Config.ini", section="DB", attr="dbhost")
DBUSER = ReadConfig(file_name="Config.ini", section="DB", attr="dbuser")
DBPWD = ReadConfig(file_name="Config.ini", section="DB", attr="dbpwd")
DBCHARSET = ReadConfig(file_name="Config.ini", section="DB", attr="dbcharset")
DBPORT = ReadConfig(file_name="Config.ini", section="DB", attr="dbport")


class Mysql:
    """
    通过普通方式操作MySQL数据库的类
    """
    # 注，python的self等于其它语言的this
    def __init__(self, log=None, dbhost=None, dbname=None, user=None, password=None, port=None, charset=None):
        self._logger = log
        # 这里的None相当于其它语言的NULL
        self._dbhost = DBHOST if dbhost is None else dbhost
        self._dbname = DBNAME if dbname is None else dbname
        self._user = DBUSER if user is None else user
        self._password = DBPWD if password is None else password
        self._port = DBPORT if port is None else port
        self._charset = DBCHARSET if charset is None else charset
        self.conn = None
        self.get_conn_result = self.is_connection_db(get_data_method='tuple')
        if self.get_conn_result:  # 只有数据库连接上才获取数据游标
            self._cursor = self.conn.cursor()

    def is_connection_db(self, get_data_method='dict'):
        """
        数据库连接方法，默认获取的数据类型为字典，它以字段为key，以字段下的数据为value
        :param get_data_method:
        :return:
        """
        try:
            if get_data_method == 'dict':
                # 1.获取一行数据，返回的是dict类型，它以数据表中的字段为key，以字段下的数据为value
                # 2.获取多行数据，返回的是tuple类型，tuple序列内容为dict类型，它以数据表中的字段为key，以字段下的数据为value
                self.conn = MySQLdb.connect(host=self._dbhost,
                                            user=self._user,
                                            passwd=self._password,
                                            db=self._dbname,
                                            port=int(self._port),
                                            cursorclass=MySQLdb.cursors.DictCursor,
                                            charset=self._charset,
                                            )
            elif get_data_method == 'tuple':
                self.conn = MySQLdb.connect(host=self._dbhost,
                                            user=self._user,
                                            passwd=self._password,
                                            db=self._dbname,
                                            port=int(self._port),
                                            charset=self._charset,
                                            )
            else:
                self._logger.warn("please give correct method for getting data!")
                return False
        except Exception as e:
            self._logger.warn("query database exception,%s" % e)
            return False
        else:
            return True

    def get_more_row(self, sql):
        """
        从数据库中获取多行数据方法
        :param sql:
        :return:
        """
        record = ""
        if self.get_conn_result:
            try:
                self._cursor.execute(sql)
                record = self._cursor.fetchall()  # 获取多行数据函数
                if record == () or record is None:
                    record = False
                self._cursor.close()  # 关闭游标
                self.conn.close()  # 关闭数据库
            except Exception as e:
                record = False
                self._logger.warn("query database exception,sql= %s,%s" % (sql, e))
        return record

    def get_one_row(self, sql):
        """
        从数据库中获取一行数据方法
        :param sql:
        :return:
        """
        record = ""
        if self.get_conn_result:
            try:
                self._cursor.execute(sql)
                record = self._cursor.fetchone()  # 获取多行数据函数
                if record == () or record is None:
                    record = False
                self._cursor.close()  # 关闭游标
                self.conn.close()  # 关闭数据库
            except Exception as e:
                record = False
                self._logger.warn("query database exception,sql= %s,%s" % (sql, e))
        return record

    def modify_sql(self, sql):
        """
        更新、插入、删除数据库数据方法
        :param sql:
        :return:
        """
        flag = False
        if self.get_conn_result:
            try:
                self._cursor.execute(sql)
                self.conn.commit()
                self._cursor.close()
                self.conn.close()
                flag = True
            except Exception as e:
                flag = False
                self._logger.warn("query database exception,sql= %s,%s" % (sql, e))
        return flag


class MysqlPool:
    """
    通过数据库连接池的方式操作MySQL数据库
    """
    pool = None
    limit_count = 5  # 最低预启动数据库连接数量

    def __init__(self, log=None, dbname=None, dbhost=None):
        if dbname is None:
            self._dbname = DBNAME
        else:
            self._dbname = dbname
        if dbhost is None:
            self._dbhost = DBHOST
        else:
            self._dbhost = dbhost

        self._dbuser = DBUSER
        self._dbpassword = DBPWD
        self._dbcharset = DBCHARSET

        self._dbport = int(DBPORT)
        self._logger = log
        self.is_connect_first = False
        try:
            self.pool = PooledDB(MySQLdb, self.limit_count, host=self._dbhost, user=self._dbuser, passwd=self._dbpassword,
                                 db=self._dbname, port=self._dbport, charset=self._dbcharset, use_unicode=True)
        except:
            if self._logger is not None:
                self._logger.warn("无法连接数据库")
            else:
                print("无法连接数据库")
            self.is_connect_first = True

    def ping(self):
        print(self.pool)

    def do_sql(self, sql):
        """
        从数据库中获取多行数据
        :param sql:
        :return:
        """
        res = ''
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            res = cursor.fetchall()
            cursor.close()
            conn.close()
        except Exception as data:
            res = False
            print("query database exception,sql= %s,%s" % (sql, data))
        return res

    def do_sql_one(self, sql):
        """
        从数据库获取一行数据
        :param sql:
        :return:
        """
        res = ''
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            res = cursor.fetchone()
            cursor.close()
            conn.close()
        except Exception as data:
            res = False
            if data[0] == 2006:  # 掉线
                return res
            if self._logger is not None:
                self._logger.debug("query database exception,sql= %s,%s" % (sql, data))
                self._logger.warn("query database exception %s" % ( data))
            else:
                print("query database exception,sql= %s,%s" % (sql, data))
        return res

    def upda_sql(self, sql):
        """
        更新数据库语句，还可以用于创建库和表
        :param sql:
        :return:
        """
        res = True
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as data:
            res = False
            if data[0] == 2006:  # 掉线
                return res

            if self._logger is not None:
                self._logger.debug("query database exception,sql= %s,%s" % (sql, data))
                self._logger.warn("query database exception %s" % (data))
            else:
                print("query database exception,sql= %s,%s" % (sql, data))
        return res

    def insert(self, sql):
        """
        插入数据库数据语句
        :param sql:
        :return:
        """
        conn = self.pool.connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
            return {'result': True, 'id': int(cursor.lastrowid)}
        except Exception as err:
            conn.rollback()
            return {'result': False, 'err': err}
        finally:
            cursor.close()
            conn.close()


def CreateDB(db_name):
    """
    创建数据库的函数
    :param db_name: 待创建数据库的名称
    :return: True or False
    """
    exist = False
    db = MysqlPool(dbname="information_schema")  # 先到information_schema.SCHEMA_NAME表中查找是否存在需要创建的数据库
    dblists = db.do_sql("SELECT `SCHEMA_NAME` FROM `SCHEMATA` WHERE 1")
    for dl in dblists:
        if dl[0] == db_name:
            exist = True
    if not exist:
        sql = "CREATE DATABASE `%s` /*!40100 COLLATE 'utf8_general_ci' */;" % db_name
        rev = db.upda_sql(sql)
        if not rev:
            print("The DB named %s is not successful to create")
        return rev
    else:
        print("The DB named %s is exist" % db_name)
        return False


def create_info_expert_table(db, table_name):
    """
    创建表的示例：info_expert表
    :param db:数据库实例
    :param table_name:待创建表名
    :return:
    """
    CreateDBTableSql = "CREATE TABLE IF NOT EXISTS `%s` (\
        `index` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,\
        `expert_id` VARCHAR(255) DEFAULT NULL,\
        `expert_name` VARCHAR(255) DEFAULT NULL,\
        `expert_unit` VARCHAR(255) DEFAULT NULL,\
        `article_title` VARCHAR(255) DEFAULT NULL,\
        `article_url` VARCHAR(255) DEFAULT NULL,\
        `other_author_name` VARCHAR(255) DEFAULT NULL,\
        `other_url` TEXT DEFAULT NULL,\
        `publish_year` VARCHAR(255) DEFAULT NULL,\
        `State` INT(11) DEFAULT 0 COMMENT '0：初始状态；5：该单位专家爬取完成，无需再爬取;10：进一步爬取已完成',\
        PRIMARY KEY (`index`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;" % table_name
    flag = db.modify_sql(CreateDBTableSql)
    if not flag:
        print("The table named %s is not successful to create" % table_name)


if __name__ == '__main__':
    # mysql = Mysql()
    # data = mysql.get_more_row("SELECT * FROM `databuff` WHERE 1")
    # for my in data:
    #     print(my)

    mysqlpool = MysqlPool()
    datapool = mysqlpool.do_sql("SELECT * FROM `databuff` WHERE 1")
    for dp in datapool:
        print(dp)

    # CreateDB('mydb')
