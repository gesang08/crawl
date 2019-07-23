#!/usr/bin/env python3
# encoding:utf-8
# name:WanfangMain.py


import queue
import sys
import threading
import requests
from urllib.error import URLError, HTTPError
import math
from urllib.parse import quote
import multiprocessing
from ThreadHelper import LoopTimer
import urllib.request
from bs4 import BeautifulSoup
import time
import random
import re
from ConfigHelper import WriteConfig, ReadConfig
from MysqlHelper import MysqlPool
from PublicMethod import RemoveSpecialCharacter, InitDict, CreatResultDBTable, CreatUrlBuffTable

SearchDBName = 'Wanfang'
ConfigName = 'Config.ini'
concurrent = 3  # 采集线程数
conparse = 5  # 解析线程数

req_queue = queue.Queue()  # 生成请求队列
data_queue = queue.Queue()  # 生成数据队列 ，请求以后，响应内容放到数据队列里
ex_dbname = ReadConfig(file_name=ConfigName, section=SearchDBName, attr='ex_dbname')
DbDatabuff = "databuff"+str(ex_dbname)
Dbresult = "result"+str(ex_dbname)


class Parse(threading.Thread):
    # 初始化属性
    def __init__(self, number, data_list, req_thread):
        super(Parse, self).__init__()
        self.number = number  # 线程编号
        self.data_list = data_list  # 数据队列
        self.req_thread = req_thread  # 请求队列，为了判断采集线程存活状态
        self.is_parse = True  # 判断是否从数据队列里提取数据

    def run(self):
        print('启动%d号解析线程' % self.number)
        # 无限循环，
        while True:
            # 如何判断解析线程的结束条件
            for t in self.req_thread:  # 循环所有采集线程
                if t.is_alive():  # 判断线程是否存活
                    break
            else:  # 如果循环完毕，没有执行break语句，则进入else
                if self.data_list.qsize() == 0:  # 判断数据队列是否为空
                    self.is_parse = False  # 设置解析为False
            # 判断是否继续解析

            if self.is_parse or int(ReadConfig(file_name=ConfigName, section=SearchDBName, attr='stopflag')) == 0:  # 解析

                try:
                    url, data = self.data_list.get(timeout=3)  # 从数据队列里提取一个数据
                except Exception as e:  # 超时以后进入异常
                    data = None
                # 如果成功拿到数据，则调用解析方法
                if data is not None and Wanfang.running:
                    Paper = Wanfang.GetFurtherPaper(url, data)
            else:
                break  # 结束while 无限循环

        print('退出%d号解析线程' % self.number)


class Crawl(threading.Thread):  # 采集线程类
    # 初始化
    def __init__(self, number, req_list, data_list):
        # 调用Thread 父类方法
        super(Crawl, self).__init__()
        # 初始化子类属性
        self.number = number
        self.req_list = req_list
        self.data_list = data_list

    # 线程启动的时候调用
    def run(self):
        # 输出启动线程信息
        print('启动采集线程%d号' % self.number)
        # 如果请求队列不为空，则无限循环，从请求队列里拿请求url
        while self.req_list.qsize() > 0 or int(ReadConfig(file_name=ConfigName, section=SearchDBName, attr='stopflag'))==0:
            # 从请求队列里提取url
            url = self.req_list.get()  # 从queue中(queue[0]) get到一个数据，该数据就会在queue中删除(删除的是queue[0]，然后queue[1]就会移动到queue[0],...)
            # print('%d号线程采集：%s' % (self.number, url))
            # 防止请求频率过快，随机设置阻塞时间
            time.sleep(random.randint(30, 50)/10)
            # 发起http请求，获取响应内容，追加到数据队列里，等待解析
            response = Wanfang.VisitHtml(url)
            self.data_list.put([url, response])  # 向数据队列中添加列表数据


class WanFangCrawler:
    """
    爬取万方数据库的类，主要为2方面：1.获取待解析网页的所有URL；2.解析网页，将获得信息存储数据库
    """
    def __init__(self, db, StartTime=None, EndTime=None, StartPage=None):
        """
        :param db: 数据库的实例化
        :param StartTime: 开始年份
        :param EndTime: 结束年份
        :param StartPage: 开始页码
        """
        self.db = db
        self.SearchName = SearchDBName  # 万方
        self.ConfigPath = ConfigName  # 配置文件地址
        self._Perpage = 50  # 每页显示50
        self.running = False  # 标记程序是否正常运行
        self.further_url = list()
        if StartTime is None and EndTime is None and StartPage is None:
            self.StartTime = ReadConfig(file_name=ConfigName, section=self.SearchName, attr='starttime')  # 开始年份
            self.EndTime = ReadConfig(file_name=ConfigName, section=self.SearchName, attr='endtime')  # 结束年份
            self.StartPage = ReadConfig(file_name=ConfigName, section=self.SearchName, attr='startpage')  # 开始页数
            self.MaxPage = ReadConfig(file_name=ConfigName, section=self.SearchName, attr='maxpage')  # 最大页数
            self.title = ReadConfig(file_name=ConfigName, section=self.SearchName, attr='title')
            self.authors = ReadConfig(file_name=ConfigName, section=self.SearchName, attr='authors')
            self.keywords = ReadConfig(file_name=ConfigName, section=self.SearchName, attr='keywords')
            self.unit = ReadConfig(file_name=ConfigName, section=self.SearchName, attr='unit')
            self.BaseKeyword = ""
            if RemoveSpecialCharacter(self.title) != "":
                self.BaseKeyword = self.BaseKeyword + " 标题:" + self.title
            if RemoveSpecialCharacter(self.authors) != "":
                self.BaseKeyword = self.BaseKeyword + " 作者:" + self.authors
            if RemoveSpecialCharacter(self.keywords) != "":
                self.BaseKeyword = self.BaseKeyword + " 关键词:" + self.keywords
            if RemoveSpecialCharacter(self.unit) != "":
                self.BaseKeyword = self.BaseKeyword + " 作者单位:" + self.unit
        else:
            pass

    def GetBaseUrl(self, mod=False):
        """
        获取爬虫网址的入口URL
        :param mod: False:以URL Decode解码方式访问网页（网址中的中文以中文字符呈现）；True:以URL Encode编码方式访问网页（网址中的中文以编码后的字符呈现）
        :return:爬虫入口URL
        """
        index_url1 = 'http://g.wanfangdata.com.cn/search/searchList.do?searchType=all&pageSize=50&searchWord='  # pageSize=50每页记录限制为50条
        index_url2 = '&showType=detail&isHit=null&isHitUnit=&firstAuthor=false&rangeParame=all&navSearchType='
        index_url = index_url1 + self.BaseKeyword + ' 起始年:' + self.StartTime + ' 结束年:' + self.EndTime + index_url2  # 搜索时加上时间限制
        if mod:
            index_url = index_url1 + quote(self.BaseKeyword + ' 起始年:' + self.StartTime + ' 结束年:' + self.EndTime) + index_url2
        print(index_url)
        return index_url

    def GetMaxPage(self):
        """
        获取搜索结果的最大数页数（每页50条）
        :return: 总记录数、最大页数、入口URL
        """
        total_record_num = 0
        index_url = self.GetBaseUrl()
        response = self.VisitHtml(url=index_url)
        if self.running:
            html = BeautifulSoup(response.text, "html.parser")  # 获取HTML代码
            total_record_text = html.find('div', class_='left_sidebar_border')
            for item in total_record_text:
                if '条结果' in item:
                    total_record_num = re.findall(r"\d+\.?\d*", item)
                    if str.isdigit(total_record_num[0]):
                        total_record_num = int(total_record_num[0])
                        break
            print("查询到共%s相关文献" % total_record_num)
            self.MaxPage = int(math.ceil(total_record_num / self._Perpage))  # 总页数
            if self.MaxPage > 100:  # 最大记录数只能达到5000条，即100页每页50条
                self.MaxPage = 100
            WriteConfig(file_name=ConfigName, section=SearchDBName, attr='maxpage', value=self.MaxPage)
            return total_record_num, self.MaxPage, index_url

    def VisitHtml(self, url):
        """
        请求访问网页
        :param url: 访问网页的URL
        :return: 请求结果对象
        """
        headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                  'Connection': 'keep-alive',
                  'Host': 'g.wanfangdata.com.cn',
                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'}
        attempts = 0
        success = False
        while attempts < 20 and not success:
            try:
                response = requests.get(url, timeout=4, headers=headers)  # 获取网页HTML的URL编码数据
                success = True
                self.running = True
            except:
                attempts += 1
                print("第" + str(attempts) + "次重试！")
                if attempts == 20:
                    self.running = False
                    return False
            # except requests.exceptions.ReadTimeout or requests.exceptions.ConnectionError:
            #     print("请求连接超时")
            #     return False
            else:
                return response

    def GetAllUrl(self):
        total_record_num, self.MaxPage, index_url = self.GetMaxPage()  # 最大页数
        self.StartPage = ReadConfig(file_name=ConfigName, section=SearchDBName, attr='startpage')  # 开始页数
        t = time.time()
        WriteConfig(file_name=ConfigName, section=SearchDBName, attr='flag_get_all_url', value=0)
        for i in range(int(self.StartPage), self.MaxPage + 1):
            print("共有%s页，当前为%s页，获得文献链接的进度完成%.2f" % (self.MaxPage, i, (int(i) / int(self.MaxPage)) * 100))
            WriteConfig(file_name=ConfigName, section=SearchDBName, attr='startpage', value=i + 1)
            url_list = self.GetFurtherUrl(i, index_url)
            threading.Thread(target=self.WriteUrlIntoDB, args=(url_list,)).start()
            # self.further_url.extend(self.GetFurtherUrl(i, index_url))
            time.sleep(0.5)
        WriteConfig(file_name=ConfigName, section=SearchDBName, attr='flag_get_all_url', value=1)
        print(time.time() - t)

    def GetFurtherUrl(self, page_num, index_url):
        """
        获取详细网页的URL
        :param page_num:
        :param base_url:
        :return:
        """
        url_list = []
        index_url = index_url + '&page=' + str(page_num)  # 翻页
        response = self.VisitHtml(index_url)
        if self.running:
            bs = BeautifulSoup(response.text, "html.parser")
            info_url = bs.find_all('i', class_='icon icon_Miner')
            for item in info_url:
                onclick = item.attrs['onclick']
                _id = onclick.split(',')[1].strip("\\'")
                _type = onclick.split(',')[2].lstrip("\\'").rstrip("\\')")
                further_url = 'http://g.wanfangdata.com.cn/details/detail.do?_type=' + _type + '&id=' + _id
                url_list.append(further_url)
            return url_list

    def WriteUrlIntoDB(self, url):
        for i in range(len(url)):
            sql = "INSERT INTO `%s` (`Url`, `source`) VALUES ('%s', '%s');\n" % (DbDatabuff, url[i], SearchDBName)
            row = self.db.insert(sql)
            # if not row['result']:
            #     print('_'*40)
            #     print(url[i])

    def GetFurtherPaper(self, _url, _soup):
        _Paper = InitDict()
        _Paper['url'] = _url
        all_author = ''
        if self.running:
            try:
                html = BeautifulSoup(_soup.text, "html.parser")  # 获取HTML代码
                title = html.find('font', {'style': 'font-weight:bold;'})
                title = title.get_text()
                abstract = html.find('div', class_='abstract')
                if abstract:
                    abstract = abstract.text.split('摘要')[0].replace('\n', '')
                    _Paper['abstract'] = abstract.replace("'", "")
                literature_type = html.find('div', class_='crumbs')
                if '期刊' in literature_type.text:
                    literature_type = 'J'
                    author_item = html.find_all('input', class_='dw')
                    for auth in author_item:
                        author = auth.attrs['value']
                        all_author = all_author + author + ";"
                    all_author = all_author.rstrip(';')
                elif '学位' in literature_type.text:
                    literature_type = 'D'
                    author_item = html.find('a', id='card01')
                    all_author = author_item.get_text()
                elif '会议' in literature_type.text:
                    literature_type = 'C'
                    author_item = html.find_all('a', class_='info_right_name')
                    for auth in author_item:
                        all_author = all_author + auth.get_text() + ';'
                    all_author = all_author.rstrip(';')
                elif '标准' in literature_type.text:
                    literature_type = 'S'
                elif '科技报告' in literature_type.text:
                    literature_type = 'R'
                elif '专利' in literature_type.text:
                    literature_type = 'P'
                else:
                    literature_type = "Z"  # 未定义类型文献
                info = html.find_all('div', class_='info_right')

                _Paper['authors'] = all_author
                for item in info:
                    screen = item.parent.text
                    if 'doi：' in screen:
                        doi = item.get_text()
                        _Paper['doi'] = doi
                    if '关键词：' in screen:
                        keywords = item.get_text().rstrip('\n').lstrip('\n\n').replace('\n\n', ';')
                        _Paper['keywords'] = keywords
                    if '作者单位' in screen and ('Author' not in screen):
                        unit = item.get_text().strip('\n').replace('\n', ';')
                        _Paper['unit'] = unit
                    if '学位授予单位' in screen:
                        _Paper['unit'] = item.get_text().strip('\n')
                        _Paper['publication'] = item.get_text().strip('\n')
                    if '会议名称' in screen:
                        _Paper['publication'] = item.get_text().strip('\n')
                    if '年，卷(期)：' in screen and ('作者单位' not in screen):
                        year_volume_date = item.get_text().strip('\n')
                        year = year_volume_date.split(',')[0]  # 出版年份
                        volume = year_volume_date.split(',')[1].split('(')[0]  # 卷
                        date = get_string_start_end(item.get_text(), '(', ')')  # 期
                        _Paper['year'] = year
                        _Paper['volume'] = volume
                        _Paper['issue'] = date
                    if '在线出版日期' in screen:
                        _Paper['year'] = item.get_text().replace('\r\n', '').replace('\t', '').strip().split('年')[0]
                    if '学位年度' in screen:
                        _Paper['year'] = item.get_text()
                    if '基金项目：' in screen and ('作者单位' not in screen):
                        sponser = item.get_text().strip('\n').replace('\n', ';')  # 基金项目
                        _Paper['sponser'] = sponser.replace("'", "")
                    if '页数：' in screen and ('作者单位' not in screen):
                        page_num = item.get_text()  # 页数
                    if '页码：' in screen and ('作者单位' not in screen):
                        page_number = item.get_text()  # 页码
                        _Paper['pagecode'] = page_number
                    if '刊名：' in screen and ('作者单位' not in screen):
                        publication = item.get_text().strip('\n')  # 刊名
                        _Paper['publication'] = publication.replace("'", "")
                _Paper['title'] = title.replace("'", "")

                _Paper['abstract'] = abstract.replace("'", "")
                _Paper['type'] = literature_type
                print(_Paper)
            except:
                db.upda_sql("update `%s` set `State`=-15 where `Url`='%s'" % (DbDatabuff, _Paper['url']))
                print(_Paper['url'], "goup解析出现错误")
        # print(_Paper)
        return _Paper

    def GetUrlFromDb(self, num=20):
        sql = "SELECT `Source`,`Url` from `%s` where `State`in (0,-10) limit %s " % (DbDatabuff, num)  # 一次读20条URL用于爬取数据
        _rows = self.db.do_sql(sql)
        if _rows:
            if len(_rows) > 0:
                _UrlList = [x[1] for x in _rows]
                for i in _UrlList:
                    self.db.upda_sql("update `%s` set `State`=10 where `Url`='%s'" % (DbDatabuff, i))
                return _UrlList
        else:
            return ""


def get_string_start_end(string, start, end):
    """
    获取字符串中，两个字符之间的字符
    :param string:字符串
    :param start: 开始字符
    :param end: 结束字符
    :return: start-end之间字符
    """
    start_index = string.find(start)
    end_index = string.find(end)
    return string[start_index + 1:end_index]


class ClockProcess(multiprocessing.Process):  # multiprocessing.Process产生的是子进程
    def __init__(self):
        multiprocessing.Process.__init__(self)

    def run(self):  # 在调用子进程的start() 的时候，默认会执行run() 方法
        _db = MysqlPool()
        _Wanfang = WanFangCrawler(db=_db)
        _Wanfang.GetAllUrl()
        print("采集链接结束")


def PutUrlToQueue(Wanfang, num):
    UrlList = Wanfang.GetUrlFromDb(num=num)
    if UrlList:
        if len(UrlList) > 0:
            for url in UrlList:
                req_queue.put(url)  # 将从数据库获得的20条数据放入queue
    else:
        pass

def main():
    ClockProcess().start()  # 开启一个子进程，用于获取所有需解析的URL,并存储到数据库
    PutUrlToQueue(Wanfang, 20)  # 往队列queue放20条数据，进行数据待处理
    LoopTimer(0.5, PutUrlToQueue, args=(Wanfang, 20,)).start()  # 开启一个线程，比如Thread-7,它会0.5秒执行一下PutUrlToList函数
    # LoopTimer(1, ShowStatePro, args=(db, SearchDBName, DbDatabuff, Dbresult,)).start()  # 开启一个线程，比如Thread-13,它会1秒执行一下ShowStatePro函数
    # # 生成N个采集线程
    # req_thread = []
    # for i in range(concurrent):
    #     t = Crawl(i + 1, req_list, data_list)  # 创造线程
    #     t.start()  # 只要遇到一个start()，程序都会重新开启一个线程，该线程的名字Thread-i随机
    #     req_thread.append(t)
    # # 生成N个解析线程
    # parse_thread = []
    # for i in range(conparse):
    #     t = Parse(i + 1, data_list, req_thread)  # 创造解析线程
    #     t.start()
    #     parse_thread.append(t)
    # for t in req_thread:
    #     t.join()
    # for t in parse_thread:
    #     t.join()


def init():
    if '1' in str(ReadConfig(file_name=ConfigName, section=SearchDBName, attr='restart')):
        CreatResultDBTable(db, Dbresult)
        CreatUrlBuffTable(db, DbDatabuff)
        time.sleep(0.1)
        WriteConfig(file_name=ConfigName, section=SearchDBName, attr='restart', value=0)
        WriteConfig(file_name=ConfigName, section=SearchDBName, attr='startpage', value=1)
        WriteConfig(file_name=ConfigName, section=SearchDBName, attr='stopflag', value=0)
        WriteConfig(file_name=ConfigName, section=SearchDBName, attr='flag_get_all_url', value=0)
    if '0' in str(ReadConfig(file_name=ConfigName, section=SearchDBName, attr='restart')):
        db.upda_sql("Update `%s` set `State`=0 where `State`=10" % DbDatabuff)
    time.sleep(1)


def WanfangProcess():
    global db, Wanfang
    multiprocessing.freeze_support()  # 在Windows下编译需要这行：windows创建进程没有fork方法，默认是spawn，而linux创建进程默认是fork方法
    db = MysqlPool()
    Wanfang = WanFangCrawler(db=db)
    init()
    if '0' in str(ReadConfig(file_name=ConfigName, section=SearchDBName, attr='stopflag')):
        main()


if __name__ == '__main__':
    db = MysqlPool()
    w = WanFangCrawler(db)
    w.GetAllUrl()
