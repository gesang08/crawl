#!/usr/bin/env python3
# _*_ coding: UTF-8 _*_

from configparser import ConfigParser
from urllib.parse import quote
import socket
import os
import math
import urllib.request
from bs4 import BeautifulSoup
import wx
import wx.lib.mixins.inspection
import time
import requests
import datetime
import time
import threading
from urllib.error import URLError, HTTPError
import MyDb3

AUTHOR = "李一博"
CURRENT_ORGANIZATION = "天津大学"
ONCE_ORGANIZATION = "哈尔滨理工大学"
LIB_NAME = "中国知网"


def main():
    info_expert = 'info_expert'
    info_other_author = 'info_other_author'
    db = MyDb3.Database()
    try:
        create_info_expert_table(db, info_expert)
        create_info_other_author_table(db, info_other_author)
    except Exception as e:
        print(e)
        pass
    app = MyApp()
    app.MainLoop()


class MyApp(wx.App, wx.lib.mixins.inspection.InspectionMixin):
    def OnInit(self):
        mainframe = MyFrame(parent=None, id=wx.ID_ANY, title="产生评审专家库UI", pos=wx.DefaultPosition,
                               size=(400, 500), style=wx.DEFAULT_FRAME_STYLE & ~ (wx.MAXIMIZE_BOX | wx.MINIMIZE_BOX))
        mainframe.Center(dir=wx.BOTH)
        mainframe.Show()
        self.SetTopWindow(mainframe)
        return True


class MyFrame(wx.Frame):
    """
    创建评审专家库UI:
    1.专家名：一个StaticText,一个TextCtrl;
    2.工作单位：一个StaticText,一个TextCtrl;
    3.数据库名：一个StaticText,一个ComboBox(下拉选择框);包括中国知网，SCI,EI三个
    4.确定、取消：两个按钮
    5.结果输出框：多行TextCtrl
    """
    def __init__(self, parent, id, title, pos, size, style):
        wx.Frame.__init__(self, parent, id, title, pos, size, style)
        self.panel = wx.Panel(self, -1)
        self.expert_name_label = None
        self.expert_name_text = None
        self.work_unit_name_label = None
        self.work_unit_name_text = None
        self.lib_name_label = None
        self.lib_name_combobox = None
        self.ensure_button = None
        self.cancel_button = None
        self.output_label = None
        self.output_text = WriteLog(self.panel, value='', pos=wx.Point(0, 500 - 220), size=wx.Size(390, 190),
                                    style=wx.TE_MULTILINE | wx.TE_READONLY | wx.TE_MULTILINE | wx.VERTICAL | wx.HSCROLL)
        self.lib_lists = ['中国知网', 'SCI', 'EI']
        self.create_control()

    def create_control(self):   # 创建控件并绑定事件
        self.expert_name_label = wx.StaticText(self.panel, label="专 家 名：", pos=(75, 53))
        self.expert_name_text = wx.TextCtrl(self.panel, pos=(150, 50), size=(150, 25))
        self.work_unit_name_label = wx.StaticText(self.panel, label="工作单位：", pos=(75, 103))
        self.work_unit_name_text = wx.TextCtrl(self.panel, pos=(150, 100), size=(150, 25))
        self.lib_name_label = wx.StaticText(self.panel, label="数 据 库：", pos=(75, 153))
        self.lib_name_combobox = wx.ComboBox(self.panel, value='中国知网', pos=(150, 150), size=(150, 25),
                                             choices=self.lib_lists)
        self.ensure_button = wx.Button(self.panel, label='确定', pos=(80, 220))
        self.cancel_button = wx.Button(self.panel, label='取消', pos=(240, 220))
        self.output_label = wx.StaticText(self.panel, label="输  出：", pos=(5, 260))
        self.ensure_button.Bind(wx.EVT_BUTTON, self.ensure_click_response)
        self.cancel_button.Bind(wx.EVT_BUTTON, self.cancel_click_response)

    def ensure_click_response(self, event):
        author = self.expert_name_text.GetValue().replace(' ', '')
        work_unit = self.work_unit_name_text.GetValue().replace(' ', '')
        lib_name = self.lib_name_combobox.GetValue().replace(' ', '')
        if author == '' or work_unit == '' or lib_name not in self.lib_lists:
            self.output_text.write_textctrl_txt('输入数据有错误，请重新输入 \r')
        else:
            """
            检测数据库是否已生成该专家的信息
            """
            sql = "SELECT `State` FROM `info_expert` WHERE `expert_name`='%s' and `expert_unit`='%s'" % (author, work_unit)
            my = MyDb3.Database()
            state = my.get_one_row(sql)
            if not state:   # 不存在才爬取
                crawl = Crawl(author, work_unit, lib_name, self.output_text)
                t = threading.Thread(target=crawl.crawl_main)
                t.start()
                # crawl.crawl_main()
            else:
                self.output_text.write_textctrl_txt(work_unit + " " + author + "数据已爬取! \r")
        event.Skip()

    def cancel_click_response(self, event):
        self.Close()


class WriteLog(wx.TextCtrl):
    """
    三种记录日志的方法：1.记录日志到界面的日志TextCtrl;2.记录日志到数据库info_log表；3.记录日志到项目文件下的log.txt
    """
    def __init__(self, parent, id=-1, value="", pos=wx.Point(0, 0), size=wx.Size(150, 90),
                 style=wx.NO_BORDER | wx.TE_MULTILINE | wx.TE_READONLY):  # value即是TextCtrl中的Text的值
        wx.TextCtrl.__init__(self, parent, id, value, pos, size, style)
        self.log_file = 'log.txt'
        self.log_table_name = 'info_log'

    def write_textctrl_db(self, text, enable=True, font=wx.NORMAL_FONT, colour=wx.BLACK):
        """
        写日志到TextCtrl和数据库中122
        :param text:
        :param enable:
        :param font:
        :param colour:
        :return:
        """
        if enable:
            text = current_time() + text
            wx.TextCtrl.SetFont(self, font)
            wx.TextCtrl.SetForegroundColour(self, colour)
            try:
                wx.TextCtrl.WriteText(self, text)
            except Exception as e:
                wx.TextCtrl.WriteText(self, current_time() + str(e))

    def write_textctrl_txt(self, text, enable=True, font=wx.NORMAL_FONT, colour=wx.BLACK):
        if enable:
            text = current_time() + "  " + text
            all_files = os.listdir(os.getcwd())  # 获取当前工程项目文件夹下所有文件名
            if self.log_file not in all_files:  # 若该文件不存在，在当前目录创建一个日志文件
                self.log_file = open(self.log_file, 'w+')
                self.log_file.close()
                self.log_file = self.log_file.name
            with open(self.log_file, 'a+') as file_obj:  # 向日志文件加log
                file_obj.write(text)
            wx.TextCtrl.SetFont(self, font)
            wx.TextCtrl.SetForegroundColour(self, colour)
            try:
                wx.TextCtrl.WriteText(self, text)
                print(text)
            except Exception as e:
                wx.TextCtrl.WriteText(self, current_time() + str(e))


def current_time():
    t = time.strftime('%Y{y}%m{m}%d{d} %H{h}%M{f}%S{s}').format(y='年', m='月', d='日', h='时', f='分', s='秒')
    return t


class Crawl:
    """
    爬取网页数据的类
    """
    def __init__(self, author, work_unit, lib_name, log):
        self._author = author
        self._work_unit = work_unit
        self._lib_name = lib_name
        self.running = False
        self.log = log
        self.thread_status = True

    def crawl_main(self):
        while self.thread_status:
            if self._lib_name == '中国知网':
                self.crawl_cnki_main()
            elif self._lib_name == 'SCI':
                self.crawl_sci_main()
            else:
                self.crawl_ei_main()
            time.sleep(6)

    def crawl_cnki_main(self):
        """
        完成对给定专家名称和工作单位要求的网页请求
        :return:
        """
        doc_url_set = list()  # 存储所有与作者文章相关的列表
        response = ""
        base_url = "http://yuanjian.cnki.net/Search/Result"
        data = {
            "searchType": "MulityTermsSearch",
            "Author": self._author,
            "Unit": self._work_unit
        }
        """
        在请求超时的情况下，捕捉超时错误并连续发送多次请求，直到请求连接成功。
        """
        try:
            response = requests.post(base_url, data=data, timeout=5)
            if response.status_code == 200:
                self.running = True
        except requests.exceptions.Timeout:
            global NETWORK_STATUS
            NETWORK_STATUS = False  # 请求超时改变状态
            if not NETWORK_STATUS:
                """
                请求超时
                """
                for k in range(0, 10):
                    print("请求超时，第 % s次重复请求" % (k + 1))
                    self.log.write_textctrl_txt("请求超时，第 % s次重复请求 \r" % (k + 1))
                    response = requests.post(base_url, data=data, timeout=5)
                    if response.status_code == 200:
                        self.running = True
                        break
                    if k == 10:
                        self.log.write_textctrl_txt("查找不到此人信息 \r")
                        return self.running

        if self.running:
            html = BeautifulSoup(response.text, "html.parser")   # 获取HTML代码
            total_count = int(html.find("input", {"id": "hidTotalCount"})["value"])   # 文章总数量
            count_per_page = 20  # 每页的数量
            page_count = int(math.ceil(total_count / count_per_page))  # 总页数
            for i in range(1, page_count + 1):
                doc_url_set.extend(self.get_next_page_doc_url_set(i, base_url))
            # te = doc_url_set[-3:-1]
            for j in range(len(doc_url_set)):
                # print(j)
                # print(doc_url_set[j])
                self.get_paper_url(doc_url_set[j])
                if j == len(doc_url_set) - 1:
                    self.get_further_url()
                    self.thread_status = False  # 爬取完结束线程
                    self.log.write_textctrl_txt(self._author + "的" + self._lib_name + "信息爬取结束！ \r")

    def get_next_page_doc_url_set(self, page_num, base_url, md=0):
        """
        获取文章详细内容的URL
        :param page_num:
        :return:
        """
        # base_url = "http://yuanjian.cnki.net/Search/Result"
        if md == 0:
            data = {
                "searchType": "MulityTermsSearch",
                "Author": self._author,
                "ParamIsNullOrEmpty": "true",
                "Islegal": "false",
                "Order": "1",
                "Page": page_num,
                "Unit": self._work_unit
            }
        else:
            data = {
                "searchType": "MulityTermsSearch",
                "Author": self._author,
                "ParamIsNullOrEmpty": "true",
                "Islegal": "false",
                "Order": "1",
                "Page": page_num,
                # "Unit": self._work_unit
            }

        r = requests.post(base_url, data=data)

        bs = BeautifulSoup(r.text, "html.parser")
        doc_url_set = [item["href"] for item in bs.find_all("a", {"target": "_blank", "class": "left"})]
        return doc_url_set

    def get_paper_url(self, page_url):
        """
        爬取详细信息
        :param page_url:
        :return:
        """
        other_author = []
        author_link = []
        flag = False
        detail_unit = None
        html = ""
        attempts = 0
        success = False
        while attempts < 20 and not success:
            try:
                html = urllib.request.urlopen(page_url)  # 获取网页HTML的URL编码数据
                socket.setdefaulttimeout(5)  # 设置5秒后连接超时
                success = True
            except socket.error:
                attempts += 1
                print("第" + str(attempts) + "次重试！！！")
                self.log.write_textctrl_txt("第" + str(attempts) + "次重试！！！ \r")
                if attempts == 20:
                    return False
            except HTTPError or URLError:
                attempts += 1
                self.log.write_textctrl_txt("第" + str(attempts) + "次重试！！！ \r")
                print("第" + str(attempts) + "次重试！！")
                if attempts == 20:
                    return False

            else:
                html = html.read()
                soup = BeautifulSoup(html, 'html.parser')  # 解析网页的实际HTML代码，其中文已从HTML的URL编码数据解析成了实际中文
                try:
                    article_name = soup.find('h1', class_='xx_title').get_text()
                    year = soup.find('font', color='#0080ff').get_text().strip().rstrip('\r\n')  # 年和期号
                except:
                    article_name = ""
                    year = ""

                _all = soup.find_all('div', class_='xx_font')
                all_link = soup.find_all('a', target='_blank')
                for item in _all:
                    item = item.find('a', target='_blank')
                    if item is not None:
                        detail_unit = item.string
                for i in range(len(all_link)):  # 截取作者和链接
                    if all_link[i].previous_elelment == '【基金】：' or all_link[i].text == detail_unit or all_link[i].text.strip().rstrip('\r\n') == '下载全文':
                        flag = False
                        break
                    if year in all_link[i].get_text() or flag:
                        if '投稿' in all_link[i + 1].text:
                            ta = all_link[i + 2]
                        else:
                            ta = all_link[i + 1]
                        flag = True
                        other_author.append(ta.text)  # 将其余作者和专家详细机构添加到列表中
                        author_link.append(ta.get('href').lstrip('\n').strip())  # 获取作者链接和机构链接
                        continue
                if self._author not in other_author:
                    self.log.write_textctrl_txt("查找不到此人文章信息 \n")
                    return False
                other_author, author_link = self.del_author_link(other_author, author_link, other_author[0])
                combine_other_author = ""
                combine_other_author_url = ""
                for j in range(len(other_author)):
                    combine_other_author = combine_other_author + other_author[j] + "；"  # 中文分号隔开
                    combine_other_author_url = combine_other_author_url + author_link[j] + "；"
                self.log.write_textctrl_txt(self._author + '\t' + article_name + '\t' + combine_other_author + '\r')
                # print(((self._author, self._work_unit, article_name, page_url, combine_other_author, combine_other_author_url, year)))
                sql = "INSERT INTO `info_expert` (`expert_name`, `expert_unit`, `article_title`, `article_url`, `other_author_name`, `other_url`, `publish_year`, `State`) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
                      (self._author, self._work_unit, article_name, page_url, combine_other_author, combine_other_author_url, year, 5)
                try:
                    db = MyDb3.Database()
                    db.modify_sql(sql)
                except:
                    self.log.write_textctrl_txt("get_paper_url()方法中数据库连接失败！")

    def del_author_link(self, author, link, first_author):
        """
        删除不符合作者和作者链接的item
        :param author:
        :param link:
        :param first_author:
        :return:
        """
        list1 = []
        list2 = []
        del author[-1]
        del link[-1]
        if author.count(first_author) == 2:
            for j in author:
                if not j in list1:
                    list1.append(j)
            for k in link:
                if not k in list2:
                    list2.append(k)
        else:
            for i in range(len(author)):
                if author[i] == self._author:
                    del author[i]
                    del link[i]
                    list1 = author
                    list2 = link
                    break
        for i in range(len(list1)):
            if list1[i] == self._author:
                del list1[i]
                del list2[i]
                break
        return list1, list2

    def get_further_url(self):
        sql = "SELECT `index`, `expert_name`,`other_author_name`, `other_url` FROM `info_expert` WHERE `State`=5"
        my = MyDb3.Database()
        info = my.get_more_row(sql)
        if info is None or info == ():
            return False
        else:
            for item in info:
                for i in range(len(item)):
                    index = item[0]
                    expert = item[1]
                    author = item[2]
                    url = item[3]
                    author_list = author.split('；')
                    url_list = url.split('；')
                    assert len(author_list) == len(url_list) # 检测作者与链接是否一一对应
                    if len(author_list) != 0:
                        del url_list[-1]
                        del author_list[-1]
                        for k in range(len(author_list)):
                            self.further_paper_url(url_list[k], author_list[k], index, expert)
                            if k == len(author_list) - 1:
                                self.log.write_textctrl_txt("二级网页数据获取完成！ \r")
                                return False
                    else:
                        self.log.write_textctrl_txt("info_expert表索引为" + str(index) + "的文章无其他作者！ \r")
                        return False
            print(info)

    def further_paper_url(self, page_url, author, index, expert):
        """
        进一步爬取各网页下作者的信息
        :param page_url: 进一步爬取网页的URL
        :param author:其他作者
        :param index: 文章的index，用于表示进一步爬取所属的文章的index
        :param expert: 所属专家名
        :return:
        """
        html = ""
        attempts = 0
        success = False
        while attempts < 20 and not success:
            try:
                html = urllib.request.urlopen(page_url)  # 获取网页HTML的URL编码数据
                socket.setdefaulttimeout(5)  # 设置5秒后连接超时
                success = True
            except socket.error:
                attempts += 1
                print("二级网页" + "第" + str(attempts) + "次重试！")
                self.log.write_textctrl_txt("二级网页" + "第" + str(attempts) + "次重试！ \r")
                if attempts == 20:
                    return False
            except HTTPError or URLError:
                attempts += 1
                self.log.write_textctrl_txt("二级网页" + "第" + str(attempts) + "次重试！ \r")
                print("二级网页" + "第" + str(attempts) + "次重试！！")
                if attempts == 20:
                    return False
            else:
                pass







    def crawl_sci_main(self):
        pass

    def crawl_ei_main(self):
        pass


def create_info_expert_table(db, table_name):
    """
    创建info_expert表
    :param db:
    :param table_name:
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
        print("创建%s表出现问题" % table_name)


def create_info_other_author_table(db, table_name):
    """
    创建info_other_author表
    :param db:
    :param table_name:
    :return:
    """
    CreateDBTableSql = "CREATE TABLE IF NOT EXISTS `%s` (\
        `index` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,\
        `author_name` VARCHAR(255) DEFAULT NULL,\
        `author_unit` VARCHAR(255) DEFAULT NULL,\
        `article_title` VARCHAR(255) DEFAULT NULL,\
        `article_url` VARCHAR(255) DEFAULT NULL,\
        `other_author_name` VARCHAR(255) DEFAULT NULL,\
        `other_url` TEXT DEFAULT NULL,\
        `publish_year` VARCHAR(255) DEFAULT NULL,\
        `expert_id` VARCHAR(255) DEFAULT NULL COMMENT '所属专家编号',\
        `id` INT(11) NOT NULL COMMENT '所属文章的id，对应专家表的index',\
        `State` INT(11) DEFAULT 0 COMMENT '0:初始状态；5：表示一级文章下的作者链接文章爬取完后才能',\
        `modify_time` DATETIME  NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
        PRIMARY KEY (`index`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;" % table_name
    flag = db.modify_sql(CreateDBTableSql)
    if not flag:
        print("创建%s表出现问题" % table_name)


if __name__ == '__main__':
    main()
