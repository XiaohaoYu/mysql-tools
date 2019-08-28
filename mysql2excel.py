#!/bin/env python
#coding:utf-8
#Author: Hogan
#Descript : MySQL导出数据到excel


import pymysql
from openpyxl import Workbook
from openpyxl.writer.excel import ExcelWriter
from openpyxl.cell.cell import get_column_letter
import datetime,time,logging
import os

# 开始日期
start_date = datetime.date(datetime.date.today().year,datetime.date.today().month,1)
# 结束日期
stop_date = datetime.date(datetime.date.today().year,datetime.date.today().month + 1,1)
# 当天日期
Today = time.strftime('%Y-%m-%d')
# 表格输出路径
DirName = '/xxxx/xxxx/xxxxxx/'
# 表格名字
FileName = '%sxxxxxxx-%s.xlsx' % (DirName, Today)

# 需要执行的SQL
SQL=('select xxxxx')

# SQL转换成小写
SQL=SQL.lower()

# 定义日志
logging.basicConfig(filename='%saccess.log' %DirName,
                    format='%(asctime)s - %(name)s - %(levelname)s -%(module)s:  %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p',
                    level=10)

class MYSQL:
    def __init__(self):
        pass

    def __del__(self):
        self._cursor.close()
        self._connect.close()

    def connectDB(self):
        """
        连接数据库
        :return:
        """
        try:
            self._connect = pymysql.Connect(
                host='',
                port=3306,
                user='',
                passwd='',
                db='',
                charset='utf8'
            )

            return 0
        except:
            return -1

    def export(self, table_name, output_path):
        self._cursor = self._connect.cursor()
        count = self._cursor.execute(SQL)
        logging.info('返回%s行数据' %count)
        # 搜取所有结果
        results = self._cursor.fetchall()
        # 获取字段名
        fields_complex = self._cursor.description
        # 字段名加入li 列表中
        fields = []
        for i in range(len(fields_complex)):
            fields.append(fields_complex[i][0])
        # 创建表格
        wb = Workbook()
        ws = wb.active
        ws = wb.worksheets[0]
        # 字段名添加到表格
        ws.append(fields)
        # 数据添加到表格
        for ret in results:
            ws.append(ret)
        #保存
        wb.save(output_path)

if __name__ == '__main__':
    mysql = MYSQL()
    flag = mysql.connectDB()
    if flag == -1:
        logging.info('数据库连接失败')
    else:
        logging.info('数据库连接成功')
    logging.info('导出数据的开始时间: %s' %start_date)
    logging.info('导出数据的结束时间: %s' %stop_date)
    mysql.export('t1', '%s' % FileName)
    if FileName:
        logging.info('数据导出成功！\n')
    else:
        logging.info('数据导出失败！\n')
    os.system('chown expftp:expftp %s' %FileName)