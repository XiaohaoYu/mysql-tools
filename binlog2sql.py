#!/bin/env python
#coding:utf-8
#Author: Hogan
#Descript : 解析binlog生成MySQL回滚脚本

import getopt
import sys
import os
import re
import pymysql


# 设置默认值
host = '127.0.0.1'
port = 3306
user = ''
password = ''
start_datetime = '1971-01-01 00:00:00'
stop_datetime = '2037-01-01 00:00:00'
start_position = '4'
stop_position = '18446744073709551615'
database = ''
mysqlbinlog = 'mysqlbinlog -v --base64-output=decode-rows '
binlogfile = ''
output = 'rollback.sql'


# 提示信息
def usage():
    help_info="""==========================================================================================
Command line options :
    --help                  # OUT : print help info
    -f, --binlogfile        # IN  : binlog file. (required)
    -o, --outfile           # OUT : output rollback sql file. (default 'rollback.sql')
    -h, --host              # IN  : host. (default '127.0.0.1')
    -u, --user              # IN  : user. (required)
    -p, --password          # IN  : password. (required)
    -P, --port              # IN  : port. (default 3306)
    --start-datetime        # IN  : start datetime. (default '1970-01-01 00:00:00')
    --stop-datetime         # IN  : stop datetime. default '2070-01-01 00:00:00'
    --start-position        # IN  : start position. (default '4')
    --stop-position         # IN  : stop position. (default '18446744073709551615')
    -d, --database          # IN  : List entries for just this database (No default value).
    --only-primary          # IN  : Only list primary key in where condition (default 0)

Sample :
   shell> python rollback.py -f 'mysql-bin.000001' -o '/tmp/rollback.sql' -h 192.168.0.1 -u 'user' -p 'pwd' -P 3307 -d dbname
=========================================================================================="""

    print(help_info)
    sys.exit()

# 获取参数，生成binlog解析文件
def getops_parse_binlog():
    global host
    global user
    global password
    global port
    global database
    global start_datetime
    global stop_datetime
    global start_position
    global stop_position
    global binlogfile
    global only_primary
    global fileContent
    global output

    try:
        options, args = getopt.getopt(sys.argv[1:], "f:o:h:P:u:p:d", ["help", "binlogfile=","--output=","host=","port=","user=","password=","database=","start-datetime=",
                                                                      "stop-datetime=","start-position=","stop-position=","only-primary="])
    except getopt.GetoptError:
        print('参数错误！')
        options = []
    if options == [] or 'help' in options[0][0]:
        usage()
        sys.exit()
    print("正在获取参数......")
    # print(options)
    for name, value in options:
        if name in ('-f', '--binlogfile='):
            binlogfile = value
        if name in ('-o', '--output='):
            output = value
        if name in ('-h', '--host='):
            host = value
        if name in ('-P', '--port='):
            port = value
        if name in ('-u', '--user='):
            user = value
        if name in ('-p', '--password='):
            password = value
        if name in ('-d', '--database='):
            database = value
        if name == '--start-datetime=':
            start_datetime = value
        if name == '--stop-datetime=':
            stop_datetime = value
        if name == '--start-position=':
            start_position = value
        if name == '--stop-position=':
            stop_position = value
        if name == '--only-primary':
            only_primary = value
    if not binlogfile:
        print("错误:请指定binlog文件名")
        usage()
    if not user:
        print("错误:请指定用户名！")
        usage()
    if not password:
        print("错误:请指定密码！")
        usage()
    if database:
        condition_database = "--database='" + database + "'"
    else:
        condition_database = ''
    print("正在解析binlog......")
    cmd = ("%s  --start-position=%s --stop-position=%s  --start-datetime='%s' --stop-datetime='%s'   %s %s| grep '###' -B 2 | sed -e  's/### //g' | sed -e 's/^INSERT/##INSERT/g' -e 's/^UPDATE/##UPDATE/g'\
                                -e 's/^DELETE/##DELETE/g'" % (mysqlbinlog, start_position, stop_position, start_datetime, stop_datetime, binlogfile, condition_database ))

    fileContent = os.popen(cmd).read()

# 初始化binlog里的表名和列名，用全局字典result_dict来存储表名，列名
def init_clo_name():
    global result_dict
    global col_dict
    result_dict = {}
    # 统计binlog中出现的所有库名.表名
    table_list = list(set(re.findall('`.*`\\.`.*`', fileContent)))

    for table in table_list:
        db_name = table.split('.')[0].strip('`')
        table_name = table.split('.')[1].strip('`')
        # 连接数据库获取字段id
        try:
            conn = pymysql.connect(host=host, port=int(port), user=user, password=password)
            cursor = conn.cursor()
            # 获取字段名，字段position
            cursor.execute("select ordinal_position, column_name from information_schema.columns where table_schema='%s' and table_name='%s'" %(db_name,table_name))
            result = cursor.fetchall()
            if result == ():
                print('Warning: ' + db_name + '.' + table_name + '已删除')
            result_dict[db_name+'.'+table_name] = result
        except pymysql.Error as e:
            try:
                print("Error %d:%s" % (e.args[0], e.args[1]))
            except IndexError:
                print("MySQL Error:%s" % str(e))
            sys.exit()

# 拼接反向生成回滚SQL
def gen_rollback_sql():
    # 打开输出文件
    fileOutput = open(output, 'w')
    print('正在拼凑SQL......')
    # 将binlog解析的文件通过'--'进行分割，每块代表一个sql
    area_list = fileContent.split('--\n')
    # 逆序读取分块
    for area in area_list[::-1]:
        sql_list = area.split('##')
        for sql_head in sql_list[0].splitlines():
            sql_head = '#' + sql_head + '\n'
            fileOutput.write(sql_head)
        # 逐条对SQL进行替换更新，逆序
        for sql in sql_list[::-1][:-1]:
            try:
                # 对insert语句进行拼接
                if sql.split()[0] == 'INSERT':
                    rollback_sql = re.sub('^INSERT INTO', 'DELETE FROM', sql, 1)
                    rollback_sql = re.sub('SET\n' , 'WHERE\n', rollback_sql, 1)
                    table_name = rollback_sql.split()[2].replace('`','')
                    # 获取该SQL所有列
                    col_list = sorted(list(set(re.findall('@\d+', rollback_sql))))
                    # 因为第一个列前面没有逗号或者and，所以单独替换
                    rollback_sql = rollback_sql.replace('@1', result_dict[table_name][0][1] )
                    # 替换其他列
                    for col in col_list[1:]:
                        col_int = int(col[1:]) -1
                        rollback_sql = rollback_sql.replace(col, 'and '+ result_dict[table_name][col_int][1],1 )

                #对update语句进行拼接
                if sql.split()[0] == 'UPDATE':
                    rollback_sql = re.sub('SET\n', '#SET#\n', sql, 1)
                    rollback_sql = re.sub('WHERE\n', 'SET\n', rollback_sql, 1)
                    rollback_sql = re.sub('#SET#\n', 'WHERE\n',rollback_sql, 1)
                    table_name = rollback_sql.split()[1].replace('`','')
                    # 获取该SQL所有列
                    col_list = sorted(list(set(re.findall('@\d+', rollback_sql))))
                    # 因为第一个列前面没有逗号或者and，所以单独替换
                    rollback_sql = rollback_sql.replace('@1', result_dict[table_name][0][1] )
                    # 替换其他列
                    for col in col_list[1:]:
                        col_int = int(col[1:]) -1
                        rollback_sql = rollback_sql.replace(col, ','+ result_dict[table_name][col_int][1],1 ).replace(col,'and '+result_dict[table_name][col_int][1])

                # 对delete语句进行拼接
                if sql.split()[0] == 'DELETE':
                    rollback_sql = re.sub('^DELETE FROM', 'INSERT INTO', sql, 1)
                    rollback_sql = re.sub('WHERE', 'SET', rollback_sql, 1)
                    table_name = rollback_sql.split()[2].replace('`','')
                    # 获取该SQL所有列
                    col_list = sorted(list(set(re.findall('@\d+', rollback_sql))))
                    # 因为第一个列前面没有逗号或者and，所以单独替换
                    rollback_sql = rollback_sql.replace('@1', result_dict[table_name][0][1] )
                    # 替换其他列
                    for col in col_list[1:]:
                        col_int = int(col[1:]) -1
                        rollback_sql = rollback_sql.replace(col, ', '+ result_dict[table_name][col_int][1],1 )
                #SQL结尾加;
                rollback_sql = re.sub('\n$', ';', rollback_sql)
                rollback_sql = re.sub('\n', '', rollback_sql)
                rollback_sql = re.sub(';', ';\n', rollback_sql)
                fileOutput.write(rollback_sql)
            except IndexError as e:
                print ("Error:%s" % str(e))
                sys.exit()
    print ("done!")




if __name__ == '__main__':
    getops_parse_binlog()
    init_clo_name()
    gen_rollback_sql()
