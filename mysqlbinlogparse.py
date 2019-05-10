# coding:utf-8
# Author: YuXiaohao
# 解析binlog，统计热度表，表的DML个数


import sys
import os



# mysqlbinlog解析binlog日志
def binlog_output():
    binlog_file = sys.argv[1]
    file_num = binlog_file.split('.')[1]

    binlog_log = 'binlog_%s.log' % file_num
    os.system('/usr/local/mysql/bin/mysqlbinlog -v --base64-output=decode-rows %s > %s' %(binlog_file, binlog_log))

    return binlog_log

# 对binlog日志处理
def binlog_parse(binlog_log):

    delete_count = 0
    update_count = 0
    insert_count = 0
    update_li = []
    insert_li = []
    delete_li = []
    stop_time_li = []

    binlog_f = open(binlog_log, 'r',encoding='utf-8')
    for line in binlog_f.readlines():
        if line.startswith('### INSERT INTO'):
            insert_count += 1
            insert_li.append(line[16:].replace('`', '').strip())
        if line.startswith('### UPDATE'):
            update_count += 1
            update_li.append(line[11:].replace('`', '').strip())
        if line.startswith('### DELETE FROM'):
            delete_count += 1
            delete_li.append(line[16:].replace('`','').strip())
        if 'Start: binlog' in line:
            start_time = line.split('server')[0].replace('#', '')
        if 'end_log_pos' in line:
            stop_time_li.append(line)
    binlog_f.close()
    stop_time = stop_time_li[-1].split('server')[0].replace('#', '')

    start_time = '20' + start_time[:2] + '-' + start_time[2:4] + '-' + start_time[4:]
    stop_time = '20' + stop_time[:2] + '-' + stop_time[2:4] + '-' + stop_time[4:]

    return delete_count,update_count,insert_count,update_li,insert_li,delete_li,start_time,stop_time

# 对库和表进统计排序
def DbTableCount(arr):
    result = {}
    for i in set(arr):
        result[i] = arr.count(i)
    sort_li = sorted(result.items(), key=lambda x: x[1], reverse=True)

    return sort_li



if __name__ == '__main__':

    binlog_log = binlog_output()
    delete_count, update_count, insert_count, update_li, insert_li, delete_li, start_time,stop_time= binlog_parse(binlog_log)
    sort_update_li = DbTableCount(delete_li)
    sort_inser_li  = DbTableCount(insert_li)
    sort_delete_li = DbTableCount(update_li)

    # 删除解析日志
    os.system('rm -f %s' % binlog_log)

    # 收集时间
    print('开始时间: ', start_time)
    print('结束时间: ', stop_time)

    # 统计DML总次数
    print('\nDML总次数: ', insert_count+update_count+delete_count)

    # 统计DML次数
    print('  Insert总次数: ', insert_count)
    print('  Delete总次数: ', delete_count)
    print('  Update总次数: ', update_count)

    # Insert统计
    print('\nInsert 统计: ')
    for i in sort_inser_li:
        print('  DB.Table:', i[0], ',  Count:', i[1])

    # Delete统计
    print('\nDelete 统计: ')
    for i in sort_delete_li:
        print('  DB.Table:', i[0], ',  Count:', i[1])

    # Update统计
    print('\nUpdate 统计: ')
    for i in sort_update_li:
        print('  DB.Table:', i[0], ',  Count:', i[1])