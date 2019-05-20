#!/bin/env python
#coding:utf-8
#Author: Hogan
#Descript : MySQL数据库备份脚本

import os
import time
import paramiko
import logging

# 参数设置
TODAY = time.strftime("%Y%m%d", time.localtime())
# mysql_info
XTRABACKUP = "/usr/local/mysql5637/mysql/xtrabackup/bin/innobackupex"
MYSQL_CNF = "/usr/local/mysql5637/mysql/etc/my.cnf"
#MYSQL_CNF = "/usr/local/mysql5637/mysql/etc/my3306.cnf"
MYSQL_SOCKET = "/tmp/mysql5637.sock"
MYSQL_USER="backup"
MYSQL_PWD="backup"
MYSQL_IP_PORT = "10.157.138.200_5637"

# dir_info
LOCAL_DIR="/data/backup/"
BAK_DIR_NAME = "%s_hotfull_%s" % (MYSQL_IP_PORT,TODAY)

# ftp_info
FTP_HOST = '10.157.138.200'
FTP_PORT = 22
FTP_USER = 'mysql'
FTP_PWD = 'mysql'
LOCAL_FILE = LOCAL_DIR + BAK_DIR_NAME + '.tar.gz'
FTP_FILE = "/data/backup/backup137/" + BAK_DIR_NAME + '.tar.gz'

# 日志输出路径
DIRNAME = './'

# 定义日志
logging.basicConfig(filename='%smysql_backup.log' % DIRNAME,
                    format='%(asctime)s - %(name)s - %(levelname)s -%(module)s:  %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p',
                    level=10)

# 定义告警
TITLE = 'MySQL备份信息'
CONTENT = {'satus':'',
           'backup':'',
           'ftp':''}

# 时间装饰器
def calc_time(func):

    def calc(*args, **kwargs):
        begin_time = time.time()
        func(*args, **kwargs)
        cost_time = time.time() - begin_time
        logging.info("备份花费时间：%s秒" % cost_time)
        return cost_time
    return calc

# 备份mysql数据库
@calc_time
def mysql_backup():

    backup_cmd = "%s --defaults-file=%s  --socket=%s  --user=%s  --password=%s --no-timestamp --parallel=2  %s%s  " % (XTRABACKUP,MYSQL_CNF,MYSQL_SOCKET,MYSQL_USER,MYSQL_PWD,LOCAL_DIR,BAK_DIR_NAME)
    tar_cmd = "cd %s ; tar cvzf %s.tar.gz %s" % (LOCAL_DIR, BAK_DIR_NAME, BAK_DIR_NAME)
    rm_cmd = "rm -rf %s%s" % (LOCAL_DIR,BAK_DIR_NAME)

    # 清理目录
    os.system(rm_cmd)
    logging.info('------------------------------------------------华丽的分割线----------------------------------------------------')
    logging.info('开始备份:%s' % MYSQL_IP_PORT)
    ret = os.system(backup_cmd)

    if ret == 0:
        logging.info('备份完成')
        CONTENT['backup'] = '成功'
        logging.info('开始打包压缩')
        ret1 = os.system(tar_cmd)
        if ret1 == 0:
            logging.info('打包完成')
            logging.info('开始删除备份目录')
            ret2 = os.system(rm_cmd)
            if ret2 == 0:
                logging.info('备份目录删除成功')
                logging.info('备份完成！！！')
                # 清理过期备份集
                logging.info('清理过期备份集')
                os.system('find %s -mtime +5 -name "*.gz" | xargs rm -rf' % LOCAL_DIR )
            else:
                logging.error('备份目录删除失败')
        else:
            logging.error('打包失败')
    else:
        logging.error('备份失败')
        CONTENT['backup'] = '失败'

# 备份文件ftp到备份server
def ftp2_backup_server():
    try:
        logging.info('开始传输文件')
        transport = paramiko.Transport((FTP_HOST, FTP_PORT))
        transport.connect(username=FTP_USER, password=FTP_PWD)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(LOCAL_FILE, FTP_FILE)
        transport.close()
        logging.info('传输文件结束')
        CONTENT['ftp'] = '成功'
        CONTENT['satus'] = '成功'
    except Exception as e:
        logging.error(e)
        logging.error('传输文件失败')
        CONTENT['ftp'] = '失败'
        CONTENT['satus'] = '失败'
    return CONTENT




#发送微信告警
def send_to_wechat(CONTENT):
    # 发送微信信息
    CONTENT = '时间:%s,%s数据库备份%s,文件传输%s。' % (TODAY,MYSQL_IP_PORT, CONTENT['backup'], CONTENT['ftp'])
    w = os.system('python /root/python/wechat.py %s %s'% (TITLE, CONTENT))
    if w == 0:
        logging.info('微信告警发送成功')
    else:
        logging.error('微信告警发送失败')

if __name__ == '__main__':
    mysql_backup()
    CONTENT = ftp2_backup_server()
    send_to_wechat(CONTENT)
