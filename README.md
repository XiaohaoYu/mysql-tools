# mysql-tools
# mysql数据库日常运维实用的python脚本

1. mysqlbinlogparse.py   # 对mysqlbinlog进行解析，统计表的DML操作次数。
执行：   python3 mysqlbinlogparse.py mysql-bin.000010
输出：  
开始时间:  2019-04-15 20:22:06 
结束时间:  2019-04-30  1:47:32 

DML总次数:  18395234
  Insert总次数:  9161095
  Delete总次数:  9163882
  Update总次数:  70257

Insert 统计: 
  DB.Table: zabbix.history_uint ,  Count: 5284051
  DB.Table: zabbix.history ,  Count: 3707065


Delete 统计: 
  DB.Table: zabbix.item_discovery ,  Count: 66690
  DB.Table: zabbix.sessions ,  Count: 1622


Update 统计: 
  DB.Table: zabbix.history_uint ,  Count: 5241448
  DB.Table: zabbix.history ,  Count: 3708638



2. binlog2sql.py   # 对mysqlbinlog进行解析，生成回滚语句
执行：python rollback.py -f 'mysql-bin.000001' -o '/tmp/rollback.sql' -h 192.168.0.1 -u 'user' -p 'pwd' -P 3307 -d dbname


3. mysql_backup.py    # mysql备份脚本，推送微信信息
执行： python mysql_backup.py
微信:  Title:  MySQL备份信息
	   Content:  时间:20190520,10.157.138.200_5637数据库备份成功,文件传输成功。


4. wechat.py		 # 微信推送信息脚本。
执行:   python wechat.py  title  content


5. mysql2excel.py      #mysql导出数据到excel
