# yuxh_useful
# 一些mysql数据库日常运维实用的python脚本

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
