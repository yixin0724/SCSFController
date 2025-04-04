"""
这个脚本是如何对系统工作负载建模的一个示例。它从数据库中读取作业日志跟踪并保存模型。

如果原始数据格式不在数据库中或不同的数据库方案中，则应该重新定义load_from_db。将模型保存在数据文件夹中。

Parameters:
- Env vars: NERSCDB_USER、NERSCDB_PASS包含连接数据库的用户名和密码。
- 如果db_is_local，则假定数据库在sstandard端口上是本地的。
- 如果不是，db_is_local db端口为5050，意味着ssh-fwd到远程db。
 
Trace数据库应该具有以下模式 (torque db format):
 
 CREATE TABLE `summary` (
  `stepid` varchar(64) NOT NULL DEFAULT '',
  `jobname` varchar(64) DEFAULT NULL,
  `owner` varchar(8) DEFAULT NULL,
  `account` varchar(8) DEFAULT NULL,
  `jobtype` varchar(32) DEFAULT NULL,
  `cores_per_node` smallint(6) DEFAULT NULL,
  `numnodes` int(11) DEFAULT '1',
  `class` varchar(64) DEFAULT NULL,
  `status` varchar(64) DEFAULT NULL,
  `dispatch` bigint(20) DEFAULT NULL,
  `start` bigint(20) DEFAULT NULL,
  `completion` bigint(20) NOT NULL DEFAULT '0',
  `queued` bigint(20) DEFAULT NULL,
  `wallclock` bigint(20) DEFAULT NULL,
  `mpp_secs` bigint(20) DEFAULT NULL,
  `wait_secs` bigint(20) DEFAULT NULL,
  `raw_secs` bigint(20) DEFAULT NULL,
  `superclass` varchar(64) DEFAULT NULL,
  `wallclock_requested` bigint(20) DEFAULT '0',
  `hostname` varchar(16) NOT NULL DEFAULT 'franklin',
  `memory` bigint(20) DEFAULT '0',
  `created` bigint(20) DEFAULT '0',
  `refund` char(1) DEFAULT '',
  `tasks_per_node` int(11) DEFAULT '0',
  `vmemory` bigint(20) DEFAULT '0',
  `nodetype` varchar(20) DEFAULT '',
  `classG` varchar(64) DEFAULT NULL,
  `filtered` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`stepid`,`completion`),
  KEY `owner` (`owner`,`completion`),
  KEY `account-completion` (`account`,`completion`),
  KEY `topsearch` (`completion`,`owner`),
  KEY `hostname-completion` (`hostname`,`completion`),
  KEY `hostname` (`hostname`,`owner`),
  KEY `starthost` (`start`,`hostname`),
  KEY `start` (`start`),
  KEY `startASChostfilter` (`start`,`hostname`,`filtered`) USING BTREE,
  KEY `created` (`created`),
  KEY `createdASChostfilter` (`created`,`hostname`,`filtered`),
  KEY `hostnameAlone` (`hostname`),
  KEY `queuedTime` (`queued`,`hostname`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 

"""
import datetime

from machines import Edison

db_is_local = True

# Machine class. Edison
edison = Edison()

start = datetime.date(2015, 1, 1)
end = datetime.date(2015, 12, 31)
print
"Loading workload trace and generating model..."
edison.load_from_db(start, end, db_is_local)
print
"Saving model..."
# 是否将模型保存为2015-edison
edison.save_to_file("./data", "2015")
print
"DONE"
