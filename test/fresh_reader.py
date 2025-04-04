from commonLib.DBManager import DB
from orchestration.definition import ExperimentDefinition

import os

# 创建数据库对象，连接测试数据库
# 使用环境变量提供的数据库信息，如果没有提供，则使用默认值
db_obj  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                   os.getenv("TEST_DB_NAME", "test"),
                   os.getenv("TEST_DB_USER", "root"),
                   os.getenv("TEST_DB_PASS", ""))

# 初始化标志变量，用于指示是否有更多的数据需要处理
there_are_more=True
# 初始化列表，用于存储处理过的实验定义的追踪ID
ids = []
# 循环以加载所有可用的实验定义
while there_are_more:
    # 创建一个新的实验定义对象
    ed_f = ExperimentDefinition()
    # 尝试从数据库中加载新的实验定义
    there_are_more  = ed_f.load_fresh(db_obj)
    # 如果成功加载，将实验定义的追踪ID添加到列表中
    if there_are_more:
        ids.append(ed_f._trace_id)

# 打印所有处理过的实验定义的追踪ID
print "END2:", ids
# 打印结束标志
print "END3"