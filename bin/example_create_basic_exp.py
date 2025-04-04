""" 这个脚本创建了一个运行三天小时的实验集：两个是预加载，一个是实际跟踪。它不包括工作流，并且使用假的Edison模型（不是实际的Edison系统）。

Env vars:
- ANALYSIS_DB_HOST: 数据库所在系统的主机名。
- ANALYSIS_DB_NAME: 要读取的数据库名称。
- ANALYSIS_DB_USER: 访问数据库的用户。
- ANALYSIS_DB_PASS: 用于访问数据库的密码。
- ANALYSIS_DB_PORT: 数据库运行的端口。
""" 

from orchestration.definition import ExperimentDefinition
from orchestration import get_central_db

import sys

db_obj = get_central_db()

overload=0.0

if len(sys.argv)>=2:
    overload=float(sys.argv[1])


exp = ExperimentDefinition(
                 seed="AAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[],
                 workflow_policy="no",
                 workflow_period_s=0,
                 workflow_handling="single",
                 workload_duration_s=3600*24*7,
                  preload_time_s = 0,
                 overload_target=overload)
exp.store(db_obj)

