"""
为trace_id标识的实验生成跟踪文件。将结果跟踪文件存储在tmp/[name]中，其中包含实验的名称。

usage:

python generate_trace.pt trace_id

trace_id: numeric id of the experiment.
"""

from orchestration import ExperimentDefinition
from orchestration.running import ExperimentRunner
from orchestration import get_central_db

import sys
    

trace_id=None
# 检查命令行参数数量，以确定是否提供了trace_id
if len(sys.argv)>=2:
    # 如果提供了trace_id，将其赋值给变量trace_id
    trace_id=sys.argv[1]
else:
    # 如果没有提供trace_id，打印错误消息并退出程序
    print "Missing experiment trace_id."
    exit()
# 配置ExperimentRunner，设置实验所需的文件夹路径和其他配置
ExperimentRunner.configure(
           trace_folder="/home/gonzalo/cscs14038bscVIII",
           trace_generation_folder="tmp", 
           local=False,
           run_user=None,
           scheduler_conf_dir="/home/gonzalo/cscs14038bscVIII/slurm_conf",
           local_conf_dir="configs/",
           scheduler_folder="/home/gonzalo/cscs14038bscVIII",
           manifest_folder="manifests")

# 获取中央数据库对象
central_db_obj = get_central_db()
# 创建ExperimentDefinition实例
ed = ExperimentDefinition()
# 加载实验定义，使用中央数据库对象和trace_id
ed.load(central_db_obj, trace_id)
# 创建ExperimentRunner实例，传入实验定义对象
er = ExperimentRunner(ed)
# 生成实验所需的跟踪文件
er._generate_trace_files(ed)
