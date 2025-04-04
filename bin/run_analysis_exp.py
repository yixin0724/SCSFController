"""
对已模拟但未分析的单个实验运行分析。数据读取和写入通过环境变量配置的数据库。

Env vars:
- ANALYSIS_DB_HOST: 数据库所在系统的主机名。
- ANALYSIS_DB_NAME: database name to read from.
- ANALYSIS_DB_USER: user to be used to access the database.
- ANALYSIS_DB_PASS: password to be used to used to access the database.
- ANALYSIS_DB_PORT: port on which the database runs. 
"""
from orchestration import AnalysisWorker
from orchestration import get_central_db
from orchestration.running import ExperimentRunner
import sys  # 导入sys模块，用于访问命令行参数

# 配置ExperimentRunner，设置实验运行所需的各项参
ExperimentRunner.configure(
           trace_folder="/home/gonzalo/cscs14038bscVIII",
           trace_generation_folder="tmp", 
           local=False,
           run_user=None,
           scheduler_conf_dir="/home/gonzalo/cscs14038bscVIII/slurm_conf",
           local_conf_dir="configs/",
           scheduler_folder="/home/gonzalo/cscs14038bscVIII",
           manifest_folder="manifests")
# 初始化trace_id为None，后续将根据命令行参数决定其值
trace_id=None
# 检查命令行参数数量，以确定是否提供trace_id
if len(sys.argv)>=2:
    trace_id=sys.argv[1]
# 获取中央数据库对象，用于分析任务中的数据交互
central_db_obj = get_central_db()
# 创建AnalysisWorker实例，用于执行单个分析任务
ew = AnalysisWorker()
# 调用do_work_single方法执行单个分析任务，传入中央数据库对象和trace_id
ew.do_work_single(central_db_obj, trace_id=trace_id)
