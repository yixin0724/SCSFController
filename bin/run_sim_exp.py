"""
在指定主机名作为输入参数的工作虚拟机上对单个实验运行模拟。
它需要一个数据库来存储仿真结果，并且可以访问worker中的slurm数据库。数据库访问数据通过环境变量进行配置。

实验信息是从中央数据库中提取的。

Usage:

python run_sim_exp-py [ip_of_simulator] [trace_id]

- ip_of_simulator: 运行模拟的机器的IP地址。
- trace_id: 如果设置，它只运行具有该trace_id的实验，如果没有设置，它将以"fresh"状态运行所有实验。

Env vars:
- ANALYSIS_DB_HOST: 中央数据库所在系统的主机名。
- ANALYSIS_DB_NAME: 写入中心和读取实验信息的数据库名称。
- ANALYSIS_DB_USER: 访问中心数据库的用户。
- ANALYSIS_DB_PASS: 用于访问中心数据库的密码。
- ANALYSIS_DB_PORT: 中心数据库运行的端口。
- SLURM_DB_NAME: slurm中slurm worker的数据库名称。如果没有设置，则取slurm_acct_db。
- SLURMDB_USER: 访问slurm数据库的用户。
- SLURMDB_PASS: 访问slurm数据库的密码。
- SLURMDB_PORT: slurm数据库运行的端口。

 
"""
import os
import random
import sys
import time

from orchestration import ExperimentWorker
from orchestration import get_central_db, get_sim_db
from orchestration.running import ExperimentRunner


# 设置模拟器的默认IP地址
simulator_ip = "192.168.56.24"

# 如果命令行提供了至少两个参数，则使用第一个额外参数作为模拟器的IP地址
if len(sys.argv)>=2:
    simulator_ip = sys.argv[1]
    
# 初始化跟踪ID为None
trace_id=None

# 如果命令行提供了至少三个参数，则使用第二个额外参数作为跟踪ID
if len(sys.argv)>=3:
    trace_id=sys.argv[2]

# 获取环境变量 SIM_MAX_WAIT 的值，如果存在则用于设置最大等待时间
mysleep=os.getenv("SIM_MAX_WAIT", None)

# 如果设置了最大等待时间，则在此范围内随机选择一个等待时间并暂停执行
if mysleep is not None:
    sleep_time=random.randrange(int(mysleep))
    print ("Doing a wait before starting ({0}): {1}s".format(simulator_ip, sleep_time)) # 打印即将开始的等待时间和对应的模拟器IP
    time.sleep(sleep_time)
    print ("Wait done, let's get started...")

# 配置实验运行器的各种参数，包括跟踪文件夹位置、是否本地运行、调度器配置目录等
ExperimentRunner.configure(
           trace_folder="/tmp/",  # 跟踪文件保存路径
           trace_generation_folder=os.getenv("TRACES_TMP_DIR", "tmp"),  # 跟踪生成文件夹路径，默认为'tmp'
           local=False, # 是否本地运行
           run_hostname=simulator_ip,       # 运行主机名（模拟器IP）
           run_user=None,        # 运行用户，默认为None
           scheduler_conf_dir="/scsf/slurm_conf",      # 调度器配置目录
           local_conf_dir="configs/",       # 本地配置目录
           scheduler_folder="/scsf/",       # 调度器文件夹
           manifest_folder="manifests")     # 清单文件夹

# 获取中心数据库对象
central_db_obj = get_central_db()

# 获取特定于模拟器的数据库对象
sched_db_obj = get_sim_db(simulator_ip)

# 创建实验工作者实例
ew = ExperimentWorker()

# 开始执行工作，传递中心数据库对象、模拟器数据库对象以及跟踪ID给do_work方法
ew.do_work(central_db_obj, sched_db_obj, trace_id=trace_id)
