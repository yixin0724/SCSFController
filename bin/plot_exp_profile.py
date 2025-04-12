""" 绘制numericStats表中指定实验id的作业和工作流程结果的cdf。
它还打印实验的现有状态的数值。

Usage:

python ./plot_exp_profile.py trace_id [name]

Args:
- trace_id: numeric id of the experiment to plot and print about.
- name: 实验的名称，附加到输出文件中。如果没有设置，则使用数据库名称。

Output:
- 输出waittime、turnaround、slowdown、runtime、requested_wc、cpus_alloc等指标的cdf图
- 绘图PNG文件存放在“./out”文件夹中。

Env vars:
- ANALYSIS_DB_HOST: hostname of the system hosting the database.
- ANALYSIS_DB_NAME: database name to read from.
- ANALYSIS_DB_USER: user to be used to access the database.
- ANALYSIS_DB_PASS: password to be used to used to access the database.
- ANALYSIS_DB_PORT: port on which the database runs.
"""
import matplotlib
matplotlib.use('Agg')   # 设置matplotlib为非交互式后端，适用于无GUI环境
import os
import sys

from orchestration import get_central_db
from orchestration.definition import ExperimentDefinition
from plot import histogram_cdf
from stats.trace import ResultTrace


"""
脚本功能：根据给定的trace_id生成实验结果统计图表及CDF图，并保存到指定目录

主要流程：
1. 配置matplotlib为非交互式后端并导入依赖
2. 连接中央数据库并验证输入参数
3. 加载实验定义和结果跟踪数据
4. 遍历任务结果和工作流结果生成可视化图表
"""

db_obj = get_central_db()

# 处理命令行参数
if len(sys.argv)<2:
    raise ValueError("At least one argument must specified with the trace_id"
                     " to plot.")
trace_id = int(sys.argv[1])
arg_name=None    # 图表文件名前缀，默认为实验名称
dest_dir="./out"     # 输出目录配置
if not(os.path.exists(dest_dir)):
    os.makedirs(dest_dir)
    
if len(sys.argv)==3:
    arg_name = sys.argv[2]   # 获取可选的图表文件名前缀参数

# 加载实验基础数据
ed = ExperimentDefinition()
ed.load(db_obj, trace_id)
# 加载实验结果分析数据
rt = ResultTrace()
rt.load_analysis(db_obj, trace_id)
if arg_name is None:
    arg_name = ed._name # 使用实验名称作为默认文件名前缀

# 处理任务级别结果数据
for (key, result) in rt.jobs_results.iteritems():
    # 生成CDF图表：键名包含'_cdf'的结果数据
    if "_cdf" in key:
        bins, edges = result.get_data()
        histogram_cdf(edges, bins, key, file_name=arg_name+"-"+key, 
                      x_axis_label=key, y_axis_label="Norm share",
                      target_folder=dest_dir, do_cdf=True,
                      x_log_scale=True)
    # 输出统计信息：键名包含'_stats'的结果数据
    elif "_stats" in key:
        print key, result.get_data()

# 处理工作流级别结果数据（逻辑与任务级别处理类似）
for (key, result) in rt.workflow_results.iteritems():
    if "_cdf" in key:
        bins, edges = result.get_data()
        if bins is None or edges is None:
            print key, "no workflows detected"  # 处理空数据情况
            continue
        histogram_cdf(edges, bins, key, file_name=arg_name+"-"+key, 
                      x_axis_label=key, y_axis_label="Norm share",
                      target_folder=dest_dir, do_cdf=True,
                      x_log_scale=True) # 生成对数坐标轴的CDF图
    elif "_stats" in key:
        print key, result.get_data()
        