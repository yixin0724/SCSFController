""" 绘制了一个实验的作业和工作流程结果的cdf。
它还打印实验的现有状态的数值。
它使用run_analysis_XXX.py脚本生成的数据库中预先计算的数据。

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
matplotlib.use('Agg')
import os
import sys

from orchestration import get_central_db
from orchestration.definition import ExperimentDefinition
from plot import histogram_cdf
from stats.trace import ResultTrace


db_obj = get_central_db()

if len(sys.argv)<2:
    raise ValueError("At least one argument must specified with the trace_id"
                     " to plot.")
trace_id = int(sys.argv[1])
arg_name=None
dest_dir="./out"
if not(os.path.exists(dest_dir)):
    os.makedirs(dest_dir)
    
if len(sys.argv)==3:
    arg_name = sys.argv[2]
    
ed = ExperimentDefinition()
ed.load(db_obj, trace_id)
rt = ResultTrace()
rt.load_analysis(db_obj, trace_id)
if arg_name is None:
    arg_name = ed._name
  
for (key, result) in rt.jobs_results.iteritems():
    if "_cdf" in key:
        bins, edges = result.get_data()
        histogram_cdf(edges, bins, key, file_name=arg_name+"-"+key, 
                      x_axis_label=key, y_axis_label="Norm share",
                      target_folder=dest_dir, do_cdf=True,
                      x_log_scale=True)
    elif "_stats" in key:
        print key, result.get_data()

for (key, result) in rt.workflow_results.iteritems():
    if "_cdf" in key:
        bins, edges = result.get_data()
        if bins is None or edges is None:
            print key, "no workflows detected"
            continue
        histogram_cdf(edges, bins, key, file_name=arg_name+"-"+key, 
                      x_axis_label=key, y_axis_label="Norm share",
                      target_folder=dest_dir, do_cdf=True,
                      x_log_scale=True)
    elif "_stats" in key:
        print key, result.get_data()
        