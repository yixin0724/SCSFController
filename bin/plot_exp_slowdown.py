""" 箱线图分析了一组实验作业的减速情况。
从数据库中读取数据。它从28个实验中读取数据，并将其与另一个被认为具有理想减速的实验进行比较。
它假设了一些实验条件（时间、算法、工作流和边缘键）。

用法:
python plot_exp_slowdown.py (first_exp_id) (no_slowdown_exp_id)

Args:
- first_exp_id: 该系列中第一个实验的数字id。
- no_slowdown_exp_id: 带有理想“减速”的数字id。


Env vars:
- ANALYSIS_DB_HOST: hostname of the system hosting the database.
- ANALYSIS_DB_NAME: database name to read from.
- ANALYSIS_DB_USER: user to be used to access the database.
- ANALYSIS_DB_PASS: password to be used to used to access the database.
- ANALYSIS_DB_PORT: port on which the database runs.
"""


import sys

import matplotlib

from commonLib.nerscPlot import (paintHistogramMulti, paintBoxPlotGeneral,
                                 paintBarsHistogram)
from orchestration import get_central_db
from orchestration.definition import ExperimentDefinition
from stats.trace import ResultTrace

"""
本模块用于分析不同实验条件下的作业减速情况，并生成对应的比较箱线图。

主要功能：
1. 从命令行参数获取基准实验ID和无工作流减速实验ID
2. 从中央数据库加载实验数据
3. 计算不同核心使用量区间的作业减速指标
4. 按时间策略、调度算法、工作流类型进行分类统计
5. 生成核心使用量分组的减速比较箱线图
"""


matplotlib.use('Agg')

# 参数校验与初始
if len(sys.argv)<3:
    raise ValueError("At two integert argument must specified with the trace_id"
                     " to plot: first_exp_id, no_slowdown_exp_id")
first_id = int(sys.argv[1])
no_wf_slowdown_id=int(sys.argv[1])

# 实验参数范围定义
last_id=first_id+27
time_keys={60:"60/h", 600:"6/h", 1800:"2/h", 3600:"1/h"}    # 工作流提交频率映射
algo_keys={"manifest":"wfware", "single":"single", "multi":"dep"}   # 调度算法映射
workflow_keys={"floodplain.json":"floodP", "synthLongWide.json":"longWide",
               "synthWideLong.json":"wideLong"} # 工作流类型映射
edge_keys= {0: "[0,48] core.h", 48*3600:"(48, 960] core.h", 
            960*3600:"(960, inf.) core.h"}  # 核心使用量区间映射




db_obj = get_central_db()

def get_slowdown(db_obj, trace_id):
    """
       获取指定实验的作业减速数据
       参数：
       db_obj: Database  - 数据库连接对象
       trace_id: int     - 实验追踪ID
       返回值：
       dict - 按核心使用量区间分组的作业减速字典，结构：{edge: [slowdown_values]}
       """
    # 加载实验数据和结果追踪
    rt = ResultTrace()
    rt.load_trace(db_obj, trace_id)
    exp = ExperimentDefinition()
    exp.load(db_obj, trace_id)

    # 获取核心使用量分组的作业时间指标
    (jobs_runtime, jobs_waittime, jobs_turnaround, jobs_timelimit,
                jobs_cores_alloc, jobs_slowdown, jobs_timesubmit) = (
                                    rt.get_job_times_grouped_core_seconds(
                                    exp.get_machine().get_core_seconds_edges(),
                                    exp.get_start_epoch(),
                                    exp.get_end_epoch(), True))
                
    return jobs_slowdown

# 初始化数据结构存储所有实验结果
jobs_slowdown={}
no_wf_slowdown=get_slowdown(db_obj, no_wf_slowdown_id)   # 获取无工作流干扰的基准数据

# 处理所有实验数据
for trace_id in range(first_id, 
                    last_id):
    print "TRACE_ID", trace_id
    exp = ExperimentDefinition()
    exp.load(db_obj, trace_id)

    # 处理未完成实验的情况
    if not exp.is_it_ready_to_process():
        print "not ready"
        slowdown = {}
        for edge in edge_keys:
            slowdown[edge] = []
    else:
        slowdown = get_slowdown(db_obj, trace_id)
    # 按维度分类存储数据
    for edge in slowdown:
        edge_key=edge_keys[edge]
        workflow_key=workflow_keys[exp._manifest_list[0]["manifest"]]
        time_key=time_keys[exp._workflow_period_s]
        algo_key=algo_keys[exp._workflow_handling]
        comb_key=time_key+"\n"+algo_key # 组合时间策略和算法作为分类键

        # 初始化嵌套字典结构
        if edge_key not in jobs_slowdown.keys():
            jobs_slowdown[edge_key] = {}
        # 存储基准数据和实验数据
        if workflow_key not in jobs_slowdown[edge_key].keys():
            jobs_slowdown[edge_key][workflow_key]={}
            jobs_slowdown[edge_key][workflow_key]["0/h"]=no_wf_slowdown[edge]
        jobs_slowdown[edge_key][workflow_key][comb_key]=slowdown[edge]
        
# 设置不同核心区间的Y轴显示范围
yLim={"[0,48] core.h": (0, 1000),
      "(48, 960] core.h":(0,100),
      "(960, inf.) core.h":(0,20)}

# 生成并保存箱线图
for edge in jobs_slowdown:
    paintBoxPlotGeneral("SlowDownCompare: {0}".format(edge),
                jobs_slowdown[edge], labelY="slowdow", 
                yLogScale=True,
                graphFileName="slowdown/firstcompare-boxplot-{0}".format(edge),
                yLim=yLim[edge]) 
