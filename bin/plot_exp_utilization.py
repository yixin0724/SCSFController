"""
该脚本分析利用演变和在跟踪中提交的工作。
后者表示为：对于时间t，需要多少生产的核心小时能力来处理提交的核心小时。

输出是四个PNG文件，其中包含利用率和工作效率的变化：
- 作为线形图: "PL-Utilization-[trace_id]-[workflow_name].png"
- 作为线形图: "PL-UtilizationBack-[trace_id]-[workflow_name].png"
- 作为线形图: "PL-OnlyUtilization-[trace_id]-[workflow_name].png"
- 作为线形图: "PL-OnlyUtilizationBack-[trace_id]-[workflow_name].png"


Usage:
python plot_exp_utilization.py dest_dir trace_id

dest_dir: PNG文件存放的路径。
trace_id: 需要分析的跟踪记录的ID。

Env vars:
- ANALYSIS_DB_HOST: hostname of the system hosting the database.
- ANALYSIS_DB_NAME: database name to read from.
- ANALYSIS_DB_USER: user to be used to access the database.
- ANALYSIS_DB_PASS: password to be used to used to access the database.
- ANALYSIS_DB_PORT: port on which the database runs. 
"""
import sys

import matplotlib

from commonLib.nerscPlot import (paintScatterSeries, paintPlotMultiV2,
                                paintPlotMultiV2_axis)
from orchestration import get_central_db
from orchestration.definition import ExperimentDefinition
from stats.trace import ResultTrace


matplotlib.use('Agg')


"""
该脚本分析集群资源利用率随时间的变化情况，并生成可视化图表。
主要功能包括：
1. 从数据库加载实验配置和跟踪数据
2. 计算系统利用率、核心小时提交量等关键指标
3. 生成多种可视化图表展示利用率变化趋势
"""

db_obj = get_central_db()
target_dir="out/"   # 默认目标目录

if len(sys.argv)==3:
    target_dir= sys.argv[1] # 检查命令行参数是否正确
    trace_id = sys.argv[2]   # 跟踪记录ID
else:
    raise ValueError("Missing trace id to analyze")

# 加载实验配置
exp = ExperimentDefinition()
exp.load(db_obj, trace_id)  # 加载实验定义

# 加载跟踪数据
rt = ResultTrace()
rt.load_trace(db_obj, trace_id)  # 加载实验结果数据

# 获取机器配置信息
machine = exp.get_machine()
max_cores = machine.get_total_cores()  # 系统总核心数
max_submit_time = rt._lists_submit["time_submit"][-1]  # 最后提交时间

def adjust_ut_plot(ut_stamps, ut_values):
    """
    调整利用率曲线的时间戳，使折线图连续显示
    参数:
        ut_stamps - 原始时间戳列表
        ut_values - 原始利用率值列表
    返回:
        调整后的时间戳和值列表，每个值点前后插入相同时间点保持连续性
    """
    new_stamps=[]
    new_values=[]
    last_value=None
    for (st, vl) in zip(ut_stamps, ut_values):
        if last_value is not None:
            new_stamps.append(st)
            new_values.append(last_value)
        new_stamps.append(st)
        new_values.append(vl)
        last_value=vl
    return new_stamps, new_values


# 计算利用率、时间戳、累计浪费等指标
(integrated_ut, utilization_timestamps, utilization_values, acc_waste, 
    corrected_integrated_ut) = (
     rt.calculate_utilization(machine.get_total_cores(),
                            #do_preload_until=exp.get_start_epoch(),
                            #endCut=exp.get_end_epoch(),
                            store=False,
                            ending_time=max_submit_time))
# 调整利用率的时间戳和值
utilization_timestamps, utilization_values = adjust_ut_plot(
                                                        utilization_timestamps,
                                                        utilization_values)

# 计算工作负载指标（60秒聚合窗口）
acc_period=60
# 计算等待时间、提交的核心小时数等指标
(waiting_ch_stamps, waiting_ch_values,
 core_h_per_min_stamps, 
 core_h_per_min_values,
 waiting_requested_ch_values,
 requested_core_h_per_min_values) = rt.calculate_waiting_submitted_work_all(
                                                  acc_period,
                                                  ending_time=max_submit_time)

base_stamp=waiting_ch_stamps[0] # 基准时间戳
# 调整时间戳，使其相对于基准时间
utilization_timestamps=[x-base_stamp
                        for x in utilization_timestamps]
core_h_per_min_stamps=[x-base_stamp
                   for x in core_h_per_min_stamps]
waiting_ch_stamps=[x-base_stamp
                   for x in waiting_ch_stamps]


# 将利用率和等待时间归一化
utilization_values=[float(x)/float(max_cores) for x in utilization_values]
waiting_ch_values= [float(x)/(float(max_cores)*3600) for x in waiting_ch_values]
waiting_requested_ch_values= [float(x)/(float(max_cores)*3600) 
                       for x in waiting_requested_ch_values]
core_h_per_min_values= [float(x)/(float(max_cores)) 
                        for x in core_h_per_min_values]
stamp_dic={}
value_dic={}

# 准备绘图数据
stamp_dic["Utilization"]=utilization_timestamps
stamp_dic["Submit/Produced"]=core_h_per_min_stamps



value_dic["Utilization"]=utilization_values
value_dic["Submit/Produced"]=core_h_per_min_values


def get_sched_waits(trace_id):
    """
    计算调度间隔（作业开始时间之间的间隔）
    返回:
        sched_gaps_stamp - 调度间隔对应的时间戳
        sched_gaps - 调度间隔时长列表
    """
    rt = ResultTrace()
    rt.load_trace(db_obj, trace_id)
    machine = exp.get_machine()
    max_cores = machine.get_total_cores()
    
    
    start_times = rt._lists_start["time_start"]
    end_times = rt._lists_start["time_end"]
    id_jobs = rt._lists_start["id_job"]     # 作业ID列表
    
    sched_gaps = []
    sched_gaps_stamp = []
    the_max=0
    the_max_id=-1

    # 遍历计算连续作业开始时间差
    for s1, s2, id_job in zip(start_times[:-1], start_times[1:], id_jobs[1:]):
        if (s1!=0 and s2!=0):
            sched_gap=s2-s1
            if sched_gap>0:
                sched_gaps.append(sched_gap)
                sched_gaps_stamp.append(s2)
                if sched_gap>the_max:
                    the_max=sched_gap
                    the_max_id=id_job
    print "MAX scheduling interval：", the_max, "Job Id：",the_max_id
    return sched_gaps_stamp, sched_gaps

# 获取调度间隔数据
sched_gaps_stamp, sched_gaps = get_sched_waits(trace_id)
sched_gaps_stamp=[x-base_stamp
                   for x in sched_gaps_stamp]

value_dic_gap={"gaps":sched_gaps}
stamps_dic_gap={"gaps":sched_gaps_stamp}


def extract_workflow(field):
    """
    精确模式匹配
    适用于严格遵循 m[version|filename.json] 格式的情况
    """
    # 找到包含模式的分段
    target_segment = next((s for s in field.split('-') if s.startswith('m[')), None)

    if target_segment:
        # 提取方括号内的内容
        content = target_segment.split('[', 1)[1].split(']', 1)[0]
        # 分割并处理文件名
        filename = content.split('|', 1)[-1]  # synthLongWide.json
        return filename.rsplit('.', 1)[0]  # 去除扩展名
    return field  # 默认返回原字段

# 生成图表文件名
workflow_name = extract_workflow(exp._name)
name = "{0}-{1}".format(trace_id, workflow_name)


# 绘制带调度间隔的利用率图
paintPlotMultiV2_axis("PL-Utilization\n{0}".format(name),
          stamp_dic, value_dic,
           dir=target_dir, graphFileName="PL-Utilization-{0}".format(name),
           xLim=(0,exp.get_end_epoch()-base_stamp), \
           labelX="time", 
       labelY="System share", \
          logScale=False, trend=False, tickFrequence=None, hLines=[], \
              xLogScale=False, alpha=1.0, legendLoc=0, \
                  fontSizeX=12,
             second_axis_series_dic=stamps_dic_gap,
             second_axis_values_dic=value_dic_gap)

stamp_dic["Waiting work"]=waiting_ch_stamps
value_dic["Waiting work"]=waiting_ch_values

stamp_dic["Waiting requested work"]=waiting_ch_stamps
value_dic["Waiting requested work"]=waiting_requested_ch_values


# 绘制利用率、积压和压力图
paintPlotMultiV2("PL-Utilization, backlog, Pressure\n{0}".format(name),
          stamp_dic, value_dic,
           dir=target_dir, graphFileName="PL-UtilizationBack-{0}".format(name),
           xLim=(0,exp.get_end_epoch()-base_stamp), 
           labelX="time", 
       labelY="System share", \
          logScale=False, trend=False, tickFrequence=None, hLines=[], \
              xLogScale=False, alpha=1.0, legendLoc=0, \
                  fontSizeX=12)
del stamp_dic["Submit/Produced"]
del value_dic["Submit/Produced"]

# 绘制仅包含利用率和积压的图
paintPlotMultiV2("PL-Only utilization and backlog\n{0}".format(name),
          stamp_dic, value_dic,
           dir=target_dir, graphFileName="PL-OnlyUtilizationBack-{0}".format(name),
           xLim=(0,exp.get_end_epoch()-base_stamp), 
           labelX="time", 
       labelY="System share", \
          logScale=False, trend=False, tickFrequence=None, hLines=[], \
              xLogScale=False, alpha=1.0, legendLoc=0, \
                  fontSizeX=12)
del stamp_dic["Waiting work"]
del value_dic["Waiting work"]
del stamp_dic["Waiting requested work"]
del value_dic["Waiting requested work"]

# 绘制仅包含利用率的图
paintPlotMultiV2("PL-Only utilization\n{0}".format(name),
          stamp_dic, value_dic,
           dir=target_dir, graphFileName="PL-OnlyUtilization-{0}".format(name),
           xLim=(0,exp.get_end_epoch()-base_stamp), 
           labelX="time", 
       labelY="System share", \
          logScale=False, trend=False, tickFrequence=None, hLines=[], \
              xLogScale=False, alpha=1.0, legendLoc=0, \
                  fontSizeX=12, yLim=(0, 1.2))
