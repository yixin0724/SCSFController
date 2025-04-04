"""
在trace_id传递的参数中计算每分钟等待时间的中位数。

它产生了四个图：
    -两个图表（对数和线性比例），显示在任何给定时刻等待的作业数量。
    -在任何给定时刻提交的作业的等待时间中值的两个图（对数和线性比例）。
在当前的wait_time目录下
    
Usage:

python ./plot_exp_waittime_in_time.py (trace_id) [pbs_hostname pbs_db_name start stop] 

Args:
- trace_id: numeric id of the experiment to plot and print about.
(next are optional and only used when reading from a torque format db)
- pbs_hostname: 如果设置了trace_id，则忽略trace_id，并从pbs_hostname中运行的数据库读取数据。
- pbs_db_name: NERSC格式扭矩作业日志数据库的名称。
- start: 表示分析应该开始的历元时间的整数。
- stop: 表示分析应该停止的历元时间的整数。

Env vars:
- ANALYSIS_DB_HOST: hostname of the system hosting the database.
- ANALYSIS_DB_NAME: database name to read from.
- ANALYSIS_DB_USER: user to be used to access the database.
- ANALYSIS_DB_PASS: password to be used to used to access the database.
- ANALYSIS_DB_PORT: port on which the database runs.
"""
from numpy import median
import os
import sys

from dateutil.parser import parse

from commonLib.nerscPlot import paintPlotMultiV2
from generate import TimeController
from orchestration import get_central_db
from orchestration.definition import ExperimentDefinition
from stats.trace import ResultTrace


db_obj = get_central_db()

if len(sys.argv)<2:
    raise ValueError("At least one argument must specified with the trace_id"
                     " to plot.")
do_pbs=False
start_date=None
end_date=None

# 如果提供了5个或更多参数，表示使用PBS数据库
if len(sys.argv)>=5:
    do_pbs=True
    pbs_hostname=pbs_db_name=sys.argv[2]
    pbs_db_name=sys.argv[3]
    start_date=TimeController.get_epoch(parse(sys.argv[4]))
    end_date=TimeController.get_epoch(parse(sys.argv[5]))
trace_id = int(sys.argv[1])

# 创建输出目录
dest_dir="./wait_time"
if not(os.path.exists(dest_dir)):
    os.makedirs(dest_dir)

exp = ExperimentDefinition()
rt = ResultTrace()
if not do_pbs:
    exp.load(db_obj, trace_id)
    rt.load_trace(db_obj, trace_id)
else:
    db_obj.dbName=pbs_db_name
    exp._machine="edison"
    exp._name="pbsload-m{0}-{1}-{2}".format(pbs_hostname, start_date, end_date)
    rt.import_from_pbs_db(db_obj, "summary", start=start_date, end=end_date, 
                          machine=pbs_hostname)



name="{0}_{1}".format(trace_id, exp._name)



def get_waittimes_median_per_period_corrected(submit_stamps_list,
                                              start_stamps_list,
                                              period=60):
    """
    计算每个时间段内的等待时间中位数

    Args:
        submit_stamps_list: 提交时间戳列表
        start_stamps_list: 开始时间戳列表
        period: 时间段长度，默认为60秒
    Returns:
        final_stamps: 时间戳列表
        final_medians: 等待时间中位数列表
        final_formal_stamps: 正式时间戳列表
        final_formal_medians: 正式等待时间中位数列表
    """
    if not submit_stamps_list:
        return [], []
    period_start=submit_stamps_list[0]
    active_submit_stamps = []
    active_start_stamps = {}
    
    sorted_starts=list(set(start_stamps_list))
    sorted_starts.sort()
    
    
    
    final_stamps=[]
    final_medians=[]
    final_formal_medians=[]
    final_formal_stamps=[]
    
    
    formal_wait_times=[]
    
    for (submit, start) in zip(submit_stamps_list, start_stamps_list):
        if start>0:
            formal_wait_times.append(start-submit)
            try:
                active_start_stamps[start]
            except KeyError:
                active_start_stamps[start]=[]
            active_start_stamps[start].append(submit)
        
        # add the job as submitted
        active_submit_stamps.append(submit)
        # check if jobs have ended since the last time we checked.
        while sorted_starts and submit>sorted_starts[0]:
            if sorted_starts[0]>0:
                for starting_job in active_start_stamps[sorted_starts[0]]:
                    active_submit_stamps.remove(starting_job)
                del active_start_stamps[sorted_starts[0]]
            del sorted_starts[0]
        if submit-period_start>period:
            current_waits = [submit-x for x in active_submit_stamps]
            final_stamps.append(submit)
            final_medians.append(median(current_waits))
            if formal_wait_times:
                final_formal_medians.append(median(formal_wait_times))
                final_formal_stamps.append(submit)
            formal_wait_times=[]
            period_start=submit
            
        
    return (final_stamps, final_medians, final_formal_stamps,
            final_formal_medians)

edges = exp.get_machine().get_core_seconds_edges()
(jobs_runtime, jobs_waittime, jobs_turnaround, jobs_timelimit,
                jobs_cores_alloc, jobs_slowdown, jobs_timesubmit) = (
                                    rt.get_job_times_grouped_core_seconds(
                                    edges))
(jobs_values_dic) = (rt.get_job_values_grouped_core_seconds(edges))
 

stamp_dic={}
values_dic={}
stamps_dic_corrected={}
values_dic_correced={}

stamps_dic_formal={}
values_dic_formal={}

first_stamp=0
for edge in edges:
    if jobs_values_dic["time_submit"][edge]:
        the_val=jobs_values_dic["time_submit"][edge][0]
        if not first_stamp:
            first_stamp=the_val
        else:
            first_stamp=min(first_stamp, the_val)

for edge in edges:
    (stamps_dic_corrected[edge], values_dic_correced[edge],
     stamps_dic_formal[edge], values_dic_formal[edge]) =( 
       get_waittimes_median_per_period_corrected(
                                         jobs_values_dic["time_submit"][edge],
                                         jobs_values_dic["time_start"][edge]))
    stamps_dic_corrected[edge]=[float(x-first_stamp)/3600 
                                for x in stamps_dic_corrected[edge]]
    stamps_dic_formal[edge]=[float(x-first_stamp)/3600 
                                for x in stamps_dic_formal[edge]]

(all_stamps, all_values, all_stamps_formal, all_values_formal)=(
                    get_waittimes_median_per_period_corrected(
                                    rt._lists_submit["time_submit"],
                                    rt._lists_submit["time_start"]))
stamps_dic_corrected["all"]=all_stamps
stamps_dic_corrected["all"]=[float(x-first_stamp)/3600 
                                for x in stamps_dic_corrected["all"]]
stamps_dic_formal["all"]=all_stamps_formal
stamps_dic_formal["all"]=[float(x-first_stamp)/3600 
                                for x in stamps_dic_formal["all"]]

values_dic_correced["all"]=all_values
values_dic_formal["all"]=all_values_formal

# 绘制四张图表
paintPlotMultiV2("Median current wait time each minute\n{0}".format(name),
        stamps_dic_corrected, values_dic_correced,
        dir=dest_dir, graphFileName="Ok-WaitTimes_{0}".format(name),
           #xLim=(0,exp.get_end_epoch()-base_stamp), 
           labelX="time (h)", 
       labelY="Wait time (s)", \
          logScale=False, trend=False, tickFrequence=None, hLines=[], \
              xLogScale=False, alpha=1.0, legendLoc=0, \
                  fontSizeX=12
                  )

paintPlotMultiV2(
        "Median current wait time each minute (log scale)\n{0}".format(name),
        stamps_dic_corrected, values_dic_correced,
        dir=dest_dir, graphFileName="Ok-WaitTimes_{0}-log".format(name),
           #xLim=(0,exp.get_end_epoch()-base_stamp), 
           labelX="time (h)", 
       labelY="Wait time (s), log scale", \
          logScale=True, trend=False, tickFrequence=None, hLines=[], \
              xLogScale=False, alpha=1.0, legendLoc=0, \
                  fontSizeX=12
                  )

paintPlotMultiV2(
        "Median wait time of jobs submitted in each minute\n{0}".format(name),
        stamps_dic_formal, values_dic_formal,
        dir=dest_dir, graphFileName="Ok-WaitTimes_{0}-formal".format(name),
           #xLim=(0,exp.get_end_epoch()-base_stamp), 
           labelX="time (h)", 
       labelY="Wait time (s)", \
          logScale=False, trend=False, tickFrequence=None, hLines=[], \
              xLogScale=False, alpha=1.0, legendLoc=0, \
                  fontSizeX=12
                  )

paintPlotMultiV2(
        "Median wait time of jobs submitted in each minute (log scale)\n"
        "{0}".format(name),
        stamps_dic_formal, values_dic_formal,
        dir=dest_dir, graphFileName="Ok-WaitTimes_{0}-formal-log".format(name),
           #xLim=(0,exp.get_end_epoch()-base_stamp), 
           labelX="time (h)", 
       labelY="Wait time (s), log scale", \
          logScale=True, trend=False, tickFrequence=None, hLines=[], \
              xLogScale=False, alpha=1.0, legendLoc=0, \
                  fontSizeX=12
                  )


waittime_median = median(jobs_waittime[edge])

print "Wait time median: {0}".format(waittime_median)