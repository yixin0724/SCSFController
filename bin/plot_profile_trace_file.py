"""
绘制来自跟踪文件的作业变量的直方图/CDF。

Usgges:

python ./plot_profile_trace_file.py trace_file.trace
"""
import sys

from analysis import jobAnalysis
from plot import profile

"""
该文件的主要功能是从跟踪文件（trace file）中读取作业数据，并绘制这些作业变量的直方图和累积分布函数（CDF）。具体来说，它会绘制以下几种图表：
作业的墙钟时间（wall clock time）。
作业分配的核心数（number of allocated cores）。
作业请求的墙钟时间限制（wall clock limit）。
作业的到达间隔时间（inter-arrival time）。
"""

if len(sys.argv) < 2:
    raise ValueError("At least one argument must specified with the file name "
                     "containing the trace.")

print
"LOADING DATA"
# 从指定的跟踪文件中加载作业数据
data_dic = jobAnalysis.get_jobs_data_trace("./data/edison-1000jobs.trace")

# 绘制作业的墙钟时间直方图/CDF
profile(data_dic["duration"], "Trace\nJobs' wall clock (s)",
        "./graphs/trace-duration", "Wall clock (s)",
        x_log_scale=True)

# 绘制作业分配的核心数直方图/CDF
profile(data_dic["totalcores"], "Trace\nJobs' allocated number of cores",
        "./graphs/trace-cores", "Number of cores",
        x_log_scale=True)

# 绘制作业请求的墙钟时间限制直方图/CDF
profile(data_dic["wallclock_requested"], "Trace\nJobs' "
                                         "wall clock limit(min)",
        "./graphs/trace-limit", "Wall clock limit(min)",
        x_log_scale=True)

# 计算作业的到达间隔时间
inter_data = jobAnalysis.produce_inter_times(data_dic["created"])

# 绘制作业的到达间隔时间直方图/CDF
profile(inter_data, "Trace\nJobs' inter-arrival time(s)",
        "./graphs/trace-inter", "Inter-arrival time(s)",
        x_log_scale=True)
