"""
以NERSC扭矩格式绘制数据库中跟踪的作业变量的直方图/CDF。
假设数据库是可连接的本地数据库，并称为custom2。

生成文件在graphs中

Env vars:
- NERSCDB_USER: 用于访问数据库的用户。
- NERSCDB_PASS: 待使用的密码用于访问数据库的密码。
"""
import datetime

from analysis import jobAnalysis
from plot import profile

# 定义开始和结束日期，用于筛选数据的时间范围
start=datetime.date(2015, 1, 1)
end=datetime.date(2015, 1, 2)
print "LOADING DATA"

# 调用 jobAnalysis 模块中的 get_jobs_data 函数获取指定时间范围内的作业数据
data_dic=jobAnalysis.get_jobs_data("edison", start.year, start.month, start.day,
                          end.year, end.month, end.day)

# 绘制作业运行时间（wall clock）的分布图
profile(data_dic["duration"], "Edison Logs\nJobs' wall clock (s)",
        "./graphs/edison-log-duration", "Wall clock (s)",
        x_log_scale=True)

# 绘制作业分配的核心数（cores）的分布
profile(data_dic["totalcores"], "Edison Logs\nJobs' allocated number of cores",
        "./graphs/edison-log-cores", "Number of cores",
        x_log_scale=True)

# 绘制作业请求的墙钟时间限制（wallclock limit）的分布图
profile(data_dic["wallclock_requested"], "Edison Logs\nJobs' wall clock limit(s)",
        "./graphs/edison-log-limit", "Wall clock limit(s)",
        x_log_scale=True)

# 计算作业之间的到达间隔时间（inter-arrival time
inter_data = jobAnalysis.produce_inter_times(data_dic["created"])

# 绘制作业到达间隔时间的分布图
profile(inter_data, "Edison Logs\nJobs' inter-arrival time(s)",
        "./graphs/edison-log-inter", "Inter-arrival time(s)",
        x_log_scale=True)
