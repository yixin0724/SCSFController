"""
使用wideLong、longWide和floodplain工作流对第一个种子实验进行箱线图比较。
包括：2wf/h、6wf/h、60wf/h。它涵盖了wfs运行时、周转时间、等待时间、拉伸系数。
它涵盖了三种工作流操作技术：wf感知回填、单个作业和依赖项。

分析只关注工作减速，将其分为三个工作范围：
- 0-48 c.h
- 48-990 c.h
- 906- c.h

"""


from orchestration import get_central_db
from stats.trace import ResultTrace
from orchestration.definition import ExperimentDefinition
from plot import (plot_multi_exp_boxplot, produce_plot_config, extract_results,
                  gen_trace_ids_exps)

"""
主分析绘图脚本：根据实验数据生成不同核心使用时长的任务减速箱线图

全局流程：
1. 定义核心使用时长分类标准
2. 生成实验跟踪ID配置
3. 设置绘图参数和样式
4. 按核心时长分类处理数据并生成可视化图表
"""


# 非交互式环境配置matplotlib后端（无显示器时使用）
import matplotlib
matplotlib.use('Agg')

# 初始化中央数据库连接
db_obj = get_central_db()

# 核心使用时长分类定义（单位：核心小时）
edge_keys= {0: "[0,48] core.h", 48*3600:"(48, 960] core.h", 
            960*3600:"(960, inf.) core.h"}

# 文件名后缀映射
file_name_edges = {0: "small", 48*3600:"medium",
            960*3600:"large"}

# 实验配置初始化
trace_id_rows = []
base_exp=3189
base_trace_id=4166

# 加载实验定义并获取核心时长分界点
exp=ExperimentDefinition()
exp.load(db_obj, base_exp)
core_seconds_edges=exp.get_machine().get_core_seconds_edges()


# 生成跟踪ID矩阵（用于组织实验数据
trace_id_rows= gen_trace_ids_exps(base_trace_id, base_exp,
                                      group_size=3,
                                      group_count=5,
                                      block_count=6,# 每个时间点的数据块数
                                      group_jump=18)
 
# 可视化参数配置
time_labels = ["", "", "10%", "", "", "25%", "", 
               "", "50%", "", "", "75%", "", 
               "",  "100%", ""]
manifest_label=["floodP", "longW", "wideL",
                "cybers", "sipht", "montage"]

result_type="jobs_slowdown"

# Y轴范围配置（根据不同核心使用时长分类
y_limits_dic={"[0,48] core.h": (1, 1000),
      "(48, 960] core.h":(1,100),
      "(960, inf.) core.h":(1,20)}

target_dir="percent"    # 输出目录

grouping=[1,3,3,3,3,3]  # 数据分组配置（1个基准组+5个实验组）

# 生成绘图样式配置（颜色/图案/图例
colors, hatches, legend = produce_plot_config(db_obj, trace_id_rows)

name="Slowdown"

# 主处理循环：按核心时长分类生成图表
for edge in core_seconds_edges:
    # 获取当前分类的结果类型
    edge_result_type=ResultTrace.get_result_type_edge(edge,result_type)
    print "Loading "+edge_result_type
    # 提取当前分类的实验结果数据
    edge_plot_results = extract_results(db_obj, trace_id_rows,
                                        edge_result_type)
    # 配置图表参数
    edge_formated=edge_keys[edge]
    title="Jobs slowdow: {0}".format(edge_formated)
    y_limits=y_limits_dic[edge_formated]
    print "Plotting figure"

    # 生成箱线图
    plot_multi_exp_boxplot(
        name=title,
        file_name=target_dir+"/percent-slow_down_jobs-{0}.png".format(
                                                        file_name_edges[edge]),
        title=title,
        exp_rows=edge_plot_results,
        y_axis_labels=manifest_label,
        x_axis_labels=time_labels,
        y_axis_general_label=name,
        grouping=grouping,
        colors=colors,
        hatches=hatches,
        y_limits=y_limits,
        y_log_scale=True,
        legend=legend,
        y_tick_count=3,
        y_tick_count_alt=3,
        grouping_alt=grouping,
        percent_diff=True
        ) 

    