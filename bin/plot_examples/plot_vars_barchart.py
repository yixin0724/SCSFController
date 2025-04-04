"""
对不同工作流程类型和不同工作流程核心小时占工作量百分比的实验的工作流程变量进行了绘图分析。

结果被绘制成条形图，显示了vas在单一和多重情况下偏离aware的程度。
"""
from orchestration import get_central_db
from stats.trace import ResultTrace
from orchestration.definition import ExperimentDefinition
from plot import (plot_multi_bars, produce_plot_config, extract_results,
                  gen_trace_ids_exps,calculate_diffs, get_args, join_rows,
                  replace)


# 主程序函数：执行工作流分析并生成对比结果图表
# 注意：该脚本需在无显示环境的服务器上运行，使用matplotlib的Agg后端
# 主要流程分为：参数获取、数据准备、结果提取、差异计算和可视化四个阶段

# 配置matplotlib使用非交互式后端（适用于无显示环境）

# 远程使用无显示
import matplotlib
matplotlib.use('Agg')

# 获取实验参数
# 参数：
# default_id - 默认基准实验ID（2459）
# use_lim - 是否使用受限工作流分析（True）
# 返回：
# base_trace_id_percent - 基准追踪ID百分比
# lim - 是否使用受限分析的布尔值
base_trace_id_percent, lim = get_args(2459, True)
print "Base Exp", base_trace_id_percent
print "Using analysis of limited workflows:", lim

# 初始化中央数据库连接
db_obj = get_central_db()

# 定义核心小时数区间的显示标签
edge_keys= {0: "[0,48] core.h", 48*3600:"(48, 960] core.h", 
            960*3600:"(960, inf.) core.h"}

# 准备实验追踪ID数据
trace_id_rows = []
base_exp=170    # 基础实验ID
exp=ExperimentDefinition()
exp.load(db_obj, base_exp)  # 加载实验定义
core_seconds_edges=exp.get_machine().get_core_seconds_edges()   # 获取机器核心时间分段

# trace_id_rows = [
#      [ 4166, 4167, 4168, 4184, 4185, 4186, 4202, 4203, 4204, 
#        4220, 4221, 4222, 4238, 4239, 4240 ],
#      [ 4169, 4170, 4171, 4187, 4188, 4189, 4205, 4206, 4207,
#        4223, 4224, 4225, 4241, 4242, 4243 ],
#      [ 4172, 4173, 4174, 4190, 4191, 4192, 4208, 4209, 4210,
#        4226, 4227, 4228, 4244, 4245, 4246 ],
#      [ 4175, 4176, 4177, 4193, 4194, 4195, 4211, 4212, 4213,
#       4229, 4230, 4231, 4247, 4248, 4249],
#      [ 4178, 4179, 4180, 4196, 4197, 4198, 4214, 4215, 4216,
#       4232, 4233, 4234, 4250, 4251, 4252],
#      [ 4181, 4182, 4183, 4199, 4200, 4201, 4217, 4218, 4219,
#       4235, 4236, 4237, 4253, 4254, 4255],
#                 ]

# 生成实验组追踪ID矩阵（包含预基准组和主基准组）
pre_base_trace_id_percent = 2549+18 # 预基准组起始百分比
trace_id_rows=  join_rows(
                        gen_trace_ids_exps(pre_base_trace_id_percent,       # 生成预基准组ID
                                      inverse=False,
                                      group_jump=18, block_count=6,
                                      base_exp_group=None,
                                      group_count=1),
                         gen_trace_ids_exps(base_trace_id_percent,       # 生成主基准组ID
                                      inverse=False,
                                      group_jump=18, block_count=6,
                                      base_exp_group=None,
                                      group_count=5)
                          )
# 生成颜色标记专用的追踪ID矩阵（用于区分不同系列）
trace_id_colors=join_rows(
                        gen_trace_ids_exps(pre_base_trace_id_percent+1,      # 预基准组颜色ID
                                      inverse=False, skip=1,
                                      group_jump=18, block_count=6,
                                      base_exp_group=None,
                                      group_count=1,
                                      group_size=2),
                         gen_trace_ids_exps(base_trace_id_percent+1,         # 主基准组颜色ID
                                      inverse=False,skip=1,
                                      group_jump=18, block_count=6,
                                      base_exp_group=None,
                                      group_count=5,
                                      group_size=2)
                          )

# 替换特定追踪ID（兼容旧版本实验数据
print "IDS", trace_id_rows
trace_id_rows=replace(trace_id_rows,
                      [2489, 2490, 2491,
                       2507, 2508, 2509,
                       2525, 2526, 2527],
                      [2801, 2802, 2803,
                       2804, 2805, 2806,
                       2807, 2808, 2809])

print "IDS", trace_id_rows

# 配置可视化参数
print "COLORS", trace_id_colors
time_labels = ["", "5%", "", "10%", "", "25%", 
               "", "50%", "", "75%", 
               "",  "100%"]
manifest_label=["floodP", "longW", "wideL",
                "cybers", "sipht", "montage"]


y_limits_dic={"[0,48] core.h": (1, 1000),
      "(48, 960] core.h":(1,100),
      "(960, inf.) core.h":(1,20)}

# 初始化绘图配置
target_dir="percent"
grouping_types = [["bar", "bar"],
                  ["bar", "bar"],
                  ["bar", "bar"],
                  ["bar", "bar"],
                  ["bar", "bar"],
                  ["bar", "bar"]]
colors, hatches, legend = produce_plot_config(db_obj, trace_id_colors)  # 生成颜色/图案配置
#head_file_name="percent"
head_file_name="wf_percent-b{0}".format(base_trace_id_percent)  # 输出文件前缀



# 主处理循环：遍历不同指标类型进行结果分析和可视化
for (name, result_type) in zip(["Turnaround speedup", "wait time(h.)",
                                "runtime (h.)", "stretch factor"],
                          ["wf_turnaround", "wf_waittime", 
                           "wf_runtime", "wf_stretch_factor"]):
    # 结果类型处理
    if lim:
        result_type="lim_{0}".format(result_type)
    print "Loading: {0}".format(name)
    # 数据转换系数设置（将秒转换为小时）
    factor=1.0/3600.0
    if result_type in ("wf_stretch_factor", "lim_wf_stretch_factor"):   # 无量纲指标不需要转换
        factor=None
    # 从数据库提取结果数据
    edge_plot_results = extract_results(db_obj, trace_id_rows,
                                        result_type, factor=factor,
                                        second_pass=lim)
    # 计算相对于基准实验的差异（基准索引为0，分3组，计算加速比）
    diffs_results = calculate_diffs(edge_plot_results, base_index=0,
                                    group_count=3, speedup=True)
#     for res_row  in edge_plot_results:
#         print [ x._get("median") for x in res_row]
    
    title="{0}".format(name)
    # 配置绘图参数
    y_limits=(0,4)
    print "Plotting figure"
    ref_level=1.0

    # 生成饼状图
    plot_multi_bars(
        name=title,
        file_name=target_dir+"/{0}-{1}-bars.png".format(head_file_name,
                                                           result_type),
        title=title,
        exp_rows=diffs_results,
        y_axis_labels=manifest_label,
        x_axis_labels=time_labels,
        y_axis_general_label=name,
        type_rows=grouping_types,
        colors=colors,
        hatches=hatches,
        y_limits=y_limits,
        y_log_scale=False,
        legend=legend, 
        y_tick_count=3,
        subtitle="% workflow workload",
        ncols=2,
        ref_line=ref_level
        )


    