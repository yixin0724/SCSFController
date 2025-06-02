"""
@author：YiXin
@createTime：2025/5/9 11:33
@description 绘制实验结果图
"""

# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 读取数据
df = pd.read_csv('workflow_data.csv')

# 设置绘图风格
sns.set(style="whitegrid")
plt.figure(figsize=(14, 12))
plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用微软雅黑字体
plt.rcParams['axes.unicode_minus'] = False    # 正常显示负号

# 图1: 各工作流资源利用率对比（柱状图）
plt.subplot(2, 2, 1)
ax1 = sns.barplot(x="workflow", y="utilization", hue="policy", data=df, errorbar=None, palette='viridis')
plt.title("不同工作流下的资源利用率")
plt.ylabel("Utilization (%)")
ax1.set_ylim(0, 35)  # 显式设置纵轴范围

# 图2: 周转时间中位数对比（柱状图）
plt.subplot(2, 2, 2)
ax2 = sns.barplot(x="workflow", y="turnaround", hue="policy", data=df, errorbar=None)
plt.title("不同工作流下的平均周转时间")
plt.ylabel("Turnaround Time (s)")
plt.yscale("log")  # 对数尺度展示时间差异

# 图3: 常规作业减速对比（箱线图）
plt.subplot(2, 2, 3)
sns.barplot(x="workflow", y="slowdown", hue="policy", data=df, errorbar=None)
plt.title("不同工作流下的常规作业缓速影响")
plt.ylabel("Slowdown Factor")
plt.ylim(1, 3)  # 限制纵轴范围以凸显差异

# 图4: 资源浪费对比（柱状图）
plt.subplot(2, 2, 4)
sns.barplot(x='workflow', y='wasted_cores', hue='policy', data=df, errorbar=None, palette='viridis')
plt.title('不同工作流下的试点作业的资源浪费')
plt.ylabel('Wasted Cores (x1e3)')
plt.yscale('log')  # 对数尺度展示资源浪费量级差异

# 调整布局并展示
plt.tight_layout()
plt.show()