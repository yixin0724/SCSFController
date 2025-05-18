"""
@author：YiXin
@fileName：plot_histogram.py
@createTime：2025/4/9 11:25
"""

import matplotlib.pyplot as plt
import numpy as np

"""
    绘制histogram中记录数据的直方累积图
"""
def plot_histogram(bins, edges,type):
    # 数据校验
    if len(edges) - len(bins) != 1:
        raise ValueError("edges长度必须比bins大1")

    # 计算直方图参数
    widths = np.diff(edges)
    density = np.array([b / w if w > 0 else 0 for b, w in zip(bins, widths)])

    # 计算CDF参数
    cumulative = np.cumsum(bins)
    cdf_edges = np.concatenate([[0], cumulative])  # CDF阶梯点

    # 创建画布和双轴
    fig, ax1 = plt.subplots(figsize=(12, 7))
    ax2 = ax1.twinx()

    # 绘制直方图（左轴）- 显式设置标签
    bars = ax1.bar(
        x=edges[:-1],
        height=density,
        width=widths,
        align='edge',
        edgecolor='k',
        color='skyblue',
        label='Probability Density'  # 关键修复：显式设置标签
    )

    # 绘制CDF曲线（右轴）
    cdf_line, = ax2.plot(
        edges,
        cdf_edges,
        color='crimson',
        lw=2,
        linestyle='--',
        label='CDF'
    )

    # 自动调整x轴显示范围
    if widths[-1] > 10 * sum(widths[:-1]) and density[-1] == 0:
        display_limit = edges[-2] * 1.1
        ax1.set_xlim(left=edges[0], right=display_limit)
        ax2.set_xlim(left=edges[0], right=display_limit)

    # 设置轴标签
    ax1.set_xlabel("Job Runtime (seconds)", fontsize=12)
    ax1.set_ylabel("Probability Density", color='skyblue', fontsize=12)
    ax2.set_ylabel("Cumulative Probability", color='crimson', fontsize=12)
    ax1.tick_params(axis='y', labelcolor='skyblue')
    ax2.tick_params(axis='y', labelcolor='crimson')

    # 组合图例（不再需要手动提取标签）
    ax1.legend(loc='upper left', fontsize=10)
    ax2.legend(loc='upper right', fontsize=10)

    # 网格和标题
    ax1.grid(axis='y', alpha=0.3)
    plt.title("Job Runtime Distribution with CDF", fontsize=14, pad=20)

    plt.tight_layout()
    plt.show()


# 示例数据使用
bins = [0.07749, 0.0738, 0.21402, 0.14022, 0.10332, 0.04705, 0.03229, 0.0203, 0.02768, 0.03137, 0.04613, 0.03137, 0.02399, 0.0203, 0.01661, 0.0, 0.00092, 0.00461, 0.0572, 0.00185, 0.00185, 0.0, 0.00369, 0.0, 0.00554, 0.0, 0.00092, 0.0, 0.00092, 0.0, 0.00185, 0.0, 0.00092, 0.0, 0.00092, 0.00092, 0.0, 0.00185, 0.0, 0.00185, 0.00185, 0.00092, 0.0, 0.00369, 0.00092, 0.00092, 0.0]
edges = [0, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 660, 720, 780, 840, 900, 1080, 1140, 1200, 1260, 1320, 1380, 1620, 1680, 2400, 2460, 6300, 6360, 6420, 6480, 6960, 7020, 7140, 7200, 7260, 7320, 7380, 7560, 7620, 9780, 9840, 9900, 9960, 14220, 14280, 14340, 14400, 2592060]
plot_histogram(bins, edges,"job_runtime_cdf")






"""
# 不带cdf的直方图
def plot_histogram(bins, edges,type):
    # 确保数据有效性
    if len(edges) - len(bins) != 1:
        raise ValueError("edges的长度必须比bins大1")

    widths = np.diff(edges)
    density = [b / w if w > 0 else 0 for b, w in zip(bins, widths)]

    fig, ax = plt.subplots(figsize=(10, 6))

    # 绘制直方图
    bars = ax.bar(
        x=edges[:-1],
        height=density,
        width=widths,
        align='edge',
        edgecolor='black'
    )

    # 自动调整x轴范围（针对尾部大区间优化显示）
    if widths[-1] > 10 * sum(widths[:-1]) and density[-1] == 0:
        ax.set_xlim(left=edges[0], right=edges[-2] * 1.1)

    # 标签和标题
    # ax.set_xlabel("", fontsize=12)
    ax.set_ylabel("Probability Density", fontsize=12)
    ax.set_title(type, fontsize=14)

    # 优化刻度显示
    ax.tick_params(axis='both', which='major', labelsize=10)
    plt.grid(axis='y', alpha=0.3)
    #保存
    plt.savefig("D:/game/chart.png")
    #显示
    plt.show()


"""