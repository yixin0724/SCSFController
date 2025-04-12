"""
@author：YiXin
@fileName：get_data_histogram.py
@createTime：2025/4/5 14:20
"""

import pymysql
import numpy as np
import pickle
from io import BytesIO
import matplotlib.pyplot as plt
import random
import string




# ----------------------------直方图绘制函数----------------------------
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
    plt.title(type, fontsize=14, pad=20)

    plt.tight_layout()

    # 生成 4 位随机字符串（包含字母和数字）
    # random_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
    plt.savefig("D:/game/{}.png".format(metric_name))
    # plt.show()



# ----------------------------数据库字段获取----------------------------
# 1. 连接数据库
conn = pymysql.connect(
    host='192.168.217.92',
    user='yixin',
    password='1234',
    database='scsf',
    charset='latin1'
)

# 2. 查询数据
try:
    with conn.cursor() as cursor:
        cursor.execute("SELECT type, bins, edges FROM histograms WHERE trace_id = 10")
        rows = cursor.fetchall()

        for row in rows:
            type_, bins_blob, edges_blob = row
            metric_name = type_  # 直接使用 type 字段的值

            try:

                # 使用 pickle 加载二进制数据（返回 Python 列表）
                bins = pickle.load(BytesIO(bins_blob), encoding='latin1')
                edges = pickle.load(BytesIO(edges_blob), encoding='latin1')

                # 将bins转换为 NumPy 数组
                bins = np.array(bins, dtype=np.float64)
                bins_rounded = np.round(bins, 5)
                bins_list = bins_rounded.tolist()

                # ----- 绘制直方图 -------
                # 定义需要排除的关键字列表
                keywords = ['g0', 'g172800', 'g3456000', 'json']

                # 检查字符串是否不包含任何关键字，若不包含则绘制直方图
                if not any(keyword in metric_name for keyword in keywords):
                    # 调用绘制直方图的函数
                    plot_histogram(bins, edges,metric_name)

                    #打印指标内容
                    print(f"\n=== 指标名称: {metric_name} ===")
                    print("[原始列表] bins:", bins_list)
                    print("[原始列表] edges:", edges)

                # -----------------------

                # 检查数据维度是否合法（edges 长度应为 bins 长度 +1）
                if len(edges) != len(bins) + 1:
                    print(f"数据维度不匹配: {metric_name} (edges长度={len(edges)}, bins长度={len(bins)})")
                    continue

            except Exception as e:
                print(f"处理 {metric_name} 时出错: {str(e)}")
                import traceback

                traceback.print_exc()  # 打印完整错误堆栈
                continue
except Exception as e:
    print(f"数据库操作失败: {str(e)}")

finally:
    conn.close()