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
        cursor.execute("SELECT type, bins, edges FROM histograms WHERE trace_id = 9")
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

                # 打印原始列表内容
                print(f"\n=== 指标名称: {metric_name} ===")
                print("[原始列表] bins:",bins_list)
                print("[原始列表] edges:", edges)

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