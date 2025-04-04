
"""UNIT TESTS for the the plotting functions

 python -m unittest test_plot
 需要自行测试
 
 
"""

from stats import Histogram, NumericStats

import numpy as np
import os
import unittest
from plot import histogram_cdf

class TestPlot(unittest.TestCase):

    def test_histogram_cdf(self):
        """
           测试累积分布函数（CDF）图的生成。

           该方法使用一组数据和参数来测试histogram_cdf函数是否能够正确地生成CDF图。
           它验证了图是否根据给定的输入数据、作为CDF的选项、对数刻度选项等正确保存。
        """
        # 调用histogram_cdf函数来生成累积分布函数图。
        # 这里传递了数据、直方图参数、图表标题、保存路径、轴标签以及是否使用对数刻度等参数
        histogram_cdf([-1, 0.5, 1, 2], {"h1":[0.25, 0.15, 0.6],
                                        "h2":[0.25, 0.25, 0.5]}, "Test Hist",
                      "tmp/testhist", "x_label", "y_label", do_cdf=True,
                      y_log_scale=False,
                      cdf_y_log_scale=False)
        # 验证生成的图表文件是否存在，以确保图表生成成功。
        self.assertTrue(os.path.exists("tmp/testhist.png"))
        