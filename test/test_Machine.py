"""UNIT TESTS for machine modeling classes.

 python -m unittest test_Machine
 已测试成功

"""

import datetime
import random
import string
import unittest
from machines import Machine, Edison


class TestMachine(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        self._edison =  Machine("edison", 24)
    def test_init(self):
        edison = Machine("edison", 24)
        self.assertEqual(edison._cores_per_node, 24)
        self.assertEqual(edison._machine_name, "edison")
        
    def test_populate_inter_generator(self):
        """
        测试_populate_inter_generator方法的正确性
        验证生成器是否能正确计算：
        1. 事件发生时间的概率分布
        2. 对应时间间隔的数值范围
        测试步骤：
        - 使用预设的create_times时间序列初始化生成器
        - 验证生成的概率分布是否符合预期
        - 验证生成的数值范围区间是否正确
        注意：
        - 本测试不包含实际参数传递，测试对象为类内部方法
        - 断言预期值基于特定create_times数据计算得出
        """
        create_times = [2, 10, 10, 20, 20] # 8, 0, 10, 0
        
        generator = self._edison._populate_inter_generator(create_times)
        self.assertEqual(generator.get_probabilities(),
                         [0.50, 0.75, 1.0])
        self.assertEqual(generator.get_value_ranges(),
                         [(0,1), (8,9), (10,11)])
    
    def test_populate_cores_generator(self):
        cores = [24,24,48,96] 
        generator = self._edison._populate_cores_generator(cores)

        self.assertEqual(generator.get_probabilities(),
                         [0.50, 0.75, 1.0])
        self.assertEqual(generator.get_value_ranges(),
                         [(24,48), (48,72), (96,120)])
        
    def test_populate_wallclock_limit_generator(self):
        wallclock = [120,120,240,960] 
        generator = self._edison._populate_wallclock_limit_generator(wallclock)

        self.assertEqual(generator.get_probabilities(),
                         [0.50, 0.75, 1.0])
        self.assertEqual(generator.get_value_ranges(),
                         [(2,3), (4,5), (16,17)])
        
    def test_populate_wallclock_accuracy(self):
        wallclock = [120,120,240,960]
        runtime = [60, 120, 120, 240] 
        generator = self._edison._populate_wallclock_accuracy(wallclock,
                                                              runtime)
        
        self.assertEqual(generator.get_probabilities(),
                         [0.25, 0.75, 1.0])
        
        the_ranges = generator.get_value_ranges()
        r0=the_ranges[0]
        r1=the_ranges[1]
        r2=the_ranges[2]
        self.assertTrue(0.25>=r0[0] and 0.25<=r0[1])
        self.assertTrue(0.50>=r1[0] and 0.50<=r1[1])
        self.assertTrue(1.0>=r2[0] and 1.0<=r2[1])
    
    def test_create_empty_generators(self):
        self._edison._create_empty_generators()
        
    def test_get_new_job_details(self):
        create_times = [2, 10, 10, 20, 20]
        self._edison._generators["inter"] = (
                        self._edison._populate_inter_generator(create_times))
        
        cores = [24,24,24,24] 
        self._edison._generators["cores"] = (
                        self._edison._populate_cores_generator(cores))
        
        wallclock = [120,120,120,120] 
        self._edison._generators["wc_limit"] = \
            self._edison._populate_wallclock_limit_generator(wallclock)

        wallclock = [120,120,120,120]
        runtime = [60, 60, 60, 60] 
        self._edison._generators["accuracy"] = \
             self._edison._populate_wallclock_accuracy(wallclock,
                                                              runtime)
        cores, wc_limit, run_time = self._edison.get_new_job_details()
        self.assertEqual(cores, 24)
        self.assertLess(abs(wc_limit-2),1)
        self.assertLess(abs(run_time-60),30)
        
    def test_save_load(self):
        """
        测试Edison对象数据的保存和加载功能

        验证通过不同生成器创建的数据在序列化和反序列化后的一致性：
        1. 创建inter/cores/wc_limit/accuracy四种类型的生成器
        2. 将对象状态保存到临时文件
        3. 从文件加载到新对象
        4. 断言验证各生成器的概率分布和数值范围
        """
        # 初始化inter生成器：基于作业创建时间分布
        create_times = [2, 10, 10, 20, 20]
        self._edison._generators["inter"] = (
                        self._edison._populate_inter_generator(create_times))
        
        cores = [24,24,48,96] 
        self._edison._generators["cores"] = (
                        self._edison._populate_cores_generator(cores))
        
        wallclock = [120,120,240,960] 
        self._edison._generators["wc_limit"] = \
            self._edison._populate_wallclock_limit_generator(wallclock)

        wallclock = [120,120,240,960]
        runtime = [60, 120, 120, 240] 
        self._edison._generators["accuracy"] = \
             self._edison._populate_wallclock_accuracy(wallclock,
                                                              runtime)
        
        
        
        file_name_base=gen_random_string()
        self._edison.save_to_file("/tmp", file_name_base)
        new_edison = Edison()
        new_edison.load_from_file("/tmp", file_name_base)
        self.assertEqual(new_edison._generators["inter"].get_probabilities(),
                         [0.50, 0.75, 1.0], "Data didn't load correctly")
        self.assertEqual(new_edison._generators["inter"].get_value_ranges(),
                         [(0,1), (8,9), (10,11)])
        self.assertEqual(new_edison._generators["cores"].get_probabilities(),
                         [0.50, 0.75, 1.0], "Data didn't load correctly")
        self.assertEqual(new_edison._generators["cores"].get_value_ranges(),
                         [(24,48), (48,72), (96,120)])
        self.assertEqual(new_edison._generators["wc_limit"].get_probabilities(),
                         [0.50, 0.75, 1.0], "Data didn't load correctly")
        self.assertEqual(new_edison._generators["wc_limit"].get_value_ranges(),
                         [(2,3), (4,5), (16,17)])
        self.assertEqual(new_edison._generators["accuracy"].get_probabilities(),
                         [0.25, 0.75, 1.0], "Data didn't load correctly")
        
def gen_random_string(N=10):
    """生成包含大写字母和数字的加密安全随机字符串

    使用加密安全的随机数生成器(SystemRandom)从大写字母和数字中抽样，
    生成指定长度的随机字符串。适用于生成安全令牌、验证码等场景。

    Args:
        N (int, optional): 需要生成的字符串长度，默认为10

    Returns:
        str: 由大写字母和数字组成的随机字符串，固定长度为N
    """
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase +
                                            string.digits) for _ in range(N))                 
        
        
        