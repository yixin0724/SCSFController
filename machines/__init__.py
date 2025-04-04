from analysis.jobAnalysis import (get_jobs_data, produce_inter_times,
                                  calculate_histogram,
                                  calculate_probability_map)
from analysis import ProbabilityMap

import os


class Machine(object):
    """
    Machine类为要模拟的系统及其工作负载建模：名称、每个节点的核数和作业的随机变量（到达时间、估计时钟、估计时钟精度和分配的核数）。
    它的配置可以从：
        —包含调度程序日志的数据库
        —机器配置文件。
    
    检查“analysis. get_jobs_data“，以了解从数据库中提取数据时的连通性需求。
    """

    def __init__(self, machine_name, cores_per_node=1, inter_times_filter=None,
                 num_nodes=1):
        """
        Creation method:
        Args:
        -  machine_name: 包含系统名称的字符串。用于生成文件名.
        - cores_per_node: 数字表示系统节点中存在的核心。
        - num_nodes: 系统节点数量.
        """
        self._cores_per_node = cores_per_node
        self._num_nodes = num_nodes
        self._inter_times_filter = inter_times_filter
        self._machine_name = machine_name
        self._generators = {}
        self._create_empty_generators()

    def load_from_db(self, start, stop, local=False, dbName="custom2"):
        """
        从数据库加载作业日志数据，分析并生成各类作业特征的随机变量生成器。

        该方法通过数据库连接获取指定时间范围内的作业调度数据，随后基于不同维度的作业特征
        （到达间隔、核心数需求等）构建对应的概率分布生成器，用于仿真建模。

        Args:
            start (float): epoch时间戳，表示数据检索的起始时间
            stop (float): epoch时间戳，表示数据检索的结束时间
            local (bool): 数据库连接模式，True表示直连本地MySQL，False表示通过本地5050端口的远程隧道连接
            dbName (str): 目标数据库名称，默认为"custom2"

        Returns:
            None: 该方法直接更新实例的_generators字典，无返回值
        """
        """
        连接到数据库，检索调度器日志行，对其进行分析，并配置作业的随机变量CDFs。
        检查“get_jobs_data”的定义，了解连接性需求。
        Args:
        - start: 表示日志检索应该从何处开始的epoch时间戳的数字。
        - stop: 表示日志检索应该停止的epoch时间戳的数字。
        - local: 如果为True，它将尝试连接到本地MySQL数据库。
            如果为false，它将连接到localhost:5050，并期望它是通往远程数据库的隧道。
        - dbName: 要从中提取数据的MySQL数据库的名称.
        """
        # 从数据库获取原始作业数据（包含创建时间、核心数、wallclock需求等字段）
        print "Loading data..."
        data_dic = get_jobs_data(self._machine_name,
                                 start.year, start.month, start.day,
                                 stop.year, stop.month, stop.day,
                                 dbName=dbName, forceLocal=local)

        # 基于作业创建时间生成到达间隔时间的概率分布生成器
        print "Producing inter-arrival time generator."
        self._generators["inter"] = self._populate_inter_generator(
            data_dic["created"])

        # 生成作业所需核心数的概率分布生成器
        print "Producing #cores per job  generator."
        self._generators["cores"] = self._populate_cores_generator(
            data_dic["totalcores"])

        # 生成作业wallclock限制时长的概率分布生成器
        print "Producing wc_limit per job generator."
        self._generators["wc_limit"] = (
            self._populate_wallclock_limit_generator(
                data_dic["wallclock_requested"]))

        # 生成作业实际运行时长与申请时长的准确率分布生成器
        print "Producing accuracy per job generator."
        self._generators["accuracy"] = self._populate_wallclock_accuracy(
            data_dic["wallclock_requested"],
            data_dic["duration"])

    def get_inter_arrival_generator(self):
        """
        返回到达时间间隔随机变量的值生成器.
        要求装入类.
        """
        return self._generators["inter"]

    def get_new_job_details(self):
        """
        返回模拟作业的核数、请求的挂钟和运行时间。
        """
        cores = self._generators["cores"].produce_number()
        wc_limit = self._generators["wc_limit"].produce_number()
        acc = self._generators["accuracy"].produce_number()
        run_time = int(float(wc_limit) * 60 * acc)
        return cores, wc_limit, run_time

    def save_to_file(self, file_dir, description):
        """
        保存“[file_dir]/[description]-[machine name]-[var name].gen”中随机变量的CDFs。要求file_dir存在
        """
        for (key, generator) in self._generators.iteritems():
            generator.save(self._get_files_gen(file_dir, description, key))

    def load_from_file(self, file_dir, description):
        """
        从"[file_dir]/[description]-[machine name]-[var name].gen"中加载随机变量的CDFs".
        """
        self._create_empty_generators()
        for (key, generator) in self._generators.iteritems():
            generator.load(self._get_files_gen(file_dir, description, key))

    def _get_files_gen(self, file_dir, description, generator_name):
        """根据组件参数生成规范化文件路径
        使用os.path.join进行跨平台路径拼接，自动处理路径分隔符。当参数中出现绝对路径时，
        会从第一个绝对路径参数开始拼接（参见posixpath.py实现逻辑）
        Args:
            file_dir (str): 基础目录路径。若传入绝对路径会触发路径重置
            description (str): 文件描述标识，用于构建文件名前缀
            generator_name (str): 生成器名称，作为文件名的组成部分
        Returns:
            str: 由目录/描述-机器名-生成器.gen组成的完整路径，如：
                /data/test-ubuntu-python38.gen
        """
        return os.path.join(file_dir, "{}-{}-{}.gen".format(description,
                                                            self._machine_name,
                                                            generator_name))

    def _create_empty_generators(self):
        """初始化空生成器字典，用于存储不同维度的参数生成器

        该方法为实验参数生成系统创建基础生成器结构，包含四个维度：
        - inter: 间隔参数生成器
        - cores: 核心资源配置生成器
        - wc_limit: 最大运行时间限制生成器
        - accuracy: 运行时间精度计算生成器

        无参数和返回值，结果直接存储到实例的_generators字典属性中
        """

        # 初始化间隔参数生成器，使用固定参数列表[1,2]
        # 可能表示不同实验间隔或并行等级配置
        self._generators["inter"] = self._populate_inter_generator(
            [1, 2])

        # 初始化核心资源配置生成器，基于实例的_cores_per_node属性值
        # 用于生成不同核心资源配置方案
        self._generators["cores"] = self._populate_cores_generator(
            [self._cores_per_node])

        # 初始化最大运行时间限制生成器，默认设置600秒（10分钟）
        # 用于控制实验的最大执行时长
        self._generators["wc_limit"] = (
            self._populate_wallclock_limit_generator([600]))

        # 初始化运行时间精度生成器，使用相同的时间基准参数（600秒）
        # 可能用于计算时间精度或时间误差范围
        self._generators["accuracy"] = self._populate_wallclock_accuracy([600],
                                                                         [600])

    def _populate_inter_generator(self, create_times):
        """
        填充事件间隔生成器。
        该方法根据给定的创建时间生成事件间隔时间的概率映射，用于事件间隔生成器。
        参数:
        - create_times: 创建时间列表或数组，用于生成事件间隔时间。
        返回值:
        - 事件间隔时间的概率映射，基于处理后的事件间隔时间生成。
        """
        # 根据创建时间和最大过滤值生成事件间隔x时间
        inter_times = produce_inter_times(create_times,
                                          max_filter=self._inter_times_filter)
        # 计算事件间隔时间的直方图，使用1作为区间大小
        # 区间大小的选择是为了在细节和直方图可读性之间取得平衡
        bins, edges = calculate_histogram(inter_times, interval_size=1)
        # 任何数字都可以，包括小数
        # 根据直方图数据计算事件间隔时间的概率映射
        # 使用 "absnormal" 区间策略来处理概率映射计算中的特殊情况
        return calculate_probability_map(bins, edges,
                                         interval_policy="absnormal")

    def _populate_cores_generator(self, cores):
        """根据给定的核心数生成资源分布概率映射
        通过计算核心数的直方图分布，生成基于节点核心容量策略的概率分布图，
        用于指导资源调度决策
        Args:
            cores (list/int): 输入的核心数集合或单个核心数值，表示待分配的计算资源

        Returns:
            dict/object: 概率分布映射对象，键/属性为资源量级，值为对应的分布概率值
        """

        # 生成核心数的直方图分布数据
        # 使用预设的节点核心容量作为分箱间隔大小，保证结果为该数值的整数倍
        bins, edges = calculate_histogram(cores,
                                          interval_size=self._cores_per_node)

        # 基于低区间优先策略生成概率映射
        # 强制概率值按节点核心容量进行粒度对齐，保证输出结果的可调度性
        return calculate_probability_map(bins, edges, interval_policy="low",
                                         value_granularity=self._cores_per_node)

    def _populate_wallclock_limit_generator(self, wallclock):
        """
        生成基于墙钟时间限制的概率映射生成器
        Args:
            wallclock: list[float] 原始时间限制列表（单位：秒），用于生成概率分布的数据集
        Returns:
            dict/ProbabilityMap 返回概率映射对象，表示不同时间区间的概率分布
        """

        # 将秒单位转换为分钟单位的时间数据标准化处理
        wallclock = [x / 60 for x in wallclock]

        # 生成直方图数据：bins表示各区间计数，edges表示区间边界
        # interval_size=1表示使用1分钟作为直方图区间宽度
        bins, edges = calculate_histogram(wallclock, interval_size=1)
        # Any number is good, with decimals

        # 根据直方图数据构建概率映射，用于后续的概率抽样
        return calculate_probability_map(bins, edges)

    def _populate_wallclock_accuracy(self, requested, actual):
        """
         计算请求时间与实际执行时间的准确度分布概率
         参数:
         requested: list[float] - 请求的时钟时间列表（单位：秒），预期非零值
         actual: list[float]    - 实际的时钟时间列表（单位：秒），预期非零值
         返回值:
         dict: 概率分布映射表，键为精度区间（浮点数），值为对应的概率值
         处理流程：
         1. 过滤零值并计算每个时间对的准确率（实际时间/请求时间）
         2. 生成0.01间隔的直方图数据
         3. 转换为概率分布映射
         """
        # 计算有效时间对的准确率比值

        accuracy = []
        for (r, a) in zip(requested, actual):
            if r == 0 or a == 0:
                continue
            accuracy.append(float(a) / float(r))

        bins, edges = calculate_histogram(accuracy, interval_size=0.01)

        # Any number is good, with decimals
        return calculate_probability_map(bins, edges)

    def get_total_cores(self):
        """Returns the total number of cores in the system."""
        return self._num_nodes * self._cores_per_node

    def get_core_seconds_edges(self):
        """以核心秒为单位返回作业的主要组。"""
        return [0]

    def get_filter_values(self):
        """返回此机器中作业的运行时、内核数和内核小时限制。
            如果一个作业的核心时间或运行时间以及核心小时数都超过了所过滤的值，则应该丢弃。
        """
        return None, None, None

    def get_max_interarrival(self):
        return None

    def job_can_be_submitted(self, cores, runtime):
        return True


class Edison(Machine):
    """机器Edison的定义，没有作业变量的CDF数据。"""

    def __init__(self):
        super(Edison, self).__init__("edison", cores_per_node=24,
                                     inter_times_filter=1800,
                                     num_nodes=5576)


class Edison2015(Machine):
    """为作业变量加载2015年的CDF值的Edison机器。"""

    def __init__(self):
        super(Edison2015, self).__init__("edison", cores_per_node=24,
                                         inter_times_filter=1800,
                                         num_nodes=5576)
        self.load_from_file("./data", "2015")

    def get_max_interarrival(self):
        return 25

    def job_can_be_submitted(self, cores, runtime):
        if (cores > self.get_total_cores() / 4):
            return False
        if (runtime > 24 * 3600 * 5):
            return False
        if ((cores > 15000 or runtime > 15000)
                and runtime * cores > 10000 * 10000):
            return False
        return True

    def get_core_seconds_edges(self):
        """以核心秒为单位返回作业的主要组。"""
        return [0, 48 * 3600, 960 * 3600]
