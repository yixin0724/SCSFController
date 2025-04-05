"""
一组用于分析与HPC工作负载相关的作业的功能。
"""

from analysis import ProbabilityMap
from commonLib.nerscLib import (getDBInfo, parseFromSQL_LowMem, 
                                getSelectedDataFromRows)

from slurm.trace_gen import extract_records 

 
import numpy as np


def get_jobs_data_trace(file_name, list_trace_location="./list_trace"):
    """
    解析跟踪文件并返回一个字典，其中包含作业的持续时间、分配的内核、请求的wallclock和创建的epoch时间戳。
    Args:
        file_name (str): 要解析的跟踪文件路径
        list_trace_location (str): slurm模拟器list_trace命令的路径，默认为"./list_trace"
    Returns:
        dict: 包含作业特征数据的字典，结构为：
            - duration (list[int]): 作业持续时间列表（秒）
            - totalcores (list[int]): 每个作业分配的总CPU核心数（NUM_TASKS * CORES_PER_TASK）
            - wallclock_requested (list[int]): 作业请求的最大挂钟时间列表（秒）
            - created (list[int]): 作业创建时间的UNIX时间戳列表
    处理流程：
        1. 调用extract_records解析原始跟踪文件
        2. 初始化包含四个空列表的结果字典
        3. 遍历所有作业记录，提取并转换关键字段
    """
    # 解析原始跟踪文件获取作业记录集合
    jobs = extract_records(file_name, list_trace_location)

    # 初始化结果字典，包含四个关键指标的列表容器
    data_dic = dict(
        duration=[],  # 存储转换后的作业持续时间
        totalcores=[],  # 存储计算得到的总核心数
        wallclock_requested=[],  # 存储作业请求的wallclock限制
        created=[]  # 存储作业提交时间戳
    )

    # 处理每条作业记录，提取并转换需要的字段
    for job in jobs:
        # 将字符串格式的原始数据转换为整型存储
        data_dic["duration"].append(int(job["DURATION"]))

        # 计算实际分配的核心总数（任务数 × 单任务核心数）
        data_dic["totalcores"].append(job["NUM_TASKS"] * job["CORES_PER_TASK"])

        # 转换wallclock请求时间为整型数值
        data_dic["wallclock_requested"].append(int(job["WCLIMIT"]))

        # 记录作业提交的原始时间戳
        data_dic["created"].append(int(job["SUBMIT"]))

    return data_dic


def get_jobs_data(hostname, startYear, startMonth, startDay, stopYear,
                  stopMonth, stopDay, dbName="scsf", forceLocal=True):
    """
   从MySQL数据库检索PBS日志数据信息，并返回一组作业属性列表。
   不同列表中的相同职位与同一工作相关联。
   它使用NERSCDB_USER和NERSCDB_PASS作为用户名和密码进行连接。
    Args:
        hostname (str): 记录日志的系统名称（如集群节点名称）
        startYear (int): 检索起始时间的年份
        startMonth (int): 检索起始时间的月份（1-12）
        startDay (int): 检索起始时间的日期（1-31）
        stopYear (int): 检索结束时间的年份
        stopMonth (int): 检索结束时间的月份（1-12）
        stopDay (int): 检索结束时间的日期（1-31）
        dbName (str): 目标数据库名称，默认为custom2
        forceLocal (bool): 连接控制标志。为True时直连本地数据库，为False时通过5050端口隧道连接
    Returns:
        dict: 包含四个键值对的字典，每个值都是等长列表，对应同一作业的不同属性：
            - duration (list): 实际运行时间（秒）
            - totalcores (list): 分配的CPU核心数
            - wallclock_request (list): 请求的最大运行时间（秒）
            - created (list): 作业提交时间的时间戳（epoch秒）
    Raises:
        隐式抛出数据库连接相关异常，当获取数据库信息失败时打印错误信息
    """
    # 获取数据库连接信息并验证凭证
    info = getDBInfo(forceLocal)
    if (info != None):
        (dbHost, user, password, dbPort) = info
    else:
        print("Error retrieving data to connect to DB")

    # 执行低内存消耗的SQL查询，解析时间范围参数
    rows, start = parseFromSQL_LowMem(
        dbName=dbName, hostname=hostname, dbHost=dbHost, dbPort=dbPort,
        userName=user, password=password,
        year=startYear, month=startMonth, day=startDay,
        endYear=stopYear, endMonth=stopMonth, endDay=stopDay,
        orderingField="created")

    # 定义需要提取的核心字段
    dataFields = ["duration", "totalcores", "wallclock_requested", "created"]
    classFields = []

    # 处理原始查询结果，进行数据结构转换
    (numberSamples, outputDic, outputAcc,
     queues, queuesDic, queuesG, queueGDic) = getSelectedDataFromRows(rows,
                                                                      dataFields, classFields)

    # 输出检索统计信息
    print("Retrieved {0} records from database {1} at {2}:{3}".format(
        numberSamples, dbName, dbHost, dbPort))

    return outputDic


def _filter_data(self, series, limitMax):
    """
    过滤数据序列中超过指定阈值的元素
    遍历输入数据序列，保留所有小于等于指定阈值的元素，生成并返回新的过滤后序列。
    本函数适用于数据清洗场景，常用于去除异常大值。
    Args:
        series (iterable): 待过滤的原始数据序列，支持任意可迭代对象
        limitMax (int/float): 过滤阈值，所有大于该值的元素将被过滤掉
    Returns:
        list: 由符合条件（x <= limitMax）的元素组成的新列表
    """
    new_series = []

    # 遍历原始数据并执行阈值过滤
    # 时间复杂度为O(n)，适用于线性数据结构的处理
    for x in series:
        if x <= limitMax:
            new_series.append(x)

    return new_series


def produce_inter_times(timestamps, max_filter=None):
    """
    计算并过滤时间戳序列中相邻非零时间戳的时间间隔（单位：秒）。当提供max_filter参数时，可过滤超过指定阈值的间隔
    Args:
        timestamps: 有序的时间戳序列（单位：秒），要求满足严格递增关系
            类型：List[float]
            取值范围：所有元素应为正数
        max_filter: 可选的最大间隔阈值（单位：秒）
            类型：float | None
            功能：当提供该参数时，仅保留间隔时间≤该值的记录
    Returns:
        List[float]: 相邻非零时间戳之间的间隔时间列表（单位：秒）
            特性：返回列表长度比输入时间戳列表少1
    Raises:
        ValueError: 当时间戳序列出现非递增情况时抛出
    处理逻辑：
    1. 遍历输入时间戳序列
    2. 跳过首个元素（无前驱时间点）
    3. 验证时间戳严格递增特性
    4. 仅处理两个相邻非零时间戳的情况
    5. 应用max_filter过滤规则（当启用时）
    """
    inter_times = []
    last_t = None
    for t in timestamps:
        if (not last_t is None) and (not t is None):
            if t < last_t:
                raise ValueError("Timestamps are not increasing!")
            if t != 0 and last_t != 0:
                if max_filter is None or t - last_t <= max_filter:
                    inter_times.append((t - last_t))
        last_t = t
    return inter_times


def _join_var_bins(hist, bin_edges, th_min=0.01, th_acc=0.1):
    """ 
    合并相邻的直方图分箱，当满足以下两个条件时执行合并：
    1. 分箱贡献度小于th_min
    2. 合并后的新分箱总贡献度不超过th_acc
    Args:
        hist: 归一化直方图，数值范围[0,1]的数组，所有元素之和必须为1.0
        bin_edges: 直方图边界数组，长度比hist多1个元素
        th_min: 分箱合并阈值，当相邻分箱都小于该值时触发合并（默认0.01）
        th_acc: 合并后分箱的最大允许总贡献度（默认0.1）
    Returns:
        composed_hist: 合并处理后的直方图数组
        composed_edges: 合并后的边界数组，长度比composed_hist多1个
    实现逻辑：
    - 遍历每个分箱，动态合并满足条件的小分箱
    - 使用累加器记录合并中的分箱总和
    - 当遇到需要保留的分箱时，将累积结果写入最终输出
    """
    composed_hist = []
    composed_edges = []

    # 初始化边界数组和累加器
    max_edge = None
    composed_edges.append(bin_edges[0])
    acc = 0

    # 遍历每个分箱及其对应的边界对
    for (share, edge) in zip(hist, zip(bin_edges[:-1], bin_edges[1:])):
        # 判断是否需要结束当前合并过程：
        # 1. 当前分箱超过合并阈值 或
        # 2. 累加结果将超过允许的最大贡献度
        if (share > th_min or acc + share > th_acc):
            # 将之前累积的分箱写入结果（如果有）
            if max_edge != None:
                composed_hist.append(acc)
                composed_edges.append(max_edge)
                acc = 0
            # 单独保留当前分箱
            composed_hist.append(share)
            composed_edges.append(edge[1])
            max_edge = None
            # 满足合并条件，继续累积分箱
        else:
            acc += share
            max_edge = edge[1]

    # 处理最后的累积分箱（如果有）
    if max_edge != None:
        composed_hist.append(acc)
        composed_edges.append(max_edge)

        # 转换为numpy数组返回
    hist = np.array(composed_hist, dtype=float)
    bin_edges = composed_edges
    return hist, bin_edges


def calculate_probability_map(hist, bin_edges, **kwargs):
    """ 
    将直方图转换为概率映射对象。该映射表示随机变量落在特定数值区间的概率，
    并过滤掉概率为零的区间。
    通过累积非零概率区间构建累积分布函数(CDF)，最终生成ProbabilityMap对象，
    可用于概率抽样等场景。
    Args:
        hist: 直方图概率值列表。不必均匀分布，但所有元素之和必须为1.0
        bin_edges: 直方图的区间边界列表。区间可以不均匀
        **kwargs: ProbabilityMap构造器接受的额外参数，如随机数生成器配置等
    Returns:
        ProbabilityMap: 包含以下属性的概率映射对象：
            - probabilities: 累积概率列表，表示CDF
            - value_ranges: 对应的数值区间列表，每个元素为表示区间[a,b)的元组

    实现说明：
        过滤零概率区间后，累加剩余区间的概率形成CDF，保留对应的数值区间
    """
    # 初始化累积概率和有效区间容器
    prob_values = []
    value_range = []
    acc = 0  # 累积概率计数器

    # 遍历每个直方条柱及其对应区间
    # 过滤零概率区间，同时构建累积概率分布
    for (share, edge) in zip(hist, zip(bin_edges[:-1], bin_edges[1:])):
        if (share != 0):
            # 累积非零概率并记录对应区间
            acc += share
            prob_values.append(acc)
            value_range.append(edge)

    # 使用过滤后的数据构造概率映射对象
    return ProbabilityMap(probabilities=prob_values,
                          value_ranges=value_range, **kwargs)


def calculate_histogram(data, th_min=0,
                        th_acc=1, range_values=None, interval_size=1,
                        total_count=None, bins=None):
    """
    生成非归一化的非均匀区间直方图，自动合并过小的相邻区间
    通过阈值控制区间合并逻辑，确保合并后的区间满足最小贡献度和最大总贡献限制

    Args:
        data: 输入数据列表，用于生成直方图
        th_min: 分箱合并阈值，相邻分箱的贡献度都小于该值时触发合并（默认0）
        th_acc: 合并后分箱的最大允许总贡献度（默认1）
        range_values: 直方图取值范围元组（最小值，最大值），默认使用数据极值
        interval_size: 初始分箱的固定宽度（默认1）
        total_count: 归一化基数，默认使用data的总样本数
        bins: 自定义分箱边界数组，若指定则忽略range_values和interval_size参数

    Returns:
        hist: numpy数组，表示各分箱的归一化贡献度（总和为1）
        bin_edges: 分箱边界数组，长度比hist多1个元素

    实现步骤：
        1. 初始化分箱范围和边界
        2. 生成初始等宽直方图
        3. 执行归一化处理
        4. 合并满足条件的小分箱
    """
    # 处理数据范围参数
    if range_values is None:
        range_values = (min(data), max(data))
    # 扩展最大值边界确保包含数据极值
    range_values = (range_values[0], range_values[1] + interval_size)

    # 生成等宽分箱数组
    if bins is None:
        bins = np.arange(range_values[0], range_values[1] + interval_size,
                         interval_size)

    # 计算原始直方图计数
    hist_count, bin_edges = np.histogram(data,
                                         density=False, range=range_values,
                                         bins=bins)

    # 确定归一化基数
    if total_count is None:
        total_count = sum(hist_count)

    # 执行归一化处理
    hist = np.array(hist_count) / float(total_count)

    # 执行分箱合并优化
    hist, bin_edges = _join_var_bins(hist, bin_edges, th_min=th_min, th_acc=th_acc)
    return hist, bin_edges





        