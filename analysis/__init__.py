"""
ProbabilityMap stores in memory, dump to disk, and load from disk probability
 maps.  
"""
from bisect import bisect_right, bisect_left
import pickle
import random
import random_control

class ProbabilityMap(object):
    """
    该类可以配置为生成随机数，以控制生成值在特定范围内的概率。
    """

    def __init__(self, probabilities=None, value_ranges=None,
                 interval_policy="random", value_granularity=None,
                 round_up=False):
        """
        初始化值生成器的配置参数和验证规则
        Args:
            probabilities (list[float], optional): 累积概率列表，决定取值区间的概率分布。
                必须满足递增且最后一个元素接近1.0（如[0.2, 0.5, 1.0]）
            value_ranges (list[tuple], optional): 数值区间列表，每个元组表示一个取值范围。
                格式必须为[(min1, max1), (min2, max2), ...]，且min<=max
            interval_policy (str): 区间内取值策略，可选：
                "random" - 区间内均匀随机取值（默认）
                "midpoint" - 取区间中值
                "low" - 取区间最小值
                "high" - 取区间最大值
            value_granularity (int, optional): 数值生成粒度，生成的数值将是该值的整数倍
            round_up (bool): 当启用粒度时，True表示向上取整，False表示向下取整
        Raises:
            ValueError: 当参数不满足以下任一条件时抛出：
                - value_ranges和probabilities长度不一致
                - value_ranges包含非法区间（非二元组或min>max）
                - probabilities非递增或数值越界（超出[0,1]范围）
                - 最后一个概率值小于0.99
        """
        # 参数完整性校验
        if probabilities is not None and value_ranges is not None:
            # 验证区间列表和概率列表长度匹配
            if len(value_ranges) != len(probabilities):
                raise ValueError("Value ranges must have the same number of "
                                 "elements than "
                                 "probabilities")

            # 验证每个区间格式合法性
            for one_interval in value_ranges:
                if not len(one_interval) == 2:
                    raise ValueError("Value ranges have to be tuples: {}".format(
                        one_interval))
                if one_interval[1] < one_interval[0]:
                    raise ValueError("Wrong Tuple: {}".format(one_interval))

            # 验证概率递增性和范围有效性
            prev_val = None
            for prob in probabilities:
                if (prev_val is not None):
                    if prev_val > prob:
                        raise ValueError("Probabilities don't increase")
                if prob < 0 or prob > 1.1:
                    raise ValueError("A single probability cannot be under 0 or"
                                     " over 1: {}".format(prob))
                prev_val = prob
            # 强制最后一个概率为1.0确保完全覆盖
            if probabilities[-1] < 0.99:
                raise ValueError("maximum probability is not present")
            probabilities[-1] = 1.0

        # 存储配置参数
        self._container = dict(probabilities=probabilities,
                               value_ranges=value_ranges,
                               value_granularity=value_granularity,
                               interval_policy=interval_policy,
                               round_up=round_up)

        # 初始化随机数生成器
        self.random_gen = random_control.get_random_gen()

    def get_probabilities(self):
        return self._container["probabilities"]
    
    def get_value_ranges(self):
        return self._container["value_ranges"]
    
    def get_value_granularity(self):
        return self._container["value_granularity"]
    
    def get_interval_policy(self):
        return self._container["interval_policy"]
    
    def get_round_up(self):
        return self._container["round_up"]

    def save(self, file_route):
        """将容器对象序列化并保存到指定文件路径。

        使用pickle模块将内部_container对象序列化为二进制格式，并写入指定路径文件。
        该方法使用显式文件句柄操作，写入完成后会确保关闭文件资源。

        Args:
            file_route (str): 要保存的目标文件路径，需包含文件名及扩展名（如：'data/container.pkl'）

        Raises:
            IOError: 当文件路径无效或不可写时抛出
            pickle.PicklingError: 当对象序列化失败时抛出

        Returns:
            None: 该方法没有返回值
        """
        output = open(file_route, 'wb')
        pickle.dump(self._container, output)
        output.close()
    
    def load(self, file_route):
        """从指定文件加载数据填充到对象容器中
        反序列化给定路径的pickle文件，将数据加载到对象的_container属性。
        该方法会覆盖容器中原有的数据。
        Args:
            file_route (str): pickle文件路径，需确保文件存在且有读取权限
        Returns:
            None
        """
        # 读取并解析pickle文件的核心操作
        pkl_file = open(file_route, 'rb')
        self._container = pickle.load(pkl_file)
        pkl_file.close()
        
    def produce_number(self):
        """根据配置生成一个随机数。
        方法执行流程:
        1. 验证概率配置和数值范围配置是否有效
        2. 生成[0.0,1.0)区间的均匀分布随机数
        3. 根据随机数选取对应的数值区间
        4. 按区间策略在选定区间内生成具体数值
        5. 对结果进行舍入处理
        异常:
            ValueError: 概率未配置或数值范围配置为空时抛出
        返回:
            float: 经过区间策略计算和舍入处理后的最终数值
        """
        # 前置校验：确保概率配置和数值范围已正确初始化
        if not self.get_probabilities() or not self.get_value_ranges():
            raise ValueError("Probability not configured correctly")
        # 生成用于概率分片选择的基准随机数
        r = self.random_gen.uniform(0.0, 1.0)
        # 根据随机数定位到对应的数值区间
        value_interval = self._get_range_for_number(r)
        # 使用指定策略在目标区间生成具体数值
        n = self._get_value_in_range(value_interval, self.get_interval_policy())
        # 对结果执行数值精度处理
        n = _round_number(n, self.get_round_up())
        return n
        
    def _get_range_for_number(self, number):
        """根据概率数值获取对应的值范围
        通过二分查找在预定义的概率分布中定位输入数值对应的区间，返回该区间对应的值范围对象
        Args:
            number (float): 输入的概率数值，必须在[0,1]区间内。该数值表示在概率分布中的位置
        Returns:
            object: 对应的值范围对象/元组。具体类型由self.get_value_ranges()返回的类型决定
        Raises:
            ValueError: 当输入数值超出[0,1]范围时触发
        """
        # 验证输入数值的合法性
        if number < 0 or number > 1:
            raise ValueError("number({0}) has to be in [0, 1]".format(number))
        # 使用二分查找定位概率区间
        position = bisect_left(self.get_probabilities(), number)
        # 返回对应的值范围
        return self.get_value_ranges()[position]

    def _get_value_in_range(self, value_range, policy="random"):
        """根据指定策略返回数值范围内的一个有效值
        支持多种策略来生成区间范围内的数值，包括随机采样、固定边界值和特殊分布模式
        Args:
            value_range (tuple[float, float]): 数值区间的上下界元组，格式为(最小值, 最大值)
            policy (str, optional): 数值生成策略，可选值包括：
                - "random" : 在区间内均匀随机采样（默认策略）
                - "midpoint" : 始终返回区间的几何中点
                - "low" : 始终返回区间下限值
                - "high" : 始终返回区间上限值
                - "absnormal" : 基于截断正态分布采样，取绝对值后映射到区间范围
        Returns:
            float: 根据指定策略生成的区间数值
        Raises:
            ValueError: 当传入不支持的策略时抛出异常
        """
        # 随机策略：使用均匀分布生成区间内随机值
        if policy == "random":
            r = self.random_gen.uniform(
                float(value_range[0]),
                float(value_range[1]))
            return r

        # 中点策略：计算区间的线性中心值
        if policy == "midpoint":
            return value_range[0] + ((float(value_range[1]) -
                                      float(value_range[0])) / 2)

        # 下限策略：直接返回区间最小值
        if policy == "low":
            return value_range[0]

        # 上限策略：直接返回区间最大值
        if policy == "high":
            return value_range[1]

        # 绝对正态策略：生成正数正态值后映射到区间
        if policy == "absnormal":
            # 生成均值为0，标准差为0.1的正态分布绝对值
            r = abs(self.random_gen.normalvariate(0.0, 0.1))
            r = min(1.0, r)  # 限制最大值不超过1.0
            # 将[0,1]区间的比例值映射到目标区间
            return ((float(value_range[1]) - float(value_range[0])) * r +
                    float(value_range[0]))

        # 无效策略检测
        raise ValueError("Undefined interval policy: " + str(policy))


def _round_number(n, value_granularity=None, up=False):
    """
    按照指定精度对数值进行向下/向上舍入
    Args:
        n: int/float             - 需要舍入的原始数值
        value_granularity: int   - (可选)舍入粒度，当值为None时不进行舍入
        up: bool                 - (可选)舍入方向，False为向下取整(默认)，True为向上取整

    Returns:
        int/float                - 舍入后的数值。当value_granularity为None时返回原值
    """
    if not value_granularity:
        return n
    else:
        # 根据舍入方向选择处理方式
        if not up:
            # 向下舍入：减去余数得到最接近的较小粒度倍数
            return n - (n % value_granularity)
        else:
            # 向上舍入：减去余数后加上粒度得到最接近的较大粒度倍数
            return n - (n % value_granularity) + value_granularity

        


    