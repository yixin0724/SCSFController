from analysis.jobAnalysis import calculate_histogram
import cPickle
import MySQLdb
import numpy as np


class Result(object):
    """
    用于工作负载分析结果的抽象类。这样的类必须能够在一个数据集上计算一些数据。该类将结果存储在数据库中，也可以从数据库中检索数据。
    应该以一种可以绘制的方式公开数据。
    """
    
    def __init__(self, table_name, keys=None):
        """初始化结果存储对象
        该构造函数用于创建存储数据库查询结果的实例对象，包含数据存储结构、
        表名信息及唯一标识键的初始化配置
        Args:
            table_name (str): 存储该结果数据的数据库表名称
            keys (list[str], optional): 标识结果各组成部分的唯一字符串列表。
                当需要区分同类结果的不同变体时使用，默认为空列表
        Returns:
            None: 构造函数无返回值
        """
        if keys is None:
            keys=[]
        # 初始化实例属性
        # _data: 核心字典结构，用于存储结果数据
        # _table_name: 持久化存储的目标表名
        # _keys: 用于区分结果变体的唯一标识键集合
        self._data = {}
        self._table_name=table_name
        self._keys= keys
        
    def calculate(self, dataset):    
        """
        计算对数据集的统计信息，并将结果存储在 self._data。
        """
        pass
    
    def store(self, db_obj, trace_id, measurement_type):
        """
        存储self的内容。_data中的self。_table_name表。
        Args:
        - db_obj: DBManager对象与数据库进行交互。
        
        Returns: 在数据库中标识结果条目的主键。
        
        如果insertiln失败将引发SystemError异常。
        """
        # 获取数据字典的键
        keys  = self._data.keys()
        # 根据键编码数据值
        values = [self._encode(self._data[key], key) for key in keys]
        # 在键列表中添加trace_id和type，以便在数据库中跟踪和分类数据
        keys = ["trace_id", "type"] + keys
        # 在值列表中添加trace_id和measurement_type，以对应新增的键
        values= [trace_id, measurement_type] + values
        # 使用DBManager对象将键和值插入到表中，并获取插入ID
        ok, insert_id = db_obj.insertValues(self._table_name, keys, values,
                                            get_insert_id=True)
        # 如果数据插入失败，抛出SystemError异常
        if not ok:
            raise SystemError("Data insertion failed")
        # 返回插入数据的主键ID
        return insert_id
    
    def load(self, db_obj, trace_id, measurement_type):
        """
        从数据库表中加载指定条件的记录到对象属性
        通过给定的trace_id和measurement_type作为查询条件，从预定义表名的数据库表中
        获取对应记录，并将字段值解码后设置到对象的_data属性中
        Args:
            db_obj (DBManager): 数据库管理对象，提供数据库访问接口
            trace_id (int): 需要加载数据的追踪记录唯一标识
            measurement_type (str): 测量类型标识符，用于区分不同类型的数据记录
        Returns:
            None: 无直接返回值，但会将加载的数据存储到对象实例的_data属性中
        Notes:
            - 使用self._keys定义的字段列表进行数据查询
            - 自动对数据库返回值进行解码处理
            - 仅加载符合条件的第一条记录（如果存在多个匹配记录）
        """
        keys  = self._keys
        # 从数据库获取指定条件的字段值字典列表
        data_dic=db_obj.getValuesDicList(self._table_name, keys, condition=
                                        "trace_id={0} and type='{1}'".format(
                                        trace_id, measurement_type))
        # 如果存在有效查询结果则设置对象属性
        if data_dic is not None and data_dic != ():
            # 遍历所有预定义字段进行解码和赋值
            for key in keys:
                self._set(key, self._decode(data_dic[0][key], key))
    
    def get_data(self):
        return self._data
    
    def _set(self, data_name, data_value):
        self._data[data_name] = data_value
    
    def _get(self, data_name):
        if data_name in self._data.keys():
            return self._data[data_name]
        return None
    
    def _encode(self, data_value, key):
        """
        Encodes data_value to the format of a column of the table used by
        this class. To be re-implemented in child classes as the table
        defintion will change."""
        return data_value
    def _decode(self, blob, key):
        """
        Decodes blob from the format outputed by a dabatase query. To be
        re-implemented in child classes as the table implementation will
        change."""
        return blob 

    def create_table(self, db_obj):
        """
        Creates the table associated with this Result class.
        Args:
        - db_obj: DBManager object allows access to a database.
        """
        db_obj.doUpdate(self._create_query())
    
    def _create_query(self):
        """Returns a string with the query needed to create a table
        corresponding to this Result class. To be modifed according to the table
        formats required by the child classes."""
        return  ""
    
    def get_list_of_results(self, db_obj, trace_id):
        """Returns a list of the result types corresponding to this Result that
        are for a trace identified by trace_id.
        Args:
        - db_obj: DBMaster connected object.
        - trace_id: integer id of a trace
        """
        lists = db_obj.getValuesAsColumns(
                self._table_name, ["type"], 
                condition = "trace_id={0}".format(trace_id))
        return lists["type"]
    
    def plot(self, file_name):
        """Plots results on a filename"""
        pass
    
class Histogram(Result):
    """
    直方图结果类。它在数据集上生成直方图（箱和边）。
    """
    def __init__(self):
        super(Histogram,self).__init__(table_name="histograms",
                                       keys = ["bins", "edges"])
    
    def calculate(self, data_set, bin_size, minmax=None, input_bins=None):
        """
        根据输入数据集计算直方图并存储结果
        Args:
            data_set (list[float]): 用于生成直方图的输入数据集
            bin_size (float|None): 期望的直方图分箱宽度。当提供input_bins参数时该参数会被忽略，
                若未提供minmax参数时必须设置此参数
            minmax (tuple[float,float]|None): 直方图计算的范围约束(min, max)。
                当未提供bin_size参数时必须设置此参数
            input_bins (list[float]|None): 预定义的分箱边界列表。指定后将覆盖bin_size参数，
                直接使用这些精确的分箱边界
        Returns:
            None: 结果通过_set()方法存储在对象的'bins'和'edges'属性中
        Raises:
            ValueError: 当bin_size和minmax同时为None时抛出
        """
        if bin_size is None and minmax is None:
            raise ValueError("Either bin_size or bin has to be set")
        # 核心直方图计算逻辑（支持灵活配置）
        bins, edges  = calculate_histogram(data_set, th_min=0.0, th_acc=0.0,
                                               range_values=minmax, 
                                               interval_size=bin_size,
                                               bins=input_bins)

        # 持久化计算结果
        self._set("bins", bins)
        self._set("edges", edges)
        
    def get_data(self):
        return self._get("bins"), self._get("edges")
    
    def _create_query(self):
        return """create table {0} (
                    id INT NOT NULL AUTO_INCREMENT,
                    trace_id INT(10) NOT NULL,
                    type VARCHAR(128) NOT NULL,
                    bins LONGBLOB,
                    edges LONGBLOB,
                    PRIMARY KEY(id, trace_id, type)
                )""".format(self._table_name)
    
    def _encode(self, data_value, key):
        """将Python对象序列化并转义为适合MySQL存储的格式

        对数据进行cPickle序列化后执行MySQL特殊字符转义，用于安全存储到BLOB类型字段。
        注意：参数key在当前实现中未使用，保留作未来扩展。

        Args:
            data_value (any): 需要存储的Python对象，会被序列化为字节流
            key (any): 预留参数，当前版本未参与实际处理逻辑

        Returns:
            str: 经过MySQL转义处理的序列化字符串，可直接插入数据库

        实现流程:
            1. 使用cPickle进行高性能序列化
            2. 对二进制数据进行数据库安全转义
        """
        pickle_data = cPickle.dumps(data_value)
        return MySQLdb.escape_string(pickle_data)
    def _decode(self, blob, key):
        return cPickle.loads(blob)

class NumericList(Result):
    
    def _create_query(self):
        cad= """create table `{0}` (
                    id INT NOT NULL AUTO_INCREMENT,
                    trace_id INT(10) NOT NULL,
                    type VARCHAR(128) NOT NULL,
                    """.format(self._table_name)
        for field in self._keys:
            cad+=" {0} DOUBLE,".format(field)
        cad+="""   PRIMARY KEY(id, trace_id, type))"""
        return cad

    def set_dic(self, the_dic):
        for (key,value) in the_dic.iteritems():
            self._set(key, value)
    
    def apply_factor(self, factor):
        for key in self._keys:
            self._set(key, float(self._get(key))*float(factor))
            
class NumericStats(Result):
    """
    对数据集的基本分析是否包括：最小值、最大值、平均值、标准差、数据集计数、中位数和五个百分位数(5, 25, 50, 75, 95).
    get_data返回的对象是一个由以下键索引的字典："min","max", "mean", "std", "count", "median", "p05", "p25", "p50", "p75", "p95".
    """
    def __init__(self):
        """
        初始化数值统计信息存储对象
        调用父类构造函数创建指定结构的表，用于存储基础统计指标（最小值、最大值等）
        和分位数统计指标（中位数、百分位数等）。表结构通过keys参数定义指标字段名称，
        通过table_name参数指定存储表名。
        参数说明：
        - table_name: str 类型，指定存储统计结果的表名称，固定为"numericStats"
        - keys: list[str] 类型，定义统计指标字段，包含：
            * min: 最小值
            * max: 最大值
            * mean: 平均值
            * std: 标准差
            * count: 数据计数
            * median: 中位数（等同于p50）
            * p05: 第5百分位数
            * p25: 第25百分位数（第一四分位数）
            * p50: 第50百分位数（中位数）
            * p75: 第75百分位数（第三四分位数）
            * p95: 第95百分位数
        返回值：None
        """
        super(NumericStats,self).__init__(table_name="numericStats",
            keys = ["min", "max", "mean", "std", "count", "median",
                    "p05", "p25", "p50", "p75", "p95" ])
    
    def apply_factor(self, factor):
        """
        将给定的因子应用到所有统计指标上，对各统计量进行数值缩放

        参数：
        - factor: float，乘数因子，所有统计指标将与该因子相乘进行数值缩放

        返回值：
        None
        """
        # 遍历所有统计量键名进行数值更新
        for key in ["min", "max", "mean", "std", "median",
                    "p05", "p25", "p50", "p75", "p95" ]:
            # 获取原值->应用因子->设置新值的完整更新流程
            self._set(key, float(self._get(key))*float(factor))
            
    def calculate(self, data_set):
        """对数据集执行多项统计指标计算，并将结果存储在类属性中
        Args:
            data_set (list): 包含数值数据的列表，支持整型和浮点型数据。数据将被转换为numpy数组进行计算。
        Returns:
            None: 结果通过类的_set方法存储，不直接返回计算结果。存储的指标可通过类属性获取。
        功能说明:
            计算最小值、最大值、均值、标准差、数据总数、中位数及5%、25%、50%、75%、95%分位数
        """
        # 将输入数据转换为numpy数组进行向量化计算
        x = np.array(data_set, dtype=np.float)

        # 基础统计量计算：最小值、最大值、均值、标准差、样本数量
        self._set("min", min(x))
        self._set("max", max(x))
        self._set("mean", np.mean(x))
        self._set("std", np.std(x))
        self._set("count", x.shape[0])

        # 分位数计算与存储：包含中位数(p50)及多个常用分位点
        percentile_name=["p05", "p25", "p50", "p75", "p95"]
        percentlie_values = np.percentile(x, [5, 25, 50, 75, 95])
        self._set("median", percentlie_values[2])
        for (key, per) in zip(percentile_name, percentlie_values):
            self._set(key, per)
        

    def _encode(self, data_value, key):
        return data_value
    def _decode(self, blob, key):
        return float(blob) 
    
    def _create_query(self):
        return """create table {0} (
                    id INT NOT NULL AUTO_INCREMENT,
                    trace_id INT(10) NOT NULL,
                    type VARCHAR(128) NOT NULL,
                    min DOUBLE,
                    max DOUBLE,
                    mean DOUBLE,
                    std DOUBLE,
                    count int, 
                    median DOUBLE,
                    p05 DOUBLE,
                    p25 DOUBLE,
                    p50 DOUBLE,
                    p75 DOUBLE,
                    p95 DOUBLE,
                    PRIMARY KEY(id, trace_id, type)
                )""".format(self._table_name)
    
    def get_values_boxplot(self):
        data_names = "median", "p25", "p75", "min", "max" 
        return [self._get(x) for x in data_names]
    
    
def calculate_results(data_list, field_list, bin_size_list,
                      minmax_list, store=False, db_obj=None, trace_id=None):
    """对多组数据集进行统计分析，生成直方图（CDF）和数值统计结果
    本函数处理多组输入数据集，为每个数据集同时生成两种分析结果：
    1. 使用Histogram类计算累积分布函数（CDF）
    2. 使用NumericStats类计算基础统计指标
    支持将结果持久化到数据库，返回包含所有分析结果的字典
    Args:
        data_list (List[List[float]]): 多组待分析数据集，每组数据为一个数值列表
        field_list (List[str]): 数据集名称列表，与data_list顺序对应
        bin_size_list (List[float]): CDF分析的箱宽配置列表，与data_list顺序对应
        minmax_list (List[Tuple[float]]): 各数据集CDF分析的范围约束（最小值，最大值）
        store (bool, optional): 是否持久化到数据库，默认False
        db_obj (DBManager, optional): 数据库连接对象，存储时需要提供
        trace_id (int, optional): 数据溯源标识符，存储时需要提供
    Returns:
        Dict[str, Union[Histogram, NumericStats]]: 包含分析结果的字典，键格式为：
            "[字段名]_cdf"：对应的Histogram直方图对象
            "[字段名]_stats"：对应的NumericStats统计对象
    实现说明：
    - 对空数据集仍会生成NumericStats对象（不含计算结果）
    - 存储操作依赖db_obj和trace_id参数的正确配置
    """
    # 生成结果字典键名列表
    cdf_field_list = [x+"_cdf" for x in field_list]
    stats_field_list = [x+"_stats" for x in field_list]
    results_dic={}

    # 并行处理每个数据集及其配置参数
    for (data, cdf_field, stats_field, bin_size, minmax) in zip(data_list,
            cdf_field_list, stats_field_list, bin_size_list, minmax_list):

        # 处理直方图分析（仅当数据集非空时）
        if data:
            cdf = Histogram()
            # 执行带范围约束的直方图计算
            cdf.calculate(data, bin_size=bin_size, minmax=minmax)
            # 条件存储到数据库
            if store:
                cdf.store(db_obj, trace_id, cdf_field)
            results_dic[cdf_field]=cdf

        # 处理基础统计（对空数据集生成空对象）
        stats = NumericStats()
        if data:
            stats.calculate(data)
            if store:
                stats.store(db_obj, trace_id, stats_field)
            results_dic[stats_field]=stats
    return results_dic

def load_results(field_list, db_obj, trace_id):
    """从数据库加载统计结果并构建结果对象集合

    根据字段列表生成对应的直方图（CDF）和数值统计对象，通过数据库查询加载数据，
    最终返回包含所有结果对象的字典。

    Args:
        field_list (list[str]): 需要加载的原始字段名称列表，自动生成对应的
            "_cdf"和"_stats"后缀字段用于数据库查询
        db_obj (DBManager): 已配置的数据库管理对象，用于执行数据查询操作
        trace_id (int): 标识目标数据轨迹的唯一数字ID

    Returns:
        dict: 包含统计结果对象的字典，键为带后缀的字段名（如"field_cdf"），
            值为对应的Histogram或NumericStats对象实例

    处理流程：
    1. 根据输入字段列表生成带_cdf和_stats后缀的查询字段
    2. 遍历所有字段组合，从数据库加载对应数据
    3. 将成功加载的结果对象存入返回字典
    """
    results = {}

    # 生成带不同后缀的查询字段列表
    cdf_field_list = [x+"_cdf" for x in field_list]
    stats_field_list = [x+"_stats" for x in field_list]

    # 遍历所有字段组合加载数据
    for (cdf_field, stats_field) in zip(cdf_field_list, stats_field_list):
        # 加载并存储直方图数据
        cdf = Histogram()
        cdf.load(db_obj, trace_id, cdf_field)
        if cdf is not None:
            results[cdf_field] = cdf

        # 加载并存储数值统计信息
        stats = NumericStats()
        stats.load(db_obj, trace_id, stats_field)
        if stats is not None:
            results[stats_field] = stats
    return results
        
        
    
    
