from stats import calculate_results, load_results
from stats.trace import ResultTrace

class WorkflowDeltas(object):
    """ 这个类用于计算同一工作流在两个不同跟踪（trace）中的运行时间、等待时间、周转时间和延展因子（stretch factor）的差值。
    其核心目的是对比分析当调度算法不同时，对同一工作流性能指标的影响。具体计算方式为：第二个跟踪（trace）的数值减去第一个跟踪（trace）的数值。
    """
    
    def __init__(self):
        """初始化工作流差异对比指标的存储容器
        用于存储两个调度算法追踪数据及其对比指标的实例变量初始化。
        存储内容包括追踪数据及其标识符，以及计算得到的各项指标差异值列表。
        Args:
            无参数
        实例变量:
            _first_trace: 存储第一个调度算法的追踪数据（如列表/字典）
            _first_trace_id: 第一个追踪数据的标识符（如字符串/整型）
            _second_trace: 存储第二个调度算法的追踪数据
            _second_trace_id: 第二个追踪数据的标识符

            _runtime_deltas: 总执行时间差异列表（第二追踪值 - 第一追踪值）
            _waitime_deltas: 等待时间差异列表
            _turnaround_deltas: 完成时间与到达时间差的差异列表
            _stretch_deltas: 周转时间与实际运行时间比值的差异列表
            _wf_names: 工作流标识符/名称容器（如列表）
        """
        self._first_trace = None
        self._first_trace_id=None
        self._second_trace = None
        self._second_trace_id=None
        
        self._runtime_deltas = None
        self._waitime_deltas = None
        self._turnaround_deltas = None
        self._stretch_deltas = None
        self._wf_names = None
    
    def load_traces(self, db_obj, first_id, second_id):
        """
        加载并预处理两个跟踪记录用于后续比较

        Args:
            db_obj (object): 数据库连接对象，用于访问跟踪数据存储
            first_id (int): 第一个跟踪记录的trace_id标识符
            second_id (int): 第二个跟踪记录的trace_id标识符

        Returns:
            None: 该方法不返回数据，处理结果存储在实例变量中
        """
        # 初始化两个跟踪记录容器
        self._first_trace = ResultTrace()
        self._first_trace_id=first_id

        self._second_trace = ResultTrace()
        self._second_trace_id=second_id

        # 从数据库加载原始跟踪数据
        self._first_trace.load_trace(db_obj, self._first_trace_id)
        self._second_trace.load_trace(db_obj, self._second_trace_id)

        # 对跟踪记录进行工作流预处理
        self._first_workflows=self._first_trace.do_workflow_pre_processing()
        self._second_workflows=self._second_trace.do_workflow_pre_processing()
    
    def produce_deltas(self, append=False):
        """
        生成并存储两个跟踪结果之间的差异值
        根据append标志决定追加或重置现有差异数据。当首次调用或要求重置时，
        会初始化存储列表，否则将新产生的差异值追加到现有数据中。
        Args:
            append (bool): 数据追加模式控制标志
                - True: 保留现有差异数据，将新产生的差异值追加到列表中
                - False: 重置所有历史差异数据，仅保留本次产生的差异值
        Returns:
            tuple: 包含当前所有累积差异数据的元组，结构为：
                (工作流名称列表,
                 运行时间差异列表,
                 等待时间差异列表,
                 周转时间差异列表,
                 扩展时间差异列表)
        实现逻辑：
            1. 调用内部方法生成新差异数据
            2. 根据append标志决定存储列表初始化
            3. 追加新数据到实例存储列表
            4. 返回当前所有累积数据
        """
        # 生成新的差异数据集
        (wf_names, runtime_deltas ,waitime_deltas, turnaround_deltas,
                 stretch_deltas) = self._internal_produce_deltas()

        # 控制存储列表的初始化逻辑：首次调用或强制重置时创建空列表
        if not append or self._runtime_deltas == None:
            (self._wf_names, self._runtime_deltas, 
                 self._waitime_deltas,
                 self._turnaround_deltas,
                 self._stretch_deltas) = ([], [],
                                          [], [],
                                          [])
        # 追加新数据集到存储列表
        self._wf_names+= wf_names
        self._runtime_deltas+= runtime_deltas
        self._waitime_deltas+= waitime_deltas
        self._turnaround_deltas+= turnaround_deltas
        self._stretch_deltas+= stretch_deltas
        
        return  (self._wf_names, self._runtime_deltas, 
                 self._waitime_deltas,
                 self._turnaround_deltas,
                 self._stretch_deltas)
        
    def _internal_produce_deltas(self):
        """
        计算并返回两个工作流跟踪之间的共有工作流差异数据
        本方法通过比较两个工作流集合（_first_workflows和_second_workflows）中的共有工作流，
        生成运行时、等待时间、周转时间和处理延展四个维度的差异数据
        Returns:
            tuple: 包含五个元素的元组，按顺序分别为:
                - wf_names (list): 两个工作流集合共有的工作流名称列表
                - runtime_deltas (list): 每个工作流的运行时间差异列表
                - waitime_deltas (list): 每个工作流的等待时间差异列表
                - turnaround_deltas (list): 每个工作流的周转时间差异列表
                - stretch_deltas (list): 每个工作流的处理延展差异列表

        """
        # 初始化差异数据存储容器
        runtime_deltas = []
        waitime_deltas = []
        turnaround_deltas = []
        stretch_deltas = []
        wf_names = []

        # 遍历第一个工作流集合的所有工作流
        for wf_name in self._first_workflows.keys():
            # 仅处理两个集合共有的工作流
            if wf_name in self._second_workflows.keys():
                # 获取两个工作流实例进行对比
                wf_1=self._first_workflows[wf_name]
                wf_2=self._second_workflows[wf_name]
                # 计算四个维度的差异值
                runtime_d, waittime_d, turnaround_d, stretch_d = (
                          self.compare_wfs(wf_1, wf_2))
                
                runtime_deltas.append(runtime_d)
                waitime_deltas.append(waittime_d)
                turnaround_deltas.append(turnaround_d) 
                stretch_deltas.append(stretch_d)
                wf_names.append(wf_name)
        
        return  (wf_names, runtime_deltas ,waitime_deltas, turnaround_deltas,
                 stretch_deltas)
    
    def compare_wfs(self, wf_1, wf_2):
        """比较两个工作流的关键指标差异，返回四维差值元组
        Args:
            wf_1: 基准工作流对象，应包含get_runtime等四个指标方法
            wf_2: 对比工作流对象，应包含get_runtime等四个指标方法
        Returns:
            (runtime_diff, waittime_diff, turnaround_diff, stretch_diff):
            由四个数值组成的元组，分别表示：
            - runtime_diff: 运行时间差（wf_2结果 - wf_1结果）
            - waittime_diff: 等待时间差
            - turnaround_diff: 周转时间差
            - stretch_diff: 拉伸因子差
        """
        # 计算四个关键指标的差值并打包返回
        return (wf_2.get_runtime()-wf_1.get_runtime(),
                wf_2.get_waittime()-wf_1.get_waittime(),
                wf_2.get_turnaround()-wf_1.get_turnaround(),
                wf_2.get_stretch_factor()-wf_1.get_stretch_factor())
    
    def calculate_delta_results(self, store, db_obj=None, trace_id=None):
        """
        计算并存储运行时、等待时间等指标的增量结果数据

        参数:
        store (bool): 是否将结果存储到数据库的标志
        db_obj (object, optional): 数据库连接对象，store为True时必须提供
        trace_id (str, optional): 追踪ID，store为True时必须提供

        返回:
        object: 计算结果，包含统计信息和/或数据库记录ID

        异常:
        ValueError: 当store为True但缺少必要参数时抛出

        注意:
        - 需要至少调用过一次produce delta方法生成数据
        - 结果包含四类指标：运行时增量、等待时间增量、周转时间增量、扩展时间增量
        """
        # 参数有效性校验：当需要存储结果时，必须提供数据库连接对象和追踪ID
        if store and db_obj is None:
            raise ValueError("db_obj must be set to store jobs data")
        if store and trace_id is None:
            raise ValueError("trace_id must be set to store jobs data")
                 
        # 准备四种指标数据集及其对应配置
        # runtime_deltas: 运行时差异数据集
        # waitime_deltas: 等待时间差异数据集
        # turnaround_deltas: 周转时间差异数据集
        # stretch_deltas: 扩展系数差异数据集
        data_list = [self._runtime_deltas, self._waitime_deltas,
                     self._turnaround_deltas, self._stretch_deltas]
        # 数据库字段名配置
        field_list=["delta_runtime", "delta_waittime", "delta_turnaround",
                    "delta_stretch"]

        # 直方图分箱配置：前三个指标使用30个分箱，最后一个使用0.01间隔
        bin_size_list = [30,30, 30, 0.01]

        # 数值范围限制配置（当前未设置限制）
        minmax_list = [None, None, None, None]

        # 调用通用结果计算方法并返回
        return calculate_results(data_list, field_list,
                      bin_size_list,
                      minmax_list, store=store, db_obj=db_obj, 
                      trace_id=trace_id)
    
    def load_delta_results(self, db_obj, trace_id):
        """创建直方图和数值统计对象，并加载工作流差异分析结果。

        从数据库获取两条trace之间的工作流差异数据（delta信息），生成Histogram和NumericStats
        对象，并将其设置为实例属性self._[analyzed job field]。本方法是差异分析结果的核心加载方法。

        Args:
            db_obj (DBManager): 数据库管理对象，用于从数据库提取分析结果数据
            trace_id (int): 追踪标识符，标识数据所属的特定追踪记录

        Returns:
            list|None: 返回load_results函数的结果，通常是包含分析结果的列表，失败时可能返回None

        功能说明:
            - 定义需要分析的四个差异指标字段
            - 调用底层load_results方法实现实际数据加载
            - 结果对象会被自动设置为实例属性（根据字段名自动生成属性名称）
        """
        field_list=["delta_runtime", "delta_waittime", "delta_turnaround",
                    "delta_stretch"]
        return load_results(field_list, db_obj, trace_id)



        
        