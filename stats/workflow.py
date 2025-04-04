from stats import calculate_results, load_results, Histogram, NumericStats
from generate.pattern import WorkflowGeneratorMultijobs
import bisect
import os

class WorkflowsExtractor(object):
    """ 提取ResultTrace对象内部的工作流。它还生成了这些工作流的指标。
    """
    def __init__(self):
        """Constructor"""
        self._workflows={}

    def extract(self, job_list, reset_workflows=False):
        """从作业数据中提取工作流信息并构建跟踪结构
        处理跟踪作业集合以组织成工作流，验证输入数据一致性，并可选重置现有工作流数据
        Args:
            job_list (dict of lists): 包含并行作业数据数组的字典。必须包含以下键：
                - "job_name": 作业标识字符串
                - "time_start": 作业开始时间戳
                - "time_end": 作业结束时间戳
                - "id_job": 唯一作业ID
                各个列表中相同索引位置的元素属于同一个作业
            reset_workflows (bool, optional): 为True时会在处理前重置内部工作流状态。默认为False
        Returns:
            None: 直接更新内部_workflows字典
        Raises:
            ValueError: 当输入列表长度不一致时抛出
        """
        # 根据要求重置工作流跟踪结构
        if reset_workflows:
            self._workflows={}
        # 验证输入数据一致性
        size=None
        for value in job_list.values():
            if size is not None and size!=len(value):
                raise ValueError("All lists in job_list should have the same"
                                 " length.")
        
        #self._workflows = {}
        # 按顺序处理每个作业条目
        count = len(job_list.values()[0])
        for i in range(count):
            # 验证并将作业数据整合到工作流中
            self.check_job(job_list, i)
        
    
    def do_processing(self):
        """对类中所有工作流实例进行依赖填充和关键路径分析
        遍历所有已解析的工作流实例，依次执行以下操作：
        1. 填充当前工作流的依赖关系
        2. 探索工作流执行的关键路径
        Args:
            self: 类实例对象，包含需要处理的_workflows字典属性
        Returns:
            None: 本方法不返回任何值，处理结果直接作用于工作流对象
        """
        # 遍历所有注册的工作流实例，依次进行依赖关系构建
        for wf in self._workflows.values():
            wf.fill_deps()
        
    
    def check_job(self, job_list, pos):
        """检查给定的作业是否属于工作流，并将其注册到对应的工作流跟踪器中
        工作流作业的命名格式为wf_[manifest]-[job_id]_[stage]_[deps]，且以"wf"开头。
        该方法会解析作业名称，创建或获取对应的工作流跟踪器，并注册作业信息。
        Args:
            job_list (dict): 包含作业信息的字典，字典值为列表结构。各键对应列表存储：
                - time_end: 作业结束时间戳列表
                - time_start: 作业开始时间戳列表
                - job_name: 作业名称列表
                - id_job: 作业ID列表
            pos (int): 在job_list各列表中对应作业的索引位置
        Returns:
            bool: 如果作业属于工作流返回True，否则返回False。注意未开始/未结束的作业直接返回False
        """
        # 过滤未开始或未结束的作业（时间戳为0表示无效状态）
        if job_list["time_end"][pos]==0 or job_list["time_start"][pos]==0:
            return False
        job_name = job_list["job_name"][pos]
        id_job = job_list["id_job"][pos]

        # 检测工作流作业特征：以"wf"开头的作业名称
        if "wf" == job_name[:2]:
            # 解析工作流名称的三个组成部分：主名称、阶段ID、依赖项
            name, stage_id, deps = TaskTracker.extract_wf_name(job_name)

            # 为新的工作流创建跟踪器实例（如果不存在）
            if not name in self._workflows.keys():
                self._workflows[name] = WorkflowTracker(name)

            # 注册任务到工作流跟踪器，stage_id空字符串表示主工作流任务
            self._workflows[name].register_task(job_list, pos, stage_id=="")
            return True
        else:
            return False 
    def get_workflow(self, wf_name):
        """根据工作流名称获取对应的工作流追踪器对象
        通过组合键（清单名称_作业ID）从存储字典中检索并返回工作流对象。
        函数名中的manifest name指工作流配置文件名，job id指父作业的唯一标识符。
        Args:
            wf_name (str): 复合标识字符串，格式为"[manfiest name]_[job id]"，其中：
                - manfiest name: 工作流配置文件的名称
                - job id: 关联的父作业ID，用于区分同名配置的不同实例
        Returns:
            WorkflowTracker: 与输入标识符匹配的工作流追踪器对象，直接返回内部字典的对应值
        """
        return self._workflows[wf_name]
    
    def _get_workflow_times(self, submit_start=None, submit_stop=None): 
        """提取工作流时序指标和资源使用数据
        处理存储的工作流数据，计算六个关键指标。根据提交时间范围过滤工作流，
        跳过未完成的工作流，最终聚合所有符合条件工作流的指标。
        Args:
            submit_start (int, 可选): 包含的工作流最早提交时间戳（纪元时间）。
                早于此时间戳提交的工作流将被排除。默认为 None（无下限）。
            submit_stop (int, 可选): 包含的工作流最晚提交时间戳（纪元时间）。
                晚于此时间戳提交的工作流将被排除。默认为 None（无上限）。
        Returns:
            tuple: 包含六个列表的元组，顺序为：
                - wf_runtime (list): 每个工作流的总执行时长
                - wf_waittime (list): 每个工作流的队列等待时间
                - wf_turnaround (list): 每个工作流从提交到完成的总耗时
                - wf_stretch_factor (list): 每个工作流关键路径等待时间与运行时长的比值
                - wf_jobs_runtime (list): 所有工作流中全部作业的运行时长集合
                - wf_jobs_cores (list): 所有工作流中全部作业使用的核心数集合
        实现要点：
            - 明确排除未完成的工作流
            - 作业级指标（运行时长/核心数）会跨工作流合并
            - 时间过滤基于提交时间而非执行时间
        """
        # 工作流级别指标容器
        wf_runtime = []
        wf_waittime = []
        wf_turnaround= []
        wf_stretch_factor = []

        # 跨工作流累计的作业级指标容器
        wf_jobs_runtime = []
        wf_jobs_cores = []

        # 遍历内部注册的工作流
        for wf in self._workflows.values():
            # 跳过标记为未完成的工作流
            if wf._incomplete_workflow:
                continue

            # 基于提交时间戳进行过滤
            submit_time = wf.get_submittime()
            if (submit_start is not None and submit_time < submit_start):
                continue
            if (submit_stop is not None and submit_stop<submit_time):
                continue
            # 采集工作流级时序指标
            wf_runtime.append(wf.get_runtime())
            wf_waittime.append(wf.get_waittime())
            wf_turnaround.append(wf.get_turnaround())
            wf_stretch_factor.append(wf.get_stretch_factor())

            # 聚合跨工作流的作业级指标
            wf_jobs_runtime = wf_jobs_runtime + wf.get_jobs_runtime()
            wf_jobs_cores = wf_jobs_cores + wf.get_jobs_cores()
           
        return (wf_runtime, wf_waittime, wf_turnaround, wf_stretch_factor,
                 wf_jobs_runtime, wf_jobs_cores)
    
    def _get_per_manifest_workflow_times(self,
                                         submit_start=None,
                                         submit_stop=None):
        """
        按manifest类型统计工作流性能指标
        遍历所有已识别的工作流，按manifest类型分组统计运行时、等待时间、周转时间、
        拉伸系数（关键路径中的作业间等待时间/运行时）、作业运行时和作业核心数。
        Args:
            submit_start (int, optional): 提交时间起始过滤阈值（epoch时间戳），
                早于此时间提交的工作流将被忽略。默认为None表示无限制
            submit_stop (int, optional): 提交时间截止过滤阈值（epoch时间戳），
                晚于此时间提交的工作流将被忽略。默认为None表示无限制
        Returns:
            dict: 按manifest名称索引的字典，每个值包含六个指标列表：
                - wf_runtime: 工作流总运行时列表
                - wf_waittime: 工作流等待时间列表
                - wf_turnaround: 工作流周转时间列表
                - wf_stretch_factor: 关键路径拉伸系数列表
                - wf_jobs_runtime: 所有作业运行时累计列表
                - wf_jobs_cores: 所有作业核心使用累计列表
        """
        manifests = {}
        # 遍历所有工作流进行过滤和分类
        for (name, wf) in self._workflows.iteritems():
            # 跳过未完成的工作流
            if wf._incomplete_workflow:
                continue
            # 应用时间范围过滤
            submit_time = wf.get_submittime()
            if (submit_start is not None and submit_time < submit_start):
                continue
            if (submit_stop is not None and submit_stop<submit_time):
                continue

            # 从工作流名称提取manifest类型（前缀分隔符"-"前的部分）
            manifest = name.split("-")[0]

            # 初始化manifest的指标存储结构
            if not manifest in manifests.keys():
                manifests[manifest]=dict(wf_runtime = [],
                                        wf_waittime = [],
                                        wf_turnaround= [],
                                        wf_stretch_factor = [],
                                        wf_jobs_runtime = [],
                                        wf_jobs_cores = [])
            # 收集基础指标
            manifests[manifest]["wf_runtime"].append(wf.get_runtime())
            manifests[manifest]["wf_waittime"].append(wf.get_waittime())
            manifests[manifest]["wf_turnaround"].append(wf.get_turnaround())
            manifests[manifest]["wf_stretch_factor"].append(
                                                    wf.get_stretch_factor())
            # 合并作业级别的指标（列表拼接
            manifests[manifest]["wf_jobs_runtime"] = (
                manifests[manifest]["wf_jobs_runtime"] + wf.get_jobs_runtime())
            manifests[manifest]["wf_jobs_cores"] = (
                manifests[manifest]["wf_jobs_cores"] + wf.get_jobs_cores())
        return manifests
    
    def calculate_wf_results(self, db_obj,trace_id, wf_runtime, wf_waittime,
                         wf_turnaround, wf_stretch_factor,
                         wf_jobs_runtime, wf_jobs_cores, store=False, 
                         prefix=None):
        """计算工作流相关指标的统计分布和直方图数据
        对工作流运行时、等待时间、周转时间、拉伸系数等指标进行统计分析，生成累积分布函数(CDF)
        和数值统计结果。计算结果以字典形式返回，键名遵循特定命名规范。
        Args:
            db_obj (object): 数据库连接对象，用于存储结果（当store为True时）
            trace_id (int): 跟踪数据集的唯一标识符
            wf_runtime (list): 工作流总运行时间列表（单位：秒）
            wf_waittime (list): 工作流等待时间列表（单位：秒）
            wf_turnaround (list): 工作流周转时间列表（单位：秒）
            wf_stretch_factor (list): 工作流拉伸系数列表（关键路径等待时间/总周转时间）
            wf_jobs_runtime (list): 工作流内所有作业的运行时间集合（单位：秒）
            wf_jobs_cores (list): 工作流内所有作业使用的核心数集合
            store (bool, optional): 是否将结果持久化到数据库，默认为False
            prefix (str, optional): 结果键名前缀，用于命名空间隔离，默认为None
        Returns:
            dict: 包含统计结果的字典，键名格式为：
                - <前缀>wf_<指标名称>_cdf: 直方图对象
                - <前缀>wf_<指标名称>_stats: 数值统计对象
        实现细节：
        - 不同指标采用差异化的分箱策略（如运行时按分钟分箱，拉伸系数按0.01精度分箱）
        - 支持通过prefix参数实现多组结果的命名空间隔离
        - 时间类指标最大统计范围覆盖30天（2592000秒）
        """
        # 待处理的原始数据数组列表，顺序与field_list对应
        data_list = [wf_runtime, wf_waittime, wf_turnaround, wf_stretch_factor,
                 wf_jobs_runtime, wf_jobs_cores]
        # 基础字段名称列表，用于生成结果字典的键名
        field_list = ["wf_runtime", "wf_waittime", "wf_turnaround",
                      "wf_stretch_factor", "wf_jobs_runtime", "wf_jobs_cores"]

        # 添加命名空间前缀（如果指定）
        if prefix!=None:
            field_list = [prefix+"_"+x for x in field_list]

        # 直方图分箱大小配置：每个指标对应的分箱粒度（秒/核心数/比率）
        bin_size_list = [60,60,120, 0.01, 60, 24]

        # 数值范围配置：每个指标的最小/最大值元组（单位：秒/核心数/比率）
        minmax_list = [(0, 3600*24*30), (0, 3600*24*30), (0, 2*3600*24*30),
                       (0, 1000), (0, 3600*24*30), (0, 24*4000)]

        # 调用核心计算方法返回统计结果
        return calculate_results(data_list, field_list, bin_size_list,
                      minmax_list, store=store, db_obj=db_obj, 
                      trace_id=trace_id)            
    
    def fill_overall_values(self, start=None, stop=None, append=False):
        """填充工作流相关的时间指标数据到实例变量中

        根据给定的时间范围获取工作流运行时指标，并根据append参数决定追加或重置存储列表

        Args:
            start (optional): 起始时间戳，用于筛选工作流提交的起始时间。默认为None表示不限制
            stop (optional): 结束时间戳，用于筛选工作流提交的结束时间。默认为None表示不限制
            append (bool): 控制数据存储模式。当为False时重置所有存储列表，当为True时保留历史数据并追加新数据
        """
        # 获取指定时间范围内的工作流时间指标(包含6个维度数据)
        (wf_runtime, wf_waittime, wf_turnaround, wf_stretch_factor,
                 wf_jobs_runtime, wf_jobs_cores) = self._get_workflow_times(
                                         submit_start=start, submit_stop=stop)
        if not append:
            self._wf_runtime = []
            self._wf_waittime = []
            self._wf_turnaround = []
            self._wf_stretch_factor = []
            self._wf_jobs_runtime = []
            self._wf_jobs_cores = []
            
        self._wf_runtime += wf_runtime
        self._wf_waittime += wf_waittime
        self._wf_turnaround += wf_turnaround
        self._wf_stretch_factor += wf_stretch_factor
        self._wf_jobs_runtime += wf_jobs_runtime
        self._wf_jobs_cores += wf_jobs_cores

    def get_first_workflows(self, keys, num_workflows):
        """返回第一个num_workflows工作流的键列表。"""
        """
            获取按编号排序后的前N个工作流键值
            根据工作流键的"-"分隔最后一段数字编号进行升序排序，返回排序后的前指定数量元素
            参数:
            keys (Iterable[str]): 工作流键集合，键应包含数字编号作为最后一段（如"prefix-123"）
            num_workflows (int): 需要获取的工作流数量
            返回:
            list[str]: 按编号升序排列后的前num_workflows个键，若总数量不足则返回全部
            """
        return sorted(keys, key=lambda x: int(x.split("-")[-1]))[:num_workflows]
        
        
    def truncate_workflows(self, num_workflows):
        """消除检测到的工作流，只保留第一个num_工作流。"""
        """截断存储的工作流，仅保留指定数量的最早工作流。
        根据插入顺序筛选工作流字典，仅保留前'num_workflows'个条目。该方法会直接修改实例的工作流存储。
        Args:
            num_workflows (int): 需要保留的最大工作流数量。超过该数量的后续工作流将被永久移除。
        Returns:
            None: 该方法直接修改内部状态，不返回任何值。
        """
        new_dic={}
        # 获取需要保留的有序工作流键集合
        keys_sub_set = self.get_first_workflows(self._workflows.keys(),
                                                 num_workflows)
        # 使用保留条目重建工作流字典
        for key in keys_sub_set:
            new_dic[key]  = self._workflows[key]
        self._workflows = new_dic
    def rename_workflows(self, pre_id):
        """
        重命名工作流字典中的键名

        参数:
        pre_id: str | None
            用于构建新键名的前缀标识符。当不为None时，会为原始键名添加前缀；
            当为None时，会移除原始键名中的特殊前缀符号（.）

        返回值:
        None: 直接修改实例的_workflows属性
        """
        new_dic={}
        for key in self._workflows.keys():
            if key[0]==".":
                if pre_id is None:
                    new_key=key[1:]
                else:
                    new_key=key
            else:
                if pre_id is not None:
                    workflow_type=key.split("-")[0]
                    workflow_id=key.split("-")[1]
                    new_key=".{0}-{1}{2}".format(workflow_type, pre_id,
                                                 workflow_id)
                else:
                    new_key=key
            new_dic[new_key]  = self._workflows[key]
        self._workflows = new_dic
    
    def calculate_and_store_overall_results(self, store=False, db_obj=None,
                                        trace_id=None,
                                        limited=False):
        """
        计算并存储工作流整体统计结果
        根据运行时参数计算工作流级别的统计指标，可选择将结果存储到数据库。
        当limited模式开启时，结果会添加特定前缀用于区分简化版统计结果。
        Args:
            store (bool, optional): 是否将计算结果存储到数据库，默认False
            db_obj (DBObject, optional): 数据库连接对象，默认None表示不使用数据库
            trace_id (str, optional): 跟踪标识符，用于关联日志/调试，默认None
            limited (bool, optional): 是否生成有限统计结果，开启时会给结果添加前缀，默认False
        Returns:
            dict: 包含工作流级别统计指标的字典，包括：
                - 总运行时间
                - 总等待时间
                - 任务周转时间
                - 伸缩系数
                - 作业运行时间分布
                - 核心使用情况
        """
        # 根据limited参数设置结果前缀
        prefix=None
        if limited:
            prefix="lim"
        # 调用底层计算方法并返回结果
        return self.calculate_wf_results(db_obj,trace_id, self._wf_runtime,
                                  self._wf_waittime, self._wf_turnaround,
                                  self._wf_stretch_factor,
                                  self._wf_jobs_runtime,
                                  self._wf_jobs_cores, store=store,
                                  prefix=prefix)

    def calculate_overall_results(self, store=False, db_obj=None,trace_id=None,
                                  start=None, stop=None, limited=False):
        """生成工作流变量的分析结果并返回统计字典
        对工作流变量进行分析计算，生成累积分布函数(CDF)和统计指标，结果以字典形式返回。
        支持将结果存储到数据库，可通过时间范围限定分析数据范围。
        Args:
            store (bool, optional): 是否将结果存储到数据库，默认为False
            db_obj (DBManager, optional): 数据库管理对象，存储时必需提供
            trace_id (int, optional): 关联数据的追踪ID，存储时必需提供
            start (int, optional): 起始时间戳(epoch)，排除早于此时间的工作流
            stop (int, optional): 终止时间戳(epoch)，排除晚于此时间的工作流
            limited (bool, optional): 是否限制分析范围，默认为False
        Returns:
            dict: 包含分析结果的字典，键名为：
                - "wf_[变量类型]_cdf": 对应变量的累积分布数据
                - "wf_[变量类型]_stats": 对应变量的统计指标
        Raises:
            ValueError: 当存储参数不完整时抛出异常
        实现说明：
        1. 前置条件校验确保存储参数的有效性
        2. 调用fill_overall_values初始化分析数据集
        3. 通过calculate_and_store_overall_results执行核心计算逻辑
        """
        # 存储参数完整性校验
        if store and db_obj is None:
            raise ValueError("db_obj must be set to store jobs data")
        if store and trace_id is None:
            raise ValueError("trace_id must be set to store jobs data")

        # 初始化分析数据集（强制刷新模式
        self.fill_overall_values(start=start, stop=stop, append=False)
        # 执行核心计算并处理存储逻辑
        return self.calculate_and_store_overall_results(store=store,
                                                        db_obj=db_obj,
                                                        trace_id=trace_id,
                                                        limited=limited)
    
    def load_wf_results(self, db_obj, trace_id, prefix=None):
        """从trace trace_id中检索对工作流变量的分析，
        并将其返回到索引为："[[prefix]_]wf_[variable type]_cdf" and"[[prefix]_]wf_[variable type]_stats".
        Args:
            db_obj (DBManager): 数据库管理对象，用于执行数据库查询操作
            trace_id (int): 追踪数据的唯一数字标识符，关联需要分析的工作流数据
            prefix (str, optional): 结果字典键名前缀。若提供，会生成形如"prefix_wf_xxx"的键名
        Returns:
            dict: 包含工作流分析结果的字典，键名为组合后的字段名，值为从数据库加载的对应结果数据。
                具体键名格式为"[prefix_]wf_[variable type]_{cdf|stats}"
        """
        # 预定义基础字段列表，表示要获取的工作流变量类型
        field_list = ["wf_runtime", "wf_waittime", "wf_turnaround",
                      "wf_stretch_factor", "wf_jobs_runtime", "wf_jobs_cores"]

        # 当指定前缀时，为所有字段添加统一前缀
        if prefix!=None:
            field_list = [prefix+"_"+x for x in field_list]

        # 调用底层结果加载方法，返回整合后的结果字典
        return load_results(field_list, db_obj, trace_id)

        
    def load_overall_results(self, db_obj, trace_id):
        """核心统计计算方法
        特性：
        - 时间指标最大统计范围30天（2592000秒）
        - 支持通过prefix参数实现多租户数据隔离
        - 不同指标使用差异化分箱策略：
            * 时间指标：60秒分箱
            * 核心数：24核分箱
            * 拉伸系数：0.01精度分箱
        Returns:
            dict: 包含统计直方图和数值摘要的字典，键名示例：
                "wf_runtime_cdf", "wf_jobs_cores_stats"
        """
        return self.load_wf_results(db_obj, trace_id)

    def fill_per_manifest_values(self, start=None, stop=None, append=False):
        """填充/更新每个manifest的工作流时间数据
        根据给定的时间范围获取新数据，可选择追加或覆盖现有数据。合并同名manifest的时间数据，
        最终更新检测到的manifest列表。
        Args:
            start (optional): 数据获取的起始时间戳，默认为None表示不限制
            stop (optional): 数据获取的结束时间戳，默认为None表示不限制
            append (bool): 是否追加数据。False时重置原有数据，True时保留并合并原有数据
        Returns:
            None: 直接修改实例的_manifests_values和_detected_manifests属性
        """
        # 初始化存储结构（当需要覆盖现有数据时）
        if not append:
            self._manifests_values = {}
        
        new_manifests_values = self._get_per_manifest_workflow_times(
                                                        submit_start=start,
                                                        submit_stop=stop)
        
        for man_name in new_manifests_values:
            if man_name in self._manifests_values.keys():
                self._manifests_values[man_name] = (
                    WorkflowsExtractor.join_dics_of_lists(
                                        self._manifests_values[man_name],
                                        new_manifests_values[man_name]))
            else:
                self._manifests_values[man_name]=new_manifests_values[man_name]  
        self._detected_manifests=list(self._manifests_values.keys())
        
    
    def calculate_and_store_per_manifest_results(self, store=False, db_obj=None,
                                        trace_id=None,limited=False):
        """
        计算并存储每个manifest的工作流指标结果
        Args:
            store (bool): 是否将计算结果持久化存储，默认为False
            db_obj: 数据库连接对象，用于结果存储
            trace_id: 追踪标识符，用于关联相关计算记录
            limited (bool): 是否限制处理的数据范围，True时会在结果前缀添加'lim_'标识
        Returns:
            dict: 以manifest为键，对应计算结果为值的字典。每个计算结果通过
                calculate_wf_results方法获得，包含工作流运行时、等待时间等指标
        """
        # 根据limited参数设置存储前缀
        prefix=""
        if limited:
            prefix="lim_"
        results_per_manifest = {}
        # 遍历所有manifest数据并计算结果
        for (manifest, data) in self._manifests_values.iteritems():
            # 调用核心计算方法获取该manifest的完整结果
            results_per_manifest[manifest]= self.calculate_wf_results(
                db_obj,trace_id,
                data["wf_runtime"], data["wf_waittime"],
                data["wf_turnaround"], data["wf_stretch_factor"],
                data["wf_jobs_runtime"], data["wf_jobs_cores"],
                store=store,prefix=prefix+"m_"+manifest)
        return results_per_manifest
        
    def calculate_per_manifest_results(self, store=False,db_obj=None, 
                                       trace_id=None, start=None, stop=None,
                                       limited=False):
        """按工作流类型（manifest）生成多维指标分析结果
        对检测到的每种工作流类型（按manifest分类）进行运行时、等待时间、周转时间等指标分析，
        返回嵌套字典结构的结果集合。支持结果存储到数据库，并可按时间范围过滤分析数据。
        Args:
            store (bool, optional): 是否存储到数据库。设为True时：
                - 必须提供有效的db_obj和trace_id
                - 默认False
            db_obj (DBManager, optional): 数据库连接对象，store=True时必需
            trace_id (int, optional): 关联数据追踪ID，store=True时必需
            start (int, optional): 起始时间戳（epoch格式），过滤早于此时间提交的工作流
            stop (int, optional): 终止时间戳（epoch格式），过滤晚于此时间提交的工作流
            limited (bool, optional): 二次处理标志。设为True时：
                - 存储结果添加'lim_'前缀
                - 用于增量/部分结果分析
                - 默认False
        Returns:
            dict: 嵌套字典结构的分析结果，格式为：
                {
                    "manifest类型A": {
                        "wf_运行时_cdf": Result对象,
                        "wf_运行时_stats": Result对象,
                        ...其他指标...
                    },
                    ...其他manifest类型...
                }
        Raises:
            ValueError: 当store=True但缺少必要参数时抛出

        处理流程:
        1. 参数有效性校验：检查存储必需参数
        2. 数据准备：根据时间范围获取指定manifest的工作流数据
        3. 结果计算：生成各manifest的CDF直方图和统计指标
        4. 存储处理：根据参数决定是否持久化到数据库
        """
        if store and db_obj is None:
            raise ValueError("db_obj must be set to store jobs data")
        if store and trace_id is None:
            raise ValueError("trace_id must be set to store jobs data")
        self.fill_per_manifest_values(start=start, stop=stop, append=False)
        return self.calculate_and_store_per_manifest_results(store=store,
                                                            db_obj=db_obj,
                                                            trace_id=trace_id,
                                                            limited=limited)
    def load_per_manifest_results(self, db_obj, trace_id):
        """从数据库加载按工作流清单类型分类的分析结果
        检索指定跟踪ID中不同工作流清单（manifest）的指标分析结果，组织为双层字典结构。
        结果包含每个清单类型对应的累积分布函数（CDF）和统计摘要数据。
        Args:
            db_obj (DBManager): 数据库管理对象，用于执行查询操作
            trace_id (int): 跟踪数据集的唯一标识符
        Returns:
            dict: 嵌套字典结构，包含以下层级：
                - 外层键 (str): 检测到的工作流清单类型（如"docker"、"singularity"）
                - 内层键 (str): 指标分类标识符，格式为：
                    * "wf_[变量类型]_cdf" 表示累积分布数据
                    * "wf_[变量类型]_stats" 表示统计摘要数据
                - 值 (Result): 从数据库加载的分析结果对象
        实现流程：
        1. 查询数据库获取当前跟踪ID中存在的所有清单类型
        2. 为每个清单类型加载带前缀的工作流结果数据
        3. 构建分层结果字典便于后续分析
        """
        per_manifest_results = {}
        # 从数据库获取当前跟踪包含的清单类型列表
        self._detected_manifests = self._get_manifests_in_db(db_obj, trace_id)
        # 遍历每个检测到的清单类型加载对应结果
        for manifest in self._detected_manifests:
            # 使用清单前缀加载特定类型的工作流结果
            per_manifest_results[manifest]=self.load_wf_results(db_obj,
                                                trace_id, prefix="m_"+manifest)
        return per_manifest_results
            
    def _get_manifests_in_db(self, db_obj, trace_id):
        """返回指定trace中存在的所有工作流清单类型
        从Histogram和NumericStats两个统计模块中获取结果数据，提取其中符合特定命名规则的
        清单类型名称，合并后返回去重的结果列表。
        Args:
            db_obj: 数据库连接对象，用于执行查询操作
            trace_id: 跟踪记录的唯一标识符，用于定位特定跟踪数据
        Returns:
            list: 包含所有唯一清单类型名称的列表，格式为字符串元素列表
        """
        # 从Histogram模块获取结果并过滤非清单类型数据
        hist = Histogram()
        hist_man = _filter_non_man(hist.get_list_of_results(db_obj, trace_id))
        # 从NumericStats模块获取结果并过滤非清单类型数据
        stats = NumericStats()
        stats_man =  _filter_non_man(stats.get_list_of_results(db_obj, trace_id))

        # 提取下划线分隔后的第二部分作为清单名称（假设原始格式为"prefix_manifestname"）
        # 处理Histogram模块的清单结果
        hist_man=[x.split("_")[1] for x in hist_man]
        # 处理NumericStats模块的清单结果
        stats_man=[x.split("_")[1] for x in stats_man]
        # 合并两个来源的结果并去重返回
        return  list(set(hist_man+stats_man))
    def get_waste_changes(self):
        """
        计算并聚合所有工作流产生的资源浪费变化数据
        Returns:
            list stamps_list: 时间戳列表，表示资源浪费变化发生的时间点（epoch时间）
            list wastedelta_list: 资源浪费变化量列表，正数表示浪费增加，负数表示浪费减少，所有元素总和必须为0
            int acc_waste: 总累计浪费量，单位为核-秒，表示该时间段内的总资源浪费
        """
        # 初始化返回数据结构：时间戳列表、变化量列表、累计值
        stamps_list = []
        wastedelta_list =[]
        acc_waste=0
        # 遍历所有工作流进行数据聚合
        for wf in self._workflows.values():
            # 获取单个工作流的三元组数据：时间戳序列、变化量序列、本工作流累计值
            stamps, usage, acc = wf.get_waste_changes()
            # 累加当前工作流的累计浪费到全局累计值
            acc_waste+=acc
            # 将当前工作流的变化数据与全局数据进行时间线融合
            stamps_list, wastedelta_list = _fuse_delta_lists(stamps_list,
                                                          wastedelta_list,
                                                          stamps, usage)
        return stamps_list, wastedelta_list, acc_waste
    
    @classmethod   
    def join_dics_of_lists(self, dic1, dic2):
        """
        合并两个值类型为列表的字典，将相同键的列表内容合并
        参数:
        dic1 (dict): 第一个字典，值为列表类型
        dic2 (dict): 第二个字典，值为列表类型
        返回:
        dict: 新字典，包含两个输入字典的所有键。每个键对应的值是两个字典中该键列表的合并结果
        """
        new_dic = {}
        # 合并两个字典的所有键并去重
        keys = dic1.keys()+dic2.keys()
        keys = list(set(keys))
        # 对每个键合并对应的列表
        for key in keys:
            new_dic[key]=[]
            # 合并第一个字典中的列表（如果存在该键）
            if key in dic1.keys():
                new_dic[key]+=dic1[key]
            # 合并第二个字典中的列表（如果存在该键
            if key in dic2.keys():
                new_dic[key]+=dic2[key]
        return new_dic

def _fuse_delta_lists(stamps_list, deltas_list, stamps, deltas):
    """合并两个时间戳对应的增量列表，保持时间顺序并合并相同时间戳的增量
    Args:
        stamps_list: list[float], 现有时间戳列表（要求按升序排列）
        deltas_list: list[float], 现有增量值列表，与stamps_list一一对应
        stamps: list[float], 新增时间戳列表（要求按升序排列）
        deltas: list[float], 新增增量值列表，与stamps一一对应
    Returns:
        tuple: 合并后的 (stamps_list, deltas_list)，保持时间升序排列，
               相同时间戳的增量值会被相加合并
    Note:
        使用二分查找维护时间戳有序性，时间复杂度为O(n log n)
    """
    # 遍历新增的每个时间戳及其对应增量
    for (st, us) in zip(stamps, deltas):
        pos = bisect.bisect_left(stamps_list, st)
        if (pos<len(stamps_list)) and stamps_list[pos]==st:
            deltas_list[pos]+=us
        else:
            stamps_list.insert(pos, st)
            deltas_list.insert(pos, us)
    return stamps_list, deltas_list       
            
    
def _filter_non_man(manifests):
    """
    过滤出以'm_'开头的数据条目
    从输入清单列表中筛选出符合特定前缀格式的条目，用于识别特定类型的清单数据。
    本函数主要用于处理基础设施即代码(IaC)场景中的清单文件过滤。
    Args:
        manifests: list[str] 待过滤的原始清单列表，每个元素应为字符串类型的清单条目
    Returns:
        list[str] 过滤后的新列表，仅包含以'm_'前缀开头的清单条目。当输入为空列表时返回空列表。
    """
    return [ x for x in manifests if x[0:2]=="m_"]
    

class WorkflowTracker(object):
    """对象来存储跟踪工作流及其子任务和特征的信息。
    """
    def __init__(self, name):
        """初始化工作流实例
        用于创建工作流对象时进行基础属性初始化，构造函数会自动接收self参数
        并设置工作流的核心数据结构
        Args:
            name (str):
                工作流的唯一标识字符串，通常采用"[manifest名称]_[父作业ID]"格式。
                用于在追踪系统中识别特定工作流实例
        Attributes:
            _tasks (dict):
                用于存储工作流中所有任务的字典结构，键为任务标识，值为任务对象
            _critical_path (list):
                记录工作流的关键路径序列，保存对任务执行顺序有决定性影响的任务列表
            _critical_path_runtime (int):
                关键路径的总运行时长统计，单位与具体实现相关
            _parent_job (None/Job):
                指向父级作业对象的引用，初始状态为未关联父作业
            _incomplete_workflow (bool):
                标记工作流是否处于未完成状态的布尔标识，用于异常处理判断
        """
        self._name = name
        self._tasks={}
        self._critical_path=[]
        self._critical_path_runtime=0
        self._parent_job=None
        self._incomplete_workflow=False;
    
    def get_runtime(self):
        return self._critical_path_runtime
    
    def get_waittime(self):
        if self._parent_job is not None:
            return self._parent_job.get_waittime()
        return (self.get_first_task().get_waittime())        
    def get_turnaround(self):
        return self.get_runtime()+self.get_waittime()
    
    def get_submittime(self):
        return  (self.get_first_task().get_submittime())  
    
    def get_stretch_factor(self):
        acc_wait = self.get_waittime()
        for (t1,t2) in zip(self._critical_path[:-1], self._critical_path[1:]):
            acc_wait += t2.data["time_start"]-t1.data["time_end"]
        return float(acc_wait)/float(self.get_turnaround())
    
    def get_jobs_runtime(self):
        return [x.get_runtime() for x in self.get_all_tasks()]
    
    def get_jobs_cores(self):
        return [x.get_cores() for x in self.get_all_tasks()]
    
    def register_task(self, job_list, pos, parent_job=False):
        """将指定作业注册为工作流任务
        Args:
            job_list (dict): 包含作业信息的字典，字典值为列表结构。各键对应列表存储：
                - job_name: 作业名称列表
                - time_start: 开始时间戳列表
                - time_end: 结束时间戳列表
                - id_job: 作业ID列表
            pos (int): 在job_list各列表中对应作业的索引位置
            parent_job (bool, optional): 是否作为父任务注册。当为True时：
                - 会将该任务设置为工作流的_parent_job属性
                - 通常用于标识工作流入口任务
                默认为False
        Returns:
            None: 直接修改工作流内部状态，无返回值
        功能说明：
        - 创建TaskTracker实例来封装作业数据和任务跟踪逻辑
        - 根据parent_job标志决定存储位置：
          * True: 设置为工作流父任务（工作流入口）
          * False: 按stage_id存储到任务字典（普通子任务）
        """
        # 创建任务跟踪器实例（封装作业数据和解析逻辑）
        task = TaskTracker(job_list, pos, self)
        # 处理父任务注册或普通任务注册
        if parent_job:
            # 设置为工作流父任务（工作流入口点）
            self._parent_job=task
        else:
            # 按阶段ID存储到任务字典（常规子任务）
            self._tasks[task.stage_id] = task
    
    def get_first_task(self):
        if len(self._tasks)==0:
            return self._parent_job
        else:
            return self._critical_path[0]
    def get_all_tasks(self):
        if len(self._tasks)==0:
            return [self._parent_job]
        else:
            return self._tasks.values()
            

    
    def fill_deps(self):
        """解析任务依赖关系并计算关键路径
        该方法通过分析任务间的依赖关系构建任务拓扑结构，确定工作流的起始任务，
        并计算出关键路径及其执行耗时。处理过程中会检测不完整的依赖关系。
        特殊处理:
        - 当工作流没有子任务且无父任务时抛出异常
        - 自动识别单任务工作流模式(single_job_wf)
        - 标记存在外部依赖的不完整工作流状态
        类属性更新:
        _start_task: 最早开始的起始任务
        _critical_path: 关键路径任务列表
        _critical_path_runtime: 关键路径总耗时
        _incomplete_workflow: 是否包含外部依赖的标记
        """
        # 检测空工作流的异常情况
        if len(self._tasks)==0 and self._parent_job is None:
            raise ValueError("Workflow has no tasks inside")
        # 设置单任务工作流标志
        elif len(self._tasks)==0:
            self.single_job_wf=True
        else:
            self.single_job_wf=False
        # 遍历所有任务寻找起始节点并建立依赖关系
        start_task=None
        for task in self.get_all_tasks():
            # 处理无依赖任务（候选起始节点）
            if not task.deps:
                if not start_task:
                    start_task = task
                elif start_task.data["time_start"]>task.data["time_start"]:
                    start_task = task
            # 处理有依赖任务
            else:
                for dep in task.deps:
                    # 有效内部依赖处理
                    if dep in self._tasks.keys():
                        self._tasks[dep].add_dep_to(task)
                    # 外部依赖标记为不完整
                    else:
                        self._incomplete_workflow=True
        # 存储关键路径计算结果
        self._start_task=start_task
        self._critical_path, self._critical_path_runtime = (
                                            self._get_critical_path(start_task))
        # 关键路径不存在时标记工作流不完整
        self._incomplete_workflow = not self._critical_path
        
    def _get_critical_path(self, task, min_time=0):
        """如果关键路径的长度（包括任务运行时和等待时间）大于min_time，则返回从任务到工作流结束的关键路径。
        """
        """计算从指定任务开始的关键路径
        Args:
            task: 起始任务对象，需包含time_start/time_end时间属性
            min_time: 最小时间阈值，仅返回总耗时超过该值的路径（默认0）
        Returns:
            tuple: (路径列表，路径总耗时)
            当存在满足条件的路径时返回有效结果，否则返回空列表和0
        """
        path = []
        # 检查未完成的工作流状态
        if self._incomplete_workflow:
            return [], 0
        # 计算当前任务执行耗时
        task_runtime=task.data["time_end"]-task.data["time_start"]
        
        path_time = 0
        # 遍历所有后续依赖任务
        for sub_task in task.dependenciesTo:
            # 计算当前任务到子任务的等待时间
            sub_task_wait_time=sub_task.data["time_start"]-task.data["time_end"]
            # 递归获取子任务关键路径
            sub_path, sub_time = self._get_critical_path(sub_task,
                                                       path_time)
            # 判断是否更新当前最优路径
            if (sub_path and task_runtime+sub_task_wait_time+sub_time>min_time
                and task_runtime+sub_task_wait_time+sub_time>path_time):
                path=sub_path
                path_time=sub_task_wait_time+sub_time

        # 合并当前任务到最终路径
        path = [task] + path
        path_time += task_runtime
        # 最终结果过滤
        if min_time<path_time:
            return path, path_time
        
        return [], 0
    
    def get_waste_changes(self):
        if not self.single_job_wf:
            return [], [], 0
        else:
            manifest = "-".join(self._name.split("-")[0:-1])
            we = WasteExtractor(manifest)
            return we.get_waste_changes(self._start_task.data["time_start"])
            
def paint_path(path):
    return [t.stage_id for t in path]

class WasteExtractor(object):
    """该类来提取单作业工作流与多作业和清单驱动工作流相比所浪费的核心时间。
    """
    def __init__(self, manifest):
        """初始化实验配置加载器
          Args:
              manifest (str): 清单文件名或相对路径。最终会被转换为绝对路径，
                  优先级：环境变量MANIFEST_DIR > 默认的manifest目录
        """
        # 存储原始manifest参数，后续会被完整路径覆盖
        self._manifest = manifest
        # 延迟导入避免循环依赖，ExperimentRunner提供路径处理方法
        from orchestration.running import ExperimentRunner
        # 获取manifest目录路径：环境变量优先，否则使用默认配置目录
        man_dir = os.getenv("MANIFEST_DIR",
                            ExperimentRunner.get_manifest_folder())
        # 构建完整的manifest文件绝对路径
        self._manifest = os.path.join(man_dir, self._manifest)
    
    def get_waste_changes(self, start_time):
        """计算工作流执行过程中各时间点的资源浪费变化
        Args:
            start_time (datetime): 工作流执行的起始时间，用于时间轴计算

        Returns:
            tuple: 包含三个元素的元组
            - time_stamps (list): 资源分配变化的时间点序列
            - waste_list (list): 每个时间点对应的资源浪费变化量（瞬时值）
            - acc_waste (float): 累计资源浪费总量（通过get_acc_waste计算得到）
        """
        # 初始化时间戳和资源分配变化记录
        self._time_stamps = []
        self._allocation_changes = []
        # 展开工作流获取总资源需求和总运行时间
        total_cores, total_runtime=self._expand_workflow(self._manifest,
                                                     start_time)

        # 计算资源浪费变化
        waste_list = []
        waste = None
        # 遍历每个资源分配变化点
        for x in self._allocation_changes:
            if waste==None:
                waste = total_cores - x
                waste_list.append(waste)
            else:
                # 计算相对于前次变化的差值（新增/减少的浪费量）
                new_waste = waste - x
                waste_list.append(new_waste-waste)
        # 最后一个时间点需扣除初始分配的全部资源
        waste_list[-1]-=total_cores
        # 返回时间轴、瞬时浪费值列表和累计浪费量
        return self._time_stamps, waste_list, self.get_acc_waste(
                                                            self._time_stamps,
                                                            waste_list)

    def get_acc_waste(self, stamp_list, waste_list):
        """计算时间戳序列对应的累积消耗量
        通过遍历相邻时间戳间隔及其对应的消耗增量，累加计算得到总累积消耗量。
        算法逻辑：每个时间间隔的消耗量 = 间隔时长 * 该时刻的累计消耗强度
        Args:
            stamp_list: List[float]，有序时间戳序列，单位需统一
            waste_list: List[float]，每个时间间隔对应的消耗增量序列，长度比stamp_list少1
        Returns:
            float: 总累积消耗量，计算方式为各时间段消耗量的累加和
        """
        current_waste = 0  # 当前时刻的累计消耗强度
        acc_waste = 0  # 总累积消耗量
        # 遍历每对相邻时间戳及其对应的消耗增量
        for (s1,s2,w) in zip(stamp_list[:-1], stamp_list[1:], waste_list):
            # 更新当前时刻的消耗强度：累加本时间段的消耗增量
            current_waste+=w
            # 计算本时间段的消耗量并累加到总量：时间差(s2-s1) * 当前消耗强度
            acc_waste+=(s2-s1)*current_waste
        return acc_waste
    def _expand_workflow(self, manifest, start_time):
        """扩展工作流并生成作业调度序列
        参数:
        manifest (dict): 工作流描述文件，包含任务资源配置和依赖关系
        start_time (int): 工作流调度的基准开始时间戳
        返回值:
        tuple: 包含两个元素
            - int: 工作流所需的总计算核心数
            - int: 工作流的总模拟运行时间
        """
        # 解析manifest获取工作流元数据
        total_cores, runtime, tasks = WorkflowGeneratorMultijobs.parse_all_jobs(
                                                                    manifest)
        job_count = 0
        remaining_tasks = list(tasks.values())# 待处理任务队列
        # 迭代处理任务直到所有任务都被调度
        while (remaining_tasks):
            new_remaining_tasks  = []
            # 处理当前可运行的任务，不可运行的保留到下一轮迭代
            for task in remaining_tasks:
                if self._task_can_run(task):
                    job_count+=1
                    # 计算任务的实际开始时间，考虑资源依赖约束
                    feasible_start_time=self._get_feasible_start_time(task,
                                                                    start_time)
                    # 设置任务结束时间并记录核心数变更事件
                    task["time_end"]=feasible_start_time+task["runtime_sim"]
                    cores = task["number_of_cores"]
                    self._add_job_change(feasible_start_time, cores)
                    self._add_job_change(task["time_end"], -cores)
                    task["job_id"]=job_count    # 为任务分配唯一作业ID
                else:
                    new_remaining_tasks.append(task)
            # 更新待处理任务列表
            remaining_tasks=new_remaining_tasks
        return total_cores, runtime
    
    def _add_job_change(self, time_stamp, cores):
        """记录任务资源分配变化到时间线中

        在指定时间戳位置记录核心数变化量，当时间戳已存在时进行累计操作，
        不存在时插入新的时间节点

        Args:
            time_stamp (float): 资源分配发生变化的时间戳
            cores (int): 该时刻核心数变化量（正数表示增加，负数表示减少）
        """
        # 使用二分查找确定插入位置
        pos=bisect.bisect_left(self._time_stamps, time_stamp)
        # 处理已存在相同时间戳的情况
        if pos<len(self._time_stamps) and self._time_stamps[pos]==time_stamp:
            # 合并相同时间点的资源变更
            self._allocation_changes[pos]+= cores
        else:
            # 插入新时间节点并记录资源变更
            elf._time_stamps.insert(pos, time_stamp)
            self._allocation_changes.insert(pos, cores)
        
            
            

    def _task_can_run(self, task):
        """
        检查当前任务是否满足运行条件
        参数:
        task (dict): 待检查的任务字典对象，需要包含以下结构：
            - dependencyFrom (list): 依赖任务列表，每个元素为字典类型，
              需要包含"job_id"字段表示依赖任务的标识
        返回值:
        bool:
            - True: 满足以下任一条件：
                * 任务没有前置依赖
                * 所有前置依赖都包含有效job_id
            - False: 存在不包含job_id的前置依赖
        """
        # 当任务没有前置依赖时直接允许运行
        if len(task["dependencyFrom"])==0:
            return True
        for task_dep in task["dependencyFrom"]:
            if not "job_id" in task_dep.keys():
                return False
        return True
    def _get_feasible_start_time(self, task, start_time):
        """确定任务可行的开始时间，根据依赖关系调整初始时间
        Args:
            task: 任务对象字典，需包含dependencyFrom字段
                - dependencyFrom: 前置依赖任务列表，每个元素包含time_end字段
            start_time: 初始建议开始时间，无依赖时直接返回该值
        Returns:
            int/float: 调整后的实际可行开始时间，取初始时间与依赖任务最晚结束时间的较大者
        """
        # 当任务没有前置依赖时，直接采用初始建议时间
        if task["dependencyFrom"] == []:
            return start_time
        else:
            # 存在依赖时，必须等待所有前置任务完成
            # 取所有依赖任务结束时间的最大值作为起始时间
            return max([x["time_end"] for x in task["dependencyFrom"] ])
        
                
    
    
        
        
class TaskTracker(object):
    """ 在工作流中存储作业的信息"""
    def __init__(self, job_list, pos, parent_workflow):
        """初始化任务追踪器实例，解析指定位置的作业信息并建立与工作流的关联
        从作业列表中提取指定索引位置的作业数据，解析作业名称中的工作流元数据，
        并初始化任务依赖关系等核心属性
        Args:
            job_list (dict): 包含多个作业信息的字典，键为字段名，值为对应字段值的列表
            pos (int): 需要解析的作业在job_list各字段列表中的索引位置
            parent_workflow (Workflow): 当前任务所属的父工作流实例
        Attributes:
            data (dict): 存储从job_list中提取的指定位置所有字段数据的副本
            name (str): 从data中提取的作业名称
            job_id (int): 从data中提取的作业唯一标识符
            _parent_workflow (Workflow): 父工作流实例的引用
            dependenciesTo (list): 存储当前任务的后续依赖任务列表
            wf_name (str): 从作业名称解析出的工作流名称
            stage_id (int): 从作业名称解析出的阶段标识符
            deps (list): 从作业名称解析出的前置依赖列表
        """
        # 从job_list中提取指定位置的所有字段数据
        self.data = {}
        for key in job_list.keys():
            self.data[key]=job_list[key][pos]
        
        self.name=self.data["job_name"]
        self.job_id=self.data["id_job"]
        self._parent_workflow=parent_workflow
        self.dependenciesTo = []
        # 解析作业名称中的工作流元数据
        self.wf_name, self.stage_id, self.deps = (
                                    TaskTracker.extract_wf_name(self.name))
        
    def add_dep_to(self, task):
        """添加当前任务对象所依赖的另一个任务对象
        将给定的任务对象添加到当前任务的依赖列表中，建立任务图谱中的方向性依赖关系。
        Args:
            task (TaskTracker): 被依赖的任务对象。该任务将被添加为当前任务的前置依赖，
                表示必须在该任务完成后当前任务才能继续执行。
        Returns:
            None: 本方法不返回任何值，通过修改实例的dependenciesTo列表来维护依赖关系
        Note:
            调用者需确保传入任务的有效性，并应自行避免产生循环依赖
        """
        self.dependenciesTo.append(task)
    def get_runtime(self):
        return self.data["time_end"]-self.data["time_start"]
    def get_waittime(self):
        return self.data["time_start"]-self.data["time_submit"]
    def get_cores(self):
        return self.data["cpus_alloc"]
    def get_submittime(self):
        return self.data["time_submit"]
    @classmethod    
    def extract_wf_name(self, wf_name):
        """从工作流名称中提取任务跟踪器的角色信息
        解析符合特定格式的工作流名称，提取工作流标识、阶段编号及依赖关系
        Args:
            wf_name (str): 工作流名称，预期格式为：
                wf_[manifest]-[job_id]_S[stage#]_[d1-d2-d3]
                - manifest: 描述工作流的清单文件
                - job_id: 包含工作流的父作业ID
                - S[n]: 阶段标识符，n为数字
                - [dS1-dS2-dS3]: 依赖的阶段列表，Sn为阶段名称
        Returns:
            tuple: 包含三个元素的元组
                - name (str): 工作流标识，格式为manifest-job_id
                - stage_id (str): 当前阶段编号
                - deps (list): 依赖的阶段编号列表
        实现说明：
            通过下划线分割字符串后处理不同位置的元素，当遇到混合stage和依赖信息时
            会进行二次分割处理
        """
        # 使用下划线分割原始名称
        parts = wf_name.split("_")
        # 基础信息提取：manifest-job_id
        name = parts[1]
        stage_id=""
        deps=[]
        # 处理可能混合stage和依赖信息的情况（格式不规范时）
        if len(parts)==3 and "-" in parts[2]:
            cad = parts[2]
            pos = cad.find("-")
            parts[2]=cad[:pos]
            parts.append(cad[pos+1:])

        # 提取阶段标识符（去除'S'前缀）
        if len(parts)>2:
            stage_id = parts[2][0:]
        # 处理依赖信息：转换dSn格式为Sn列表
        if len(parts)>3:
            deps = [x[1:] for x in parts[3].split("-")] 
        return name, stage_id, deps