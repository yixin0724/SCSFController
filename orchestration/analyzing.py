import numpy as np

from stats.trace import ResultTrace
from stats.compare import WorkflowDeltas
from orchestration.definition import ExperimentDefinition

class AnalysisRunnerSingle(object):
    """对单个实验的结果进行分析。这是其他可能包含多个跟踪的分析的基类。"""

    def __init__(self, definition):
        """初始化实验分析器的构造函数。
        Args:
            definition (Definition): 包含实验配置的Definition对象，该配置将用于本对象的分析工作。
                对象应包含实验运行所需的所有参数设置和配置信息，例如数据路径、模型参数等核心配置。
        Returns:
            None: 构造函数不返回任何值
        """
        self._definition = definition

    def load_trace(self, db_obj):
        """从分析数据库读取并返回实验追踪数据。

        通过数据库对象获取当前实验配置关联的追踪数据，初始化并填充ResultTrace对象。

        Args:
            db_obj (DB): 已配置的分析数据库访问对象，用于执行数据库查询操作。

        Returns:
            ResultTrace: 包含从数据库加载的完整追踪数据的对象实例。该对象通过指定的trace_id初始化，
                        且未启用额外调试模式（由load_trace方法的第三个参数False控制）。
        """
        result_trace = ResultTrace()
        result_trace.load_trace(db_obj, self._definition._trace_id, False)
        return result_trace

    def do_full_analysis(self, db_obj):
        """
        执行全面分析，包括作业和工作流变量的CDF和数值分析，以及利用率分析，并将结果存储到数据库中。
        参数:
        - db_obj: 配置用于访问分析数据库的DB对象。
        """
        # 打印当前分析的trace标识
        print "Analyzing trace:", self._definition._trace_id

        # 加载trace数据
        result_trace = self.load_trace(db_obj)

        # 计算作业结果，包括CDF和数值分析
        result_trace.calculate_job_results(True, db_obj, 
                                       self._definition._trace_id,
                                       start=self._definition.get_start_epoch(),
                                       stop=self._definition.get_end_epoch())
        # 计算按核心秒分组的作业结果
        result_trace.calculate_job_results_grouped_core_seconds(
                       self._definition.get_machine().get_core_seconds_edges(),
                       True, db_obj, 
                       self._definition._trace_id,
                       start=self._definition.get_start_epoch(),
                       stop=self._definition.get_end_epoch())
        # 进行工作流预处理
        workflows=result_trace.do_workflow_pre_processing()
        # 如果存在工作流，则计算工作流结果
        if len(workflows)>0:
            result_trace.calculate_workflow_results(True, db_obj, 
                                       self._definition._trace_id,
                                       start=self._definition.get_start_epoch(),
                                       stop=self._definition.get_end_epoch())
        # 计算系统利用率
        result_trace.calculate_utilization(
                            self._definition.get_machine().get_total_cores(),
                            do_preload_until=self._definition.get_start_epoch(),
                            endCut=self._definition.get_end_epoch(),
                            store=True, db_obj=db_obj,
                            trace_id=self._definition._trace_id)
        # 标记此分析已完成
        self._definition.mark_analysis_done(db_obj)

    def do_workflow_limited_analysis(self, db_obj, num_workflows):
        """执行限定数量工作流的跟踪分析（仅处理前num_workflows个工作流）

        Args:
            db_obj: 数据库连接对象，用于数据存取操作
            num_workflows: 要分析的最大工作流数量限制

        Returns:
            无返回值，分析结果直接写入数据库
        """
        # 加载跟踪数据并进行预处理
        result_trace = self.load_trace(db_obj)
        workflows = result_trace.do_workflow_pre_processing()

        # 当存在有效工作流时执行限定分析
        if len(workflows) > 0:
            # 截断工作流列表并计算结果指标
            result_trace.truncate_workflows(num_workflows)
            result_trace.calculate_workflow_results(
                True, db_obj,
                self._definition._trace_id,
                start=self._definition.get_start_epoch(),
                stop=self._definition.get_end_epoch(),
                limited=True  # 标记为有限数量分析模式
            )

        # 标记数据库记录为已完成第二阶段处理
        self._definition.mark_second_pass(db_obj)


class AnalysisGroupRunner(AnalysisRunnerSingle):
    """类运行的实验分析是一组实验：例如，相同的条件下，不同的随机种子10次重复。作业变量和工作流变量一起计算。计算中位数利用率。
    """
    def load_trace(self, db_obj):
        result_trace = ResultTrace()
        return result_trace

    def do_full_analysis(self, db_obj):
        """执行完整的分析流程，聚合多个子跟踪数据并计算结果

        Args:
            db_obj (object): 数据库连接对象，用于数据存取操作
        """
        # 初始化主跟踪对象并加载基础数据
        result_trace = self.load_trace(db_obj)

        # 遍历所有子跟踪进行预处理和数据填充
        first = True
        last = False
        for trace_id in self._definition._subtraces:
            # 标记是否是最后一个子跟踪（用于最终计算）
            last = trace_id == self._definition._subtraces[-1]

            # 加载当前子跟踪数据
            result_trace.load_trace(db_obj, trace_id)

            # 工作流预处理（首次不追加数据）
            result_trace.do_workflow_pre_processing(append=not first)

            # 加载实验定义配置
            one_definition = ExperimentDefinition()
            one_definition.load(db_obj, trace_id)

            # 填充作业和任务运行时数值
            result_trace.fill_job_values(
                start=one_definition.get_start_epoch(),
                stop=one_definition.get_end_epoch(),
                append=not first)
            result_trace.fill_workflow_values(
                start=one_definition.get_start_epoch(),
                stop=one_definition.get_end_epoch(),
                append=not first)

            # 计算核心时间消耗（最后一次进行最终聚合）
            result_trace.calculate_job_results_grouped_core_seconds(
                one_definition.get_machine().get_core_seconds_edges(),
                last, db_obj,
                self._definition._trace_id,
                start=one_definition.get_start_epoch(),
                stop=one_definition.get_end_epoch(),
                append=not first)
            first = False

        # 保存聚合后的计算结果到数据库
        result_trace.calculate_and_store_job_results(store=True,
                                                     db_obj=db_obj,
                                                     trace_id=self._definition._trace_id)
        result_trace._wf_extractor.calculate_and_store_overall_results(store=True,
                                                                       db_obj=db_obj,
                                                                       trace_id=self._definition._trace_id)
        result_trace._wf_extractor.calculate_and_store_per_manifest_results(
            store=True,
            db_obj=db_obj,
            trace_id=self._definition._trace_id)

        # 计算并存储系统利用率指标
        result_trace.calculate_utilization_median_result(
            self._definition._subtraces,
            store=True,
            db_obj=db_obj,
            trace_id=self._definition._trace_id)
        result_trace.calculate_utilization_mean_result(
            self._definition._subtraces,
            store=True,
            db_obj=db_obj,
            trace_id=self._definition._trace_id)

        # 标记分析任务完成状态
        self._definition.mark_analysis_done(db_obj)

    def do_only_mean(self, db_obj):
        """计算并存储指定跟踪数据的平均利用率结果

        Args:
            db_obj: 数据库操作对象，用于数据持久化操作
                - 预期为数据库连接对象或ORM模型实例

        Returns:
            None: 本函数通过副作用实现结果存储，无直接返回值
        """

        # 从数据库加载跟踪数据对象
        result_trace = self.load_trace(db_obj)

        # 计算平均利用率指标并存储到数据库
        # - 使用预定义的子跟踪项定义进行计算
        # - store=True强制持久化计算结果
        # - 传递当前跟踪的唯一标识符
        result_trace.calculate_utilization_mean_result(
            self._definition._subtraces,
            store=True,
            db_obj=db_obj,
            trace_id=self._definition._trace_id)

    def do_workflow_limited_analysis(self, db_obj, workflow_count_list):
        """
        对工作流进行有限数量分析，根据子跟踪列表逐个处理并限制工作流数量

        参数:
        - db_obj: 数据库连接对象，用于加载/存储跟踪数据
        - workflow_count_list: 每个子跟踪允许的最大工作流数量列表
        返回值:
        - 无
        """
        result_trace = self.load_trace(db_obj)
        # 验证子跟踪数量与限制列表长度一致性
        if len(workflow_count_list) != len(self._definition._subtraces):
            raise Exception("Number of subtraces({0}) is not the samas the"
                            " limit on workflow count({1})".format(
                len(workflow_count_list),
                len(self._definition._subtraces)))

        first = True
        acc_workflow_count = 0  # 累计工作流计数

        # 遍历每个子跟踪及其对应的工作流限制
        for (trace_id, workflow_count, subt) in zip(self._definition._subtraces,
                                                    workflow_count_list,
                                                    range(len(workflow_count_list))):
            """
            子跟踪处理流程:
            1. 累计工作流总数
            2. 加载当前子跟踪数据
            3. 执行预处理（首次加载不追加数据）
            4. 重命名工作流以区分不同子跟踪
            5. 根据累计总数截断多余工作流
            """
            acc_workflow_count += workflow_count
            result_trace.load_trace(db_obj, trace_id)
            result_trace.do_workflow_pre_processing(append=not first,
                                                    do_processing=False)
            # 通过重命名确保工作流顺序，便于后续截断操作
            result_trace.rename_workflows(subt)
            result_trace.truncate_workflows(acc_workflow_count)
            first = False

        # 最终处理阶段
        result_trace.rename_workflows(None)  # 恢复工作流原始名称
        result_trace._wf_extractor.do_processing()  # 执行最终工作流处理
        print("After FINAL number of WFs",
              len(result_trace._wf_extractor._workflows.values()))

        # 计算结果并写入数据库
        result_trace.calculate_workflow_results(True, db_obj,
                                                self._definition._trace_id,
                                                start=self._definition.get_start_epoch(),
                                                stop=self._definition.get_end_epoch(),
                                                limited=True)

        # 标记处理阶段为已完成
        self._definition.mark_second_pass(db_obj)


class AnalysisRunnerDelta(AnalysisRunnerSingle):
    """类以在增量实验上运行分析：计算具有相同种子但不同调度策略的相同工作流在不同路径上的值之间差异的统计信息。
    """

    def load_trace(self, db_obj):
        """
        加载并比较工作流子跟踪的差异

        参数:
        db_obj: Any
            数据库连接对象，用于从数据库加载跟踪数据

        返回值:
        WorkflowDeltas
            包含所有子跟踪对比差异结果的计算器实例
        """
        # 创建工作流差异比较器实例
        trace_comparer = WorkflowDeltas()

        # 将子跟踪列表按相邻两个为一组的方式创建配对（0&1, 2&3等）
        # 使用切片步长2将奇偶索引元素配对
        pairs = zip(self._definition._subtraces[0::2],
                    self._definition._subtraces[1::2])

        # 遍历所有子跟踪对进行差异分析
        for (first_id, second_id) in pairs:
            # 从数据库加载两个配对子跟踪的完整数据
            trace_comparer.load_traces(db_obj, first_id, second_id)
            # 生成两个子跟踪的结构化差异比较结果
            trace_comparer.produce_deltas(True)

        return trace_comparer

    def do_full_analysis(self, db_obj):
        """执行完整的跟踪数据分析流程

        该方法依次完成三个核心步骤：
        1. 加载跟踪数据比较器
        2. 执行差异结果计算并持久化
        3. 标记分析完成状态

        Args:
            db_obj: 数据库连接或操作对象，用于数据持久化和状态更新
        """
        # 从数据库加载跟踪数据比较器实例
        trace_comparer = self.load_trace(db_obj)

        # 执行差异分析计算，参数说明：
        # - 第一个bool参数表示启用详细模式
        # - db_obj用于结果存储
        # - trace_id用于关联跟踪记录
        trace_comparer.calculate_delta_results(True, db_obj,
                                               self._definition._trace_id)

        # 在数据库中更新分析完成标记
        self._definition.mark_analysis_done(db_obj)

        
        
        

