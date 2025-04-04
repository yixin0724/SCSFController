"""
此包包含许多用于导入和操作调度日志跟踪的类。
"""
import numpy as np

from stats import (calculate_results, load_results, NumericList)
from stats.workflow import WorkflowsExtractor
from commonLib.nerscUtilization import UtilizationEngine


class ResultTrace(object):
    """ 该类存储调度仿真结果跟踪。
        它旨在从slurm的会计MySQL数据库中导入这些跟踪，并将它们存储在另一个数据库中。
        也可以从第二个数据库中的副本填充它。
    
    它还处理跟踪，检索跟踪分析所需的值，并运行相应的分析。
    结果也可以存储在相应的数据库中（也已加载）。
    
    跟踪存储所需的数据库表在create_trace_table中描述。
    """

    def __init__(self, table_name="traces"):
        """初始化跟踪数据存储对象

        构造函数用于创建存储作业跟踪数据的实例，并初始化相关数据结构。

        Args:
            table_name (str, optional): 存储跟踪数据的数据库表名称。默认为 "traces"。
                该表用于持久化作业调度过程中的状态变更记录。

        Attributes:
            _lists_submit (dict): 按提交时间分类存储作业对象的字典，键为时间戳
            _lists_start (dict): 按启动时间分类存储作业对象的字典，键为时间戳
            _fields (list): 定义跟踪数据表的字段结构，包含作业元数据和资源属性字段
            _wf_extractor: 工作流特征提取器（后续初始化）
            _integrated_ut: 累积利用率统计量（后续计算）
            _acc_waste: 资源浪费统计量（后续计算）
            _corrected_integrated_ut: 校正后的利用率指标（后续计算）
        """
        # 初始化时间索引字典，用于快速按时间范围检索作业
        self._lists_submit = {}
        self._lists_start = {}

        # 配置底层存储表结构和字段定义
        self._table_name = table_name

        # 定义数据表字段结构，包含作业ID、资源需求、状态时间戳等核心字段
        self._fields = ["job_db_inx", "account", "cpus_req", "cpus_alloc",
                        "job_name", "id_job", "id_qos", "id_resv", "id_user",
                        "nodes_alloc", "partition", "priority", "state", "timelimit",
                        "time_submit", "time_start", "time_end"]

        # 初始化后续计算模块的占位符
        self._wf_extractor = None
        self._integrated_ut = None
        self._acc_waste = None
        self._corrected_integrated_ut = None

    def _clean_db_duplicates(self, db_obj, table_name):
        """
        清理指定数据库表中同一id_job的重复记录，保留最大的job_db_inx记录

        参数:
        - db_obj: 数据库连接对象，需包含getValuesAsColumns和doUpdate方法
        - table_name: str 要清理的目标表名称

        返回值:
        - 无
        """

        # 构造重复记录查询语句（分组统计+最大索引筛选）
        query = """SELECT id_job, dup, inx from 
              (SELECT `id_job`,count(*) dup, max(job_db_inx) inx 
               FROM {0}  GROUP BY id_job) as grouped
              WHERE dup>1""".format(table_name)
        # 获取当前重复数据（包含id_job、重复次数、最大索引值）
        duplicates = db_obj.getValuesAsColumns(
            table_name, ["id_job", "dup", "inx"],
            theQuery=query)
        print
        "Cleaning duplicated entries"
        print
        "Duplicated entries before:", len(duplicates["id_job"])

        # 遍历删除除最大索引外的重复记录
        for (id_job, job_db_inx) in zip(duplicates["id_job"], duplicates["inx"]):
            query = """DELETE FROM `{0}` 
                       WHERE `id_job`={1} and `job_db_inx`!={2}
            """.format(table_name, id_job, job_db_inx)
            db_obj.doUpdate(query)
        # 验证清理后的重复状态
        duplicates = db_obj.getValuesAsColumns(
            table_name, ["id_job", "dup", "inx"],
            theQuery=query)
        print
        "Duplicated entries after:", len(duplicates["id_job"])

    def import_from_db(self, db_obj, table_name, start=None, end=None):
        """从数据库导入调度器模拟跟踪数据到当前对象

        该方法会执行以下操作：
        1. 清理目标表中与当前对象重复的数据
        2. 从指定数据库表分两次获取作业提交时间和作业开始时间的数据
        3. 将获取的数据分别存储到对象的_lists_submit和_lists_start属性

        Args:
            db_obj (DBManager): 数据库连接对象，需配置为连接至Slurm记账数据库
            table_name (str): 要查询的作业表名称，表结构需符合create_import_table定义的格式要求
            start (int/None): 起始时间戳（epoch格式），用于过滤创建时间在此之后的任务。默认为None表示不设下限
            end (int/None): 结束时间戳（epoch格式），用于过滤创建时间在此之前的任务。默认为None表示不设上限

        Returns:
            None: 结果直接存储在对象的_lists_submit和_lists_start属性中
        """
        # 清理数据库中可能与当前对象产生重复的记录
        self._clean_db_duplicates(db_obj, table_name)

        # 获取作业提交时间维度数据
        # 使用_get_limit生成时间范围条件，按提交时间排序
        self._lists_submit = db_obj.getValuesAsColumns(
            table_name, self._fields,
            condition=_get_limit("time_submit", start, end),
            orderBy="time_submit")

        # 获取作业开始时间维度数据
        # 使用不同的时间字段过滤，按开始时间排序
        self._lists_start = db_obj.getValuesAsColumns(
            table_name, self._fields,
            condition=_get_limit("time_start", start, end),
            orderBy="time_start")

    def import_from_pbs_db(self, db_obj, table_name, start=None, end=None,
                           machine=None):
        """
        从PBS风格数据库中导入Torque/Moab类型的工作负载跟踪数据

        通过数据库连接对象查询指定表的工作记录，将结果转换为SLURM格式并存储在实例变量中。
        支持时间范围和主机名过滤条件。

        Args:
            db_obj (DBManager): 已配置好的数据库连接对象，需支持getValuesAsColumns方法
            table_name (str): PBS作业记录表名，表结构需符合create_import_table定义
            start (int/None): 起始时间戳（epoch秒），仅获取在此时间之后创建的作业
            end (int/None): 结束时间戳（epoch秒），仅获取在此时间之前创建的作业
            machine (str/None): 主机名过滤条件，为None时不进行主机名过滤

        Returns:
            None: 结果存储在实例变量self._lists_submit和self._lists_start中
        """
        pbs_fields = ["account", "jobname", "cores_per_node", "numnodes",
                      "class", "wallclock_requested", "created", "start", "completion"]

        # 构建主机名过滤条件（当machine参数不为空时生效）
        machine_cond = ""
        if machine:
            machine_cond = " and `hostname`='{0}'".format(machine)

        # 获取按created时间排序的作业数据并转换为SLURM格式
        self._lists_submit = self._transform_pbs_to_slurm(
            db_obj.getValuesAsColumns(
                table_name, pbs_fields,
                condition=(_get_limit("created", start, end)
                           + machine_cond),
                orderBy="created"))

        # 获取按start时间排序的作业数据并转换为SLURM格式
        self._lists_start = self._transform_pbs_to_slurm(
            db_obj.getValuesAsColumns(
                table_name, pbs_fields,
                condition=(_get_limit("created", start, end)
                           + machine_cond),
                orderBy="start"))

    def _transform_pbs_to_slurm(self, pbs_list):
        """
        将PBS格式的作业列表转换为SLURM格式的作业列表
        Args:
            pbs_list (dict): PBS格式的作业数据字典，包含以下字段：
                - account: 账户信息列表
                - jobname: 作业名称列表
                - class: 作业类别列表（对应SLURM的partition）
                - created: 作业提交时间列表
                - start: 作业开始时间列表
                - completion: 作业完成时间列表
                - numnodes: 分配的节点数列表
                - wallclock_requested: 请求的作业运行时间（秒）列表
                - cores_per_node: 每节点核心数列表
        Returns:
            dict: SLURM格式的作业数据字典，包含转换后的字段：
                - 基础字段与PBS直接对应
                - 时间相关字段转换为分钟单位
                - 部分字段使用临时占位值
                - 自动生成作业序列号
        """
        # 初始化SLURM字典结构，根据类中预定义字段创建空列表
        slurm_list = {}
        for field in self._fields:
            slurm_list[field] = []
        job_count = len(pbs_list.values()[0])
        slurm_list["account"] = pbs_list["account"]
        slurm_list["job_name"] = pbs_list["jobname"]
        slurm_list["partition"] = pbs_list["class"]
        slurm_list["time_submit"] = pbs_list["created"]
        slurm_list["time_start"] = pbs_list["start"]
        slurm_list["time_end"] = pbs_list["completion"]
        slurm_list["nodes_alloc"] = pbs_list["numnodes"]
        slurm_list["timelimit"] = [x / 60 for x in
                                   pbs_list["wallclock_requested"]]
        fake_ids = [3 for x in pbs_list["wallclock_requested"]]
        slurm_list["id_qos"] = fake_ids
        slurm_list["id_resv"] = fake_ids
        slurm_list["id_user"] = fake_ids
        slurm_list["priority"] = fake_ids
        slurm_list["state"] = fake_ids

        slurm_list["cpus_req"] = [x * y for (x, y) in zip(pbs_list["numnodes"],
                                                          pbs_list["cores_per_node"])]
        slurm_list["cpus_alloc"] = slurm_list["cpus_req"]
        slurm_list["job_db_inx"] = range(job_count)
        slurm_list["id_job"] = slurm_list["job_db_inx"]

        return slurm_list

    def store_trace(self, db_obj, trace_name):
        """将跟踪数据存储到数据库的指定表中。

        通过DBManager对象将self._lists_submit定义的数据列和值，
        与跟踪标识符关联后插入数据库表。

        Args:
            db_obj (DBManager): 数据库连接管理器对象
                - 需配置连接至包含self._table_name表的数据库
                - 表结构需符合create_trace_table定义的格式
            trace_name (str): 跟踪记录的唯一标识符

        Returns:
            None: 本方法无返回值
        """
        db_obj.insertValuesColumns(self._table_name,
                                   self._lists_submit,
                                   {"trace_id": trace_name})

    def load_trace(self, db_obj, trace_id, append=False):
        """
        从数据库中检索跟踪信息，并根据append参数决定是否追加到现有跟踪信息中。

        Args:
        - db_obj: 配置为连接到托管名为self._table_name表的数据库的DBManager对象。
        - trace_id: 要检索的跟踪的唯一ID字符串。
        - append: 如果为True，则将此跟踪添加到对象中的跟踪中，否则将覆盖类的内容。
            此外，添加的跟踪的时间戳将被重新计算，因为新加载的跟踪恰好发生在先前加载的跟踪之后。
        """
        # 根据append参数决定是否初始化或更新内部状态
        if not append:
            self._lists_submit = {}
            self._lists_start = {}
            self._load_trace_count = 0
            time_offset = 0
        else:
            self._load_trace_count += 1
            time_offset = self._lists_submit["time_submit"][-1]

        # 从数据库中获取符合trace_id条件的记录，并按提交时间排序
        new_lists_submit = db_obj.getValuesAsColumns(
            self._table_name, self._fields,
            condition="trace_id={0}".format(trace_id),
            orderBy="time_submit")
        # 获取新加载跟踪的初始时间值
        first_time_value = new_lists_submit["time_submit"][0]
        # 根据时间偏移量调整新加载的跟踪时间
        ResultTrace.apply_offset_trace(new_lists_submit, time_offset,
                                       first_time_value)
        # 将新加载的跟踪信息与现有的跟踪信息合并
        self._lists_submit = ResultTrace.join_dics_of_lists(
            self._lists_submit, new_lists_submit)

        # 从数据库中获取符合trace_id条件的记录，并按开始时间排序
        new_lists_start = db_obj.getValuesAsColumns(
            self._table_name, self._fields,
            condition="trace_id={0}".format(trace_id),
            orderBy="time_start")
        # 同样，根据时间偏移量调整新加载的跟踪时间
        ResultTrace.apply_offset_trace(new_lists_start, time_offset,
                                       first_time_value)
        # 将新加载的按开始时间排序的跟踪信息与现有信息合并
        self._lists_start = ResultTrace.join_dics_of_lists(
            self._lists_start,
            new_lists_start)

    @classmethod
    def apply_offset_trace(cls, lists, offset=0, first_time_value=0,
                           time_fields=["time_start", "time_end", "time_submit"]
                           ):
        """
        对轨迹数据中指定的时间字段应用时间偏移量。该函数会修改传入的lists字典中的时间字段值，
        偏移量计算逻辑为：(原时间值 + offset调整值) - 初始基准时间
        Args:
            lists (dict): 包含轨迹数据的字典，键为字段名，值为对应值的列表。
                例如 Trace._lists_submit 的结构
            offset (int, optional): 时间偏移基数（单位：秒）。注意实际计算时会对此值+1处理。
                默认值为0
            first_time_value (int, optional): 初始基准时间（单位：秒），所有时间值将减去该值。
                默认值为0
            time_fields (list[str], optional): 需要调整的时间字段名称列表。
                默认包含["time_start", "time_end", "time_submit"]

        Returns:
            None: 直接修改输入的lists字典，无返回值

        Note:
            - 当offset不为0时才会执行调整逻辑
            - 实际偏移计算公式为：x + (offset+1) - first_time_value
        """
        if offset != 0:
            offset += 1
            for field in time_fields:
                lists[field] = [x + offset - first_time_value for x in lists[field]]

    @classmethod
    def join_dics_of_lists(self, dic1, dic2):
        """
        合并两个列表字典，将相同键的列表内容合并

        参数:
        dic1 (dict): 第一个字典，值为列表类型
        dic2 (dict): 第二个字典，值为列表类型

        返回:
        dict: 新字典，包含两个输入字典所有键的合并列表。对于重复键，
              其值为两个字典中对应列表的拼接结果
        """
        new_dic = {}
        keys = dic1.keys() + dic2.keys()
        keys = list(set(keys))
        for key in keys:
            new_dic[key] = []
            if key in dic1.keys():
                new_dic[key] += dic1[key]
            if key in dic2.keys():
                new_dic[key] += dic2[key]
        return new_dic

    def analyze_trace(self, store=False, db_obj=None, trace_id=None):
        """计算并存储作业和工作流变量的统计指标
        Args:
            store (bool): 是否将结果持久化到数据库。当设为True时，
                必须同时提供有效的db_obj和trace_id参数
            db_obj (DB, optional): 配置好的数据库连接对象，用于存储分析结果。
                当store=True时此参数必填
            trace_id (int, optional): 跟踪记录的唯一标识符，用于关联存储结果。
                当store=True时此参数必填
        Returns:
            None: 本方法无返回值，通过副作用执行操作

        处理流程:
            - 执行作业级别的指标计算和存储（当store=True时）
            - 执行工作流级别的指标计算和存储（当store=True时）
            - 两个计算模块均支持结果持久化到分析数据库
        """
        # 计算作业相关指标并处理存储逻辑
        self.calculate_job_results(store=store, db_obj=db_obj,
                                   trace_id=trace_id)
        # 计算工作流相关指标并处理存储逻辑
        self.calculate_workflow_results(store=store, db_obj=db_obj,
                                        trace_id=trace_id)

    def load_analysis(self, db_obj, trace_id, core_seconds_edges=None):
        """为跟踪trace_id加载作业和工作流结果

        Args:
            db_obj: 数据库连接对象，用于执行数据查询操作
            trace_id: 追踪ID，用于标识需要分析的特定任务流
            core_seconds_edges: 可选参数，核心秒数分组边界值列表。当提供时，
                会按给定边界对作业结果进行分组统计

        Returns:
            None: 该函数没有返回值，结果直接加载到对象属性中
        """
        # 加载基础作业结果和工作流结果
        self.load_job_results(db_obj, trace_id)
        self.load_workflow_results(db_obj, trace_id)
        # 当存在核心秒数分组参数时，执行分组统计加载
        if core_seconds_edges:
            self.load_job_results_grouped_core_seconds(core_seconds_edges,
                                                       db_obj, trace_id)

    def load_job_results(self, db_obj, trace_id):
        """从数据库加载作业结果数据并存储到实例变量中

        通过DBManager对象从数据库提取指定trace的作业指标数据，加载结果将存储在
        self.jobs_results属性中。该函数主要用于后续生成统计指标和可视化数据。

        Args:
            db_obj (DBManager): 数据库管理对象实例，提供数据库连接和查询功能
            trace_id (int): 需要加载数据的trace的数字标识符

        Returns:
            None: 直接修改实例变量，不返回具体值
        """
        # 定义需要加载的作业指标字段列表
        field_list = ["jobs_runtime", "jobs_waittime", "jobs_turnaround",
                      "jobs_requested_wc", "jobs_cpus_alloc", "jobs_slowdown"]
        self.jobs_results = load_results(field_list, db_obj, trace_id)

    def load_job_results_grouped_core_seconds(self, core_seconds_edges,
                                              db_obj, trace_id):
        """按核心秒数分组加载作业指标结果到内存中

        根据给定的核心秒数分组边界，从数据库批量加载各分组的作业性能指标结果（包含直方图和数值统计对象），
        结果存储在实例的jobs_results字典中，字典键格式为[edge]_[metric]。

        Args:
            core_seconds_edges (list): 核心秒数分组的边界值列表，用于将作业按(allocated_cores * runtime)
                的乘积值进行分组，例如边界值[1000, 2000]会生成[0-1000), [1000-2000), (2000-...]三个分组
            db_obj (DBManager): 数据库连接管理器对象，需实现查询接口
            trace_id (int): 跟踪数据的唯一标识符，用于关联数据库中的记录

        Returns:
            None: 结果直接存储在self.jobs_results属性中
        """
        # 生成分组指标字段列表：将核心秒数边界与基础指标组合成新字段名
        new_fields = []
        for edge in core_seconds_edges:
            # 需要统计的6个基础性能指标
            field_list = ["jobs_runtime", "jobs_waittime", "jobs_turnaround",
                          "jobs_requested_wc", "jobs_cpus_alloc",
                          "jobs_slowdown"]
            # 生成格式为g[edge]_[metric]的字段名，例如g1000_jobs_runtime
            new_fields += [ResultTrace.get_result_type_edge(edge, x)
                           for x in field_list]
        # 初始化结果存储字典（如果不存在
        if not hasattr(self, "job_results"):
            self.jobs_results = {}
        # 批量加载数据库结果并更新到当前实例
        self.jobs_results.update(load_results(new_fields, db_obj, trace_id))

    def get_grouped_result(self, core_seconds_edge, result_type):
        key = ResultTrace.get_result_type_edge(core_seconds_edge, result_type)
        if key in self.jobs_results.keys():
            return self.jobs_results[key]
        return None

    @classmethod
    def get_result_type_edge(cld, core_seconds_edge, result_type):
        return "g" + str(core_seconds_edge) + "_" + result_type

    def load_workflow_results(self, db_obj, trace_id):
        """创建工作流分析结果对象，并加载数据库中的工作流统计数据

        从指定数据库加载指定trace_id对应的工作流分析结果，包含全局统计和按manifest分组的统计。
        创建WorkflowsExtractor对象执行数据提取，并将结果存储在实例属性中。

        Args:
            db_obj (DBManager): 数据库管理对象，用于执行数据库查询操作
            trace_id (int): 要加载的工作流数据对应的trace编号标识

        Returns:
            None: 结果直接存储在实例属性workflow_results和workflow_results_per_manifest中
        """
        # 初始化工作流数据提取器
        self._wf_extractor = WorkflowsExtractor()

        # 从数据库加载全局工作流统计结果
        self.workflow_results = self._wf_extractor.load_overall_results(db_obj, trace_id)

        # 从数据库加载按manifest分组的工作流统计结果
        self.workflow_results_per_manifest \
            = self._wf_extractor.load_per_manifest_results(db_obj, trace_id)

    def _get_job_times(self, submit_start=None, submit_stop=None,
                       only_non_wf=False):
        """
        获取在指定提交时间范围内的作业时间指标数据

        Args:
            submit_start (int, optional): 起始时间戳（epoch秒），仅返回提交时间不早于此值的作业数据
            submit_stop (int, optional): 结束时间戳（epoch秒），仅返回提交时间不晚于此值的作业数据
            only_non_wf (bool): 为True时排除工作流作业（job_name以"wf_"开头的作业）

        Returns:
            tuple: 包含六个列表的元组，依次为：
                - jobs_runtime: 作业实际运行时间列表（结束时间 - 开始时间）
                - jobs_waittime: 作业等待时间列表（开始时间 - 提交时间）
                - jobs_turnaround: 作业周转时间列表（结束时间 - 提交时间）
                - jobs_timelimit: 作业时间限制列表
                - jobs_cores_alloc: 作业分配的核心数列表
                - jobs_slowdown: 作业延迟率列表（周转时间/运行时间）
        """
        jobs_runtime = []
        jobs_waittime = []
        jobs_turnaround = []
        jobs_timelimit = []
        jobs_cores_alloc = []
        jobs_slowdown = []
        for (end, start, submit, time_limit, cpus_alloc, job_name) in zip(
                self._lists_submit["time_end"],
                self._lists_submit["time_start"],
                self._lists_submit["time_submit"],
                self._lists_submit["timelimit"],
                self._lists_submit["cpus_alloc"],
                self._lists_submit["job_name"]):
            if (end == 0 or start == 0 or submit == 0 or end < start or start < submit
                    or (start == end)
                    or (submit_start is not None and submit < submit_start)
                    or (submit_stop is not None and submit > submit_stop)):
                # print "discarded!", submit_start, submit_stop, submit, start, end
                continue
            if (only_non_wf and len(job_name) >= 3 and job_name[0:3] == "wf_"):
                continue
            jobs_runtime.append(end - start)
            jobs_waittime.append(start - submit)
            jobs_turnaround.append(end - submit)
            jobs_timelimit.append(time_limit)
            jobs_cores_alloc.append(cpus_alloc)
            jobs_slowdown.append(float(end - submit) / float(end - start))
        return (jobs_runtime, jobs_waittime, jobs_turnaround, jobs_timelimit,
                jobs_cores_alloc, jobs_slowdown)

    def get_job_times_grouped_core_seconds(self,
                                           core_seconds_edges,
                                           submit_start=None, submit_stop=None,
                                           only_non_wf=False):
        """获取按核心秒数分组的作业时间指标
        返回在指定提交时间范围内作业的运行时间、等待时间和周转时间等指标，
        按核心秒数区间进行分组统计。
        Args:
            core_seconds_edges (list[int]): 核心秒数区间的分界值列表。分组区间为：
                [i0, i1), [i1, i2), ..., [i_{n-1}, +∞)
            submit_start (int|None): 提交时间起始时间戳（epoch秒），过滤早于此时间的作业
            submit_stop (int|None): 提交时间截止时间戳（epoch秒），过滤晚于此时间的作业
            only_non_wf (bool): True时排除工作流相关的作业（job_name以"wf_"开头）

        Returns:
            tuple: 包含7个字典的元组，每个字典按core_seconds_edges的分界值索引：
                - jobs_runtime: 作业实际运行时间列表（秒）
                - jobs_waittime: 作业等待时间列表（提交到开始运行的间隔秒数）
                - jobs_turnaround: 作业周转时间列表（提交到完成的间隔秒数）
                - jobs_timelimit: 作业时间限制列表（分钟）
                - jobs_cores_alloc: 作业分配的CPU核心数列表
                - jobs_slowdown: 作业减速比列表（周转时间/运行时间）
                - jobs_timesubmit: 作业提交时间戳列表
        """
        # 初始化分组字典，每个键对应core_seconds_edges中的分界值
        jobs_runtime = {}
        jobs_waittime = {}
        jobs_turnaround = {}
        jobs_timelimit = {}
        jobs_cores_alloc = {}
        jobs_slowdown = {}
        jobs_timesubmit = {}
        for edge in core_seconds_edges:
            jobs_runtime[edge] = []
            jobs_waittime[edge] = []
            jobs_turnaround[edge] = []
            jobs_timelimit[edge] = []
            jobs_cores_alloc[edge] = []
            jobs_slowdown[edge] = []
            jobs_timesubmit[edge] = []

        # 处理每个作业记录
        for (end, start, submit, time_limit, cpus_alloc, job_name,
             time_submit) in zip(
            self._lists_submit["time_end"],
            self._lists_submit["time_start"],
            self._lists_submit["time_submit"],
            self._lists_submit["timelimit"],
            self._lists_submit["cpus_alloc"],
            self._lists_submit["job_name"],
            self._lists_submit["time_submit"]):
            if (end == 0 or start == 0 or submit == 0 or end < start or start < submit
                    or (start == end)
                    or (submit_start is not None and submit < submit_start)
                    or (submit_stop is not None and submit > submit_stop)):
                # print "discarded!", submit_start, submit_stop, submit, start, end
                continue
            # 排除工作流作业（当only_non_wf为True时）
            if (only_non_wf and len(job_name) >= 3 and job_name[0:3] == "wf_"):
                continue

            # 计算当前作业所属的核心秒数区间
            edge = self._get_index_in_core_seconds_list(time_limit * 60,
                                                        cpus_alloc,
                                                        core_seconds_edges)
            # 将时间指标添加到对应区间的列表中
            jobs_runtime[edge].append(end - start)
            jobs_waittime[edge].append(start - submit)
            jobs_turnaround[edge].append(end - submit)
            jobs_timelimit[edge].append(time_limit)
            jobs_cores_alloc[edge].append(cpus_alloc)
            jobs_slowdown[edge].append(float(end - submit) / float(end - start))
            jobs_timesubmit[edge].append(time_submit)
        return (jobs_runtime, jobs_waittime, jobs_turnaround, jobs_timelimit,
                jobs_cores_alloc, jobs_slowdown, jobs_timesubmit)

    def get_job_values_grouped_core_seconds(self,
                                            core_seconds_edges,
                                            submit_start=None, submit_stop=None,
                                            only_non_wf=False,
                                            fields=["time_submit", "time_start"]):
        """获取按核心秒数分组的作业性能指标数据
        根据核心秒数边界将作业分组，返回各分组指定字段的数值列表。支持按提交时间范围过滤，
        并可选排除工作流作业。

        Args:
            core_seconds_edges (list[int]): 核心秒数分组边界值列表，定义区间划分方式。
                区间划分规则为：
                - 第一个区间: [i0, i1]
                - 中间区间: (i_prev, i_current]
                - 最后一个区间: (i_last, +inf)
            submit_start (int|None): 提交时间起始时间戳（包含），None表示不限制
            submit_stop (int|None): 提交时间截止时间戳（包含），None表示不限制
            only_non_wf (bool): True时排除工作流关联的作业，默认False包含所有
            fields (list[str]): 需要收集的字段名称列表，默认包含提交时间和启动时间

        Returns:
            dict: 嵌套字典结构，第一层key为字段名，第二层key为核心秒数边界值，
                值为对应字段的数值列表。结构示例：
                {
                    "time_submit": {edge1: [ts1, ts2...], ...},
                    "time_start": {edge1: [ts1, ts2...], ...}
                }

        实现说明：
            1. 根据作业的timelimit和cpus_alloc计算核心秒数
            2. 通过_get_index_in_core_seconds_list确定所属区间
            3. 按fields参数收集指定字段的数值到对应分组
        """
        jobs_dic = {}

        # 初始化嵌套字典结构
        # 第一层key为字段名，第二层key为核心秒数边界值，值为空列表
        for field in fields:
            jobs_dic[field] = {}
            for edge in core_seconds_edges:
                jobs_dic[field][edge] = []

        # 遍历所有作业记录
        # 根据timelimit和cpus_alloc计算核心秒数，并确定所属区间
        for (timelimit, cpus_alloc, i) in zip(
                self._lists_submit["timelimit"],
                self._lists_submit["cpus_alloc"],
                range(len(self._lists_submit["timelimit"]))):
            # 计算核心秒数并匹配分组边界
            edge = self._get_index_in_core_seconds_list(timelimit * 60,
                                                        cpus_alloc,
                                                        core_seconds_edges)
            # 将当前记录的各个字段值存入对应分组
            for field in fields:
                jobs_dic[field][edge].append(self._lists_submit[field][i])

        return jobs_dic

    def _get_index_in_core_seconds_list(self, runtime, cpus_alloc,
                                        core_seconds_edges):
        """
        根据任务消耗的核心秒数，在核心秒区间列表中查找对应的区间左边界

        Args:
            runtime: float - 任务实际运行时间（单位：秒）
            cpus_alloc: int - 任务分配的核心数
            core_seconds_edges: List[float] - 核心秒区间分割点的有序列表
                                             （如 [100, 200, 300] 表示区间
                                             [100,200)、[200,300)）

        Returns:
            float - 目标核心秒值所属区间的左边界值。当超过最大边界时返回列表最后元素
        """
        # 计算总核心秒消耗：运行时间 × 分配核心数
        core_seconds = runtime * cpus_alloc
        # 遍历相邻的边界点对（当前边界，下一边界）
        for (edge, n_edge) in zip(core_seconds_edges[:-1],
                                  core_seconds_edges[1:]):
            # 当核心秒数小于等于下一个边界时，返回当前左边界
            if core_seconds <= n_edge:
                return edge
        # 处理超出所有区间的情况：返回最后一个边界值
        return core_seconds_edges[-1]

    def fill_job_values(self, start=None, stop=None, append=False):
        """从加载的跟踪数据中计算并存储作业时间指标到内存中
        获取指定提交时间范围内的作业性能指标数据，根据append参数决定追加或覆盖存储模式。
        该函数主要用于后续统计分析和结果展示的数据准备。
        Args:
            start (float, optional): 起始时间戳（epoch秒），仅处理提交时间不早于此值的作业。
                默认None表示从最早可用作业开始
            stop (float, optional): 截止时间戳（epoch秒），仅处理提交时间不晚于此值的作业。
                默认None表示处理到最新可用作业
            append (bool): 数据存储模式开关
                - True: 保留现有数据并将新结果追加到列表中
                - False: 清空现有数据后存储新结果（默认模式）

        副作用:
            直接修改以下实例变量：
            - _jobs_runtime: 作业实际运行时间列表（秒）
            - _jobs_waittime: 作业等待时间列表（秒）
            - _jobs_turnaround: 作业周转时间列表（秒）
            - _jobs_timelimit: 作业时间限制列表（分钟）
            - _jobs_cpus_alloc: 作业分配的核心数列表
            - _jobs_slowdown: 作业延迟率列表（周转时间/运行时间）
        """
        # 通过底层方法获取指定时间范围内的作业指标数据
        # only_non_wf=True表示仅处理非工作流作业
        (jobs_runtime, jobs_waittime, jobs_turnaround, jobs_timelimit,
         jobs_cpus_alloc, jobs_slowdown) = self._get_job_times(
            submit_start=start,
            submit_stop=stop,
            only_non_wf=True)
        # 非追加模式时重置所有存储列表
        if not append:
            self._jobs_runtime = []
            self._jobs_waittime = []
            self._jobs_turnaround = []
            self._jobs_timelimit = []
            self._jobs_cpus_alloc = []
            self._jobs_slowdown = []

        self._jobs_runtime += jobs_runtime
        self._jobs_waittime += jobs_waittime
        self._jobs_turnaround += jobs_turnaround
        self._jobs_timelimit += jobs_timelimit
        self._jobs_cpus_alloc += jobs_cpus_alloc
        self._jobs_slowdown += jobs_slowdown

    def calculate_job_results_grouped_core_seconds(self,
                                                   core_seconds_edges,
                                                   store=False,
                                                   db_obj=None,
                                                   trace_id=None,
                                                   start=None,
                                                   stop=None,
                                                   append=False):
        """按核心秒数分组计算作业统计指标，并支持结果存储到数据库

        对存储的跟踪数据中常规作业进行分组统计，根据作业分配的核心秒数（核心数×运行时间）
        将作业划分到预定义的分组区间，计算各分组的运行时/等待时间/周转时间等指标分布，
        并可选择将统计结果持久化到数据库。
        Args:
            core_seconds_edges (list): 核心秒数分组的边界值列表，定义分组区间
            store (bool): 是否将结果持久化到数据库。设为True时必须提供db_obj和trace_id
            db_obj (DBManager, optional): 数据库连接管理器对象，用于存储操作
            trace_id (int, optional): 分析轨迹在数据库中的唯一标识符
            start (int, optional): 起始时间戳（epoch秒），过滤此时间前提交的作业
            stop (int, optional): 结束时间戳（epoch秒），过滤此时间后提交的作业
            append (bool): True时追加到现有数据，False时重置数据存储
        Returns:
            dict: 统计结果字典，键为核心秒数边界值，值为包含以下指标的字典：
                - 运行时分布直方图
                - 等待时间分布直方图
                - 周转时间分布直方图
                - 请求核心数分布直方图
                - 分配核心数分布直方图
                - 减速比分布直方图
        """
        # 获取按核心秒数分组的作业指标数据（过滤工作流作业）
        data_list_of_dics = list(self.get_job_times_grouped_core_seconds(
            core_seconds_edges,
            start,
            stop,
            only_non_wf=True))
        if not append:
            self._data_list_of_dics = [{} for x in range(len(data_list_of_dics))]
        for (l1, l2) in zip(self._data_list_of_dics, data_list_of_dics):
            for key in l2.keys():
                if not key in l1.keys():
                    l1[key] = []
                l1[key] += l2[key]
        results = {}
        for edge in core_seconds_edges:
            data_list = [x[edge] for x in self._data_list_of_dics[:-1]]
            field_list = ["jobs_runtime", "jobs_waittime", "jobs_turnaround",
                          "jobs_requested_wc", "jobs_cpus_alloc", "jobs_slowdown"]
            field_list = ["g" + str(edge) + "_" + x for x in field_list]
            bin_size_list = [60, 60, 120, 1, 24, 100]
            minmax_list = [(0, 3600 * 24 * 30), (0, 3600 * 24 * 30), (0, 2 * 3600 * 24 * 30),
                           (0, 60 * 24 * 30), (0, 24 * 4000), (0, 800)]

            results[edge] = calculate_results(data_list, field_list,
                                              bin_size_list,
                                              minmax_list, store=store, db_obj=db_obj,
                                              trace_id=trace_id)
        return results

    def calculate_and_store_job_results(self, store=False, db_obj=None,
                                        trace_id=None):
        """
        计算并存储作业统计结果，生成直方图分布数据
        参数:
        store (bool): 是否将结果存储到数据库，默认为False
        db_obj (object): 数据库连接对象，用于数据存储操作，默认为None
        trace_id (str): 用于追踪请求的唯一标识符，默认为None

        返回:
        dict: 包含作业各项指标统计结果的字典，存储在self.jobs_results属性中
        """
        # 定义原始数据源列表与对应字段名的映射关系
        # data_list: 原始数值列表集合，包含运行时、等待时间等核心指标
        # field_list: 对应数据库字段名列表，与data_list顺序严格对应
        # bin_size_list: 各指标的直方图分箱间隔配置
        # minmax_list: 各指标的数值范围限制，用于数据截断处理
        data_list = [self._jobs_runtime, self._jobs_waittime,
                     self._jobs_turnaround,
                     self._jobs_timelimit,
                     self._jobs_cpus_alloc,
                     self._jobs_slowdown]
        field_list = ["jobs_runtime", "jobs_waittime", "jobs_turnaround",
                      "jobs_requested_wc", "jobs_cpus_alloc", "jobs_slowdown"]
        bin_size_list = [60, 60, 120, 1, 24, 100]
        minmax_list = [(0, 3600 * 24 * 30), (0, 3600 * 24 * 30), (0, 2 * 3600 * 24 * 30),
                       (0, 60 * 24 * 30), (0, 24 * 4000), (0, 800)]

        # 调用核心计算函数生成统计结果
        # 计算结果包含直方图分布、统计量等数据
        self.jobs_results = calculate_results(data_list, field_list,
                                              bin_size_list,
                                              minmax_list, store=store, db_obj=db_obj,
                                              trace_id=trace_id)
        return self.jobs_results

    def calculate_job_results(self, store=False, db_obj=None, trace_id=None,
                              start=None, stop=None):
        """计算存储跟踪中常规作业的统计指标，支持结果持久化到数据库
        对指定时间范围内的作业数据进行统计分析，可生成运行时/等待时间/周转时间等指标。
        当启用存储功能时，需要提供完整的数据库连接参数，分析结果将关联到指定跟踪记录。
        Args:
            store (bool): 是否将结果持久化到数据库。设为True时需提供完整数据库参数。默认为False
            db_obj (DBManager): 数据库连接管理对象，需配置有效连接。store=True时必填
            trace_id (int): 跟踪记录的唯一标识符，store=True时必填且需存在对应数据库记录
            start (int): 起始时间戳（epoch秒），过滤此时间前提交的作业。None表示无下限
            stop (int): 截止时间戳（epoch秒），过滤此时间后提交的作业。None表示无上限

        Returns:
            calculate_and_store_job_results()的返回值，通常包含统计指标或数据库操作状态

        Raises:
            ValueError: 当store=True但缺少必要数据库参数时抛出
        """
        # 存储模式下的参数完整性校验
        if store and db_obj is None:
            raise ValueError("db_obj must be set to store jobs data")
        if store and trace_id is None:
            raise ValueError("trace_id must be set to store jobs data")

        # 加载作业数据并重置现有统计值
        self.fill_job_values(start=start, stop=stop, append=False)
        # 执行核心计算及存储逻辑
        return self.calculate_and_store_job_results(store=store, db_obj=db_obj,
                                                    trace_id=trace_id)

    def do_workflow_pre_processing(self, append=False, do_processing=True):
        """识别并预处理跟踪数据中的工作流信息

        该方法执行工作流检测和预处理的核心流程，包含工作流提取器的初始化、工作流提取和可选的后处理操作。
        可通过参数控制是否保留已有工作流数据，并决定是否执行附加的数据处理步骤。

        Args:
            append (bool，可选): 控制工作流数据的累积模式
                - 当为False时（默认），会重置内部工作流提取器，清除所有现有工作流数据
                - 当为True时，保留现有工作流数据，并在其基础上追加新提取的工作流
            do_processing (bool，可选): 控制是否执行后处理
                - 当为True时（默认），在提取完成后执行工作流数据的后续处理
                - 当为False时，仅执行基础提取操作

        Returns:
            list: 已识别工作流对象的列表。具体对象类型取决于WorkflowsExtractor的实现，
                通常包含工作流的元数据和运行时特征

        Notes:
            1. 返回的列表直接引用内部工作流提取器的存储对象，任何修改都会影响后续处理结果
            2. 当append=False时，原有工作流数据会被完全清除
        """
        # 初始化/重置工作流提取器（非追加模式时）
        if not append:
            self._wf_extractor = WorkflowsExtractor()
        # 执行核心工作流提取逻辑
        self._wf_extractor.extract(self._lists_submit,
                                   reset_workflows=not append)
        # 执行可选的后处理阶段（如特征计算、关联分析等）
        if do_processing:
            self._wf_extractor.do_processing()
        # 返回工作流对象的直接引用（注意数据可变性）
        return self._wf_extractor._workflows

    def truncate_workflows(self, num_workflows):
        """截断工作流列表至指定数量
        通过内部的工作流提取器对象，将当前存储的工作流列表
        截断到指定的最大数量，超出部分将被移除
        Args:
            num_workflows (int): 需要保留的最大工作流数量。
                当当前数量超过该数值时，保留最早的num_workflows条记录；
                当小于等于该数值时，列表保持不变
        Returns:
            None: 该方法直接修改内部状态，不返回具体值
        """
        self._wf_extractor.truncate_workflows(num_workflows)

    def rename_workflows(self, pre_number):
        self._wf_extractor.rename_workflows(pre_number)

    def fill_workflow_values(self, start=None, stop=None, append=False):
        """计算并存储工作流分析指标数据
        对已加载的跟踪数据中的工作流执行两维度分析：
        1. 全局工作流统计指标计算
        2. 按manifest分组的工作流统计指标计算
        所有时间参数均采用UNIX时间戳格式（秒级精度）。
        Args:
            start (float, optional): 起始时间戳（包含），仅处理提交时间不早于此值的工作流
                默认None表示从最早可用工作流开始分析
            stop (float, optional): 截止时间戳（包含），仅处理提交时间不晚于此值的工作流
                默认None表示处理到最新可用工作流
            append (bool): 数据累积模式开关
                True: 保留现有分析结果并追加新计算结果
                False: 清除历史数据后重新计算（默认模式）

        Returns:
            None: 分析结果直接存储在工作流提取器对象中，无显式返回值
        """
        self._wf_extractor.fill_overall_values(start=start, stop=stop,
                                               append=append)
        self._wf_extractor.fill_per_manifest_values(start=start, stop=stop,
                                                    append=append)

    def calculate_and_store_workflow_results(self, store=False, db_obj=None,
                                             trace_id=None):
        """计算工作流作业的统计结果并可选择存储到数据库。同时执行全局统计和每个清单的专项分析。
        当存储开关开启时，会将计算结果持久化到数据库，并更新对应跟踪记录的结果ID。需要确保数据库连接参数
        和跟踪ID有效。
        Args:
            store (bool): 结果存储开关。当设为True时，必须提供有效的db_obj和trace_id参数。
                默认值：False
            db_obj (DBManager): 预配置的数据库管理对象，用于执行数据库操作。当store=True时此参数必填
            trace_id (int): 被分析跟踪记录的数据库ID。当store=True时此参数必填，且必须在结果表中存在对应条目
        Returns:
            None: 本函数无返回值，结果通过成员对象存储或直接写入数据库
        功能说明：
        - 先调用工作流提取器计算全局统计结果
        - 再调用工作流提取器计算每个清单的专项统计结果
        - 存储操作由两个内部方法根据store参数自行控制
        """
        self._wf_extractor.calculate_and_store_overall_results(store=store,
                                                               db_obj=db_obj,
                                                               trace_id=trace_id)
        self._wf_extractor.calculate_per_manifest_results(store=store,
                                                          db_obj=db_obj,
                                                          trace_id=trace_id)

    def calculate_workflow_results(self, store=False, db_obj=None,
                                   trace_id=None, start=None, stop=None,
                                   limited=False):
        """计算跟踪数据中工作流作业的统计指标，支持结果存储到数据库
        本方法执行两个维度的分析：
        1. 全局工作流统计指标计算
        2. 按manifest分组的工作流统计指标计算
        结果将存储在实例属性中，并可选择持久化到数据库

        Args:
            store (bool): 存储开关。设为True时将结果写入数据库，此时必须提供数据库参数
            db_obj (DBManager): 数据库连接对象。当store=True时需配置有效数据库连接
            trace_id (int): 跟踪记录ID。store=True时需确保该ID在结果表中存在
            start (int): 起始时间戳（epoch秒），过滤此时间前提交的工作流。None表示无下限
            stop (int): 截止时间戳（epoch秒），过滤此时间后提交的工作流。None表示无上限
            limited (bool): 限制模式开关。True时仅处理部分数据用于快速测试/部分分析
        Returns:
            None: 结果存储在实例属性中：
                - workflow_results: 全局工作流统计结果
                - workflow_results_per_manifest: 按manifest分组统计结果
        """
        # 计算全局工作流指标（执行时间/资源使用等），可选存储
        self.workflow_results = self._wf_extractor.calculate_overall_results(
            store=store,
            db_obj=db_obj,
            trace_id=trace_id,
            start=start,
            stop=stop,
            limited=limited)

        # 计算按manifest分组的工作流指标，可选存储
        self.workflow_results_per_manifest = (
            self._wf_extractor.calculate_per_manifest_results(
                store=store,
                db_obj=db_obj,
                trace_id=trace_id,
                start=start,
                stop=stop,
                limited=limited))

    def _get_job_run_info(self, fake_stop_time=None):
        """获取并处理作业运行特征数据，返回三个相互关联的特征列表。

        过滤存在0值特征的无效作业，对于未结束的作业可使用指定伪结束时间处理。
        保证返回列表中同索引位置的元素属于同一个作业的特征数据。

        Args:
            fake_stop_time (int, optional): 伪结束时间戳（epoch时间），当作业存在开始时间但无结束时间时，
                使用该值替代，避免丢弃结束时间为0的作业。默认为None表示不启用该特性。

        Returns:
            tuple: 包含三个列表的元组，按顺序分别是：
                - jobs_runtime (list[int]): 作业运行时长列表（单位：秒）
                - jobs_start_time (list[int]): 作业开始时间戳列表（epoch时间）
                - job_cores (list[int]): 作业分配的核心数列表

        Note:
            当作业存在以下情况时会被丢弃：
                1. 任意特征值为0
                2. 结束时间小于开始时间
                3. 未启用fake_stop_time时结束时间为0
        """
        jobs_runtime = []
        jobs_start_time = []
        jobs_cores = []

        # 并行遍历三个特征列表，进行联合数据清洗
        for (end, start, cores) in zip(self._lists_start["time_end"],
                                       self._lists_start["time_start"],
                                       self._lists_start["cpus_alloc"]):
            # 基础过滤：0值检查和时间逻辑校验
            if end == 0 or start == 0 or cores == 0 or end < start:
                # 特殊处理未结束作业：使用伪结束时间保留数据
                if fake_stop_time and start and not end:
                    end = fake_stop_time
                else:
                    continue
            # 收集通过校验的特征数据
            jobs_runtime.append(end - start)
            jobs_start_time.append(start)
            jobs_cores.append(cores)
        return jobs_runtime, jobs_start_time, jobs_cores

    def calculate_utilization(self, max_cores, do_preload_until=None,
                              endCut=None, store=False, db_obj=None,
                              trace_id=None,
                              ending_time=None):
        """ 生成存储跟踪上的利用率更改和集成利用率的列表。它考虑了单个作业工作流中的利用率损失。
        Args:
        - max_cores: 分配的核数被认为是100%的利用率。
        - do_preload_until: 将从中执行利用率计算的数字epoch时间戳。
                在该时间戳之前的作业将被“预加载”，即在利用率分析开始时处理以了解机器的负载状态。
                如果设置为None，则从第一个作业开始处理跟踪。
        - endCut: 将对其执行利用率计算的数字epoch时间戳。如果设置为None，则跟踪将被处理到最后一个作业。
        - ending_time: 如果设置为int epoch时间戳，则该值将用作分析完成时仍在运行的作业的结束时间。
        Returns: 
        - integrated_ut: 浮动在0.0-1.0之间，表示分析期间的综合利用率。
        - utilization_timestamps: 指向利用率发生变化的时刻的epoch时间戳列表。
        - utilization_values: 列出与utilzation_timestamps长度相同的整数。当利用率发生变化时，每个元素对应分配的核数。
        - acc_waste: 在单个作业工作流中未使用的累积核心秒数。
        """
        # 初始化利用率引擎对象。
        uEngine = UtilizationEngine()
        # 获取作业运行信息，包括作业运行时间，开始时间和占用的核数。
        jobs_runtime, jobs_start_time, jobs_cores = self._get_job_run_info(
            fake_stop_time=ending_time)

        # 加载作业，所以当我们开始测量时，“机器”是“满的”。
        # 如果指定了预加载时间，则处理在该时间之前的作业，以便在开始测量利用率时，“机器”处于满负荷状态。
        if do_preload_until:
            uEngine.processUtilization(
                jobs_start_time, jobs_runtime, jobs_cores,
                doingPreload=True,
                endCut=do_preload_until)

        # 处理利用率，计算在指定时间段内的利用率变化。
        self._utilization_timestamps, self._utilization_values = \
            uEngine.processUtilization(
                jobs_start_time, jobs_runtime, jobs_cores, endCut=endCut,
                startCut=do_preload_until,
                preloadDone=(do_preload_until is not None))
        # 初始化累积浪费的核秒数。
        self._acc_waste = 0
        # 如果存在工作流提取器，则计算在单个作业工作流中浪费的利用率，并更新利用率值。
        if self._wf_extractor:
            (stamps_list, wastedelta_list, self._acc_waste) = (
                self._wf_extractor.get_waste_changes())
            self._utilization_timestamps, self._utilization_values = \
                uEngine.apply_waste_deltas(stamps_list, wastedelta_list,
                                           start_cut=do_preload_until,
                                           end_cut=endCut)
        # 计算综合利用率
        self._integrated_ut = uEngine.getIntegralUsage(maxUse=max_cores)
        # 计算修正后的综合利用率。
        self._corrected_integrated_ut = self._calculate_corrected_ut(
            self._integrated_ut,
            self._acc_waste,
            max_cores,
            self._utilization_timestamps[-1] -
            self._utilization_timestamps[0])
        # 如果需要存储结果，则将利用率和浪费信息存储到数据库中。
        if store:
            res = self._get_utilization_result()
            res.set_dic(dict(utilization=self._integrated_ut,
                             waste=self._acc_waste,
                             corrected_utilization=
                             self._corrected_integrated_ut))
            res.store(db_obj, trace_id, "usage")
        # 返回利用率计算结果。
        return (self._integrated_ut, self._utilization_timestamps,
                self._utilization_values,
                self._acc_waste, self._corrected_integrated_ut)

    def _calculate_corrected_ut(self, integrated_ut, acc_waste, max_cores,
                                running_time_s):
        """
        计算修正后的利用率。
        该函数根据集成利用率、累计浪费、最大内核数和运行时间来计算修正后的利用率。
        参数:
        integrated_ut (float): 集成利用率。
        acc_waste (float): 累计浪费。
        max_cores (int): 最大内核数。
        running_time_s (int): 运行时间（以秒为单位）。

        返回:
        float: 修正后的利用率。
        """
        # 计算总内核秒数
        total_core_s = max_cores * running_time_s
        # 计算使用的内核秒数
        used_core_s = total_core_s * integrated_ut
        # 计算修正后的使用内核秒数
        corrected_used_cores_s = used_core_s - acc_waste
        # 返回修正后的利用率
        return float(corrected_used_cores_s) / float(total_core_s)

    def _get_job_wait_info(self, fake_stop_time=None, fake_start_time=None):
        """
        获取作业等待相关信息（支持模拟时间替换）

        参数:
        fake_stop_time (int/None): 模拟的作业停止时间戳，用于替换缺失的结束时间
        fake_start_time (int/None): 模拟的作业开始时间戳，用于替换缺失的开始时间

        返回:
        tuple: 包含四个列表的元组，格式为(
            作业实际运行时长列表[jobs_runtime],
            作业开始时间戳列表[jobs_start_time],
            作业分配核心数列表[jobs_cores],
            作业提交时间戳列表[jobs_submit_time]
        )

        处理逻辑:
        1. 遍历提交记录，过滤无效数据并修正异常时间
        2. 使用模拟时间填补缺失的start/end时间
        3. 收集有效作业的运行特征数据
        """
        jobs_runtime = []
        jobs_start_time = []
        jobs_cores = []
        jobs_submit_time = []
        for (end, start, cores, submit) in zip(self._lists_submit["time_end"],
                                               self._lists_submit["time_start"],
                                               self._lists_submit["cpus_alloc"],
                                               self._lists_submit["time_submit"]):
            # 过滤无效记录：核心数为0/时间戳异常/未完成的任务
            if cores == 0 or start == 0 or end == 0 or start > end:
                # 使用模拟时间填补缺失的开始时间（同时设置end=start）
                if fake_start_time and not start:
                    start = fake_start_time
                    end = fake_start_time
                # 处理只有开始时间的情况，使用模拟结束时间
                elif fake_stop_time and start and not end:
                    end = max(start, fake_stop_time)
                else:
                    continue
            # 收集处理后的有效数据
            jobs_runtime.append(end - start)
            jobs_start_time.append(start)
            jobs_cores.append(cores)
            jobs_submit_time.append(submit)
        return jobs_runtime, jobs_start_time, jobs_cores, jobs_submit_time

    def _get_job_wait_info_all(self):
        """收集并计算所有作业的运行状态指标

        遍历存储在self._lists_submit中的作业记录，提取运行时间、资源使用等核心指标，
        过滤无效数据条目，计算作业执行准确率相关统计量

        Returns:
            tuple: 包含多个作业指标列表和统计值的元组，结构为:
                - jobs_runtime (list): 每个作业的实际运行时间(秒)，未运行的为-1
                - jobs_start_time (list): 每个作业的开始时间戳
                - jobs_cores (list): 每个作业分配的CPU核数
                - jobs_submit_time (list): 每个作业的提交时间戳
                - jobs_timelimit (list): 每个作业的预设时间限制(分钟)
                - accuracy (float): 平均执行准确率（实际用时/预设时限）
                - median_accuracy (float): 执行准确率的中位数
        """
        # 初始化存储容器
        jobs_runtime = []
        jobs_start_time = []
        jobs_cores = []
        jobs_submit_time = []
        jobs_timelimit = []
        jobs_accuracy = []
        ended_jobs = 0.0
        accuracy = 0

        # 并行遍历多个作业属性列表
        for (end, start, cores, submit, timelimit) in zip(
                self._lists_submit["time_end"],
                self._lists_submit["time_start"],
                self._lists_submit["cpus_alloc"],
                self._lists_submit["time_submit"],
                self._lists_submit["timelimit"]):

            if cores == 0 or submit == 0 or timelimit == 0:
                continue
            if start == 0 or end == 0:
                runtime = -1
            else:
                runtime = end - start

            # 仅统计已正常结束的作业
            if start != 0 and end != 0:
                ended_jobs += 1
                # 计算单作业时间准确率：实际运行时间/(时间限制*60) → 将分钟转换为秒
                accuracy += float(runtime) / float(timelimit * 60)
                jobs_accuracy.append(float(runtime) / float(timelimit * 60))

            jobs_runtime.append(runtime)
            jobs_start_time.append(start)
            jobs_cores.append(cores)
            jobs_submit_time.append(submit)
            jobs_timelimit.append(timelimit)
        # 计算全局统计量
        accuracy /= ended_jobs
        return (jobs_runtime, jobs_start_time, jobs_cores, jobs_submit_time,
                jobs_timelimit, accuracy, np.median(jobs_accuracy))

    def calculate_waiting_submitted_work_all(self, acc_period=60,
                                             ending_time=None):
        """计算系统作业队列的等待工作量与提交工作量的时间序列指标

        该函数通过跟踪作业提交和启动事件，计算两个维度的核心时间指标：
        1. 等待队列中的累积核心时间（包括实际消耗和用户请求）
        2. 系统历史提交工作量的平均速率（基于滑动窗口统计）

        Args:
            acc_period (int): 采样窗口时长（秒），用于计算提交工作量的移动平均值，默认60秒
            ending_time (int/None): 作业时间修正基准时间戳（epoch），当设为非None时，
                用于替换start/end时间为0的作业时间，避免数据丢失

        Returns:
            tuple: 包含六个元素的元组，按顺序为：
                - waiting_work_stamps: 等待队列变化时间戳列表（按时间排序）
                - waiting_work_times: 对应时刻等待队列的实际核心秒数
                - submitted_work_stamps: 提交速率采样时间戳列表
                - submitted_work_values: 实际核心小时/秒的提交速率
                - waiting_requested_ch: 等待队列的用户请求核心秒数
                - requested_core_h_per_min_values: 用户请求核心小时/秒的提交速率

        算法说明:
            1. 双维度跟踪机制：同时维护实际资源消耗和用户请求资源两个事件流
            2. 事件驱动统计：通过提交/启动事件触发队列状态的增减操作
            3. 滑动窗口采样：当相邻提交时间超过采样周期时计算历史平均值
            4. 运行时预测补偿：对未完成作业使用准确率模型预测实际运行时间
        """
        # 初始化作业元数据（包含运行时预测结果）
        (jobs_runtime, jobs_start_time, jobs_cores, jobs_submit_time,
         jobs_timelimit,
         mean_accuracy, median_accuracy) = self._get_job_wait_info_all()

        print
        "Observed accuracy:", mean_accuracy, median_accuracy
        accuracy = mean_accuracy
        # 事件记录字典：key为时间戳，value为对应时刻的核心时间变化量
        wait_ch_events = {}  # 实际资源消耗事件（核心秒）
        wait_requested_ch_events = {}  # 用户请求资源事件（核心秒）
        submitted_core_h_per_min = {}  # 提交速率记录（实际值）
        submitted_requested_core_h_per_min = {}  # 提交速率记录（请求值）

        # 滑动窗口状态变量
        first_time_stamp = None  # 首个提交时间基准点
        previous_stamp = None  # 上一个采样点时间
        acc_submitted_work = 0  # 窗口内累计实际核心秒
        acc_submitted_requested_work = 0  # 窗口内累计请求核心秒

        # 遍历所有作业记录处理事件
        for submit_time, start_time, runtime, cores, timelimit in zip(
                jobs_submit_time,
                jobs_start_time,
                jobs_runtime,
                jobs_cores,
                jobs_timelimit):

            # 处理未完成作业的运行时预测（使用准确率模型）
            if runtime < 0:
                runtime = float(timelimit * 60) * accuracy

            # 核心时间计算
            core_h = cores * runtime  # 实际消耗 = 核心数 × 运行秒数
            requested_core_h = timelimit * cores * 60  # 用户请求 = 核心数 × 时间限制（分钟转秒）

            # --- 提交工作量统计部分 ---
            if first_time_stamp is None:
                first_time_stamp = submit_time
                previous_stamp = submit_time
            acc_submitted_work += core_h
            acc_submitted_requested_work += requested_core_h

            # 当超过采样间隔时计算平均速率
            if submit_time - previous_stamp > acc_period:
                corrected_acc = (float(acc_submitted_work) /
                                 float(submit_time - first_time_stamp))
                # 计算实际核心小时/秒的提交速率
                submitted_core_h_per_min[submit_time] = corrected_acc
                corrected_requested_acc = (float(acc_submitted_requested_work) /
                                           float(submit_time - first_time_stamp))
                # 计算请求核心小时 / 秒的提交速率
                submitted_requested_core_h_per_min[submit_time] = (
                    corrected_requested_acc)

                previous_stamp = submit_time

            # --- 等待队列统计部分 ---
            # 处理提交事件（增加队列负载）
            if submit_time > 0:
                if not submit_time in wait_ch_events.keys():
                    wait_ch_events[submit_time] = 0
                    wait_requested_ch_events[submit_time] = 0
                wait_ch_events[submit_time] += core_h
                wait_requested_ch_events[submit_time] += requested_core_h

            # 处理启动事件（减少队列负载）
            if start_time > 0:
                if not start_time in wait_ch_events.keys():
                    wait_ch_events[start_time] = 0
                    wait_requested_ch_events[start_time] = 0
                wait_ch_events[start_time] -= core_h
                wait_requested_ch_events[start_time] -= requested_core_h

        # --- 等待队列累计计算 ---
        acc_ch = 0  # 实际核心秒累计值
        acc_requested_ch = 0  # 请求核心秒累计值
        stamps = []  # 时间戳序列
        waiting_ch = []  # 实际等待队列值序列
        waiting_requested_ch = []  # 请求等待队列值序列

        # 按时间顺序处理所有事件点
        for stamp in sorted(wait_ch_events.keys()):
            acc_ch += wait_ch_events[stamp]
            acc_requested_ch += wait_requested_ch_events[stamp]
            stamps.append(stamp)
            waiting_ch.append(acc_ch)
            waiting_requested_ch.append(acc_requested_ch)

        # --- 提交数据后处理 ---
        core_h_per_min_stamps = []  # 提交速率时间戳
        core_h_per_min_values = []  # 实际提交速率值
        requested_core_h_per_min_values = []  # 请求提交速率值
        previous_stamp = None

        # 生成按时间排序的提交速率数据
        for stamp in sorted(submitted_core_h_per_min.keys()):
            core_h_per_min_stamps.append(stamp)
            core_h_per_min_values.append(submitted_core_h_per_min[stamp])
            requested_core_h_per_min_values.append(
                submitted_requested_core_h_per_min[stamp])
        # 返回三组时序数据对（实际值+请求值）
        return (stamps, waiting_ch, core_h_per_min_stamps, core_h_per_min_values,
                waiting_requested_ch, requested_core_h_per_min_values)

    def calculate_waiting_submitted_work(self, acc_period=60,
                                         ending_time=None):
        """计算系统等待队列中的工作量和已提交工作量随时间的变化
        该函数通过分析作业提交和启动时间，生成两个时间序列：
        1. 等待队列中的核心秒数变化
        2. 系统提交工作量与处理能力的比率变化
        Args:
            acc_period (int, optional): 采样间隔时间（秒），用于计算提交工作量的平均值。默认60秒
            ending_time (int, optional): 截止时间戳。当设置为非None时：
                - 用于处理start_time或submit_time为0的作业
                - 会替换这些作业的零值时间戳
        Returns:
            tuple: 包含四个元素的元组
                - waiting_work_stamps: 等待队列时间戳列表（epoch时间）
                - waiting_work_times: 对应时间点的累计等待核心秒数
                - submitted_work_stamps: 提交工作量时间戳列表（epoch时间）
                - submitted_work_values: 提交工作量占比序列（当前累计提交量/系统处理能力）

        Raises:
            Exception: 当作业运行时间runtime出现负值时抛出
        """
        # 获取作业基础信息，处理时间戳为0的特殊情况
        jobs_runtime, jobs_start_time, jobs_cores, jobs_submit_time = (
            self._get_job_wait_info(fake_stop_time=ending_time,
                                    fake_start_time=ending_time))
        # 初始化事件记录字典
        wait_ch_events = {}  # 记录等待队列变化的离散事件 {时间戳: 核心秒数变化量}
        submitted_core_h_per_min = {}  # 记录采样周期内的平均提交工作量

        # 初始化提交工作量计算相关变量
        first_time_stamp = None  # 首个作业提交时间
        previous_stamp = None    # 前一个采样点时间
        acc_submitted_work = 0   # 累计提交工作量
        # 遍历所有作业处理提交/启动事件
        for submit_time, start_time, runtime, cores in zip(jobs_submit_time,
                                                           jobs_start_time,
                                                           jobs_runtime,
                                                           jobs_cores):
            if runtime < 0:
                raise Exception()

            # 计算作业总核心秒数 = 核心数 * 运行时间
            core_h = cores * runtime
            # 处理提交工作量统计逻辑
            if first_time_stamp is None:
                first_time_stamp = submit_time
                previous_stamp = submit_time
            acc_submitted_work += core_h

            # 当超过采样间隔时计算平均值
            if submit_time - previous_stamp > acc_period:
                # 计算单位时间内的平均提交工作量
                corrected_acc = (float(acc_submitted_work) /
                                 float(submit_time - first_time_stamp))
                submitted_core_h_per_min[submit_time] = corrected_acc
                # acc_submitted_work=0
                previous_stamp = submit_time
            # 处理等待队列变化事件
            if submit_time != 0:
                if not submit_time in wait_ch_events.keys():
                    wait_ch_events[submit_time] = 0
                wait_ch_events[submit_time] += core_h
            if start_time != 0:
                if not start_time in wait_ch_events.keys():
                    wait_ch_events[start_time] = 0
                wait_ch_events[start_time] -= core_h

        # 生成等待队列时间序列数据
        acc_ch = 0  # 累计等待核心秒数
        stamps = []  # 时间戳序列
        waiting_ch = []  # 对应时间点的等待量

        # 按时间顺序处理所有事件点
        for stamp in sorted(wait_ch_events.keys()):
            acc_ch += wait_ch_events[stamp]
            stamps.append(stamp)
            waiting_ch.append(acc_ch)

        # Making sure stamps are orderd for the produced work.
        # 整理提交工作量数据（按时间排序）
        core_h_per_min_stamps = []
        core_h_per_min_values = []
        previous_stamp = None
        for stamp in sorted(submitted_core_h_per_min.keys()):
            core_h_per_min_stamps.append(stamp)
            core_h_per_min_values.append(submitted_core_h_per_min[stamp])
        return stamps, waiting_ch, core_h_per_min_stamps, core_h_per_min_values

    def _get_utilization_result(self):
        """
        创建并返回表示利用率计算结果的NumericList对象
        该对象包含以下数据字段：
        - 'utilization'：实际利用率数值
        - 'waste'：资源浪费指标
        - 'corrected_utilization'：校正后的利用率值
        返回：
            NumericList: 封装使用率相关数值的容器对象，名称为'usage_values'，
                        包含三个预定义字段
        """
        return NumericList("usage_values", ["utilization", "waste",
                                            "corrected_utilization"])

    def get_utilization_values(self):
        """获取系统资源利用率的综合指标
        Returns:
            tuple: 包含三个元素的元组，按顺序返回:
                - float: 综合利用率指标，标准化后的0-1范围浮点数
                - int: 累计浪费的核心小时数，来自所有单工作流作业的总和
                - float: 校正后的综合利用率指标，经过调整的0-1范围浮点数
        """
        return (self._integrated_ut, self._acc_waste,
                self._corrected_integrated_ut)

    def load_utilization_results(self, db_obj, trace_id):
        """创建利用率结果对象并填充相关数据
        通过数据库管理器获取指定trace的利用率数据，解析并存储核心指标到实例属性。
        主要处理三个关键指标：资源浪费量、综合利用率、修正综合利用率。
        Args:
            db_obj (DBManager): 数据库管理对象，用于执行数据库查询操作
                                - 需要实现load方法用于加载指定类型的数据
            trace_id (int): 唯一标识符，用于定位特定追踪记录的数据
                            - 对应数据库中的trace主键ID
        流程说明:
            1. 初始化空的利用率结果容器
            2. 从数据库加载'usage'类型的原始数据
            3. 从结果集中提取关键指标存储到实例变量
        """
        # 初始化利用率结果对象作为数据容器
        res = self._get_utilization_result()
        res.load(db_obj, trace_id, "usage")
        self._acc_waste = res.get_data()["waste"]
        self._integrated_ut = res.get_data()["utilization"]
        self._corrected_integrated_ut = res.get_data()["corrected_utilization"]

    def calculate_utilization_median_result(self, trace_id_list, store, db_obj,
                                            trace_id):
        """计算并存储跟踪列表中的中间利用率和浪费值。
        Args:
            trace_id_list (list): 子追踪ID列表，用于获取多个子追踪的利用率数据
            store (bool): 是否将计算结果存储到数据库的标志
            db_obj: 数据库连接对象，用于加载和存储数据
            trace_id: 主追踪ID，用于关联最终存储的计算结果
            self: 隐式参数，用于存储计算结果的对象实例
        Returns:
            None: 结果直接存储在实例属性中，无显式返回值
        """
        # 初始化三个列表用于收集不同维度的利用率数据
        integrated_values = []
        wasted_values = []
        corrected_integrated_values = []

        # 遍历所有子追踪ID，加载每个追踪的利用率结果
        for sub_trace_id in trace_id_list:
            rt = ResultTrace()
            rt.load_utilization_results(db_obj, sub_trace_id)
            integrated_values.append(rt._integrated_ut)
            wasted_values.append(rt._acc_waste)
            corrected_integrated_values.append(rt._corrected_integrated_ut)

        # 计算所有子追踪数据的中位数
        self._acc_waste = np.median(wasted_values)
        self._integrated_ut = np.median(integrated_values)
        self._corrected_integrated_ut = np.median(corrected_integrated_values)

        # 根据存储标志将结果持久化到数据库
        if store:
            res = self._get_utilization_result()
            res.set_dic(dict(utilization=self._integrated_ut,
                             waste=self._acc_waste,
                             corrected_utilization=self._corrected_integrated_ut
                             ))
            res.store(db_obj, trace_id, "usage")

    def calculate_utilization_mean_result(self, trace_id_list, store, db_obj,
                                          trace_id):
        """计算并存储跟踪列表中的中间利用率和浪费值。
            和上面函数一模一样
         """
        integrated_values = []
        wasted_values = []
        corrected_integrated_values = []
        for sub_trace_id in trace_id_list:
            rt = ResultTrace()
            rt.load_utilization_results(db_obj, sub_trace_id)
            integrated_values.append(rt._integrated_ut)
            wasted_values.append(rt._acc_waste)
            corrected_integrated_values.append(rt._corrected_integrated_ut)
        self._acc_waste = np.sum(wasted_values)
        self._integrated_ut = np.mean(integrated_values)
        self._corrected_integrated_ut = np.mean(corrected_integrated_values)
        if store:
            res = self._get_utilization_result()
            res.set_dic(dict(utilization=self._integrated_ut,
                             waste=self._acc_waste,
                             corrected_utilization=self._corrected_integrated_ut
                             ))
            res.store(db_obj, trace_id, "usage_mean")

    def create_trace_table(self, db_obj, table_name):
        """ For testing """
        query = """
           CREATE TABLE `{0}` (
           `trace_id` INT(10) NOT NULL,
          `job_db_inx` int(11) NOT NULL,
          `account` tinytext,
          `cpus_req` int(10) unsigned NOT NULL,
          `cpus_alloc` int(10) unsigned NOT NULL,
          `job_name` tinytext NOT NULL,
          `id_job` int(10) unsigned NOT NULL,
          `id_qos` int(10) unsigned NOT NULL DEFAULT '0',
          `id_resv` int(10) unsigned NOT NULL,
          `id_user` int(10) unsigned NOT NULL,
          `nodes_alloc` int(10) unsigned NOT NULL,
          `partition` tinytext NOT NULL,
          `priority` int(10) unsigned NOT NULL,
          `state` smallint(5) unsigned NOT NULL,
          `timelimit` int(10) unsigned NOT NULL DEFAULT '0',
          `time_submit` int(10) unsigned NOT NULL DEFAULT '0',
          `time_start` int(10) unsigned NOT NULL DEFAULT '0',
          `time_end` int(10) unsigned NOT NULL DEFAULT '0',
          PRIMARY KEY (`trace_id`, `id_job`),
          UNIQUE KEY `main_key` (`trace_id`,`id_job`))
         """.format(table_name)
        db_obj.doUpdate(query)

    def create_import_table(self, db_obj, table_name):
        """ For testing """
        query = """
           CREATE TABLE `{0}` (
          `job_db_inx` int(11) NOT NULL,
          `account` tinytext,
          `cpus_req` int(10) unsigned NOT NULL,
          `cpus_alloc` int(10) unsigned NOT NULL,
          `job_name` tinytext NOT NULL,
          `id_job` int(10) unsigned NOT NULL,
          `id_qos` int(10) unsigned NOT NULL DEFAULT '0',
          `id_resv` int(10) unsigned NOT NULL,
          `id_user` int(10) unsigned NOT NULL,
          `nodes_alloc` int(10) unsigned NOT NULL,
          `partition` tinytext NOT NULL,
          `priority` int(10) unsigned NOT NULL,
          `state` smallint(5) unsigned NOT NULL,
          `timelimit` int(10) unsigned NOT NULL DEFAULT '0',
          `time_submit` int(10) unsigned NOT NULL DEFAULT '0',
          `time_start` int(10) unsigned NOT NULL DEFAULT '0',
          `time_end` int(10) unsigned NOT NULL DEFAULT '0',
          PRIMARY KEY (`job_db_inx`),
          UNIQUE KEY `job_db_inx` (`job_db_inx`))
         """.format(table_name)
        db_obj.doUpdate(query)


def _get_limit(order_field, start=None, end=None):
    """
    生成SQL条件表达式，用于限定排序字段的范围
    参数：
        order_field (str): 需要限制范围的数据库字段名
        start (int/float/str, optional): 范围下限值，包含该值。默认为None
        end (int/float/str, optional): 范围上限值，包含该值。默认为None
    返回值：
        str/None: 返回构建的SQL条件字符串。当start和end均为None时返回None
    示例：
        >>> _get_limit('price', 100, 200)
        'price>=100 AND price<=200'
    """
    query = ""
    # 处理未指定范围的情况
    if start is None and end is None:
        return None
    # 构建下限条件
    if start:
        query += "{0}>={1}".format(order_field, start)
    # 构建上限条件并处理AND连接符
    if end:
        if start:
            query += " AND "
        query += "{0}<={1}".format(order_field, end)
    return query
