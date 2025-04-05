from machines import Edison2015,Edison
from datetime import datetime
from generate import TimeController
from stats.trace import ResultTrace
from stats import Histogram, NumericStats

class ExperimentDefinition(object):
    """
    该类包含单个调度实验的定义和状态。该定义用于生成工作负载和配置调度器。
    状态跟踪实验：如果它已经运行，它的输出分析，等等。它允许从数据库中加载和存储数据。
    """
    
    def __init__(self,
                 name=None,
                 experiment_set=None,
                 seed="AAAAAA",
                 machine="edison",
                 trace_type="single",
                 manifest_list=None,
                 workflow_policy="no",
                 workflow_period_s=0,
                 workflow_share=0.0,
                 workflow_handling="manifest",
                 subtraces = None,
                 start_date = datetime(2015,1,1),
                 preload_time_s = 3600*24*2,
                 workload_duration_s = 3600*24*7,
                 work_state = "fresh",
                 analysis_state = "0",
                 overload_target=0.0,
                 table_name="experiment",
                 conf_file=""):
        """构造函数允许完全配置一个实验。但是，可以使用默认值创建Definition，然后从数据库加载。
        Args:
        - name: 带有人类可读的实验描述的字符串。如果未设置，则将name设置为从其余参数派生的值。
        - experiment_set: 标识这个实验所属的实验组的字符串。如果未设置，则从实验参数构建。
        - machine: 标识调度仿真必须在硬件和优先级策略方面建模的系统的字符串.
        - trace_type: 字符串，有三个值“single”， “delta”， “group”。单个实验是对工作负载进行分析的一次运行。
            增量是两个单一轨迹（列在子轨迹中）中工作流的比较，而组实验汇总了许多单个实验的结果（列在子轨迹中）。
        - manifest_list: 字典列表。每个字典有两个键：“manifest”，其值列出工作流类型的清单文件的名称；
            并且“共享”一个0-1的值，该值指示工作流在工作负载中具有“显示”类型的机会。
        - workflow_policy: 字符串，该字符串控制如何计算要添加到工作负载中的工作流。可以取三个值“no”，表示没有工作流；
            “period”，每workflow_period_s秒一个工作流；而“share”，workflow_share %的作业将是工作流。
        - workflow_period:正数，指示工作负载中两个工作流之间需要多少秒。
        - workflow_share: 浮动在0到100之间，表示属于工作流的作业的百分比。
        - workflow_handling: 控制如何在实验中运行和调度工作流的字符串。
            它可以接受三个值：“single”，其中工作流作为单个作业提交；“multi”，工作流中的每个任务都在独立的作业中运行；
            以及“manifest”，其中工作流作为单个作业提交，但使用了感知工作流的回填.
        - subtraces: 本实验分析中应使用的迹线的trace_id （int）列表。只对delta和群实验有效。
        - start_date: Datetime对象指向生成的工作负载的开头。
        - pre_load_time_s:在start_date之前生成的工作负载秒数。此工作负载用于“加载”调度器，但是分析将仅从“start_date”开始执行。
        - workload_duration_s: 在start_date之后生成的工作负载秒数。
        - work_state: 表示实验状态的字符串。取值： "fresh", "pre_simulating", "simulating",
            "simulation_done", "simulation_error", "pre_analyzing",
            "analyzing", "analysis_done", "analysis_error".
        - analysis_state: 分析阶段的子步骤。
        - overload_target: 如果设置为> 1.0，则在预加载期间生成的工作负载将产生额外的作业，
                因此在一段时间内，将提交overload_target乘以系统的容量（在该期间产生的）。
        - table_name: 用于存储和加载实验内容的数据库表。
        - conf_file: 如果设置为sting，则实验将使用该名称的配置文件运行。其他设置将被覆盖。
        """
        if subtraces is None:
            subtraces = []
        if manifest_list is None:
            manifest_list = []
        self._name=name
        self._experiment_set=experiment_set
        self._seed=seed
        self._machine=machine
        self._trace_type=trace_type
        self._manifest_list=manifest_list
        self._workflow_policy=workflow_policy
        self._workflow_period_s=workflow_period_s
        self._workflow_share=workflow_share
        self._workflow_handling=workflow_handling
        self._subtraces = subtraces
        self._start_date = start_date
        self._preload_time_s = preload_time_s
        self._workload_duration_s = workload_duration_s
        self._work_state = work_state
        self._analysis_state = analysis_state
        self._overload_target = overload_target
        self._table_name = table_name
        self._conf_file = conf_file
        
        self._simulating_start=None
        self._simulating_end=None
        self._worker=""
        
        for man in [x["manifest"] for x in manifest_list]:
            if "_" in man or "_" in man:
                raise ValueError("A manifest name cannot contain the characters"
                                 " '_' or '-', found: {0}".format(man))
            

        self._trace_id = None
        self._owner = None
        self._ownership_stamp = None
        
        if self._experiment_set is None:
            self._experiment_set  = self._get_default_experiment_set()
        if self._name is None:
            self._name = "{0}-s[{1}]".format(self._experiment_set, self._seed)
    def get_true_workflow_handling(self):
        """
        根据工作流策略获取实际的工作流处理方式
        返回值:
            str: 当工作流策略(_workflow_policy)设置为"no"时返回"no"，
            否则返回预配置的工作流处理方式(_workflow_handling)
        """
        # 优先检查工作流策略是否为禁用状态
        if self._workflow_policy=="no":
            return "no"
        else:
            # 返回配置的实际工作流处理方式
            return self._workflow_handling

    def get_machine(self):
        """
        返回与所配置的机器相对应的Machine对象。
        """
        if self._machine == "edison":
            return Edison2015()
        elif self._machine == "default":
            return Edison()
        raise ValueError("Unknown machine set: {}".format(self._machine))

    def get_overload_factor(self):
        """
        获取当前系统的过载系数

        通过计算_overload_target属性与1000取模运算的结果，反映系统负载的基准值。
        该系数通常用于评估系统负载状态或进行资源分配决策。

        Returns:
            int: 范围在0-999之间的整数，表示当前系统的基准过载系数。数值越大表示负载压力越大
        """
        return self._overload_target % 1000;

    def get_forced_initial_wait(self):
        """
        获取强制初始等待时间

        根据_overload_target的数值决定返回结果：
        - 当_overload_target超过阈值时返回计算后的运行时间
        - 否则返回默认值0

        Returns:
            float: 计算后的运行时间（_overload_target/1000），当_overload_target>999时生效；
            否则固定返回0
        """
        # 处理高负载目标值的等待时间计算逻辑
        if self._overload_target > 999:
            # 将千级单位的数值转换为秒级单位
            runtime = self._overload_target / 1000
            return runtime
        return 0

    def get_system_user_list(self):
        return ["tester:1000",
                "root:0"
                "linpack:300",
                "nobody:99",
                "dbus:81",
                "rpc:32",
                "nscd:28",
                "vcsa:69",
                "abrt:499",
                "saslauth:498",
                "postfix:89",
                "apache:48",
                "rpcuser:29",
                "nfsnobody:65534",
                "ricci:140",
                "haldaemon:68",
                "nslcd:65",
                "ntp:38",
                "piranha:60",
                "sshd:74",
                "luci:141",
                "tcpdump:72",
                "oprofile:16",
                "postgres:26",
                "usbmuxd:113",
                "avahi:70",
                "avahi-autoipd:170",
                "rtkit:497",
                "pulse:496",
                "gdm:42",
                "named:25",
                "snmptt:495",
                "hacluster:494",
                "munge:493",
                "mysql:27",
                "bsmuser:400",
                "puppet:52",
                "nagios:401",
                "slurm:106"
                ]
    def get_user_list(self):
        """
        Returns a list of strings with the usernames to be emulated.
        """
        return ["user1"]
    def get_qos_list(self):
        """
        Returns a list of the qos policies to be used in the workload.
        """
        return ["qos1"]
    def get_partition_list(self):
        """
        Returns a list of the partitions to be used in the workload.
        """
        return ["main"]
    def get_account_list(self):
        """
        Returns a list of accounts ot be used in the workload.
        """
        return ["account1"] 
    
    def get_trace_file_name(self):
        """
        Returns a file system safe name based on the experiment name for its
        workload file.
        """
        return self.clean_file_name(self._name+".trace")
    def get_qos_file_name(self):
        """
        Returns a file system safe name based on the experiment name for its
        qos file.
        """
        return self.clean_file_name(self._name+".qos")
    def get_users_file_name(self):
        """
        Returns a file system safe name based on the experiment name for its
        users file.
        """
        return self.clean_file_name(self._name+".users")
    def get_start_epoch(self):
        """
        Returns the start date of the experiment in epoch format (int).
        """
        return TimeController.get_epoch(self._start_date)
    
    def get_end_epoch(self):
        """
        Returns the ending date of the experiment in epoch format (int).
        """
        return (TimeController.get_epoch(self._start_date) + 
                self._workload_duration_s)
    
    def clean_file_name(self, file_name):
        """Returns a string with a file-system name verions of file_name."""
        return "".join([c for c in file_name if c.isalpha() 
                                             or c.isdigit()
                                             or c=='.'
                                             or c=="-"]).rstrip()
        
    def _manifest_list_to_text(self, manifest_list):
        """将清单列表序列化为字符串"""
        list_of_text=[]
        for one_man in manifest_list:
            list_of_text.append("{0}|{1}".format(
                                             one_man["share"],
                                             one_man["manifest"]))
        
        return ",".join(list_of_text) 
    def _text_to_manifest_list(self, manifest_text):
        """将字符串反序列化为清单列表"""
        manifest_list = []
        for man in manifest_text.split(","):
            if man == "":
                continue
            man_parts  = man.split("|")
            man_share = float(man_parts[0])
            man_file = man_parts[1]
            manifest_list.append({"share":man_share, "manifest":man_file})
        return manifest_list 
    
    def _get_default_experiment_set(self):  
        """Returns the default experiment set based on the experiment
        configuration."""
        conf_file_str=""
        if self._conf_file:
            conf_file_str="-"+self._conf_file
        return ("{0}-{1}-m[{2}]-{3}-p{4}-%{5}-{6}-t[{7}]-{8}d-{9}d-O{10}{11}"
            "".format(
            self._machine,
            self._trace_type,
            self._manifest_list_to_text(self._manifest_list),
            self._workflow_policy,
            self._workflow_period_s, 
            self._workflow_share, 
            self._workflow_handling,
            ",".join([str(t) for t in self._subtraces]),
            int(self._preload_time_s/(3600*24)),
            int(self._workload_duration_s/(3600*24)),
            self._overload_target,
            conf_file_str))
    
    def store(self, db_obj):
        """将对象存储到数据库中的self._table_name表中.
        Args:
        - db_obj: 已配置的DBManager对象，用于存储数据。
        Returns trace_id
        """
        keys= ["name",
                "experiment_set",
                "seed",
                "machine",
                "trace_type",
                "manifest_list",
                "workflow_policy",
                "workflow_period_s",
                "workflow_share",
                "workflow_handling",
                "subtraces", 
                "start_date",
                "preload_time_s", 
                "workload_duration_s", 
                "work_state", 
                "analysis_state",
                "overload_target",
                "conf_file"]
        values = [self._name,
                    self._experiment_set,
                    self._seed,
                    self._machine,
                    self._trace_type,
                    self._manifest_list_to_text(self._manifest_list),
                    self._workflow_policy,
                    self._workflow_period_s,
                    self._workflow_share,
                    self._workflow_handling,
                    ",".join([str(t) for t in self._subtraces]),
                    db_obj.date_to_mysql(self._start_date),
                    self._preload_time_s, 
                    self._workload_duration_s, 
                    self._work_state,
                    self._analysis_state,
                    self._overload_target,
                    self._conf_file]
        
        ok, insert_id = db_obj.insertValues(self._table_name, keys, values,
                                        get_insert_id=True)
        if not ok:
            raise Exception("Error inserting experiment in database: {0}"
                            "".format(values))
        self._trace_id = insert_id
        return self._trace_id
    
    def mark_pre_simulating(self, db_obj):
        return self.upate_state(db_obj, "pre_simulating")
    
    def mark_simulating(self, db_obj, worker_host=None):
        """
        标记模拟状态。

        该方法主要用于更新数据库对象的状态为“simulating”（模拟中）。如果指定了worker_host，则先更新工作主机信息。

        参数:
        - db_obj: 数据库对象，表示要更新的数据库条目。
        - worker_host: 可选参数，表示当前执行任务的工作主机。

        返回:
        - 返回更新状态操作的结果。
        """
        # 如果worker_host被指定，则调用update_worker方法更新工作主机信息
        if worker_host:
            self.update_worker(db_obj,worker_host)
        # 调用update_simulating_start方法更新模拟开始的相关信息
        self.update_simulating_start(db_obj)

        # 调用upate_state方法更新数据库对象的状态为"simulating"，并返回操作结果
        return self.upate_state(db_obj, "simulating")
        
    def mark_simulation_done(self, db_obj):
        self.update_simulating_end(db_obj)
        return self.upate_state(db_obj, "simulation_done")
        
    def mark_simulation_failed(self, db_obj):
        self.update_simulating_end(db_obj)
        return self.upate_state(db_obj, "simulation_failed")
    
    def mark_pre_analyzing(self, db_obj):
        return self.upate_state(db_obj, "pre_analyzing")
    
    def mark_analysis_done(self, db_obj):
        return self.upate_state(db_obj, "analysis_done")

    def mark_second_pass(self, db_obj):
        return self.upate_state(db_obj, "second_pass_done")

    def mark_pre_second_pass(self, db_obj):
        return self.upate_state(db_obj, "pre_second_pass")
        
    def upate_state(self, db_obj, state):
        """
        Sets the state of the experiment. 
        """
        old_state=self._work_state
        self._work_state = state
        return db_obj.setFieldOnTable(self._table_name, "work_state", state,
                               "trace_id", str(self._trace_id), 
                               "and work_state='{0}'".format(old_state))
    def update_worker(self, db_obj, worker_host):
        self._worker=worker_host
        return db_obj.setFieldOnTable(self._table_name, "worker", worker_host,
                               "trace_id", str(self._trace_id))
        
    def update_simulating_start(self, db_obj):
        return db_obj.setFieldOnTable(self._table_name, "simulating_start",
                                      "now()",
                                      "trace_id", str(self._trace_id),
                                      no_commas=True)
    
    def update_simulating_end(self, db_obj):
        return db_obj.setFieldOnTable(self._table_name, "simulating_end",
                                      "now()",
                                      "trace_id", str(self._trace_id),
                                      no_commas=True)
    
    def reset_simulating_time(self, db_obj):
        db_obj.setFieldOnTable(self._table_name, "simulating_end",
                                      0,
                                      "trace_id", str(self._trace_id),
                                      no_commas=True)
        return db_obj.setFieldOnTable(self._table_name, "simulating_start",
                                      0,
                                      "trace_id", str(self._trace_id),
                                      no_commas=True)

    def load(self, db_obj, trace_id):
        """根据trace_id从数据库加载实验配置数据到当前对象
        通过指定的数据库连接对象查询指定trace_id对应的实验配置，将查询结果
        设置到当前对象的对应属性中。包含数据有效性检查和数据格式转换。
        Args:
            db_obj (DBManager): 已配置的数据库管理器对象，用于执行数据库查询操作
                - 需要实现getValuesDicList方法
                trace_id (int): 实验数据的唯一标识符
                - 对应数据库表中的trace_id字段

        Raises:
            ValueError: 当指定trace_id不存在于数据库时抛出

        Process:
            1. 执行数据库查询获取原始数据
            2. 验证数据有效性
            3. 设置对象属性
            4. 数据格式转换处理
        """
        self._trace_id = trace_id
        # 定义需要从数据库获取的字段列表（对应数据库表列名）
        keys = ["name",
                "experiment_set",
                "seed",
                "machine",
                "trace_type",
                "manifest_list",
                "workflow_policy",
                "workflow_period_s",
                "workflow_share",
                "workflow_handling",
                "subtraces",
                "start_date",
                "preload_time_s",
                "workload_duration_s",
                "work_state",
                "analysis_state",
                "overload_target",
                "conf_file",
                "simulating_start",
                "simulating_end",
                "worker"]

        # 执行数据库查询（单条记录查询）
        data_dic = db_obj.getValuesDicList(self._table_name, keys, condition=
        "trace_id={0}".format(
            self._trace_id))
        # 有效性检查：确保查询到有效数据
        if data_dic == False:
            raise ValueError("Experiment not found!")

        # 批量设置对象属性（自动添加下划线前缀）
        for key in keys:
            setattr(self, "_" + key, data_dic[0][key])

        # 特殊字段格式转换
        # 将存储的文本格式清单转换为结构化数据
        self._manifest_list = self._text_to_manifest_list(self._manifest_list)
        # 将逗号分隔的字符串转换为整数列表（过滤空值）
        self._subtraces = [int(x) for x in self._subtraces.split(",") if x != ""]

    def load_fresh(self, db_obj):
        """加载并激活首个'fresh'状态的实验
        从数据库中按trace_id排序查找首个状态为"fresh"的实验记录。若找到，
        则加载该实验配置并将状态更新为"pre_simulating"。
        Parameters:
            db_obj (object): 数据库连接对象，用于执行查询和更新操作
        Returns:
            bool:
                True - 成功加载并更新实验状态
                False - 未找到符合条件的'fresh'状态实验
        Note:
            本方法是load_next_state()的封装，固定使用"fresh"作为源状态，
            "pre_simulating"作为目标状态的状态转移操作
        """
        return self.load_next_state(db_obj, "fresh", "pre_simulating")

    def load_pending(self, db_obj):
        """从数据库加载首个待处理实验并更新其状态为预分析状态
        该方法会在数据库中查找首个状态为"simulation_done"的实验记录（按trace_id排序），
        加载其配置数据到当前对象，并将该实验状态更新为"pre_analyzing"
        Args:
            db_obj (object): 数据库连接对象
                - 需包含与数据库交互的方法，用于执行查询和更新操作
        Returns:
            bool:
                - True: 成功加载并更新符合条件的实验状态
                - False: 未找到处于"simulation_done"状态的实验记录
        Note:
            原始docstring中提到的返回条件检查"fresh"状态，这与实际实现中检查"simulation_done"状态
            存在矛盾，建议确认状态流转逻辑的正确性
        """
        return self.load_next_state(db_obj, "simulation_done", "pre_analyzing")


    def load_ready_second_pass(self, db_obj):
        """Configures the object with the data of the first experiment with
        state="simulation_done", ordered by trace_id. Then set the state to 
        "pre_analyzing".
        
        Returns True if load was succesful, False if no experiments with state
            "fresh" are available.
        """
        return self.load_next_state(db_obj, "simulation_done", "pre_analyzing")

    def load_next_state(self, db_obj, state, new_state, check_pending=False,
                        subtraces_state=None):
        """原子化加载并转换实验状态的核心方法
        本方法实现了一个带事务重试机制的状态转移流程，用于安全地将符合条件的实验从当前状态转换到新状态。
        支持子轨迹状态验证，确保并发操作不会处理同一实验。
        参数:
            db_obj (DBManager): 数据库管理器实例，用于事务操作
            state (str): 要查询的当前实验状态
            new_state (str): 要转换的目标状态
            check_pending (bool, 可选): 是否验证子轨迹状态，默认关闭
            subtraces_state (str, 可选): 当check_pending启用时要求的子轨迹状态
        返回:
            bool: 如果查询状态仍有未处理实验返回True，否则返回False
        实现特性:
            - 事务隔离: 通过数据库事务保证状态转换的原子性
            - 重试机制: 最多1000次重试以避免死锁
            - 状态验证: 可选子轨迹状态校验保证依赖完整性
        """
        update_ok = False
        data_left = True
        count = 1000  # 事务重试计数器

        # 主事务循环
        while data_left and not update_ok:
            db_obj.start_transaction()

            # 查询目标状态的实验轨迹ID
            rows = db_obj.getValuesAsColumns(self._table_name, ["trace_id"],
                                             condition="work_state='{0}' "
                                                       "and trace_type='{1}' ".format(
                                                 state,
                                                 self._trace_type),
                                             orderBy="trace_id")

            data_left = len(rows["trace_id"]) > 0  # 检查剩余数据

            if data_left:
                found_good = False
                # 遍历候选轨迹
                for trace_id in rows["trace_id"]:
                    self.load(db_obj, int(trace_id))

                    # 执行子轨迹状态验证
                    found_good = (not check_pending
                                  or self.are_sub_traces_analyzed(
                                db_obj,
                                subtraces_state))
                    if found_good:
                        break

                # 未找到有效候选时退出
                if not found_good:
                    db_obj.end_transaction()
                    break

                # 尝试状态更新
                update_ok = self.upate_state(db_obj, new_state)

            db_obj.end_transaction()

            # 防止无限循环的安全机制
            if count == 0:
                raise Exception("状态转换尝试次数超过安全限制(1000次)")
            count -= 1
        return data_left

    def get_exps_in_state(self, db_obj, state):
        rows=db_obj.getValuesAsColumns(self._table_name, ["trace_id"], 
                             condition = "work_state='{0}' "
                                         "and trace_type='{1}' ".format(
                                                           state, 
                                                           self._trace_type),
                             orderBy="trace_id")
        return rows["trace_id"]
    def pass_other_second_pass_requirements(self, db_obj):
        return True

    def load_next_ready_for_pass(self, db_obj, state="analysis_done",
                                 new_state="pre_second_pass",
                                 workflow_handling="manifest",
                                 workflow_handling_list=["single", "multi"]):
        """尝试加载下一个准备好进入下一处理阶段的实验配置并更新其状态

        Args:
            db_obj: 数据库连接对象
            state: 当前要求的工作状态（默认'analysis_done'）
            new_state: 要更新到的新状态（默认'pre_second_pass'）
            workflow_handling: 工作流处理模式（默认'manifest'）
            workflow_handling_list: 其他需要检查的工作流处理模式列表（默认['single','multi']）

        Returns:
            bool: 表示是否还有剩余未处理的数据

        功能说明:
            1. 在事务中循环查找符合要求的实验配置
            2. 检查关联的其他工作流处理是否完成
            3. 满足条件时更新状态并退出循环
            4. 包含防止无限循环的安全计数器
        """
        update_ok = False
        data_left = True
        count = 100

        """ Changes:
            - it passes over the ones that not good yet
            - does not use subtraces
        """
        # 主处理循环：在事务中尝试获取并处理符合条件的配置
        while data_left and not update_ok:
            db_obj.start_transaction()

            # 获取当前状态符合要求的trace_id列表
            rows = db_obj.getValuesAsColumns(self._table_name, ["trace_id"],
                                             condition="work_state='{0}' "
                                                       "and trace_type='{1}' "
                                                       "and workflow_handling='{2}'".format(
                                                 state,
                                                 self._trace_type,
                                                 workflow_handling),
                                             orderBy="trace_id")

            data_left = len(rows["trace_id"]) > 0
            this_is_the_one = False

            # 遍历找到的trace_id尝试处理
            if data_left:
                for trace_id in rows["trace_id"]:
                    self.load(db_obj, int(trace_id))
                    other_defs_ok = True

                    # 检查所有关联工作流处理是否就绪
                    for (other_handling, t_id) in zip(
                            workflow_handling_list,
                            [trace_id + x + 1 for x in range(
                                len(workflow_handling_list))]):
                        new_def = self.get_exp_def_like_me()
                        new_def.load(db_obj, t_id)
                        other_defs_ok = (other_defs_ok and
                                         new_def._work_state == "analysis_done" and
                                         new_def._workflow_handling == other_handling and
                                         new_def.pass_other_second_pass_requirements(db_obj))

                    # 满足所有条件时标记为候选
                    if (not other_defs_ok or
                            not self.pass_other_second_pass_requirements(db_obj)):
                        continue
                    else:
                        this_is_the_one = True
                        break

                # 成功找到候选配置时尝试更新状态
                if this_is_the_one:
                    update_ok = self.upate_state(db_obj, new_state)

            db_obj.end_transaction()

            # 安全计数器防止无限循环
            if count == 0:
                raise ValueError("Tried to load an experiment configuration many"
                                 " times and failed!!")
            count -= 1

        return data_left

    def get_exp_def_like_me(self):
        return ExperimentDefinition()
    def del_results(self, db_obj):
        """Deletes all analysis results associated with this experiment"""
        field="trace_id"
        value=self._trace_id
        db_obj.delete_rows(Histogram()._table_name, field, value)
        db_obj.delete_rows(ResultTrace()._get_utilization_result()._table_name,
                            field, value)
        db_obj.delete_rows(NumericStats()._table_name, field, value)
    
    def del_results_like(self, db_obj, like_field="type", like_value="lim_%"):
        """Deletes all analysis results associated with this experiment"""
        field="trace_id"
        value=self._trace_id
        db_obj.delete_rows(Histogram()._table_name, field, value,
                           like_field, like_value)
        db_obj.delete_rows(ResultTrace()._get_utilization_result()._table_name,
                            field, value, like_field, like_value)
        db_obj.delete_rows(NumericStats()._table_name, field, value,
                           like_field, like_value)
        
    def del_trace(self, db_obj):
        """Deletes simulation trace associated with this experiment"""
        field="trace_id"
        value=self._trace_id
        db_obj.delete_rows(ResultTrace()._table_name,
                            field, value)
    
    def del_exp(self, db_obj):
        field="trace_id"
        value=self._trace_id
        db_obj.delete_rows(self._table_name,
                            field, value)

    def are_sub_traces_analyzed(self, db_obj, state):
        """检查所有子追踪是否处于指定状态

        Args:
            db_obj: 数据库连接对象，需实现getValuesAsColumns方法
            state: 单值或列表，要求子追踪必须处于的状态值集合。支持单个状态值或状态列表
                   (会自动转换为列表处理)

        Returns:
            bool: 当所有子追踪的work_state都在state集合中返回True，否则返回False

        Raises:
            ValueError: 当在数据库中找不到对应的子追踪记录时抛出
        """
        # 将单个状态值统一转换为列表结构处理
        if not type(state) is list:
            state = [state]

        # 遍历所有子追踪ID进行状态验证
        for trace_id in self._subtraces:
            # 从数据库获取指定追踪ID的工作状态
            rows = db_obj.getValuesAsColumns(self._table_name, ["work_state"],
                                             condition="trace_id={0} ".format(trace_id))

            # 校验查询结果有效性
            if len(rows["work_state"]) == 0:
                raise ValueError("Subtrace not found!")

            # 判断子追踪状态是否符合要求
            if not rows["work_state"][0] in state:
                return False

        return True

    def create_table(self, db_obj):
        """创建用于存储Definition对象的有效表"""
        print ("Experiment table creation will fail if MYSQL Database does not"
               " support 'zero' values in timestamp fields. To zero values"
               " can be allowed by removing STRICT_TRANS_TABLES from 'sql_mode='"
               " in my.cnf."
               "")
        query = """
        create table `experiment` (
            `trace_id` int(10)  NOT NULL AUTO_INCREMENT,
            `name` varchar(512),
            `experiment_set` varchar(512),
            `seed` varchar(256),        # Alphanum seed for workload gen.
            `trace_type` varchar(64),   # single, delta, group
            `machine` varchar(64),      # Machine to simulate, e.g. 'edison'
            `manifest_list` varchar (1024), # Manifests to use in the trace. Format:
                                            # [{"manifest1.json":1.0}] or
                                            # [{"manifest1.json":0.5},{"manifest1.json":0.5}]
            `workflow_policy` varchar(1024),          # workflow submission policy:
                                                    #    'no', 'period', 'percentage'
            `workflow_period_s` INT DEFAULT 0,         # used in "period" policy.
                                                    #   seconds between two worflows.
            `workflow_share` DOUBLE DEFAULT 0.0,    # used in "percentage" policy
                                                    #    0-100% share of workflows over
                                                    #    jobs
            `workflow_handling` varchar(64),    # How workflows are submitted and
                                                #      scheduled: 'single', 'multi',
                                                #    'backfill'
            `start_date` datetime,       # epoch date where the trace should start
            `preload_time_s` int,         # lenght (in s.) to create filling workload at the
                                        # begining. It won't be analyzed.        
            `workload_duration_s` int,  # lenght (in s.) of the workload to be generated
            `subtraces` varchar(100),   # For the group and delta traces, what traces
                                        #     where used to build this one.
            `work_state` varchar(64),   # State of the simulation, analysis steps:
                                        #     'fresh', 'simulating', 'simulation done',
                                        #    'analyzing', 'analysis done'
            `analysis_state` varchar(64) DEFAULT "",  # States inside of the simulation. depending on
                                            #     trace_type and workflow_policy
            `owner` varchar(64) DEFAULT "",            # IP of the host that did the last update
            `conf_file` varchar(64) DEFAULT "", # Name of config file to be used in experiment
            `ownership_stamp` datetime,        # Time since last ownership.
            `overload_target` DOUBLE DEFAULT 1.1,   # Target cores-hours to be submitted
            `simulating_start` timestamp DEFAULT 0,
            `simulating_end` timestamp DEFAULT 0,
            `worker` varchar(256) DEFAULT "",
            PRIMARY KEY(`trace_id`)
            ) ENGINE = InnoDB;
        """
        db_obj.doUpdate(query)
    def is_it_ready_to_process(self):
        return self._work_state in ["analysis_done"]
    
    def is_analysis_done(self, second_pass=False):
        if second_pass:
            return self._work_state =="second_pass_done"
        return (self._work_state =="analysis_done" or
                self._work_state =="second_pass_done")
    
                                    
    
        
class GroupExperimentDefinition(ExperimentDefinition):
    """分组实验定义：由多个具有相同调度程序和工作负载特征，但随机种子不同的单个实验组成的实验。
    计算工作流和作业变量的统计信息，将所有跟踪信息放在一起。中位数是根据利用率计算的。
    """
    def __init__(self,
                 name=None,
                 experiment_set=None,
                 seed="AAAAAA",
                 machine="edison",
                 trace_type="group",
                 manifest_list=None,
                 workflow_policy="no",
                 workflow_period_s=0,
                 workflow_share=0.0,
                 workflow_handling="manifest",
                 subtraces = None,
                 start_date = datetime(2015,1,1),
                 preload_time_s = 3600*24*2,
                 workload_duration_s = 3600*24*7,
                 work_state = "pending",
                 analysis_state = "0", 
                 overload_target=0.0,
                 table_name="experiment"):
        super(GroupExperimentDefinition,self).__init__(
                                     name=name,
                                     experiment_set=experiment_set,
                                     seed=seed,
                                     machine=machine,
                                     trace_type=trace_type,
                                     manifest_list=manifest_list,
                                     workflow_policy=workflow_policy,
                                     workflow_period_s=workflow_period_s,
                                     workflow_share=workflow_share,
                                     workflow_handling=workflow_handling,
                                     subtraces = subtraces,
                                     start_date = start_date,
                                     preload_time_s = preload_time_s,
                                     workload_duration_s = workload_duration_s,
                                     work_state = work_state,
                                     analysis_state = analysis_state,
                                     overload_target=overload_target, 
                                     table_name=table_name)
    def load_pending(self, db_obj):
        """Configures the object with the data of the first experiment with
        state="fresh", ordered by trace_id. Then set the state to 
        "pre_simulating".
        
        Returns True if load was succesful, False if no experiments with state
            "fresh" are available.
        """
        return self.load_next_state(db_obj, "pending", "pre_analyzing", 
                                    True, ["analysis_done", "second_pass_done"])
    
    def add_sub_trace(self, trace_id):
        self._subtraces.append(trace_id)
    
    def is_it_ready_to_process(self, db_obj):
        """Returns true is the sub traces have been generated and analyzed."""
        for trace_id in self._subtraces:
            rt = ExperimentDefinition()
            rt.load(db_obj, trace_id)
            if not (rt._work_state in ["analysis_done", "second_pass_done"]):
                return False
        return True
    def pass_other_second_pass_requirements(self, db_obj):
        for sub_trace_id in self._subtraces:
            ex = ExperimentDefinition()
            ex.load(db_obj, sub_trace_id)
            if not ex.is_analysis_done():
                return False
        return True
    
    def get_exp_def_like_me(self):
        return GroupExperimentDefinition()


class DeltaExperimentDefinition(GroupExperimentDefinition):
    """Delta Experiments: Comparison between two single experiments with the
    same random seed, workload configuraion, but different scheduler
    configuration. Workflow variables are compared workflow to workflow, and
    statistics calculated over the differences.
    """
    def __init__(self,
                 name=None,
                 experiment_set=None,
                 seed="AAAAAA",
                 machine="edison",
                 trace_type="delta",
                 manifest_list=None,
                 workflow_policy="no",
                 workflow_period_s=0,
                 workflow_share=0.0,
                 workflow_handling="manifest",
                 subtraces = None,
                 start_date = datetime(2015,1,1),
                 preload_time_s = 3600*24*2,
                 workload_duration_s = 3600*24*7,
                 work_state = "pending",
                 analysis_state = "0", 
                 table_name="experiment",
                 overload_target=None):
        super(GroupExperimentDefinition,self).__init__(
                                     name=name,
                                     experiment_set=experiment_set,
                                     seed=seed,
                                     machine=machine,
                                     trace_type=trace_type,
                                     manifest_list=manifest_list,
                                     workflow_policy=workflow_policy,
                                     workflow_period_s=workflow_period_s,
                                     workflow_share=workflow_share,
                                     workflow_handling=workflow_handling,
                                     subtraces = subtraces,
                                     start_date = start_date,
                                     preload_time_s = preload_time_s,
                                     workload_duration_s = workload_duration_s,
                                     work_state = work_state,
                                     analysis_state = analysis_state, 
                                     table_name=table_name,
                                     overload_target=overload_target)
    def add_compare_pair(self, first_id, second_id):
        self.add_sub_trace(first_id, second_id)
    
    
    def is_it_ready_to_process(self, db_obj):
        """Returns true is the sub traces have been at least generated."""
        for trace_id in self._subtraces:
            rt = ExperimentDefinition()
            rt.load(db_obj, trace_id)
            if not (rt._work_state in ["simulation_done", "analysis_done"]):
                return False
        return True
    
    def get_exp_def_like_me(self):
        return DeltaExperimentDefinition()

        