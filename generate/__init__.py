""" 
函数和代码生成工作负载
"""
from analysis import ProbabilityMap
import bisect
import datetime
import random
import random_control



class TimeController(object):
    """
    Time控制器类控制跟踪生成过程所在的时间点。它还根据随机变量的特征产生间隔到达时间。
    """

    def __init__(self, inter_arrival_gen):
        """
        初始化时间控制器实例，配置事件间隔时间生成策略。
        Args:
            inter_arrival_gen: 经过配置的概率生成器对象，用于控制连续事件之间的时间间隔分布。
                该对象的内部配置将直接影响生成事件间隔时间的统计特性（如分布类型、参数等）。
        """
        # 核心组件初始化：事件间隔生成器
        self._inter_arrival_generator = inter_arrival_gen

        # 时间追踪相关属性
        self._time_counter = 0  # 当前时间计数器（时间戳）
        self._start_date = 0  # 基准起始时间

        # 运行限制参数
        self._run_limit = 0  # 最大运行时长限制（秒）
        self._max_interrival = None  # 事件间隔时间上限（可选）
    
    def set_max_interarrival(self, max_value):
        self._max_interrival=max_value
        
    def reset_time_counter(self, start_date_time=None):
        """设置轨迹计时器的起始时间和当前时间计数器。
         根据输入参数初始化计时系统，时间计数器将采用统一的epoch时间格式存储。
         该方法会同时影响_time_counter和_start_date两个核心计时属性。
         Args:
             start_date_time (datetime.datetime/None): 时间基准点参数
                 * 当传入datetime对象时：使用该时间的epoch值作为计时基准
                 * 当参数为None时：自动采用当前系统时间作为基准
         Returns:
             None: 该方法不返回任何值，直接修改对象内部状态
         """
        # 处理时间基准点参数缺省情况
        if start_date_time is None:
            start_date_time = datetime.datetime.now()
        # 同步更新计时系统核心参数
        # 通过TimeController工具类进行时间格式标准化
        self._time_counter = TimeController.get_epoch(start_date_time)
        self._start_date = self._time_counter
    def set_run_limit(self, seconds):
        """Sets the time running limit of the trace as _star_date+_run_limit"""
        self._run_limit = seconds
        
    def is_time_to_stop(self):
        """True if the interntal time counter is over the end time"""
        return self._time_counter-self._start_date > self._run_limit
    
    def get_next_job_create_time(self):
        """计算并返回下一个作业的创建时间，同时更新内部时间计数器
        根据配置的概率分布生成作业到达间隔时间，若配置了最大间隔时间限制，
        最终间隔时间不会超过该值。内部时间计数器会累加计算出的间隔时间，
        最终返回更新后的计数器整数值（表示创建时间戳）
        Returns:
            int: 下一个作业的创建时间戳（自增后的时间计数器整数值）
        """
#         step = None
#         while (step is None
#                or self._max_interrival is not None 
#                and self._max_interrival<step):
#             step = self._inter_arrival_generator.produce_number()
        # 生成作业到达间隔时间，并确保不超过最大允许间隔时间
        step = self._inter_arrival_generator.produce_number()
        if self._max_interrival is not None:
            step=min(self._max_interrival, step)
        # 累加间隔时间到计数器，返回整型时间戳
        self._time_counter=float(self._time_counter)+float(step)
        return int(self._time_counter)
    
    def get_current_time(self):
        """Returns current time"""
        return self._time_counter
    def get_runtime(self):
        return self._time_counter - self._start_date
    @classmethod
    def get_epoch(cld, the_datetime):
        """
        将datetime对象转换为Unix时间戳（epoch时间戳）
        参数:
        cld (类对象): 类方法的隐式第一个参数，表示类本身
        the_datetime (datetime.datetime): 需要转换的datetime对象
        返回:
        int: 自1970-01-01 00:00:00 UTC起经过的秒数（Unix时间戳）
        """
        return int(the_datetime.strftime('%s'))
    
        
    
class WorkloadGenerator(object):
    """
    WorkloadGenerator（工作负载生成器）生成作业轨迹，以便在调度器模拟器中运行，针对具有特定工作负载特征、用户、系统和队列配置的系统。
    
    WorkloadGenerator 使用机器配置（Machine 类）和轨迹生成器（TraceGenerator 类）来计算作业数量，并将其以轨迹格式导出。
    机器配置描述了作业的随机变量，包括到达间隔时间、预估的挂钟时间、预估的挂钟时间准确性以及分配的核数。
    轨迹生成器负责将作业信息转换为模拟器接受的特定格式。
    
    生成器还需要其他由 Slurm 批处理系统所需的额外信息：系统中的用户名、QOS 策略名称、现有的分区名称和账户名称。
    作业的用户、QOS、分区和账户是从这些列表中随机（均匀）选择的。
    若要更改此行为，请重新定义 _get_user、_get_partition、_get_account 和 _get_qos 方法。
    """
    def __init__(self, machine, trace_generator, user_list, qos_list,
                 partition_list, account_list):
        """
        初始化任务跟踪生成器的配置和核心组件

        Args:
            machine (Machine): 机器模型类，用于生成作业的随机变量参数
            trace_generator (TraceGenerator): 跟踪生成器类，用于存储最终生成的任务跟踪数据
            user_list (List[str]): 任务跟踪中出现的非重复用户标识列表
            qos_list (List[str]): 任务跟踪中需要包含的QoS策略列表
            partition_list (List[str]): 任务跟踪中使用的计算分区列表
            account_list (List[str]): 任务跟踪中涉及的账户信息列表
        """
        self._machine = machine
        self._trace_generator = trace_generator
        # 初始化时间控制器，使用机器模型的作业到达间隔生成器
        self._time_controller = TimeController(
                                        machine.get_inter_arrival_generator())
        self._job_id_counter=1
        self._user_list = user_list
        self._qos_list = qos_list
        self._partition_list=partition_list
        self._account_list=account_list

        # 获取全局随机数生成器实例
        self._random_gen = random_control.get_random_gen()
        # 动态导入并初始化工作负载比例控制器
        from generate.special.workflow_percent import WorkflowPercent

        # 创建工作负载选择器，配置核心参数和依赖组件
        self._workload_selector = WorkflowPercent(self._random_gen,
                                                  trace_generator,
                                                  self._time_controller,
                                                  machine.get_total_cores())
        # 设置剩余工作流的生成控制器
        self._workload_selector.set_remaining(self)
        # 初始化模式定时器存储列表
        self._pattern_timers = []

        # 初始化作业过滤配置参数
        self._filter_func = None
        self._filter_cores = None
        self._filter_runtime = None
        self._filter_core_hours = None
        self._disable_generate_workload_element=False
    
    def generate_trace(self, start_date_time, run_time_limit, job_limit=None):
        """
        生成并存储作业跟踪数据，根据指定的时间范围和限制条件生成作业记录。
        Args:
            start_date_time (datetime.datetime): 跟踪数据生成的起始时间戳
            run_time_limit (int): 跟踪数据的总持续时长（单位：秒）
            job_limit (int, optional): 最大作业生成数量限制，None表示无限制
        Returns:
            int: 实际生成的作业总数
        功能说明:
            - 初始化时间计数器并设置运行时长限制
            - 通过循环生成作业直到满足停止条件（时间耗尽或达到数量限制）
            - 整合工作负载生成器和定时模式生成器的作业生成结果
        """
        # 初始化时间控制器状态
        self._time_controller.reset_time_counter(start_date_time)
        self._time_controller.set_run_limit(run_time_limit)
        self.set_all_time_controllers();
        
        jobs_generated=0

        # 主生成循环：持续生成作业直到满足停止条件
        while (not self._time_controller.is_time_to_stop()):
            create_time=self._time_controller.get_next_job_create_time()
            # 检查作业数量限制
            if (job_limit!=None and jobs_generated>=job_limit):
                break
            # 生成核心工作负载元素
            if not self._disable_generate_workload_element:
                jobs_generated+=self._generate_workload_element(create_time)
            # 触发定时模式生成器的附加作业生成
            jobs_generated+=self._pattern_generator_timers_trigger(create_time)
        # 输出生成结果并返回总数
        print("{0} jobs generated".format(jobs_generated))
        return jobs_generated
    
    def disable_generate_workload_elemet(self):
        self._disable_generate_workload_element=True
    
    def _generate_workload_element(self, create_time):
        """
        生成一个工作负载元素并触发执行
        Args:
            create_time: 元素创建时间戳，用于标记工作负载的生成时间
        Returns:
            int: 执行结果状态码
                0 - 没有可用的工作负载对象
                1 - 成功生成新任务
                other - 返回被触发对象的执行结果值
        """
        # 从选择器中随机获取工作负载对象
        workload_obj = self._workload_selector.get_random_obj()
        # 处理无可用工作负载对象的情况
        if workload_obj is None:
            return 0
        # 处理选择到自身时需要生成新任务的特殊逻辑
        if workload_obj == self:
            self._generate_new_job(create_time)
            return 1
        # 触发其他工作负载对象并返回执行结果
        return workload_obj.do_trigger(create_time)
        
    
    def save_trace(self, file_route):
        """
        该方法将作业跟踪信息、用户配置和QoS策略配置分别存储到以给定路径为基础的不同扩展名文件中。
        通过TraceGenerator实例实现具体的数据持久化操作。

        Args:
            file_route (str): 输出文件的基础路径（不带扩展名）。例如输入"/path/data"将生成:
                - /path/data.trace: 作业跟踪数据
                - /path/data.users: 用户配置数据
                - /path/data.qos: QoS策略配置数据
        """
        # 通过TraceGenerator分三类持久化数据
        self._trace_generator.dump_trace(file_route+".trace")
        self._trace_generator.dump_users(file_route+".users")
        self._trace_generator.dump_qos(file_route+".qos")
    
    def set_max_interarrival(self, max_interarrival):
        """
        设置事件到达间隔时间的最大值
        将最大间隔时间参数转发给内部的时间控制器对象进行配置
        Args:
            max_interarrival (int/float): 允许的事件到达间隔时间的最大值，单位应与系统时间单位保持一致。
                该值用于控制事件生成的时间间隔上限
        Returns:
            None: 本方法仅进行参数传递，不直接返回值
        """
        self._time_controller.set_max_interarrival(max_interarrival)
    
    def config_filter_func(self, filter_func):
        """
        配置数据过滤函数到当前实例
        用于设置后续处理数据时使用的过滤逻辑，该函数会被存储在实例的_filter_func属性中
        Args:
            filter_func (callable): 过滤函数对象，需要接收一个参数并返回布尔值。
                该函数应当实现具体过滤逻辑，符合条件的数据应返回True，否则返回False
        Returns:
            None: 该方法没有返回值，仅修改实例的_filter_func属性
        """
        self._filter_func=filter_func
        
        
    def _job_pass_filter(self, cores, runtime):
        """
        检查作业是否满足资源过滤器设定的条件
        根据配置的过滤函数对作业的资源需求进行验证。若未配置过滤器则默认允许所有作业。
        Args:
            cores (int): 作业请求的CPU核心数
            runtime (float): 作业预期的运行时长（单位：秒）
        Returns:
            bool: 通过过滤返回True，未通过返回False；无过滤器时始终返回True
        Note:
            实际过滤逻辑由成员变量_filter_func实现，本方法仅作为调用入口。
            当_filter_func未设置时，所有作业都会被允许提交。
        """
        if self._filter_func is None:
            return True
        else:
            return self._filter_func(cores, runtime)
        
    def _generate_new_job(self, submit_time, cores=None,run_time=None,
                          wc_limit=None, override_filter=False):
        """
        生成并提交新的作业到跟踪生成器
        根据机器配置生成新作业的特征参数，进行过滤检查后提交作业。若作业未通过过滤检查且未强制覆盖，则返回-1
        Args:
            submit_time (float): 作业提交时间戳
            cores (int, optional): 指定CPU核心数。若为None则使用机器生成的默认值
            run_time (int, optional): 指定运行时间(秒)。若为None则使用机器生成的默认值
            wc_limit (int, optional): 指定时间限制(秒)。若为None则使用机器生成的默认值
            override_filter (bool): 是否跳过过滤检查，默认为False
        Returns:
            int: 成功时返回新作业ID，未通过过滤检查时返回-1

        """
        # 生成唯一作业ID
        self._job_id_counter+=1

        # 获取用户相关属性
        user=self._get_user()
        qos=self._get_qos()
        partition=self._get_partition()
        account=self._get_account()

        # 从机器配置获取默认参数
        cores_v, wc_limit_v, run_time_v = self._machine.get_new_job_details()

        # 处理参数：若未提供则使用机器生成的默认值
        if cores is None:
            cores=cores_v
        if wc_limit is None:
            wc_limit=wc_limit_v
        if run_time is None:
            run_time=run_time_v

        # 过滤检查（除非强制覆盖）
        if not override_filter and not self._job_pass_filter(cores, run_time):
            return -1
        # 创建并返回新作业
        return self.add_job(self._job_id_counter, user, submit_time,
                                      run_time, wc_limit,cores, 
                                      qos, partition, account,
                        reservation="", dependency="", workflow_manifest="|")
        
    def add_job(self, job_id=None, username=None, submit_time=None,
                        duration=None, wclimit=None, cores=None,
                        qosname=None, partition=None, account=None,
                        reservation=None, dependency=None,
                        workflow_manifest=None):
        """
        向跟踪系统插入一个作业，未设置的参数将使用系统配置值
        Args:
            job_id: 作业标识符，未提供时自动生成自增ID
            username: 提交用户名，未提供时随机生成
            submit_time: 提交时间戳，未设置时使用当前系统时间
            duration: 实际运行时间（秒），未提供时自动生成
            wclimit: wallclock时间限制（分钟），未提供时自动生成
            cores: 请求的计算核心数，未提供时自动生成
            qosname: 服务质量等级名称，未提供时随机选择
            partition: 计算分区名称，未提供时随机选择
            account: 账户信息，未提供时随机生成
            reservation: 资源预留信息，默认空字符串
            dependency: 作业依赖关系，默认空字符串
            workflow_manifest: 工作流描述信息，格式为"|分割的字符串"或文件路径
        Returns:
            int: 创建成功的作业ID
        Important Logic:
            1. 工作流标识检测：根据manifest长度判断是否工作流作业
            2. 核心时间计算：对工作流作业进行核心时间修正计算
            3. 自动填充机制：11个参数均有自动生成逻辑
        """
        # 检测是否为工作流作业（manifest包含多个元素时判定为工作流）
        it_is_a_workflow=(workflow_manifest and len(workflow_manifest)>1)

        # 参数自动生成区块：为未指定的参数生成系统默认值
        if job_id is None:
            self._job_id_counter+=1
            job_id=self._job_id_counter
        if username is None:
            username=self._get_user(no_random=it_is_a_workflow)
        if qosname is None:
            qosname=self._get_qos(no_random=it_is_a_workflow)
        if partition is None:
            partition=self._get_partition(no_random=it_is_a_workflow)
        if account is None:
            account=self._get_account(no_random=it_is_a_workflow)
        if submit_time is None:
            submit_time = self._time_controller.get_current_time()

        # 核心资源配置逻辑：从机器配置获取默认计算资源参数
        if not cores or not wclimit or not duration:
            cores_pre, wc_limit_pre, run_time_pre = (       
                                            self._machine.get_new_job_details())
        if cores is None:
            cores = cores_pre
        if wclimit is None:
            wclimit = wc_limit_pre
        if duration is None:
            duration = run_time_pre

        # 处理可选参数的空值默认值
        if reservation is None:
            reservation = ""
        if dependency is None:
            dependency = ""

        # 工作流核心时间修正计算（当manifest指向外部文件时）
        cores_s=None
        cores_s_real=None
        if workflow_manifest is None:
            workflow_manifest = ""
        else:
            if workflow_manifest[0]!="|":
                # 从工作流文件中提取资源浪费数据并修正核心时间
                from stats.workflow import WasteExtractor
                manifest_file=workflow_manifest.split("-")[0]
                we = WasteExtractor(manifest_file)
                stamps, waste_list, acc_waste = we.get_waste_changes(0)
                cores_s_real=(min(wclimit*60,duration) *cores)
                cores_s=cores_s_real-acc_waste

        # 将最终配置写入跟踪生成器
        self._trace_generator.add_job(job_id, username, submit_time,
                                      duration, wclimit,cores,
                                      1,self._machine._cores_per_node,
                                      qosname, partition, account,
                                      reservation=reservation,
                                      dependency=dependency,
                                      workflow_manifest=workflow_manifest,
                                      cores_s=cores_s,
                                      ignore_work=False,
                                      real_core_s=cores_s_real)
        return job_id
        
    def _get_user(self, no_random=False):
        return self._get_random_in_list(self._user_list, 
                                        no_random=no_random)
    
    def _get_qos(self, no_random=False):
        return self._get_random_in_list(self._qos_list,
                                        no_random=no_random)
    
    def _get_partition(self, no_random=False):
        return self._get_random_in_list(self._partition_list,
                                        no_random=no_random)
    
    def _get_account(self, no_random=False):
        return self._get_random_in_list(self._account_list,
                                        no_random=no_random)
    
    def _get_random_in_list(self, value_list,
                            no_random=False):
        """
        从给定列表中随机选择一个元素返回
        Args:
            value_list (list): 候选值列表，至少包含一个元素
            no_random (bool): 是否禁用随机选择。当为True时直接返回第一个元素，
                默认为False
        Returns:
            any: 从value_list中选中的值。当列表长度为1或no_random=True时返回第一个元素，
                否则返回随机位置对应的元素
        """

        # 如果只有一个候选值或禁用随机，直接返回首元素
        if len(value_list)==1 or no_random:
            return value_list[0]
        # 否则生成随机索引并返回对应元素
        pos=self._random_gen.randint(0, len(value_list)-1)
        return value_list[pos]
    
    def _pattern_generator_timers_trigger(self, timestamp):
        """
        触发所有已注册的模式定时器并清理已完成定时器
        遍历模式定时器列表，触发每个定时器的时间检查，并累计触发结果。
        过滤可清除的定时器，维护未完成定时器的新列表。
        Args:
            timestamp: 触发时间戳，用于判断定时器是否达到触发条件
        Returns:
            int: 本次触发累计的总次数（所有定时器触发次数的总和）
        """
        count = 0
        new_pattern_timers = []
        # 处理每个定时器：触发时间检查 + 过滤可清除的定时器
        for timer in self._pattern_timers:
            # 累加当前定时器的触发次数（do_trigger返回触发次数）
            count += timer.do_trigger(timestamp)
            # 保留未完成/不可清除的定时器
            if not timer.can_be_purged():
                new_pattern_timers.append(timer)

        # 更新模式定时器列表（已完成的定时器将被移除）
        self._pattern_timers= new_pattern_timers
        return count
            
    
    def register_pattern_generator_timer(self,timer):
        """
        注册基于时间控制的工作负载模式生成器定时器
        注册一个由PatternTimer对象控制触发时机的模式生成器。该定时器将被周期性检查，
        当满足时间条件时会触发关联模式生成器的作业生成逻辑。
        Args:
            timer (PatternTimer): 时间控制对象，需实现以下接口：
                - is_it_time(): 返回布尔值表示是否满足触发条件
                - do_trigger(): 触发时需要执行的作业生成逻辑
                该对象应自行维护时间跟踪状态
        Maintains:
            将timer添加到内部_pattern_timers列表用于后续的定期检查
        """
        self._pattern_timers.append(timer)
    
    def set_all_time_controllers(self):
        """
        设置所有模式定时器的时间控制器
        遍历所有模式定时器，将当前时间控制器的当前时间注册到每个定时器中。
        该方法用于同步多个模式定时器的时间基准。
        参数:
            self: 对象实例自身，用于访问类内部的时间控制器和模式定时器集合
        返回值:
            None: 该函数没有返回值
        """
        # 遍历所有模式定时器并进行时间注册
        for timer in self._pattern_timers:
            timer.register_time(self._time_controller.get_current_time())
                
    
    def register_pattern_generator_share(self, pattern_generator,
                                         share):
        """
        注册一个按指定比例分配工作负载的模式生成器
        该生成器将根据预设的到达时间间隔，在作业创建时有指定比例的概率被调用。
        支持通过参数配置核心小时数的使用上限。
        Args:
            pattern_generator (PatternGenerator): 工作负载模式生成器对象，当被选中时将生成具体作业
            share (float): 权重数值，取值范围[0.0, 100000.0]，格式为YYYXXX.xx：
                - 小数部分（XXX.xx）：表示该生成器的调用权重比例
                - 整数部分（YYY，可选）：若存在则表示核心小时数上限配置，格式为YYY/1*runtime，
                  例如100500.75表示上限为100核心小时，权重为500.75
        Raises:
            ValueError: 当share超出允许范围时抛出
        Important Logic:
            1. 当share>1时，自动解析为上限配置模式
            2. 权重数值会被分解为实际调用权重和资源上限两部分
            3. 最终同时注册生成器权重和配置资源上限（如果存在）
        """
        # 参数有效性验证：确保权重值在允许范围内
        if share<0.0 or share>100000.0:
            raise ValueError("Share has to be in the range [0.0, 100000.0]: "
                             "{0}".format(share))
        # 分解权重值为基础权重和资源上限
        actual_share=share
        upper_cap=None
        if actual_share>1:
            # 提取整数部分作为资源上限（YYY），小数部分作为实际权重
            upper_cap=float(int(actual_share))/1000
            actual_share-=upper_cap*1000
        # 注册模式生成器及其调用权重
        self._workload_selector.add_obj(pattern_generator, actual_share)
        # 配置资源上限（如果存在）
        if upper_cap is not None:
            self._workload_selector.config_upper_cap(upper_cap)
    
        
class RandomSelector(object):
    """
    随机选择器是一种对象，它能够存储一个带有各自关联概率的对象列表，并根据这些概率随机抽取一个对象。该对象包含两种类型：
    剩余对象：当其他所有对象都未被选中时被选中。
    附加对象：当随机数落在其概率区间内时被选中。
    """
    def __init__(self, random_gen):
        """
        初始化类的实例，构建核心数据结构。
        参数说明：
        - random_gen: 随机数生成器对象，需包含uniform(float, float)方法，
                      该方法接受两个浮点数参数，返回参数区间内的随机浮点数
        属性初始化：
        - _share_list: 存储共享对象的容器（后续用于概率分布计算）
        - _obj_list: 存储待处理对象的集合
        - _prob_list: 维护概率权重的列表（与_obj_list中的对象对应）
        - _remaining_obj: 记录算法运行期间未分配的对象（初始为空）
        """
        # 核心依赖注入：绑定外部随机数生成器
        self._random_gen = random_gen
        # 初始化算法核心数据结构
        self._share_list=[]
        self._obj_list=[]
        self._prob_list=[]
        self._remaining_obj = None
    def set(self, share_list, obj_list, remaining_obj=None):
        """
        设置内部共享列表、对象列表及剩余对象，并重新计算概率分布。
        该方法用于配置对象的概率分布。若指定剩余对象，该对象将获得未被分配的概率，
        并将其置于对象列表的开头，随后触发重新计算。
        Args:
            share_list (list[float]): 各对象的概率列表，元素数量需与obj_list一致。
                所有元素之和不应超过1.0。若未设置remaining_obj，总和必须等于1.0
            obj_list (list): 对应概率的对象列表，元素数量需与share_list一致
            remaining_obj (object, optional): 剩余概率的持有对象。若提供，该对象将被
                添加至对象列表前端，并自动计算剩余概率
        Note:
            - 当remaining_obj被设置时，会将其插入_obj_list的首位
            - 设置完成后会调用_recalculate()方法更新内部状态
        """
        self._share_list=share_list
        self._obj_list=obj_list
        self._remaining_obj=remaining_obj
        # 处理剩余对象逻辑：将剩余对象插入列表首位
        if remaining_obj is not None:
            self._obj_list=[self._remaining_obj] + self._obj_list
        # 触发概率分布的重新计算
        self._recalculate()
    
    def remove_remaining(self):
        """
        清理剩余对象并更新相关状态
        当存在剩余对象时：
        1. 重置_remaining_obj为空
        2. 将对象列表更新为概率列表中第一个元素之后的内容
        最后触发重新计算流程
        """
        # 清理剩余对象并重置对象列表
        if self._remaining_obj:
            self._remaining_obj=None
            self._obj_list=self._prob_list[1:]
        # 执行状态重新计算
        self._recalculate()
    def set_remaining(self, obj):
        """
        设置剩余对象并更新对象列表
        替换当前剩余对象为新的目标对象，将其插入对象列表头部，
        并触发关联数据重新计算
        Args:
            obj(Any): 要设置为剩余对象的目标对象，类型由具体实现决定
                      （预期应与其他对象类型保持一致）
        Returns:
            None: 该方法不返回有效值，直接修改对象内部状态
        """
        # 清理现有的剩余对象关联
        self.remove_remaining()
        # 绑定新对象并更新对象列表结构
        self._remaining_obj = obj
        # 将新剩余对象插入列表首位，保持列表结构规范
        self._obj_list=[self._remaining_obj] + self._obj_list
        # 触发关联数据的重新计算流程
        self._recalculate()
        
    def add_obj(self, obj, share):
        """
        将对象及其关联的份额添加到列表，并触发内部状态重计算。
        参数:
            obj (Any): 待添加的对象实例，具体类型由业务逻辑决定。
            share (int/float): 对象关联的权重份额，影响后续选择逻辑的分布比例。
        异常:
            ValueError: 若当前实例不允许继续添加对象（无剩余可添加额度）时抛出。
        说明:
            该方法会修改对象列表和份额列表，并触发 _recalculate() 更新内部状态。
            若调用时 _remaining_obj 为假值，说明已达到可添加对象上限，直接报错。
        """
        # 核心前置条件检查：若无可添加余量则拒绝操作
        if not self._remaining_obj:
            raise ValueError("Add cannot be used with RandomSelector that  has "
                             "no remaining_obj")
        # 更新对象和份额的存储列表
        self._obj_list.append(obj)
        self._share_list.append(share)
        # 触发份额变化后的内部状态重置
        self._recalculate()
        
    def _recalculate(self):
        """重新计算概率分布列表并校验总和不超过1.0
        根据_remaining_obj和_share_list的状态生成新的概率分布列表_prob_list
        执行逻辑：
        1. 如果存在剩余对象(_remaining_obj)，则创建基础概率值
        2. 处理共享概率列表(_share_list)的累加计算
        3. 最终校验所有概率总和不超过1.0
        Returns:
            None: 直接更新实例的_prob_list属性
        """
        self._prob_list=[]
        prob = 0.0
        # 处理剩余对象的基础概率计算
        if self._remaining_obj:
            # 初始概率设为1.0，再减去共享列表的总和
            prob = 1.0
            if len(self._share_list):
                prob-=sum(self._share_list)
            self._prob_list.append(prob)
        # 处理共享列表的累加概率计算
        if len(self._share_list):
            for s in self._share_list:
                prob+=s
                self._prob_list.append(prob)
        # 最终概率总和校验
        if prob>1.0:
            raise ValueError("Probabilities add more than 1.0 check "
                             "share list: {0}".format(self._share_list))
    def get_random_obj(self):
        """根据预配置的概率分布随机选择一个对象
        该方法基于累积概率分布实现高效随机选择，使用二分查找算法确定随机数对应的索引位置。
        要求_prob_list必须是预先计算好的累积概率列表，且与_obj_list顺序一致。
        Returns:
            object/None: 返回_obj_list中的随机对象实例。当没有配置概率时返回None，
            当仅有一个概率项时直接返回对应的唯一对象。
        """
        if len(self._prob_list)==0:
            return None
        elif len(self._prob_list)==1:
            return self._obj_list[0]
        # 生成随机数并在累积概率列表中定位
        r = self._random_gen.uniform(0,1)
        pos = bisect.bisect_left(self._prob_list, r)
        return self._obj_list[pos]
     
        
            
            
    