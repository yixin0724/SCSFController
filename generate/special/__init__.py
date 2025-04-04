

from generate.pattern import PatternGenerator, PatternTimer
from math import ceil

class FixedJobGenerator(PatternGenerator):
    """模式生成器，它将始终生成具有相同运行时、分配的内核数和wall_clock限制的作业"""
    def __init__(self,workload_generator, cores, run_time, wc_limit=None):
        """初始化固定参数作业生成器
        Args:
            workload_generator (WorkloadGenerator): 接收作业创建动作的工作负载生成器实例
            cores (int): 所有作业必须分配的CPU核心数（正整数）
            run_time (int): 作业运行时长（单位：秒）
            wc_limit (int, optional): 作业墙钟时间限制（单位：分钟）。若未指定，则根据运行时间自动计算：
                run_time秒转换为分钟后向上取整，并增加1分钟缓冲时间
        【初始化流程】
        - 继承父类PatternGenerator的初始化逻辑
        - 设置核心数/运行时间基础参数
        - 自动计算墙钟时间限制（当未显式指定时）
        """
        super(FixedJobGenerator, self).__init__(workload_generator)
        self._cores=cores
        self._run_time=run_time
        self._wc_limit=wc_limit
        # 自动计算墙钟限制的逻辑：
        # 1. 将秒转换为分钟（浮点运算）
        # 2. 向上取整保证最小时间单位
        # 3. 额外增加1分钟缓冲时间
        if self._wc_limit is None:
            self._wc_limit=int(ceil(float(self._run_time)/60.0)+1)
        
    def do_trigger(self, create_time):
        """由工作负载生成器触发提交事件时调用，用于生成并提交新作业
        通过工作负载生成器的内部方法提交作业，使用预定义的资源配置参数（CPU核心数、
        运行时长、等待时间限制）生成新作业实例。
        Args:
            create_time (float): 事件触发时间戳，用于标记作业的创建时间
            self._cores (int): 作业请求分配的CPU核心数
            self._run_time (float): 作业预期需要占用的运行时长
            self._wc_limit (float): 作业在队列中允许的最大等待时长
        Returns:
            int: 实际提交的作业数量（根据内部方法实现决定返回值）
        """
        self._workload_generator._generate_new_job(create_time,self._cores,
                                                   self._run_time,
                                                   self._wc_limit)
class SpecialGenerators:
    """工厂类生产特殊工作流的生成器"""
    @classmethod
    def get_generator(cls, string_id, workload_generator,
                      register_timestamp=None, register_datetime=None):
        """
        根据给定的字符串ID创建对应的负载生成器实例
        参数:
            string_id: str - 格式为'sp-<generator_type>-...'的标识字符串，必须包含有效生成器类型
            workload_generator: WorkloadGenerator - 工作负载生成器对象
            register_timestamp: int|None - 可选的时间戳参数（单位：秒）
            register_datetime: datetime|None - 可选的日期时间对象
        返回:
            SaturateGenerator | BFSaturateGenerator - 根据string_id创建的对应生成器实例
        异常:
            ValueError - 当string_id格式不符合规范或包含未知生成器类型时抛出
        """
        # 解析字符串标识并验证基础格式
        tokens = string_id.split("-")
        if tokens[0] != "sp":
            raise ValueError("sting_id does not start with sp: {0}".format(
                                                                   string_id))
        # 根据生成器类型创建对应实例
        if tokens[1] == SaturateGenerator.id_string:
            gen = SaturateGenerator(workload_generator, register_timestamp,
                                    register_datetime)
            gen.parse_desc(string_id)
            return gen
        elif tokens[1] == BFSaturateGenerator.id_string:
            gen = BFSaturateGenerator(workload_generator, register_timestamp,
                                    register_datetime)
            gen.parse_desc(string_id)
            return gen
        # 未识别生成器类型的处理
        raise ValueError("Unkown generator string_id {0}".format(string_id))

class SaturateGenerator(PatternTimer):
    """模式计时器，它产生固定数量的相同作业，并以相同的到达间隔时间（blast）提交。
        它会每隔一秒发出一次blast。在前一个blast完成之前，不能配置新的blast启动。
    
    """
    id_string = "sat"
    def __init__(self, workload_generator, register_timestamp=None,
                 register_datetime=None):
        """
        初始化饱和工作负载生成器
        参数:
        workload_generator: 工作负载生成器对象
            基础工作负载生成器实例，用于产生原始工作负载元素
        register_timestamp: Callable[[], float], optional
            时间戳注册函数，用于记录时间戳的钩子函数，默认None表示父类默认行为
        register_datetime: Callable[[], datetime], optional
            日期时间注册函数，用于记录完整时间的钩子函数，默认None表示父类默认行为
        """
        # 调用父类构造器，传递空执行器参数和时间相关配置
        super(SaturateGenerator, self).__init__(None, register_timestamp,
                                                register_datetime)
        # 配置基础工作负载生成器并禁用其自动生成机制
        # 当前类将完全控制工作负载的生成逻辑
        self._workload_generator = workload_generator
        self._workload_generator.disable_generate_workload_elemet()
   
    def parse_desc(self, desc_cad):
        """根据字符串的内容配置生成器的行为，如: sp-sat-p1-c24-r36000-t5576-b30424. 格式:
        - pX: 作业之间的秒数>0。
        - cX: 每个作业分配的核心数
        - rX: 每个作业的运行时间，以秒为单位。
        - tX: number of jobs per blast.
        - bX: number of seconds in each blast.
        contraint bX>pX*tX
        """
        tokens = desc_cad.split("-")
        self._job_period=int(tokens[2].split("p")[1])
        self._cores = int(tokens[3].split("c")[1])
        self._run_time = int(tokens[4].split("r")[1])
        self._jobs_per_blast = int(tokens[5].split("t")[1])
        self._blast_period = int(tokens[6].split("b")[1])
        
        if self._blast_period<self._jobs_per_blast*self._job_period:
            raise ValueError("Blast period is too short, a new blast cannot "
                             "start before the previous one ends.")
        
        self._pattern_generator=FixedJobGenerator(self._workload_generator,
                                                  cores=self._cores,
                                                  run_time=self._run_time)
        
    def register_time(self, timestamp):
        super(SaturateGenerator,self).register_time(timestamp)
        
        self._configure_blast(timestamp+1)
    
    def _configure_blast(self, blast_starts_at):
        self._next_blast_time=blast_starts_at
        self._jobs_submitted=0
        self._next_job_time=blast_starts_at
        
    def _submit_jobs_blast(self, stamp):
        """提交一批定时触发的作业任务
        参数:
        stamp (float): 当前时间戳，用于判断是否到达下一个作业提交时间点
        返回:
        int: 本次调用实际提交的作业数量
        """
        this_call_jobs_sub = 0

        # 在允许的作业数量和时间窗口内循环提交作业
        while (self._jobs_submitted < self._jobs_per_blast and
               self._next_job_time <= stamp):
            # 触发定时作业模式生成器
            self._pattern_generator.do_trigger(self._next_job_time)

            # 更新下次作业时间和计数器
            self._next_job_time += self._job_period
            self._jobs_submitted += 1
            this_call_jobs_sub += 1

        # 当完成全部作业提交时，配置下一批作业参数
        if self._jobs_submitted == self._jobs_per_blast:
            self._configure_blast(self._next_blast_time + self._blast_period)

        return this_call_jobs_sub
    
    def do_trigger(self, create_time):
        return self._submit_jobs_blast(create_time)       
            
    def can_be_purged(self):
        return False
    
class BFSaturateGenerator(SaturateGenerator):
    """模式计时器，它产生一个重复的作业提交模式，
    通过回填作业使系统饱和：一个长作业（j1），一个宽作业（j2），直到j1结束才能开始。
    然后在j2之前执行一组较小的作业，这些作业不会延迟j2，并使机器的利用率达到100%。
    
    """
    id_string = "bf"
    
    def parse_desc(self, desc_cad):
        """Configures the behavior of the generator from the content of a string
        like: sp-sat-p10-c24-r61160-t5756-b123520-g600-lc240-lr119920-wc133824-wr3600.
        Format:
        - pX: number of seconds between jobs >0.
        - cX: number of cores allocated by each job
        - rX: runtime in seconds of each job.
        - tX: number of jobs per blast.
        - bX: number of seconds in each blast.
        - g: gap between the small jobs and the wide job
        - lc: number of cores of the long job
        - lr: runtime in seconds of the long job
        - wc: number of cores of the wide job
        - wr: runtime in seconds of the wide job 
        contraint bX>lr+wr
        """

        # 将配置字符串按分隔符'-'拆解为参数标记
        tokens = desc_cad.split("-")

        # 从tokens中提取关键参数：
        # tokens[6]对应bX(blast周期), tokens[7]对应g(间隔)
        # tokens[8]对应lc(长期作业核心数), tokens[9]对应lr(长期作业时长)
        # tokens[10]对应wc(宽作业核心数), tokens[11]对应wr(宽作业时长)
        self._blast_period = int(tokens[6].split("b")[1])
        self._gap =  int(tokens[7].split("g")[1])
        self._long_cores =  int(tokens[8].split("lc")[1])
        self._long_runtime =  int(tokens[9].split("lr")[1])
        self._wide_cores =  int(tokens[10].split("wc")[1])
        self._wide_runtime =  int(tokens[11].split("wr")[1])

        # 校验blast周期是否满足约束条件：必须大于长/宽作业总时长
        if self._blast_period<self._long_runtime+self._wide_runtime:
            raise ValueError("Blast period is too short, a new blast cannot "
                             "start before the previous one ends.")

        # 创建长期作业和宽作业的固定参数生成器
        self._long_job_generator=FixedJobGenerator(self._workload_generator,
                                                  cores=self._long_cores,
                                                  run_time=self._long_runtime)
        self._wide_job_generator=FixedJobGenerator(self._workload_generator,
                                                  cores=self._wide_cores,
                                                  run_time=self._wide_runtime)

        # 创建并配置小型作业生成器(使用Saturate策略)
        self._small_jobs_generator = SaturateGenerator(self._workload_generator,
                                                       self._register_timestamp)
        self._small_jobs_generator.parse_desc(desc_cad)
        
    def register_time(self, timestamp):
        if hasattr(self, "_small_jobs_generator"):
            self._small_jobs_generator.register_time(timestamp)
        super(BFSaturateGenerator,self).register_time(timestamp)
        if hasattr(self, "_small_jobs_generator"):
            self._small_jobs_generator._configure_blast(timestamp+120+1)        
    
    def _configure_blast(self, blast_starts_at):
        self._next_blast_time=blast_starts_at
        self._long_job_submit_time=blast_starts_at
        self._wide_job_submit_time=blast_starts_at+60
                
        
        
    def _submit_jobs_blast(self, stamp):
        """提交三种类型的定时任务（长任务/宽任务/小任务）
        根据给定的时间戳触发对应的任务提交，主要处理长任务和宽任务的定时提交，
        并聚合小任务提交的计数。当宽任务触发时，会自动配置下一次blast参数。
        Args:
            stamp (float): 当前时间戳，用于判断是否到达任务触发时间
        Returns:
            int: 本次调用总共提交的任务数量（长+宽+小任务）
        """
        this_call_jobs_sub = 0  # 当前调用提交的任务计数器

        # 处理长任务提交：当到达预定提交时间时触发并重置状态
        if (self._long_job_submit_time is not None and
                stamp >= self._long_job_submit_time):
            self._long_job_generator.do_trigger(self._long_job_submit_time)
            self._long_job_submit_time = None
            this_call_jobs_sub += 1

        # 处理宽任务提交：触发后额外配置下一次blast参数
        if (self._wide_job_submit_time is not None and
                stamp >= self._wide_job_submit_time):
            self._wide_job_generator.do_trigger(self._wide_job_submit_time)
            self._wide_job_submit_time = None
            this_call_jobs_sub += 1
            # 配置下一次blast的时间参数
            self._configure_blast(self._next_blast_time + self._blast_period)

            # 聚合小任务提交数量（委托给专门的小任务生成器）
        this_call_jobs_sub += self._small_jobs_generator._submit_jobs_blast(stamp)

        return this_call_jobs_sub
    
   