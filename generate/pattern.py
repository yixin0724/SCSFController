from generate import RandomSelector, TimeController
from os import path

import datetime
import json
import pygraphviz as pgv
import os


class PatternGenerator(object):
    """ 定义工作行为模式的实际提交操作的基类。
        它将在特定情况下被重新定义：例如，WorkflowGenerator类现在知道如何以特定格式提交工作流.
    """
    def __init__(self, workload_generator=None):
        """初始化模式生成器实例
        初始化模式生成器并与父级工作负载生成器建立关联，用于协调模式生成与工作负载执行的关系。
        参数:
            workload_generator (WorkloadGenerator, 可选): 关联的父级工作负载生成器，
                提供对共享资源和执行上下文的访问。未显式注册时默认为None。
        注意:
            当注册到工作负载生成器时，本实例可能会在负载执行期间从父生成器接收配置参数和回调。
        """
        self._workload_generator = workload_generator
   
    def do_trigger(self, create_time):
        """处理工作负载提交模式的触发事件。
        当工作负载生成器触发预定提交模式时调用。通过内部生成器提交新作业并返回提交数量。
        Args:
            create_time (datetime): 触发事件创建时间戳，用于作业时间标记和调度
        Returns:
            int: 本次触发提交的作业数量。固定返回1，因为每次触发通过_generate_new_job生成一个作业
        """
        self._workload_generator._generate_new_job(create_time)
        return 1

class PatternTimer(object):
    """ 基类，用于在仿真时间戳等于特定值时控制触发PatternGenerator的动作。
    具体行为可通过继承该基类并重写can_be_purged和is_it_time方法来实现。该基类本身永远不会被触发。
    """
    
    def __init__(self, pattern_generator, register_timestamp=None,
                 register_datetime=None):
        """初始化PatternTimer对象并设置模拟时间注册点。
        构造对象时会根据参数设置时间注册点，register_timestamp和register_datetime参数
        不能同时被设置。时间注册点用于在模拟时间到达指定时间时触发模式生成器。
        Args:
            pattern_generator (PatternGenerator): 当模拟时间到达预定时间时将被触发的模式生成器对象
            register_timestamp (float, optional): 用时间戳格式表示的模拟注册时间（epoch时间格式）
            register_datetime (datetime, optional): 用datetime对象表示的模拟注册时间
        Raises:
            ValueError: 当同时设置了register_timestamp和register_datetime参数时抛出
        Note:
            - 优先处理register_datetime参数，当该参数不为None时会忽略register_timestamp
            - 通过调用regiter_datetime()或register_time()方法最终设置注册时间
        """
        self._pattern_generator = pattern_generator
        if register_datetime is not None:
            if register_timestamp is not None:
                raise ValueError("Both register_timestamp and "
                                 "register_timestamp can not be set at the same"
                                 " time")
            self.regiter_datetime(register_datetime)
        else:
            self.register_time(register_timestamp)
    
    def regiter_datetime(self, the_datetime):
        """Sets the current time stamp and time of registration of the generator
        to the datetime object the_datetime."""
        self.register_time(TimeController.get_epoch(the_datetime))
    
    def register_time(self, timestamp):
        """设置生成器的时间注册点为指定的datetime对象。
        将输入的datetime对象通过TimeController工具转换为epoch时间戳后，
        调用register_time()方法完成时间注册操作。
        Args:
            the_datetime (datetime.datetime): 需要设置的注册时间对象。
                该参数将通过TimeController.get_epoch()方法转换为epoch时间戳
        Returns:
            None: 本方法无返回值，仅修改实例状态
        """
        self._register_timestamp = timestamp
        self._current_timestamp = timestamp  
    
    def is_it_time(self, current_timestamp):
        """判断当前时间戳是否满足触发条件，并返回应触发的次数。
        根据传入的时间戳计算当前周期内应该触发内部模式生成器的次数。
        该方法会同时更新内部记录的当前时间戳。
        Args:
            current_timestamp (float): 当前模拟时间的epoch时间戳值，单位为秒
        Returns:
            int: 需要触发内部模式生成器的次数。当前版本默认返回0，
                表示暂时不需要触发（待实现逻辑的占位返回值）
        """
        self._current_timestamp=current_timestamp
        return 0
    def do_trigger(self, create_time):
        """根据时间条件触发内部模式生成器多次执行
        通过is_it_time()获取当前时间步的触发次数，循环执行模式生成器触发操作。
        支持通过do_reentry()检查是否需要进行重新进入触发（如周期性触发场景）
        Args:
            create_time (float): 当前模拟时间的epoch时间戳（单位：秒）
                用于计算本次时间步应触发的次数
        Returns:
            int: 总触发次数的累加值，来自_pattern_generator.do_trigger()的多次调用结果
        """
        call_count = self.is_it_time(create_time)
        count=0
        while call_count>0:
            for i in range(call_count):
                count+= self._pattern_generator.do_trigger(create_time)
            call_count=0
            if self.do_reentry():
                call_count = self.is_it_time(create_time)
        return count
    
   
    def can_be_purged(self):
        """如果这个计时器以后不应该再被调用，则返回True，否则返回False。"""
        return True
    
    def do_reentry(self):
        return False
    
    

class WorkflowGenerator(PatternGenerator):
    """ 类以清单格式提交工作流。
        它接收一个清单和概率。
        每次提交的时候，它都会在这些概率上映射一个随机数，并提交相应的清单。
    """
    def __init__(self,manifest_list, share_list, workload_generator=None):
        """WorkflowGenerator 构造函数，用于创建工作流生成器实例
        Args:
            manifest_list (list[str]): manifest文件路径列表，每个路径指向一个待处理的manifest配置
            share_list (list[float]): 概率权重列表，每个元素表示对应manifest被提交的概率。
                必须满足以下条件：
                - 与manifest_list长度一致
                - 所有元素之和严格等于1.0
            workload_generator (WorkloadGenerator, optional): 当前生成器注册到的主工作负载生成器对象
        Raises:
            ValueError: 当manifest_list和share_list长度不一致时抛出
        """
        super(WorkflowGenerator,self).__init__(workload_generator)
        if len(manifest_list)!=len(share_list):
            raise ValueError("manifest_list and share_list must have the same"
                             " length.")
        self._manifest_selector = RandomSelector(workload_generator._random_gen)
        self._manifest_selector.set(share_list, manifest_list)
        self._workflow_count = 0
    
    def do_trigger(self, create_time):
        """从清单列表中选取并提交一个带权重随机选择的工作流作业
        根据self._share_list定义的权重值，从self_manifest列表中进行加权随机选择。
        生成包含该清单的作业并提交到工作负载生成器。
        Args:
            create_time (float/int): 作业创建时间戳，用于设置作业提交时间
                                    格式应为与系统时间基准一致的数值类型
        Returns:
            int: 固定返回1，可能用于表示基础状态码(如成功提交计数)
        """
        # 工作流计数器递增，用于生成唯一作业标识
        self._workflow_count+=1
        manifest  = self._manifest_selector.get_random_obj()
        manifest_name = manifest+"-"+str(self._workflow_count)
        cores, runtime  = self._parse_manifest(manifest)
        self._workload_generator.add_job(submit_time=create_time,
                                         duration=runtime,
                                         wclimit=int(runtime/60),
                                         cores=cores, 
                                         workflow_manifest=manifest_name)
        return 1
    
    def _parse_manifest(self, manifest_route):
        """
        解析清单文件内容并提取核心数及运行时间参数
        从指定路径读取JSON格式的清单文件，提取max_cores和total_runtime字段值。
        优先尝试通过ExperimentRunner获取清单目录，若失败则使用当前工作目录。
        Args:
            manifest_route (str): 清单文件相对路径，会与manifest_folder组合成完整路径
        Returns:
            tuple: 包含两个元素的元组，格式为(最大核心数, 总运行时间)
                  cores (int): 允许分配的最大CPU核心数
                  runtime (int/float): 实验总运行时间(单位取决于清单文件定义)
        Raises:
            FileNotFoundError: 当清单文件不存在时抛出
            JSONDecodeError: 当清单文件不是有效JSON格式时抛出
            KeyError: 当清单文件缺少必需字段时抛出
        """
        # 尝试获取实验配置的标准目录，若失败则保持空字符串(默认当前目录)
        folder=""
        try:
            from orchestration import ExperimentRunner
            folder=ExperimentRunner.get_manifest_folder()
        except:
            pass
        f = open(os.path.join(folder,manifest_route), "r")
        manifest=json.load(f)
        cores = manifest["max_cores"]
        runtime  = manifest["total_runtime"]
        f.close()
        return cores, runtime
    
    
class WorkflowGeneratorSingleJob(WorkflowGenerator):
    """工作流生成器，将工作流作为带有清单的单个作业注入。
    """
     
    def do_trigger(self, create_time):
        """执行工作流触发操作，随机选择清单并提交作业
        Args:
            create_time (float): 作业创建时间戳，用于设置作业提交时间
        Returns:
            int: 固定返回1，表示操作完成状态
        功能说明:
            1. 根据_share_list权重值从self_manifest列表随机选择清单
            2. 计算实际运行时间并生成唯一工作流名称
            3. 向工作负载生成器添加新作业（不包含清单具体内容）
        """
        self._workflow_count+=1
        manifest  = self._manifest_selector.get_random_obj()
        cores, runtime  = self._parse_manifest(manifest)
        from stats.workflow import WasteExtractor
        we = WasteExtractor(manifest)
        """
        We need the real runtime to keep track of the core hours generated
        """
        stamps, waste_list, acc_waste = we.get_waste_changes(0)
        real_runtime=stamps[-1]
        manifest_name = manifest+"-"+str(self._workflow_count)
        self._workload_generator.add_job(submit_time=create_time,
                                         duration=real_runtime,
                                         wclimit=int(runtime/60),
                                         cores=cores, 
                                         workflow_manifest=manifest_name)
        return 1
    
class WorkflowGeneratorMultijobs(WorkflowGenerator): 
    """工作流生成器，将工作流注入为一组相互依赖的作业。"""
    
    def do_trigger(self, create_time):
        """执行工作流触发操作，根据权重随机选择清单文件并生成扩展工作流
        Args:
            create_time (datetime): 工作流创建时间戳，用于后续处理流程的时间基准
            self._manifest_selector (object): 清单选择器实例，需实现get_random_obj()方法
            self._share_list (list): 权重列表，影响随机选择的概率分布
        Returns:
            object: 由_expand_workflow方法生成的扩展工作流对象，包含完整执行参数
        功能说明:
            1. 增加工作流执行次数统计
            2. 根据预设权重随机选择清单文件
            3. 构造清单文件的完整路径
            4. 生成并返回扩展后的工作流配置
        """
        self._workflow_count+=1
        workflow_file=self._manifest_selector.get_random_obj()
        from orchestration.running import ExperimentRunner
        workflow_route = path.join(ExperimentRunner.get_manifest_folder(),
                                  workflow_file)
        
        return self._expand_workflow(
                         workflow_route,
                         create_time, workflow_file)
        
        
    def _expand_workflow(self, manifest_route, create_time, manifest_name):
        """将工作流的所有任务作为独立作业提交，并根据DAG处理依赖关系
        通过多轮迭代处理任务列表，每次迭代提交所有满足依赖条件的任务，直到所有任务提交完成。
        每轮未提交的任务会保留到下一轮继续处理，形成层级式的依赖解析过程。
        Args:
            manifest_route (str): 工作流清单文件路径，JSON格式包含DAG定义和任务参数
            create_time (datetime): 工作流提交时间，将作为所有作业的统一创建时间
            manifest_name (str): 工作流清单标识名称，用于生成作业的唯一标识前缀
        Returns:
            int: 成功提交的作业总数
        """
        cores, runtime, tasks =  WorkflowGeneratorMultijobs.parse_all_jobs(
                                                                manifest_route)
        job_count = 0
        remaining_tasks = list(tasks.values())
        while (remaining_tasks):
            new_remaining_tasks  = []
            for task in remaining_tasks:
                if self._task_can_run(task):
                    job_count+=1
                    manifest_field=self._get_manifest_field_for_task(
                            manifest_name, task["id"], self._workflow_count,
                            task["dependencyFrom"])
                    job_id=self._workload_generator.add_job(
                                        submit_time=create_time,
                                         duration=task["runtime_sim"],
                                         wclimit=int(task["runtime_limit"]/60),
                                         cores=int(task["number_of_cores"]), 
                                         workflow_manifest=manifest_field,
                                         dependency=self._gen_deps(task))
                    task["job_id"]=job_id
                else:
                    new_remaining_tasks.append(task)
            remaining_tasks=new_remaining_tasks
        return job_count
    
    def _get_manifest_field_for_task(self, manifest, 
                                     stage_name,
                                     first_job_id,
                                     deps):
        """
        生成特定格式的任务标识字段字符串
        参数:
        manifest (str): 工作流/任务清单的唯一标识符
        stage_name (str): 当前阶段的名称标识
        first_job_id (str): 关联的第一个任务ID
        deps (list[dict]): 依赖任务字典列表，每个字典需包含'id'字段
        返回值:
        str: 格式化的字段字符串，包含工作流标识、阶段信息及依赖任务链
        """
        # 生成基础字段格式：|wf_{清单标识}-{首任务ID}_{阶段名称}
        field= "|wf_{0}-{1}_{2}".format(manifest, first_job_id, stage_name)
        if deps:
            field+="_{0}".format("-".join(["d"+i["id"] for i in deps]))
        return field
        
        
              
    def _task_can_run(self, task):
        """
        判断指定任务是否满足运行条件
        Args:
            task: dict类型任务对象，需包含dependencyFrom字段。
                dependencyFrom字段值为依赖任务列表，每个元素应为包含job_id的字典
        Returns:
            bool: 当无依赖时直接返回True；
                当所有依赖任务都包含job_id时返回True；
                任一依赖任务缺少job_id则返回False
        """
        # 无依赖任务时直接允许运行
        if len(task["dependencyFrom"])==0:
            return True
        for task_dep in task["dependencyFrom"]:
            if not "job_id" in task_dep.keys():
                return False
        return True
    def _gen_deps(self, task):
        """
        生成SLURM作业依赖关系字符串
        参数:
        task: dict - 任务字典对象，需要包含dependencyFrom字段。
            task['dependencyFrom']应为包含依赖作业信息的字典列表，
            每个字典应包含job_id字段表示依赖的作业ID
        返回值:
        str - 由逗号分隔的依赖关系字符串，格式为"afterok:job_id"。
            当存在多个依赖时格式为"afterok:job_id1,afterok:job_id2"
        """
        dep_string = ""
        dependenciesFrom = task["dependencyFrom"]
        # 遍历所有依赖项，构建SLURM要求的依赖关系格式
        # 每个依赖项格式为afterok:job_id，多个依赖用逗号分隔
        for dep in dependenciesFrom:
            if dep_string!="":
                dep_string+=","
            dep_string+="afterok:"+str(dep["job_id"])
        return dep_string    
    
    @classmethod    
    def parse_all_jobs(self, manifest_route):
        """解析任务清单文件，构建任务依赖关系图
        Args:
            manifest_route (str): 任务清单文件路径，包含DAG定义、资源配置等信息的JSON文件
        Returns:
            tuple: 包含三个元素的元组
                - cores (int): 最大可用计算核心数
                - runtime (int): 预估总运行时间
                - tasks (dict): 任务字典，键为任务ID，值为任务对象。每个任务对象包含：
                    - dependencyFrom: 前置依赖任务列表
                    - dependencyTo: 后置被依赖任务列表
        """
        # 加载并解析JSON清单文件
        f = open(manifest_route, "r")
        manifest=json.load(f)
        f.close()
        
        cores = manifest["max_cores"]
        runtime  = manifest["total_runtime"]

        # 构建任务字典并初始化依赖关系属性
        tasks = manifest["tasks"]
        tasks = {x["id"]: x for x in tasks}
        for task in tasks.values():
            task["dependencyFrom"] = []
            task["dependencyTo"] = []
        # 解析DOT图构建任务依赖关系
        dot_graph = pgv.AGraph(string=manifest["dot_dag"])
        # 遍历图的边关系建立双向依赖
        for edge in dot_graph.edges():
            orig = edge[0]
            dest = edge[1]
            tasks[orig]["dependencyTo"].append(tasks[dest])
            tasks[dest]["dependencyFrom"].append(tasks[orig])
            
        return cores, runtime, tasks
        

        
            

class MultiAlarmTimer(PatternTimer):
    """ PatternTimer类，它允许将未来的时间戳列表编程为警报。
        它控制由register_time和is_it_time调用更新的当前时间戳。 """
    
    def __init__(self,pattern_generator, register_timestamp=None,
                 register_datetime=None):
        """初始化多重警报定时器实例
        Args:
            pattern_generator (PatternGenerator): 警报触发时调用的模式生成器对象
            register_timestamp (int, optional): 时间戳格式的注册时间（epoch秒数）
            register_datetime (datetime, optional): datetime对象格式的注册时间
        Raises:
            ValueError: 当同时指定timestamp和datetime参数时抛出
        Note:
            - 继承自PatternTimer的互斥参数检查逻辑，两个时间参数不可同时设置
            - 通过set_alarm_list方法初始化空警报列表，用于后续管理定时触发时刻
            - 调用父类PatternTimer的初始化逻辑完成基础时间注册功能
        """
        super(MultiAlarmTimer, self).__init__(pattern_generator,
                                              register_timestamp,
                                              register_datetime)
        self.set_alarm_list([])
        
    def set_alarm_list_date(self, datetime_list):
        """设置未来报警触发时间的日期时间列表
        将输入的datetime对象列表转换为时间戳，并设置报警列表。会验证时间戳的有效性。
        Args:
            datetime_list (list[datetime]): 包含未来时间的datetime对象列表，要求：
                1. 必须按时间升序排列
                2. 所有时间必须晚于当前时间
        Raises:
            ValueError: 如果遇到以下情况：
                - 列表中存在早于当前时间的时间
                - 列表不是严格升序排列
        Notes:
            内部会调用set_alarm_list方法，将datetime对象转换为UNIX时间戳
        """
        # 将datetime对象转换为UNIX时间戳列表，并设置报警列表
        self.set_alarm_list([TimeController.get_epoch(x) 
                             for x in datetime_list])
    
    def set_alarm_list(self, alarm_list):
        """设置未来报警时间戳列表
        验证并存储一组按升序排列的将来时态时间戳，用于触发报警。
        时间戳必须满足两个条件：
        1. 所有数值必须大于当前时间戳
        2. 必须严格按升序排列
        Args:
            alarm_list (list): 报警时间戳列表，要求：
                - 元素为递增的纪元时间戳数值
                - 所有时间戳必须晚于当前时间
        Raises:
            ValueError: 如果遇到以下情况会抛出：
                - 列表中存在小于当前时间的时间戳
                - 时间戳未按升序排列
        Returns:
            None
        """
        # 遍历验证每个时间戳的有效性
        prev_alarm=None
        for alarm in alarm_list:
            if (self._current_timestamp is not None and 
                alarm < self._current_timestamp):
                raise ValueError("One of the alarm values is past DUE: {0}"
                                 "".format(alarm))
            if prev_alarm is not None and prev_alarm>alarm:
                raise ValueError("Alarm list should be ascending ordered")
            prev_alarm=alarm
        self._alarm_list = alarm_list
        
    def set_delta_alarm_list(self, delta_list):
        """设置基于当前时间戳和增量列表的未来警报时间列表
        根据当前时间戳和给定的时间增量列表，计算每个警报的绝对时间戳，并设置警报列表。
        增量列表必须严格递增，否则会抛出异常。
        Args:
            delta_list (list[int/float]): 以秒为单位的时间增量列表，必须保持递增顺序。
                列表中的每个增量会被加到当前时间戳上生成警报时间。
                例如：[5, 10, 20] 表示相对当前时间5秒、10秒、20秒后的警报
        Raises:
            ValueError: 如果满足以下任一条件时抛出：
                - 当前时间戳未设置（_current_timestamp为None）
                - delta_list中的时间增量不是严格递增
        Notes:
            最终会通过set_alarm_list方法设置计算得到的绝对时间戳列表
        """
        if self._current_timestamp is None:
            raise ValueError("Delta alarms cannot be used until current"
            " timestamp has been set")
        prev_delta=None
        alarm_list = []
        for delta in delta_list:
            if prev_delta!=None and delta<prev_delta:
                raise ValueError("Deltas have to increase")
            alarm_list.append(self._current_timestamp+delta)
            prev_delta = delta
        self.set_alarm_list(alarm_list)
          
    def is_it_time(self, current_timestamp):
        """检查当前时间是否达到或超过已注册的任一警报时间。
        Args:
            current_timestamp (float): 要检查的当前时间戳（自纪元起的秒数）
        Returns:
            int: 已触发的警报数量（current_timestamp >= alarm）。
                同时会更新内部警报列表，移除已触发的警报
        Note:
            该方法会修改内部警报列表，移除所有已检查过的触发警报
        """
        # 调用父类方法确保继承链正确执行
        super(MultiAlarmTimer, self).is_it_time(current_timestamp)
        pos=0
        for alarm in self._alarm_list:
            if current_timestamp >= alarm:
                pos+=1
            else:
                break
        self.set_alarm_list(self._alarm_list[pos:])
        return pos

    def can_be_purged(self):
        """
        检查当前实例是否满足清除条件
        通过验证内部告警列表是否为空来判断该实例是否可以被安全清除。
        当告警列表中没有未处理项时，系统允许执行清除操作。
        参数:
            self: 当前对象实例，用于访问实例属性_alarm_list
        返回:
            bool: 当告警列表为空时返回True，表示可清除；否则返回False
        """
        return len(self._alarm_list)==0
            
class RepeatingAlarmTimer(PatternTimer):  
    """扩展MultiAlarmTimer，每隔一段时间触发一次告警
    """
    
    def set_alarm_period(self, period):
        """以秒为单位设置重复告警周期。
        Args:
            - period: 两次警报之间的秒数。当第一次调用时，告警将在以后的周期秒内触发。
        """
        self._alarm_period = period   
        self._last_timestamp=self._current_timestamp       
        
    def is_it_time(self, current_timestamp):
        """判断是否到达报警触发时间，并计算触发次数
        通过对比当前时间戳与上次报警时间的关系，判断是否满足触发条件。
        当current_timestamp首次超过（最后报警时间 + 报警周期）时返回触发次数，
        后续在同一个周期窗口内不再重复触发
        Args:
            current_timestamp (int/float): 当前时间戳，单位需与_alarm_period保持一致
        Returns:
            int: 返回触发次数（>=1表示需要触发），0表示不满足触发条件
        """
        # 将当前时间对齐到最近的周期起始点（向下取整）
        adjusted_ct = (int(current_timestamp/self._alarm_period) 
                       * self._alarm_period)
        adjusted_lt = (int(self._last_timestamp/self._alarm_period+1) 
                       * self._alarm_period)
        
        if (current_timestamp==self._last_timestamp 
            or not (adjusted_ct >= adjusted_lt)):
            return 0
        self._last_timestamp=current_timestamp
        trigger_count = (adjusted_ct-adjusted_lt)/self._alarm_period + 1
        return trigger_count
    def can_be_purged(self):
        return False
