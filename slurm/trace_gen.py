""" 为slurm模拟器生成跟踪的库。

仿真跟踪由以下部分组成
- Job list: 模拟过程中需要提交的作业。
- User list: 用户列表。用户：userid，代表提交作业的用户。
- QOS list: 跟踪中存在的QOS策略列表。

作业列表是一个包含作业记录列表的二进制文件。每个作业记录被编码为一个结构体，格式如下：

"""

import struct
import os
import subprocess

class TraceGenerator(object):
    """该类来生成模拟器跟踪的所有元素。
        Qos和用户列表是根据提交作业中检测到的用户和Qos生成的。
    """
    def __init__(self):
        """
        初始化作业调度系统的核心统计组件
        属性说明：
            - 数据容器：
                  _job_list: list               # 存储作业对象的列表
                  _user_list: list              # 存储用户对象的列表
                  _account_list: list           # 存储账户对象的列表
                  _qos_list: list               # 存储服务质量(QoS)策略对象的列表
            - 时间统计：
                  _first_submit_time: int       # 记录系统首个作业提交时间戳（单位秒）
                  _last_submit_time: int        # 记录系统最后一个作业提交时间戳（单位秒）
            - 核心时间统计：
                  _submitted_core_s: int        # 累计已提交但未完成的计算核心秒数
                  _total_submitted_core_s: int  # 历史总提交计算核心秒数（包含已完成）
                  _total_actual_core_s: int     # 历史总实际消耗的计算核心秒数
                  _total_actual_wf_core_s: int  # 历史总有效工作流核心秒数（可能指扣除失败/重试后的有效值）
            - 衰减窗口统计：
                  _decay_window_size: int       # 时间衰减窗口的配置大小（单位秒）
                  _decay_window_stamps: list    # 衰减窗口时间轴（存储时间戳分段）
                  _decay_window_values: list    # 对应时间窗口的统计值（用于滑动窗口计算）
        """
        self._job_list = []
        self._user_list = []
        self._account_list = []
        self._qos_list = []
        self._submitted_core_s = 0
        self._first_submit_time = -1
        self._last_submit_time = -1
        self._decay_window_size = -1
        self._decay_window_stamps = None
        self._decay_window_values = None
        self._total_submitted_core_s = 0
        self._total_actual_core_s = 0
        self._total_actual_wf_core_s=0

    def add_job(self, job_id, username, submit_time, duration, wclimit, tasks,
                cpus_per_task, tasks_per_node, qosname, partition, account,
                reservation="", dependency="", workflow_manifest=None,
                cores_s=None, ignore_work=False, real_core_s=None):
        """
        添加作业记录到调度系统
        参数:
            job_id: str - 作业唯一标识符
            username: str - 提交作业的用户名
            submit_time: float - 作业提交时间（时间戳格式）
            duration: int - 作业实际运行时长（秒）
            wclimit: int - 作业时间限制（分钟）
            tasks: int - 总任务数量
            cpus_per_task: int - 单个任务需要的CPU核心数
            tasks_per_node: int - 单个节点上运行的任务数
            qosname: str - 服务质量等级名称
            partition: str - 调度分区名称
            account: str - 作业所属账户
            reservation: str - 资源预留名称（可选）
            dependency: str - 作业依赖关系（可选）
            workflow_manifest: Any - 工作流描述信息（可选）
            cores_s: float - 预计算的核心秒数（可选，自动计算时设为None）
            ignore_work: bool - 是否跳过工作量计算（默认False）
            real_core_s: float - 实际核心秒数（可选）
        返回值:
        None - 无返回值
        """
        """Ad a job with the observed characteristics. wclimit is in minutes"""
        # 将作业信息转换为标准格式并添加到作业列表
        self._job_list.append(get_job_trace(job_id=job_id, username=username,
                                            submit_time=submit_time,
                                            duration=duration, wclimit=wclimit,
                                            tasks=tasks,
                                            cpus_per_task=cpus_per_task,
                                            tasks_per_node=tasks_per_node,
                                            qosname=qosname, partition=partition,
                                            account=account,
                                            reservation=reservation,
                                            dependency=dependency,
                                            workflow_manifest=workflow_manifest))

        # 维护用户、账户和QoS的独立列表
        if not username in self._user_list:
            self._user_list.append(username)

        if not account in self._account_list:
            self._account_list.append(account)

        if not qosname in self._qos_list:
            self._qos_list.append(qosname)

        # 核心秒数计算逻辑：取时间限制和实际运行时长的最小值进行计算
        if cores_s is None:
            cores_s = min(wclimit * 60, duration) * tasks * cpus_per_task
        # 添加工作量统计（除非明确要求忽略）
        if not ignore_work:
            # 判断是否为工作流作业：检查工作流描述的有效性
            is_workflow = (workflow_manifest and (workflow_manifest[0] != "|" or
                                                  len(workflow_manifest) > 1))
            self._add_work(submit_time, cores_s, real_work=real_core_s,
                           is_workflow=is_workflow)

    def _add_work(self, submit_time, work, real_work=None, is_workflow=False):
        """记录作业的核心小时使用情况，并维护时间衰减窗口数据
        Args:
            submit_time (float): 作业提交的UNIX纪元时间戳
            work (float): 作业请求的核心秒数(CPU核心数×运行秒数)
            real_work (float, optional): 工作流作业的实际核心秒数(当is_workflow为True时使用)
            is_workflow (bool, optional): 标识是否为工作流类型作业
        [核心功能]
        1. 累积计算各类核心时间指标
        2. 维护基于时间衰减窗口的提交记录
        3. 跟踪首次/最后提交时间戳
        """
        # 累积常规提交的核心时间
        self._submitted_core_s+=work

        # 处理工作流作业的特殊累计逻辑：使用real_work作为真实工作量
        if real_work is not None:
            self._total_submitted_core_s+=real_work
        else:
            self._total_submitted_core_s+=work

        # 累积实际使用核心时间（区分工作流类型）
        self._total_actual_core_s+=work
        if is_workflow:
            self._total_actual_wf_core_s+=work

        # 更新初始和最新提交时间戳
        if (self._first_submit_time == -1):
            self._first_submit_time = submit_time
        self._last_submit_time=submit_time

        # 时间衰减窗口处理逻辑
        if self._decay_window_size>0:
            # 将新记录加入滑动窗口队列
            self._decay_window_stamps.append(submit_time)
            self._decay_window_values.append(work)
            # 移除超出时间窗口的过期记录
            while (self._decay_window_stamps and
                   self._decay_window_stamps[0] < 
                     (submit_time-self._decay_window_size)):
                self._submitted_core_s -= self._decay_window_values[0]
                self._decay_window_stamps = self._decay_window_stamps[1:]
                self._decay_window_values = self._decay_window_values[1:]
                self._first_submit_time = self._decay_window_stamps[0]
   
    def reset_work(self):
        """
        重置作业统计相关指标到初始状态
        用于清空当前作业周期内的所有运行时统计信息，包括：
        - 时间相关记录（首次/末次提交时间）
        - 衰减窗口数据记录
        - 核心秒数累计指标
        不包含业务逻辑重置，仅处理统计指标清零操作
        参数: 无
        返回值: 无
        """
        self._first_submit_time=-1
        self._last_submit_time=-1
        self._submitted_core_s=0
        self._decay_window_stamps=[]
        self._decay_window_values=[]
        self._total_submitted_core_s=0
        self._total_actual_core_s=0
        self._total_actual_wf_core_s=0
          
    def get_share_wfs(self):
        """
        计算工作流核心时间占总核心时间的比例
        方法说明:
            当存在有效的总核心时间时，返回工作流核心时间与总核心时间的比值；
            若总核心时间为零或不存在，则返回None
        返回值:
            float | None: 返回工作流核心时间占比（范围0.0-1.0），
                        当总核心时间无效时返回None
        """
        if not self._total_actual_core_s:
            return None
        return (float(self._total_actual_wf_core_s)
                / float(self._total_actual_core_s))
    
    def set_submitted_cores_decay(self, decay_window_size):
        """配置提交核心数的衰减窗口参数
        设置用于核心提交量衰减计算的时间窗口大小，并初始化相关跟踪缓冲区。
        必须在轨迹生成开始前调用本方法。
        参数:
            decay_window_size (int): 衰减窗口的时间长度（单位：秒）。
                当值 <= 0 时禁用衰减功能，表示不考虑历史数据的滑动窗口计算。
        注意:
            调用本方法将重置内部状态缓冲区 _decay_window_stamps 和 _decay_window_values
            该方法没有返回值
        """
        self._decay_window_size=decay_window_size
        self._decay_window_stamps = []
        self._decay_window_values = []
        
    def get_submitted_core_s(self):
        """获取已提交的核心秒数及作业时间跨度
        计算当前已提交的作业核心秒数总量，以及首尾两个作业提交时间的差值。
        当设置了set_submitted_cores_decay时，仅统计配置时间范围内的作业。
        Returns:
            tuple: 包含两个元素的元组
                - int: 累计已提交的核心秒数（core-seconds）
                - int: 首个作业与最后作业的提交时间差（秒），保证最小值为1秒
        实现说明：
            返回值中的时间差使用max(1,...)确保非零值，避免除零错误等情况
        """
        return  (self._submitted_core_s,
                 max(1,self._last_submit_time-self._first_submit_time))
    
    def get_total_submitted_core_s(self):
        return self._total_submitted_core_s
    
    def get_total_actual_cores_s(self):
        return self._total_actual_core_s;
        
    def dump_trace(self, file_name):
        """将任务列表转储到指定文件中。
        参数:
            file_name (str): 输出文件的路径名称。函数会创建或覆盖该文件
        返回值:
            None: 该函数无返回值，执行结果通过生成输出文件体现
        """
        f = open(file_name, 'w')
        for job in self._job_list:
            f.write(job)
        f.close()
    
    def dump_users(self, file_name, extra_users=[]):
        """将用户列表以 user:userid 格式逐行写入文件
        Args:
            file_name (str): 输出文件的路径
            extra_users (list, optional): 需要追加到主用户列表后的额外用户列表
                默认值为空列表，元素应为"user:userid"格式字符串
        Returns:
            None: 结果直接写入文件，无返回值
        """
        # 生成从1024开始的连续用户ID序列
        start_count = 1024
        user_ids=range(start_count, start_count+len(self._user_list))
        # 处理主用户列表：若缺少冒号分隔符则追加生成ID
        f = open(file_name, 'w')
        for (user, userid) in zip(self._user_list,user_ids):
            if not (":" in user):
                user+=":"+str(userid)
            f.write(user+"\n")
        # 追加额外用户（保持原始格式不变）
        for user in extra_users:
            f.write(user+"\n")
        f.close()
    
    def dump_qos(self, file_name):
        """将QOS策略列表转储到指定文件，每行写入一个QOS名称
        参数:
            file_name (str): 输出文件路径，用于保存QOS策略名称列表
        返回值:
            None: 本方法不返回任何值
        """
        f = open(file_name, 'w')
        for qos in self._qos_list:
            f.write(qos+'\n') 
        f.close()
    
    def free_mem(self):
        del self._job_list
        self._job_list=[]
    

def get_job_trace(job_id, username, submit_time, duration, wclimit,tasks,
                  cpus_per_task,tasks_per_node, qosname, partition, account,
                  reservation="", dependency="", workflow_manifest=None):
    """生成符合C结构体job_trace_t格式的二进制数据包
    根据输入的作业信息参数，按照指定的内存布局进行二进制数据打包。
    支持普通作业和工作流作业两种打包格式，通过workflow_manifest参数区分。
    Args:
        job_id (int): 作业唯一标识符
        username (str): 提交作业的用户名（最长30字符）
        submit_time (int/long): 作业提交时间（相对/绝对时间）
        duration (int): 作业实际运行时间（秒）
        wclimit (int): 作业时间限制（分钟）
        tasks (int): 作业总任务数
        cpus_per_task (int): 每个任务分配的CPU数
        tasks_per_node (int): 每个节点运行的任务数
        qosname (str): 服务质量名称（最长30字符）
        partition (str): 使用的计算分区（最长30字符）
        account (str): 计费账户名（最长30字符）
        reservation (str, optional): 资源预留名称（最长30字符）
        dependency (str, optional): 作业依赖关系描述（最长1024字符）
        workflow_manifest (str, optional): 工作流描述信息（存在时表示工作流作业）
    Returns:
        bytes: 打包后的二进制数据，符合C结构体job_trace_t的内存布局：
            - 普通作业格式："i30sliii30s30s30sii30s1024sP"
            - 工作流作业格式："li30sliii30s30s30sii30s1024sP1024sP"
            包含特殊头部标识0xFFFFFFFF和工作流描述字段
    """
    if workflow_manifest is None:
        # 打包普通作业数据格式
        buf=struct.pack("i30sliii30s30s30sii30s1024sP",job_id, username,
                        submit_time, 
                        duration, wclimit, tasks, qosname, partition, account, 
                        cpus_per_task, tasks_per_node, reservation, dependency,
                        0)
    else:
        # 打包工作流作业数据格式，包含特殊头部和工作流描述
        buf=struct.pack("li30sliii30s30s30sii30s1024sP1024sP",0xFFFFFFFF,
                        job_id, username,
                        submit_time, 
                        duration, wclimit, tasks, qosname, partition, account, 
                        cpus_per_task, tasks_per_node, reservation, dependency,
                        0, workflow_manifest, 0)
    
   
    return buf


def extract_task_info(field):
    """
    从SLURM任务格式字符串中提取任务配置信息
    参数:
        field (str): SLURM任务格式字符串，格式为"num_tasks(tasks_per_node,cores_per_task)"
                    示例: "23(2,1)" 表示23个任务，每个节点2个任务，每个任务1个CPU核心
    返回:
        tuple: 包含三个整数值的元组 (num_tasks, tasks_per_node, cores_per_task)
    """
    # 解析总任务数（括号前的部分）
    num_tasks = int(field.split("(")[0])

    # 提取括号内的配置参数并分割处理
    # 解析每个节点的任务数和每个任务的CPU核心数
    tasks_per_node =int(field.split("(")[1].split(")")[0].split(",")[0])
    cores_per_task =int(field.split("(")[1].split(")")[0].split(",")[1])
    return num_tasks, tasks_per_node, cores_per_task

def extract_records(file_name="test.trace", 
                    list_trace_location="./list_trace"):
    """解析二进制作业跟踪文件并提取作业记录信息
    参数:
        file_name (str, optional):
            要解析的跟踪文件路径，默认为"test.trace"
        list_trace_location (str, optional):
            用于解析二进制文件的list_trace工具路径，默认为"./list_trace"
    返回:
        list[dict]:
            包含作业记录的字典列表，每个字典包含以下键：
            - JOBID: 作业唯一标识符
            - USERNAME: 提交用户
            - PARTITION: 使用的计算分区
            - ACCOUNT: 计费账户
            - QOS: 服务质量策略
            - SUBMIT: 提交时间戳
            - DURATION: 实际运行时长（秒）
            - WCLIMIT: 作业时间限制（分钟）
            - TASKS: 任务配置字符串（原始格式）
            - RES: 资源预留信息（可选）
            - DEP: 作业依赖关系（可选）
            - NUM_TASKS: 解析后的总任务数
            - TASKS_PER_NODE: 每个节点任务数
            - CORES_PER_TASK: 每个任务CPU核心数
    处理流程:
        1. 调用外部list_trace工具解析二进制文件
        2. 逐行处理工具输出，识别表格标题和数据行
        3. 解析基础字段和扩展字段(DEP/RES)
        4. 从TASKS字段提取详细任务配置信息
    """

    # 启动外部解析工具并获取输出流
    print[list_trace_location, '-w', file_name]
    proc = subprocess.Popen([list_trace_location, '-w', file_name],
                            stdout=subprocess.PIPE)
    still_header = True
    col_names = None

    records_list = []

    # 逐行处理工具输出内容
    while True:
        line=proc.stdout.readline()
        if line is None or line=="":
            break
        # 识别表格标题行（包含JOBID的列名行）
        if "JOBID" in line and col_names is None:
            col_names=line.split()
        # 处理数据行（表头之后的内容）
        elif not still_header:
            col_values=line.split()
            record=dict()

            # 构建基础字段映射（列名->列值）
            for (key, value) in zip(col_names, col_values):
                record[key]=value
            # 解析扩展字段（RES/DEP格式为键=值）
            if (len(col_values) > len(col_names)):
                extra_values=col_values[len(col_names):]
                for extra in extra_values:
                    words=extra.split("=")
                    if (words[0]=="DEP" or words[0]=="RES") and len(words)==2:
                        record[words[0]]=words[1]
            # 提取任务配置详细信息
            num_tasks, tasks_per_node, cores_per_task=extract_task_info(
                                                            record["TASKS"])
            record["NUM_TASKS"] = num_tasks
            record["TASKS_PER_NODE"] = tasks_per_node
            record["CORES_PER_TASK"] = cores_per_task            
            records_list.append(record)
        # 检测表头结束分隔符（====行）
        elif "====" in line:
            still_header=False
    return records_list  
