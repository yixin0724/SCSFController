
import json
import pygraphviz as pgv
import xml.etree.ElementTree as ET
import sys

def convert_xml_wf_to_json_manifest(xml_route, json_route, grouped_jobs=[],
                                    max_cores=None, namespace=None):
    """ 从xml文件中读取工作流定义，转换为工作流感知的backfile json manifest，并将其写入另一个文件。
    Args:
    - xml_route: 文件系统路由字符串指向一个包含工作流定义的XML文件，格式为：
        https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
    - json_route: 文件系统路由字符串，指向将在哪里创建一个json文件，该文件包含read xml_route的Manifest版本。
    - grouped_jobs: 包含已读xml中存在的作业名称的字符串列表。
        对于grouped_jobs中的每个作业名称，具有该名称的所有作业将被分组到单个作业中。
    """
    print "Loading XML:", xml_route
    xml_wf = ET.parse(xml_route)
    print "XML Loaded"
    if namespace is None:
        namespace=_get_namespace(xml_wf)
    print "Getting Jobs and dependencies"
    jobs, deps = _get_jobs_and_deps(xml_wf, namespace=namespace)
    print "Jobs and dependencies extraced: {0} jobs and {1} deps".format(
                                len(jobs), len(deps))          
    del xml_wf    
    print "XML tree deallocated"
    print "Fusing jobs"
    jobs, job_fusing_dic= _fuse_jobs(jobs, grouped_jobs,
                                     max_cores=max_cores)
    print "Fusing jobs Done", job_fusing_dic
    print "Fusing deps", deps
    deps = _fuse_deps(deps, job_fusing_dic)
    print "Fusing deps Done", deps
    print "Sequence fusing"
    _fuse_sequence_jobs(jobs, deps)
    print "Sequence fusing Done", deps
    print "Renaming jobs"
    new_job_names=_get_jobs_names(jobs,deps)
    jobs, deps = _rename_jobs(jobs, deps, new_job_names)
    print "Renaming jobs Done"
    manifest_dic=_encode_manifest_dic(jobs, deps)
    f_out=open(json_route, "wb")
    json.dump(manifest_dic, f_out)   
    f_out.close()
        

def _encode_manifest_dic(jobs, deps):
    """将作业和依赖关系编码为manifest字典
    Args:
        jobs: list 任务对象列表，包含需要调度的基础作业信息
        deps: list 依赖关系列表，描述任务间的执行顺序约束
    Returns:
        dict 包含完整执行计划的字典，包含以下键：
            - tasks: 转换后的标准化任务列表
            - resource_steps: 带资源分配的执行步骤
            - max_cores: 最大并发计算核心需求
            - total_runtime: 工作流总执行时间
            - dot_dag: DOT格式的有向无环图表示
    """
    manifest_dic = {}

    # 转换原始任务数据为标准化结构
    manifest_dic["tasks"] = _produce_tasks(jobs)
    # 生成带资源分配的执行步骤时间轴
    manifest_dic["resource_steps"] = _produce_resource_steps(jobs, deps)
    # 计算所有步骤中最大并发核心需求
    manifest_dic["max_cores"] = max([step["num_cores"]
                                    for step in manifest_dic["resource_steps"]])

    # 取最后一个资源步骤的结束时间作为总运行时间
    manifest_dic["total_runtime"] = manifest_dic["resource_steps"][-1][
                                                                    "end_time"]
    # 生成DOT格式的工作流有向无环图
    manifest_dic["dot_dag"] = _produce_dot_graph(jobs, deps)
    return manifest_dic

def _produce_dot_graph(jobs, deps):
    """根据作业数据及其依赖关系生成DOT格式的有向图

    Args:
        jobs: 作业对象列表，每个作业必须包含"id"字段用于节点标识
        deps: 依赖关系字典，结构为{源作业id: [目标作业id1, 目标作业id2,...]}

    Returns:
        str: DOT格式的图形描述字符串，可用于可视化或文件存储
    """
    # 创建有向图对象并设置方向属性
    G=pgv.AGraph(directed=True)
    # 添加所有作业节点（使用作业id作为节点标识）
    for job in jobs:
        G.add_node(job["id"])

    # 遍历依赖字典，添加所有边关系
    for (src, dst_list) in deps.iteritems():
        for dst in dst_list:
            G.add_edge(src, dst)
    # 将图对象转换为标准DOT格式字符串
    return G.to_string()
    
def _produce_tasks(jobs):
    """
    将作业配置列表转换为任务配置字典列表

    参数:
        jobs (list[dict]): 原始作业配置字典列表。每个字典必须包含以下键:
            id (str): 作业唯一标识符
            cores (int): 作业所需CPU核心数
            runtime (int): 预估运行时间(秒)

    返回值:
        list[dict]: 任务配置字典列表，每个字典包含:
            id (str): 继承作业ID
            number_of_cores (int): 继承作业核心需求
            name (str): 复用作业ID作为任务名称
            execution_cmd (str): 生成格式为"./{job_id}.py"的执行命令
            runtime_limit (int): 实际运行时间限制(增加60秒缓冲)
            runtime_sim (int): 保持原始预估运行时间
    """
    tasks = []

    # 转换每个作业配置为任务配置字典
    for job in jobs:
        # 构造基础任务配置，补充运行时间相关参数
        task = {"id":job["id"],
                "number_of_cores":job["cores"],
                "name":job["id"],
                "execution_cmd": "./{0}.py".format(job["id"]),  # 生成可执行命令路径
                "runtime_limit": job["runtime"] + 60,  # 增加60秒作为运行缓冲时间
                "runtime_sim": job["runtime"]  # 保持原始运行时间用于模拟
                }
        tasks.append(task)
    return tasks

def get_inverse_deps(deps):
    """
    构建逆向依赖关系映射
    将原始依赖关系{源: [目标列表]}转换为逆向依赖关系{目标: [源列表]}
    Args:
        deps: dict, 原始依赖字典，格式为{src: [dst1, dst2, ...]}
    Returns:
        dict: 逆向依赖字典，格式为{dst: [src1, src2, ...]}，表示每个目标节点被哪些源节点依赖
    """
    inverse_deps = {}
    # 遍历原始依赖中的每个源节点及其依赖列表
    for (src, dst_list) in deps.iteritems():
        # 为每个目标节点建立逆向映射
        for dst in dst_list:
            # 初始化目标节点的逆向依赖列表（如果不存在）
            if not dst in inverse_deps.keys():
                inverse_deps[dst] = []
            # 将当前源节点添加到目标节点的逆向依赖列表
            inverse_deps[dst].append(src)

    return inverse_deps


def _produce_resource_steps(jobs, deps):
    """根据任务依赖关系生成资源分配的时间步骤
    根据任务依赖关系模拟调度过程，计算每个时间段内占用的计算核心数
    Args:
        jobs: 任务字典列表，每个字典必须包含：
            - id: 任务唯一标识
            - cores: 任务需要的计算核心数
            - runtime: 任务需要运行时间
            可能包含的字段：
            - dst: 后续依赖该任务的任务列表
        deps: 依赖关系字典，键为任务id，值为该任务依赖的前置任务id列表
    Returns:
        resource_steps: 资源分配步骤字典列表，每个字典包含：
            - num_cores: 该时间段占用的总核心数
            - end_time: 该时间段的结束时间戳
    """
    resource_steps = []

    # 生成逆向依赖关系映射（任务id -> 依赖该任务的任务id列表）
    inverse_deps = get_inverse_deps(deps)

    # 初始化任务关系：构建双向任务依赖链接
    for job in jobs:
        job["completed"] = False
        job["src"] = []  # 存储前置任务对象
        if not "dst" in job.keys():
            job["dst"] = []  # 存储后续任务对象

        # 建立逆向依赖对象关系
        if job["id"] in inverse_deps.keys():
            for src_job in jobs:
                if src_job["id"] in inverse_deps[job["id"]]:
                    job["src"].append(src_job)  # 添加前置任务
                    # 在源任务中维护后续任务列表
                    if not "dst" in src_job.keys():
                        src_job["dst"] = []
                    src_job["dst"].append(job)

    # 初始化第一批可执行任务（无前置依赖的任务）
    current_jobs = [job for job in jobs if job["src"] == []]
    current_time = 0
    # 设置初始任务的执行时间段
    for job in current_jobs:
        job["start"] = current_time
        job["end"] = current_time + job["runtime"]

    # 主调度循环：处理正在运行的任务队列
    while not current_jobs == []:
        # 计算当前时间段的资源占用
        current_cores = sum(job["cores"] for job in current_jobs)
        # 确定当前批次的最早结束时间
        next_current_time = min(job["end"] for job in current_jobs)
        # 记录当前时间段资源使用情况
        resource_steps.append({"num_cores": current_cores,
                               "end_time": next_current_time})
        # 分离即将完成的任务和需要继续运行的任务
        ending_jobs = [job for job in current_jobs
                       if job["end"] == next_current_time]
        remaining_jobs = [job for job in current_jobs
                          if job["end"] > next_current_time]

        # 处理任务完成后的后续调度
        for ending_job in ending_jobs:
            ending_job["completed"] = True
            for new_job in ending_job["dst"]:
                if (_job_can_run(new_job)):  # 检查后续任务是否满足运行条件
                    # 设置新任务的执行时间段
                    new_job["start"] = next_current_time
                    new_job["end"] = new_job["start"] + new_job["runtime"]
                    remaining_jobs.append(new_job)
        # 更新当前运行任务队列
        current_jobs = remaining_jobs
    return resource_steps


def _job_can_run(job):
    """
     判断指定任务是否满足运行条件
     当任务没有前置依赖任务时可以直接运行。若存在前置任务，则要求所有前置任务必须已完成。
     Args:
         job: dict 任务对象字典，需包含以下结构：
             - src: list 前置任务对象列表，每个对象应包含'completed'状态字段
             - 其他可能的任务属性（本函数不依赖）
     Returns:
         bool: 返回True表示任务可以运行，False表示存在未完成的前置任务

     """
    # 无前置任务时直接允许运行
    if job["src"] == []:
        return True

    # 检查所有前置任务的完成状态
    # 遇到任意未完成的前置任务立即返回False
    for src_job in job["src"]:
        if not src_job["completed"]:
            return False

    # 所有前置任务均已完成
    return True
        
        

"""
函数获取和操作作业列表和依赖项字典
"""        
def _rename_jobs(jobs, deps, new_job_name):
    """Renames jobs (in a jobs list dependencies dict) according to the
    new_job_name dict mapping."""
    for job in jobs:
        job["id"] = new_job_name[job["id"]]
    
    new_deps = {}
    for (src,dst) in deps.iteritems():
        new_deps[new_job_name[src]] = [new_job_name[x] for x in dst]
    
    return jobs, new_deps
        
        
    
    
def _get_jobs_names(jobs, deps):
    """
    生成作业ID的拓扑排序编号
    参数:
        jobs: list[dict] - 作业字典列表，每个字典需包含"id"字段
        deps: dict - 依赖关系字典，格式为{job_id: [dependent_job_ids]}
    返回值:
        dict - 包含新旧ID映射的字典，格式为{原始job_id: 新ID}
    实现说明:
        通过广度优先搜索(BFS)遍历依赖树，为无依赖的作业生成S0、S1...的连续编号
    """
    """Returns a list of dict {"job_id":"Si"}, where Si is the new id of job
    with id job_id."""
    # 找出所有被依赖的作业ID
    starting_jobs=[]
    all_dest=[]
    for some_dep in deps.values():
        all_dest+=some_dep
    all_dest = list(set(all_dest))
    for job in jobs:
        job_id=job["id"]
        if not job_id in all_dest:
            starting_jobs.append(job_id)
    count=0
    new_ids={}
    
    while not starting_jobs == []:
        next_step_jobs=[]
        for job_id in starting_jobs:
            if not job_id in new_ids.keys():
                new_ids[job_id]="S{0}".format(count)
                count+=1
                if job_id in deps.keys():
                    next_step_jobs+=deps[job_id]
        next_step_jobs=list(set(next_step_jobs))
        starting_jobs=next_step_jobs
    return new_ids

def get_tag(tag, namespace=None):
    if namespace is None:
        return tag
    else:
        return "{"+namespace+"}"+tag

def _get_namespace(xml_wf):
    root_node = xml_wf.getroot()
    tag = root_node.tag
    pos1  = tag.find("{")
    pos2  = tag.find("}")
    if (pos1!=-1 and pos2!=-1):
        return tag[pos1+1:pos2]
    return None
def get_runtime(cad):
    cad=cad.replace(",",".")
    return float(cad)
def _get_jobs_and_deps(xml_wf, namespace=None):
    """解析XML工作流定义，提取作业信息和依赖关系
    Args:
        xml_wf (ElementTree.ElementTree): 包含工作流定义的XML元素树
        namespace (str, optional): XML命名空间URI，默认为None
    Returns:
        tuple: 包含两个元素的元组
            - list: 作业字典列表，每个字典包含id/name/runtime/cores字段
            - dict: 依赖关系字典，键为源作业ID，值为目标作业ID列表
    Raises:
        ValueError: 当根节点不是预期的adag标签时抛出
    """
    root_node = xml_wf.getroot()
    if root_node.tag!=get_tag("adag", namespace=namespace):
        raise ValueError("XML dag format: unexpected root node {0}".format(
                         root_node.tag))

    jobs = []
    deps = {}
    total=len(root_node)
    count=0
    for child in root_node:
        if child.tag==get_tag("job", namespace=namespace):
            atts = child.attrib
            num_cores=1
            if "cores" in atts.keys():
                num_cores=int(atts["cores"])
            jobs.append({"id": atts["id"],
                        "name": atts["name"],
                        "runtime":get_runtime(atts["runtime"]),
                        "cores":num_cores})

        if child.tag==get_tag("child", namespace=namespace):
            dest_job=child.attrib["ref"]
            for dep_origin in child:
                origin_job=dep_origin.attrib["ref"]
                try:
                    the_job_dep=deps[origin_job]
                except:
                    the_job_dep = []
                    deps[origin_job]=the_job_dep
                the_job_dep.append(dest_job)
        count+=1
        progress=count*100/total
        sys.stdout.write("Processed: %d%%   \r" % (progress) )
        sys.stdout.flush()
    return jobs, deps
    
def _reshape_job(job, max_cores):
    """
    调整作业配置以适配最大核心数限制，并计算新的总运行时间

    参数：
        job (dict): 包含作业信息的字典，必须包含以下键：
            - task_count (int): 任务总数
            - cores (int): 作业所需总核心数
            - acc_runtime (float): 作业累计运行时间
        max_cores (int): 单节点最大可用核心数

    返回值：
        tuple: 包含两个元素的元组：
            - float: 调整后的预估总运行时间
            - int: 适配后的实际可用的最大核心数（调整为 cores_per_task 的整数倍）
    """
    total_tasks = job["task_count"]
    # 计算单任务资源需求（核心数）
    cores_per_task = job["cores"] / job["task_count"]

    # 将最大核心数向下对齐到单任务核心数的整数倍
    # 例如：cores_per_task=3，max_cores=10 时，实际可用核心数为9
    max_cores = (max_cores // cores_per_task) * cores_per_task

    # 计算单任务理论运行时间和每批可并行处理的任务量
    runtime_per_task = job["acc_runtime"] / job["task_count"]
    tasks_per_step = max_cores / cores_per_task  # 因max_cores已对齐，此处必为整数

    # 通过时间累加算法计算总运行时间
    # 每批任务处理耗时等于单个任务运行时间（假设完美并行）
    new_runtime = 0
    while total_tasks > 0:
        new_runtime += runtime_per_task
        # 处理完一批任务后减少剩余任务量（tasks_per_step可能为浮点数时自动向下取整）
        total_tasks -= tasks_per_step

    return new_runtime, max_cores


def _fuse_sequence_jobs(jobs, deps):
    """融合具有顺序依赖关系的作业任务，减少任务调度层级
    如果a.cores==b，则融合任意两个作业a， b。核心和b只依赖于a
    当满足以下条件时融合两个作业任务：
    1. 目标作业仅有一个依赖项
    2. 源作业仅被当前目标作业依赖
    3. 两个作业的核心资源配置相同

    Args:
        jobs: list[dict] 作业对象列表，每个作业包含cores等配置信息
        deps: dict 依赖关系字典，格式为{job_id: [dependency_job_ids]}

    Returns:
        无返回值，直接修改输入的jobs和deps参数
    """

    # 循环执行融合操作直到无法继续合并
    no_changes = False
    while not no_changes:
        no_changes = True

        # 构建逆向依赖关系字典（被依赖项 -> 依赖方列表）
        inverse_deps = get_inverse_deps(deps)

        # 遍历所有可能的目标作业及其依赖项
        for (job_dst, dep_list) in inverse_deps.iteritems():
            # 仅处理单依赖的作业链
            if len(dep_list) == 1:
                job_orig = dep_list[0]

                # 检查是否满足融合条件：单依赖关系且核心数相同
                if (len(deps[job_orig]) == 1 and
                        _get_job(jobs, job_orig)["cores"] ==
                        _get_job(jobs, job_dst)["cores"]):
                    # 执行作业融合操作
                    _fuse_two_jobs_sequence(jobs, deps, job_orig, job_dst)
                    no_changes = False
                    break  # 重新扫描修改后的依赖关系


def _fuse_two_jobs_sequence(jobs, deps, job_orig, job_dst):
    """融合两个关联任务，将目标任务合并到原始任务中
    参数:
        jobs (list[dict]): 任务列表，每个任务包含job_id和runtime等字段
        deps (dict): 任务依赖关系字典，键为job_id，值为前置依赖列表
        job_orig (str): 原始任务的job_id，将保留并合并目标任务的运行时间
        job_dst (str): 目标任务的job_id，将被合并后从jobs列表移除
    返回值:
        无返回值，直接修改传入的jobs和deps对象
    """
    # 获取两个任务对象的引用
    the_job_orig = _get_job(jobs, job_orig)
    the_job_dst = _get_job(jobs, job_dst)

    # 合并运行时间并移除目标任务
    the_job_orig["runtime"] += the_job_dst["runtime"]
    jobs.remove(the_job_dst)

    # 更新依赖关系链
    if job_dst in deps.keys():
        # 将目标任务的依赖转移给原始任务
        deps[job_orig] = deps[job_dst]
        del deps[job_dst]
    else:
        # 清除原始任务的依赖关系
        del deps[job_orig]


def _get_job(job_list, job_id):
    """
    根据给定的job_id在job_list列表中查找对应的job字典
    参数:
        job_list (list): 由job字典对象组成的列表，每个字典必须包含'id'键
        job_id (str/int): 需要查找的job唯一标识符
    返回值:
        dict/None: 匹配id的job字典对象，未找到时返回None并打印错误信息
    """
    for job in job_list:
        if job_id==job["id"]:
            return job
    print "JOB not found", job_id     
    return None

def extract_groups_cores(grouped_jobs, max_cores=None):
    """处理任务组名称列表，提取各组任务的最大核心数配置
    输入的任务组名称格式支持两种形式：
    - 任务名:核心数（如"task1:4"）
    - 纯任务名（此时使用默认max_cores参数值）
    Args:
        grouped_jobs: 任务组配置字符串列表，元素格式为：
            - "任务名"：使用默认max_cores参数值
            - "任务名:核心数"：显式指定核心数配置
        max_cores: 默认最大核心数配置，当任务组未指定核心数时使用。可选参数，默认None
    Returns:
        tuple: 包含两个元素的元组
            - new_groups: 提取后的纯任务名称列表（去除核心数配置部分）
            - max_cores_dic: 字典结构，键为任务组名称，值为对应的最大核心数配置
                当任务组字符串包含核心数时使用显式配置，否则使用max_cores参数值
    """
    new_groups = []
    max_cores_dic = {}
    for gr in grouped_jobs:
        if ":" in gr:
            tokens = gr.split(":")
            new_groups.append(tokens[0])
            max_cores_dic[tokens[0]]=(int(tokens[1]))
        else:
            new_groups.append(gr)
            max_cores_dic[gr]=max_cores
    return new_groups, max_cores_dic

def _fuse_jobs(job_list, grouped_jobs, max_cores=None):
    """融合相同名称的作业任务，返回新作业列表和任务融合映射字典
    Args:
        job_list: 原始作业列表，每个作业是包含核心数、运行时等参数的字典
        grouped_jobs: 需要合并的作业名称集合，同名的作业将被合并
        max_cores: 可选参数，全局默认的最大核心数限制，可被分组配置覆盖
    Returns:
        tuple: (new_job_list, job_fusing_dic)
            - new_job_list: 融合后的新作业列表
            - job_fusing_dic: 新作业ID到被融合原始作业ID列表的映射字典
    实现逻辑:
        1. 合并同名作业的核心数，取最大运行时，统计总运行时和任务数量
        2. 根据分组配置的最大核心数调整最终核心分配
    """
    job_dic={}
    new_job_list = []
    job_fusing_dic= {}

    # 解析分组配置和对应的最大核心数限制
    grouped_jobs, max_cores_dic=extract_groups_cores(grouped_jobs, max_cores)

    # 初始化任务融合记录字典
    for job_name in grouped_jobs:
        job_fusing_dic[job_name]=[]

    # 遍历处理原始作业列表
    for job in job_list:
        job_name=job["name"]
        the_job = None
        if job_name in grouped_jobs:
            if job_name in job_dic.keys():
                the_job=job_dic[job_name]
            if the_job is None:
                the_job = dict(job)
                the_job["cores"] = 0
                the_job["id"]=job_name
                the_job["acc_runtime"]=0
                the_job["task_count"]=0
                new_job_list.append(the_job)
                job_dic[job_name]=the_job
            job_fusing_dic[job_name].append(job["id"])
            the_job["cores"]+=job["cores"]
            the_job["runtime"]=max(the_job["runtime"], 
                                   job["runtime"])
            the_job["acc_runtime"]+=job["runtime"]
            the_job["task_count"]+=1
        else:
            new_job_list.append(job)

    # 调整合并后作业的核心分配
    for (job_name, the_job) in job_dic.iteritems():
        this_max_cores=max_cores
        if job_name in max_cores_dic.keys():
            this_max_cores=max_cores_dic[job_name]
        if this_max_cores:
            the_job["runtime"], the_job["cores"]=_reshape_job(the_job,
                                                            this_max_cores) 
        del the_job["acc_runtime"]
        del the_job["task_count"]
    
    return new_job_list, job_fusing_dic

def _fuse_deps(dep_dic, job_fusing_dic):
    """根据job_fusing_dic中的依赖来合并依赖关系。
    Args:
        dep_dic: 原始依赖字典，格式为{source_job: [dependent_jobs]}
        job_fusing_dic: 熔断映射字典，格式为{new_job: [old_jobs]}，表示旧job合并到新job
    Returns:
        dict: 熔断处理后的新依赖字典，格式为{fused_job: [fused_dependencies]}

    """
    inverse_fusing_dic={}
    for (new_dep, older_deps) in job_fusing_dic.iteritems():
        for dep in older_deps:
            inverse_fusing_dic[dep] = new_dep
    
    new_dep_dict={}
    # 首先转换源并连接dest
    # 处理每个依赖关系对
    for (source, dest) in dep_dic.iteritems():
        # 转换目标节点：将旧job替换为对应的新job
        new_dest=[]
        for one_dest in dest:
            try:
                one_dest=inverse_fusing_dic[one_dest]
            except:
                pass
            new_dest.append(one_dest)
        dest=list(set(new_dest))    # 去重处理后的目标节点

        # 转换源节点：将旧job替换为对应的新job
        try:
            source = inverse_fusing_dic[source]
        except:
            pass
        # 合并相同源节点的依赖关系，并去重
        try:
            new_dep_dict[source] += dest
            new_dep_dict[source]=list(set(new_dep_dict[source]))
        except:
            new_dep_dict[source] = dest
    return new_dep_dict
        