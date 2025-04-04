from commonLib.DBManager import DB
from orchestration.definition import (ExperimentDefinition, 
                                      GroupExperimentDefinition,
                                      DeltaExperimentDefinition)
from orchestration.running import ExperimentRunner

from orchestration.analyzing import (AnalysisRunnerSingle,
                                     AnalysisRunnerDelta,
                                     AnalysisGroupRunner)
from time import sleep
from stats import  NumericStats

import os

""" orchestration 文件夹通常用于存放与工作流编排、任务调度和协调相关的代码 """


def get_central_db(dbName="workload"):
    """获取配置好的中心数据库连接对象
    通过环境变量配置数据库连接参数创建DB实例，环境变量未设置时使用默认值配置。
    环境变量优先级高于参数默认值。
    Args:
        dbName (str, optional): 默认数据库名称，当环境变量ANALYSIS_DB_NAME未设置时使用。
            默认值："workload"
    Returns:
        DB: 初始化完成的数据库连接对象，包含以下配置参数：
            - 主机地址（通过ANALYSIS_DB_HOST配置，默认127.0.0.1）
            - 数据库名（通过ANALYSIS_DB_NAME配置，默认使用dbName参数值）
            - 用户名（通过ANALYSIS_DB_USER配置，默认root）
            - 密码（通过ANALYSIS_DB_PASS配置，默认空字符串）
            - 端口号（通过ANALYSIS_DB_PORT配置，默认3306）
    Environment variables:
        ANALYSIS_DB_HOST: 数据库服务地址，未设置时使用127.0.0.1
        ANALYSIS_DB_NAME: 数据库名称，未设置时使用dbName参数值
        ANALYSIS_DB_USER: 数据库账号，未设置时使用root
        ANALYSIS_DB_PASS: 数据库密码，未设置时使用空字符串
        ANALYSIS_DB_PORT: 数据库服务端口，未设置时使用3306
    """
    return DB(os.getenv("ANALYSIS_DB_HOST", "127.0.0.1"),
              os.getenv("ANALYSIS_DB_NAME", dbName),
              os.getenv("ANALYSIS_DB_USER", "root"),
              os.getenv("ANALYSIS_DB_PASS", ""),
              os.getenv("ANALYSIS_DB_PORT", "3306"))


def get_sim_db(hostname="127.0.0.1"):
    """返回一个配置为访问slurm调度器的内部数据库的DB对象。它通过环境变量进行配置.
    Args:
    - hostname: default hostname of the machine containing the database if not
        set through an env var.
    Env Vars:
    - SLURM_DB_HOST: slurm database host to connect to.
    - SLURM_DB_NAME: slurm database name of the slurm worker. If not set takes
    slurm_acct_db.
    - SLURMDB_USER: user to be used to access the slurm database.
    - SLURMDB_PASS: password to be used to used to access the slurm database.
    - SLURMDB_PORT: port on which the slurm database runs.
    """
    return DB(os.getenv("SLURM_DB_HOST", hostname),
               os.getenv("SLURM_DB_NAME", "slurm_acct_db"),
               os.getenv("SLURMDB_USER", None),
               os.getenv("SLURMDB_PASS", None),
               os.getenv("SLURMDB_PORT","3306"))
    
class ExperimentWorker(object):
    """该类检索实验配置、创建相应的工作负载、配置slurm实验运行器、运行实验并将结果存储在分析数据库中。
    运行环境的配置隐藏在静态环境中
    配置experimentunner类。
    """ 
    def do_work(self, central_db_obj, sched_db_obj, trace_id=None):
        """
            从数据库加载实验定义并运行实验。

        Args:
        - central_db_obj: 配置为访问分析数据库的DB对象.
        - sched_db_obj: 配置为访问实验工作者的slurm数据库的DB对象.
        - trace_id: 如果设置实验的有效trace_id，则只运行trace_id标识的实验.
        """
        there_are_more=True
        while there_are_more:
            # 创建实验定义对象
            ed = ExperimentDefinition()
            # 如果提供了trace_id，则加载特定的实验定义
            if trace_id:
                ed.load(central_db_obj, trace_id)
                ed.mark_pre_simulating(central_db_obj)
            else:
                # 否则，加载下一个待处理的实验定义
                there_are_more = ed.load_fresh(central_db_obj)
            if there_are_more:
                # 打印即将运行的实验信息
                print "开始运行的实验trace id为：{0}，实验名称为:{1}".format(
                                ed._trace_id, ed._name)
                # 创建实验运行器对象
                er = ExperimentRunner(ed)
                # 执行实验并检查结果
                if(er.do_full_run(sched_db_obj, central_db_obj)):
                    print "Exp({0}) Done".format(
                                                 ed._trace_id)
                else:
                    print "Exp({0}) Error!".format(
                    # 如果处理特定的trace_id，则只运行一次实验                                     ed._trace_id)
            if trace_id:
                break  
    
    def rescue_exp(self, central_db_obj, sched_db_obj, trace_id=None):
        """从实验工作者的数据库中检索工作跟踪，并将其存储在中央数据库中。
        Args:
        - central_db_obj: 配置为访问分析数据库的DB对象。
        - sched_db_obj: 配置为访问实验工作者的slurm数据库的DB对象。
        - trace_id: 获救跟踪所对应的实验的Trace_id。
        """
        there_are_more=True
        while there_are_more:
            ed = ExperimentDefinition()
            if trace_id:
                ed.load(central_db_obj, trace_id)
                ed.mark_simulation_done(central_db_obj)
            else:
                there_are_more = ed.load_next_state("simulation_failed",
                                                    "simulation_done")
            if there_are_more:
                print "About to run resque({0}):{1}".format(
                                ed._trace_id, ed._name)
                er = ExperimentRunner(ed)
                if(er.check_trace_and_store(sched_db_obj, central_db_obj)):
                    er.clean_trace_file()
                    print "Exp({0}) Done".format(
                                                 ed._trace_id)
                else:
                    print "Exp({0}) Error!".format(
                                                 ed._trace_id)
            if trace_id:
                break  

class AnalysisWorker(object):
    """该类对不同实验类型的结果进行处理，并将最终结果存储在分析数据库中。
    """
    def do_work_single(self, db_obj, trace_id=None):
        """处理单型实验结果.
        Args:
        - db_obj: 配置为访问分析数据库的DB对象.
        - trace_id: 如果设置为None，则处理“simulation_state”下的所有实验。
            果设置为整数，则分析trace_id标识的实验.
        """
        there_are_more=True # 假设还有更多实验需要处理
        while there_are_more:
            ed = ExperimentDefinition()     # 创建一个新的实验定义对象
            print "创建新的实验定义对象{0}".format(ed)
            if trace_id:
                # 如果提供了具体的 trace_id,则加载指定 trace_id 的实验数据
                ed.load(db_obj, trace_id)
                # 标记该实验正在预分析阶段
                ed.mark_pre_analyzing(db_obj)
            else:
                # 尝试加载下一个待处理的实验
                there_are_more = ed.load_pending(db_obj)
            if there_are_more:
                # 打印当前正在分析的实验ID
                print "Analyzing experiment {0}".format(ed._trace_id)
                # 创建一个负责执行完整分析的实例
                er = AnalysisRunnerSingle(ed)
                # 执行完整的分析过程
                er.do_full_analysis(db_obj)
            # 如果指定了 trace_id，则在处理完后立即退出循环
            if trace_id:
                break
    def do_work_second_pass(self, db_obj, pre_trace_id):
        """

        Args:
            db_obj: 数据库连接对象，用于数据存取操作
            pre_trace_id: 预指定的跟踪ID字符串，若存在则只处理指定ID，否则处理队列中准备好的ID
        Returns:
            None: 本方法无返回值，执行失败时会直接退出程序
        """
        there_are_more=True
        while there_are_more:
            # 初始化三个实验定义对象（主清单/单工作流/多工作流）
            ed_manifest = ExperimentDefinition()
            ed_single = ExperimentDefinition()
            ed_multi = ExperimentDefinition()

            # 加载实验配置数据：根据pre_trace_id存在性决定加载方式
            if pre_trace_id:
                trace_id=int(pre_trace_id)
                ed_manifest.load(db_obj, trace_id)
                there_are_more=True
            else:
                there_are_more = ed_manifest.load_next_ready_for_pass(db_obj)
                trace_id=int(ed_manifest._trace_id)
            if there_are_more:
                # 加载关联的连续跟踪ID（+1和+2）的实验配置
                ed_single.load(db_obj, trace_id+1)
                ed_multi.load(db_obj, trace_id+2)
                ed_list=[ed_manifest, ed_single, ed_multi]

                # 验证三个实验的工作流处理类型是否正确
                print ("Reading workflow info for traces: {0}".format(
                    [ed._trace_id for ed in ed_list]))
                if (ed_manifest._workflow_handling!="manifest" or
                    ed_single._workflow_handling!="single" or
                    ed_multi._workflow_handling!="multi"):
                    # 类型校验失败时的错误处理
                    print ("Incorrect workflow handling for traces"
                           "({0}, {1}, {2}): ({3}, {4}, {5})",format(
                               ed_manifest._trace_id,
                               ed_single._trace_id,
                               ed_multi._trace_id,
                               ed_manifest._workflow_handling,
                               ed_single._workflow_handling,
                               ed_multi._workflow_handling)
                           )
                    print ("Exiting...")
                    exit()

                # 预处理阶段：标记所有实验进入第二遍处理
                for ed in ed_list:
                    ed.mark_pre_second_pass(db_obj)
                num_workflows=None

                # 确定三个实验中最小的可用工作流数量
                for ed in ed_list:
                    exp_wfs=self.get_num_workflows(db_obj, ed._trace_id)
                    if num_workflows is None:
                        num_workflows = exp_wfs
                    else:
                        num_workflows=min(num_workflows, exp_wfs)
                print ("Final workflow count: {0}".format(num_workflows))

                # 执行第二遍分析流程
                for ed in ed_list:
                    print ("Doing second pass for trace: {0}".format(
                        ed._trace_id))
                    er = AnalysisRunnerSingle(ed)
                    er.do_workflow_limited_analysis(db_obj, num_workflows)
                print ("Second pass completed for {0}".format(
                    [ed._trace_id for ed in ed_list]))

            # 当存在预指定trace_id时，仅执行单次循环
            if pre_trace_id:
                break
    def get_num_workflows(self, db_obj, trace_id):
        """
        获取指定trace关联的工作流数量
        Args:
            db_obj: 数据库连接对象，用于执行数据查询操作
            trace_id: 跟踪标识符，用于关联特定流程数据
            self: 方法所属的类实例
        Returns:
            int: 统计到的工作流数量（通过count字段返回）
        """
        # 初始化结果类型并构造数据库存储键
        result_type = "wf_turnaround"
        key = result_type + "_stats"

        # 创建统计对象并加载数据库数据
        result = NumericStats()
        result.load(db_obj, trace_id, key)

        # 返回统计结果中的count字段整数值
        return int(result._get("count"))

    def do_work_delta(self, db_obj, trace_id=None, sleep_time=60):
        """处理增量型实验的分析任务，支持单次处理和持续轮询两种模式
        当提供 trace_id 时：处理指定实验一次
        当未提供 trace_id 时：持续轮询分析数据库中待处理的增量实验
        Args:
            db_obj (DB): 已配置的分析数据库连接对象
            trace_id (str, optional): 实验追踪ID。指定时处理单个实验，未指定时持续轮询。默认为None
            sleep_time (int, optional): 轮询间隔时间（秒），仅在批量处理模式下生效。默认为60秒
        Returns:
            None: 本方法无返回值
        """
        there_are_more = True
        # 主处理循环控制逻辑
        while there_are_more:
            # 初始化增量实验定义处理器
            ed = DeltaExperimentDefinition()

            # 根据trace_id选择处理模式
            if trace_id:
                # 单次处理模式：加载指定实验并更新状态
                ed.load(db_obj, trace_id)
                ed.mark_pre_analyzing(db_obj)
            else:
                # 批量处理模式：获取下一个待处理实验
                there_are_more = ed.load_pending(db_obj)

            # 存在待处理实验时的处理流程
            if there_are_more:
                # 执行实验就绪性检查
                if ed.is_it_ready_to_process(db_obj):
                    # 创建分析执行器并运行完整分析流程
                    er = AnalysisRunnerDelta(ed)
                    er.do_full_analysis(db_obj)

                # 批量模式下进行轮询间隔等待
                sleep(sleep_time)

            # 单次处理模式退出机制
            if trace_id:
                break

    def do_work_grouped(self, db_obj, trace_id=None, sleep_time=60):
        """处理分组类型实验结果的批处理逻辑

        支持两种运行模式：
        - 指定trace_id模式：处理特定实验组
        - 批量模式：持续处理所有待处理实验组，直到没有更多可处理项

        Args:
            db_obj (object): 配置好的分析数据库连接对象
            trace_id (str, optional): 指定要处理的实验组跟踪ID。若设置则进入单次处理模式，默认None为批量模式
            sleep_time (int, optional): 当子追踪未就绪时的重试间隔时间（秒），默认60秒

        Returns:
            None: 无返回值
        """
        there_are_more = True

        # 主处理循环控制
        while there_are_more:
            ed = GroupExperimentDefinition()

            # 模式分支：指定trace_id的精确处理
            if trace_id:
                # 加载指定实验组并标记为预处理中
                ed.load(db_obj, trace_id)
                ed.mark_pre_analyzing(db_obj)
            # 批量模式：获取下一个待处理实验组
            else:
                there_are_more = ed.load_pending(db_obj)

            # 存在待处理实验组时的处理逻辑
            if there_are_more:
                if ed.is_it_ready_to_process(db_obj):
                    print
                    "Analyzing grouped experiment {0}".format(ed._trace_id)
                    # 创建分析执行器并运行完整分析
                    er = AnalysisGroupRunner(ed)
                    er.do_full_analysis(db_obj)

            # 退出条件处理
            if trace_id:
                break  # 单次处理模式立即退出
            elif there_are_more:
                print("There are grouped experiments to be processed, but,"
                      "their subtrace are not ready yet. Sleeping for {0}s."
                      "".format(sleep_time))
                # sleep(sleep_time)  # 原始代码中睡眠调用被注释
            else:
                print
                "No more experiments to process, exiting."

    def do_mean_utilizatin(self, db_obj, trace_id=None):
        """
        计算分组实验的平均利用率指标

        处理指定或特定状态的分组实验数据，通过AnalysisGroupRunner执行均值计算。
        支持处理单个实验或批量处理完成分析阶段的实验。

        Parameters:
        - db_obj: 数据库连接对象，用于访问分析数据库
        - trace_id: 可选参数，指定要处理的实验跟踪ID。若未提供，则自动处理
                   "analysis_done" 和 "second_pass_done" 状态的实验
        """
        # 初始化分组实验定义对象
        ed = GroupExperimentDefinition()
        # 确定待处理实验ID列表
        if trace_id:
            trace_id_list=[trace_id]
        else:
            # 获取已完成分析阶段的实验ID
            trace_id_list=ed.get_exps_in_state(db_obj, "analysis_done")
            trace_id_list+=ed.get_exps_in_state(db_obj, "second_pass_done")
        # 打印待处理实验列表
        print "processing following group traces (utilization mean):{0}".format(
               trace_id_list)
        # 遍历处理每个分组实验
        for trace_id in trace_id_list:
            print "Calculating for", trace_id
            ed = GroupExperimentDefinition()
            ed.load(db_obj, trace_id=trace_id)  # 加载实验配置
            er = AnalysisGroupRunner(ed)    # 创建分析执行器
            er.do_only_mean(db_obj)     # 执行均值计算
            
                
                
    def do_work_grouped_second_pass(self, db_obj, pre_trace_id):
        """选取三个实验，对每个实验重复工作流分析，但只考虑每个实验中的前n个工作流。N=横跨三个路径的最小工作流数。
        Args:
        - db_obj: DB object configured to access the analysis database.
        - trace_id: If set to an integer, it will analyze the
            experiments identified by trace_id, trace_id+1, trace_id+2.
        """
        there_are_more=True
        while there_are_more:
            ed_manifest = GroupExperimentDefinition()
            ed_single = GroupExperimentDefinition()
            ed_multi = GroupExperimentDefinition()
            if pre_trace_id:
                trace_id=int(pre_trace_id)
                ed_manifest.load(db_obj, trace_id)
                there_are_more=True
            else:
                there_are_more = ed_manifest.load_next_ready_for_pass(db_obj)
                trace_id=int(ed_manifest._trace_id)
            if there_are_more:
                ed_single.load(db_obj, trace_id+1)
                ed_multi.load(db_obj, trace_id+2)
                ed_list=[ed_manifest, ed_single, ed_multi]
                print ("Reading workflow info for traces: {0}".format(
                    [ed._trace_id for ed in ed_list]))
                if (ed_manifest._workflow_handling!="manifest" or
                    ed_single._workflow_handling!="single" or
                    ed_multi._workflow_handling!="multi"):
                    print ("Incorrect workflow handling for traces"
                           "({0}, {1}, {2}): ({3}, {4}, {5})",format(
                               ed_manifest._trace_id,
                               ed_single._trace_id,
                               ed_multi._trace_id,
                               ed_manifest._workflow_handling,
                               ed_single._workflow_handling,
                               ed_multi._workflow_handling)
                           )
                    print ("Exiting...")
                    exit()

                for ed in ed_list:  
                    ed.mark_pre_second_pass(db_obj)

                list_num_workflows=[]
                for (st_1, st_2, st_3) in zip(ed_manifest._subtraces,
                                              ed_single._subtraces,
                                              ed_multi._subtraces):
                    num_workflows=None
                    for ed_id in [st_1, st_2, st_3]:
                        exp_wfs=self.get_num_workflows(db_obj, ed_id)
                        if num_workflows is None:
                            num_workflows = exp_wfs
                        else:
                            num_workflows=min(num_workflows, exp_wfs)
                    list_num_workflows.append(num_workflows)
                
                print ("Final workflow count: {0}".format(list_num_workflows))
                for ed in ed_list:
                    print ("Doing second pass for trace: {0}".format(
                        ed._trace_id))
                    er = AnalysisGroupRunner(ed)
                    er.do_workflow_limited_analysis(db_obj, list_num_workflows)
                print ("Second pass completed for {0}".format(
                    [ed._trace_id for ed in ed_list]))
            if pre_trace_id:
                break
        
        
    