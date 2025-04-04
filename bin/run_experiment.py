# -*- coding: utf-8 -*-

from datetime import datetime
import os
import unittest
from commonLib.DBManager import DB
from commonLib.filemanager import ensureDir
from commonLib.nerscUtilization import UtilizationEngine
from generate import TimeController
from orchestration import AnalysisWorker
from orchestration import ExperimentWorker
from orchestration import get_central_db, get_sim_db
from orchestration.analyzing import AnalysisRunnerSingle
from orchestration.definition import (ExperimentDefinition,
                                      DeltaExperimentDefinition,
                                      GroupExperimentDefinition)
from orchestration.definition import ExperimentDefinition
from orchestration.running import ExperimentRunner
from stats import Histogram, NumericStats, NumericList
from stats.trace import ResultTrace
from stats.compare import WorkflowDeltas
from machines import Edison2015
import slurm.trace_gen as trace_gen

"""
@author：YiXin
@fileName：run_experiment.py
@createTime：2025/2/06 12:20
"""


class TestExperiment(unittest.TestCase):
    """
    实验的定义
        实验组的概念：将具有相同条件但具有不同随机生成器初始化（实验定义中的随机种子）的单个实验分组的元实验。
        seed参数：有AAAA、AAAAA、AAAAAA、seeeed
        machine参数：目前有edison和edison2015
        trace_type参数：有三个值。
            “single”：单个实验是对工作负载进行分析的一次运行。
            “delta”：增量是两个单一轨迹（列在子轨迹中）中工作流的比较，
            “group”：而组实验汇总了许多单个实验的结果（列在子轨迹中）。
        manifest_list参数：任务清单列表。每个字典有两个键：“manifest”，其值列出工作流类型的清单文件的名称；
            并且“共享”一个0-1的值，该值指示对应的工作流在工作负载中占比的核心小时。
        workflow_policy参数：控制如何处理要添加到工作负载中的工作流。
            “no”，表示没有工作流；
            “period”：周期性提交，每workflow_period_s秒提交一个工作流；
            “share”，工作流以统一的速度提交，因此分配给工作流的核心小时数代表了用户指定的工作负载总核心小时数的份额。workflow_share的多少百分比将是工作流占核心小时的份额。
        workflow_period_s参数：正数，工作负载中一个工作流完成与下一个工作流开始之间的等待时间。
        workflow_share参数: 浮动在0到100之间，工作流中作业的占比
        workflow_handling参数表示工作流的提交方法。
            “single”：其中工作流作为单个作业提交。即试点作业
            “multi”：工作流中的每个任务都在独立的作业中运行；即链式作业
            “manifest”：其中工作流作为单个作业提交，但使用了感知工作流的回填.即Woas提交
        subtraces: 本实验分析中应使用的迹线的trace_id （int）列表。只对delta和群实验有效。
        pre_load_time_s参数：表示在start_date之前生成的工作负载秒数。此工作负载用于“加载”调度器，但是分析将仅从“start_date”开始执行。
        workload_duration_s参数：表示表示在start_date之后生成的工作负载秒数。
        overload_target: 如果设置为> 1.0，则在预加载期间生成的工作负载将产生额外的作业，
                因此在一段时间内，将提交overload_target乘以系统的容量（在该期间产生的）。
    """
    """
        分析任务的方法有6种，
            do_work_single：分析单型实验结果
            do_work_second_pass：对单型实验结果的进一步分析
            do_work_delta：分析对比型实验结果
            do_work_grouped：分析组实验结果
            do_mean_utilizatin：计算分组实验的平均利用率指标
            do_work_grouped_second_pass：对组实验结果的进一步分析
    """
    """
        实验设计
            以单型实验进行下面几个实验：
                带工作流用single提交方式以试点作业提交(可以使用不同的工作流)
                带工作流用multi提交方式以链式作业提交
                带工作流用manifest提交方式以Woas作业提交
    """

    def setUp(self):  # 每个测试方法执行前都会执行这个进行初始化，self参数是这个类实例化的第一个对象
        # 初始化数据库连接，使用环境变量来配置数据库的主机、名称、用户和密码
        self._db = DB(os.getenv("ANALYSIS_DB_HOST", "127.0.0.1"),
                      os.getenv("ANALYSIS_DB_NAME", "scsf"),
                      os.getenv("ANALYSIS_DB_USER", "scsf"),
                      os.getenv("ANALYSIS_DB_PASS", "scsf-pass"))
        ensureDir("./tmp")
        # 初始化虚拟机IP地址，使用环境变量配置
        self._vm_ip = os.getenv("TEST_VM_HOST", "192.168.56.91")

        # 配置实验运行器，包括各种路径和配置
        ExperimentRunner.configure(
            trace_folder="/tmp/",  # 追踪文件存放的文件夹
            trace_generation_folder="tmp",  # 追踪文件生成的文件夹，若local为false，则在当前的controller的当前生成
            local=False,  # 是否在本地运行
            run_hostname=self._vm_ip,  # 运行的主机名
            run_user=None,  # 运行的用户
            scheduler_conf_dir="/scsf/slurm_conf",  # 调度器配置文件夹
            local_conf_dir="configs/",  # 本地控制器配置文件夹
            scheduler_folder="/scsf/",  # 调度器文件夹
            manifest_folder="manifests",  # 清单文件夹
            drain_time=3600 * 6)

    def test_single_with_wf_single_experiment(self):
        # 获取数据库对象（已在setUp方法中初始化，用的是scsf数据库）
        db_obj = self._db

        # 创建一个实验定义对象，设置各种参数
        exp = ExperimentDefinition(
            seed="AAAAA",
            machine="edison",
            trace_type="single/delta/group",    # 选择实验类型为单一/对比/组实验
            manifest_list=[{"share": 0.0-1.0, "manifest": "floodplain.json"}],  # 这里的share是工作流占核心小时的份额，比如delta实验，一个设置0.8，一个设置0.2，两个实验的share之和为1.0
            workflow_policy="no/period/share",
            workflow_period_s=5/20/60/300,      # 设置period时，要设置workflow_period_s，它表示按固定时间间隔提交工作流
            workflow_share=0.0-100.0,       # 设置工作流中作业的占比
            workflow_handling="single/multi/manifest",
            preload_time_s=0/20/3600,   # 预加载时间
            workload_duration_s=120/400/600/3600/3600*6,    # 它表示在start_date之后生成的工作负载秒数
            overload_target=1.0/1.1/1.2/2.0)            # 如果设置为> 1.0，则在预加载期间生成的工作负载将产生额外的作业，

        # 将实验定义存储到数据库中
        exp.store(db_obj)
        # 获取模拟数据库的连接对象（即控制器的scsf数据库）
        sched_db_obj = get_sim_db(self._vm_ip)

        # 创建一个实验工作对象，并执行工作
        ew = ExperimentWorker()
        print("创建工作对象{0},开始执行任务".format(ew))
        ew.do_work(db_obj, sched_db_obj)  # 参数1是控制器数据库对象，参数2是模拟器数据库对象

        # 检查追踪数据是否存在（验证模拟实验是否成功生成了追踪数据）
        self._check_trace_is_there(db_obj, exp)

        # 创建一个分析工作对象，并执行单个分析任务
        ew = AnalysisWorker()
        print("创建分析对象{0},开始分析任务，并存储到数据库中。".format(ew))
        ew.do_work_single(db_obj)

        # # 检查分析结果是否存在（验证分析任务是否成功生成了结果数据）
        # self._check_results_are_there(db_obj, exp, True,
        #                               ["floodplain.json"])



# ----------------------------------------- #

    def test_single_with_wf_multi_experiment(self):
        overload = 1.0
        # 获取数据库对象（已在setUp方法中初始化，用的是scsf数据库）
        db_obj = self._db

        # 创建一个实验定义对象，设置各种参数
        exp = ExperimentDefinition(
            seed="AAAAA",
            machine="edison",
            trace_type="single",
            manifest_list=[{"share": 1.0, "manifest": "floodplain.json"}],
            workflow_policy="period",
            workflow_period_s=300,
            workflow_handling="multi",
            preload_time_s=0,
            workload_duration_s=3600 * 24 * 7,
            overload_target=overload)

        # 将实验定义存储到数据库中
        exp.store(db_obj)
        # 获取模拟数据库的连接对象（即Worker的scsf数据库）
        sched_db_obj = get_sim_db(self._vm_ip)

        # 创建一个实验工作对象，并执行工作
        ew = ExperimentWorker()
        ew.do_work(db_obj, sched_db_obj)  # 参数1是控制器数据库对象，参数2是模拟器数据库对象

        # 检查追踪数据是否存在（验证模拟实验是否成功生成了追踪数据）
        self._check_trace_is_there(db_obj, exp)

        # 创建一个分析工作对象，并执行单个分析任务
        ew = AnalysisWorker()
        ew.do_work_single(db_obj)

        # 检查分析结果是否存在（验证分析任务是否成功生成了结果数据）
        self._check_results_are_there(db_obj, exp, True,
                                      ["floodplain.json"])

    def test_single_with_wf_manifest_experiment(self):
        overload = 1.0
        # 获取数据库对象（已在setUp方法中初始化，用的是scsf数据库）
        db_obj = self._db

        # 创建一个实验定义对象，设置各种参数
        exp = ExperimentDefinition(
            seed="AAAAA",
            machine="edison",
            trace_type="single",
            manifest_list=[{"share": 1.0, "manifest": "floodplain.json"}],
            workflow_policy="period",
            workflow_period_s=300,
            workflow_handling="manifest",
            preload_time_s=0,
            workload_duration_s=3600 * 24 * 7,
            overload_target=overload)

        # 将实验定义存储到数据库中
        exp.store(db_obj)
        # 获取模拟数据库的连接对象（即Worker的scsf数据库）
        sched_db_obj = get_sim_db(self._vm_ip)

        # 创建一个实验工作对象，并执行工作
        ew = ExperimentWorker()
        ew.do_work(db_obj, sched_db_obj)  # 参数1是控制器数据库对象，参数2是模拟器数据库对象

        # 检查追踪数据是否存在（验证模拟实验是否成功生成了追踪数据）
        self._check_trace_is_there(db_obj, exp)

        # 创建一个分析工作对象，并执行单个分析任务
        ew = AnalysisWorker()
        ew.do_work_single(db_obj)

        # 检查分析结果是否存在（验证分析任务是否成功生成了结果数据）
        self._check_results_are_there(db_obj, exp, True,
                                      ["floodplain.json"])

    def test_delta_experiment(self):
        """测试Delta实验的正确性，验证差异分析结果的生成和数据结构

               本测试用例验证以下流程：
               1. 创建并存储两个相同配置的基础实验
               2. 基于这两个实验创建Delta实验
               3. 执行实验工作流程和差异分析
               4. 验证结果数据包含预期的统计指标和分布数据

               参数说明：
               self: 测试类实例，包含数据库连接等测试上下文

               返回值：
               无，通过断言验证测试结果
        """
        # 创建两个相同配置的基础实验用于对比
        # 第一个实验定义配置
        exp1 = ExperimentDefinition(
            seed="AAAAA",
            machine="edison",
            trace_type="single",
            manifest_list=[{"share": 1.0,
                            "manifest": "manifestsim.json"}],
            workflow_policy="period",
            workflow_period_s=60,
            workflow_handling="multi",
            preload_time_s=0,
            workload_duration_s=600 * 1)
        # 存储实验定义并获取唯一标识
        id1 = exp1.store(self._db)

        # 第二个实验定义使用完全相同的配置
        exp2 = ExperimentDefinition(
            seed="AAAAA",
            machine="edison",
            trace_type="single",
            manifest_list=[{"share": 1.0,
                            "manifest": "manifestsim.json"}],
            workflow_policy="period",
            workflow_period_s=60,
            workflow_handling="multi",
            preload_time_s=0,
            workload_duration_s=600 * 1)
        id2 = exp2.store(self._db)
        # 创建差异实验定义，基于前两个实验的对比
        exp3 = DeltaExperimentDefinition(subtraces=[id1, id2])
        id3 = exp3.store(self._db)

        # 获取调度数据库对象
        sched_db_obj = get_sim_db(self._vm_ip)
        # 执行实验工作流程
        ew = ExperimentWorker()
        ew.do_work(self._db, sched_db_obj)

        # 执行差异分析工作流程
        ew = AnalysisWorker()
        ew.do_work_delta(self._db)

        # 加载并验证差异分析结果
        trace_comparer = WorkflowDeltas()
        results = trace_comparer.load_delta_results(self._db, exp3._trace_id)

        # 验证结果数据结构完整性
        # 检查所有预期的统计指标和分布数据是否存在且类型正确
        for result_name in ["delta_runtime_cdf", "delta_waittime_cdf",
                            "delta_turnaround_cdf",
                            "delta_stretch_cdf",
                            "delta_runtime_stats", "delta_waittime_stats",
                            "delta_turnaround_stats",
                            "delta_stretch_stats"]:
            self.assertIn(result_name, results.keys())
            if "_cdf" in result_name:
                self.assertIs(type(results[result_name]), Histogram)
            elif "_stats" in result_name:
                self.assertIs(type(results[result_name]), NumericStats)

    def test_grouped_experiment(self):
        """测试分组实验定义及执行流程的端到端功能

        本测试用例验证：
        1. 创建并存储多个独立实验定义
        2. 创建包含多个子实验的分组实验定义
        3. 执行实验调度和数据分析工作
        4. 验证分组实验结果是否正常生成

        参数说明：
        self: 测试类实例对象，包含数据库连接等测试环境配置

        返回值：
        无返回值，通过断言验证测试结果
        """
        # 创建并存储第一个基础实验定义
        exp1 = ExperimentDefinition(
            seed="AAAAA",
            machine="edison",
            trace_type="single",
            manifest_list=[{"share": 1.0,
                            "manifest": "manifestsim.json"}],
            workflow_policy="period",
            workflow_period_s=60,
            workflow_handling="multi",
            preload_time_s=0,
            workload_duration_s=600 * 1)
        id1 = exp1.store(self._db)

        # 创建与第一个实验配置相同的第二个实验定义
        exp2 = ExperimentDefinition(
            seed="AAAAA",
            machine="edison",
            trace_type="single",
            manifest_list=[{"share": 1.0,
                            "manifest": "manifestsim.json"}],
            workflow_policy="period",
            workflow_period_s=60,
            workflow_handling="multi",
            preload_time_s=0,
            workload_duration_s=600 * 1)
        # 同样将实验定义存储到数据库中，并获取实验ID
        id2 = exp2.store(self._db)

        # 创建包含两个子实验的分组实验配置
        exp3 = GroupExperimentDefinition(subtraces=[id1, id2])
        id3 = exp3.store(self._db)

        # 获取虚拟机的模拟数据库连接对象
        sched_db_obj = get_sim_db(self._vm_ip)

        # 执行实验调度工作
        ew = ExperimentWorker()
        ew.do_work(self._db, sched_db_obj)

        # 执行数据分析工作（单实验和分组实验）
        aw = AnalysisWorker()
        aw.do_work_single(self._db)
        aw.do_work_grouped(self._db)

        # 验证分组实验结果文件和工作流数据是否生成
        self._check_results_are_there(self._db, exp3, wf=True,
                                      manifest_list=["manifestsim.json"])

    def _check_trace_is_there(self, db_obj, exp):
        """
            验证指定的跟踪记录是否存在，并且检查相关的状态和时间戳。
            :param db_obj: 数据库对象或接口，用于与数据库交互。
            :param exp: 实验定义对象的实例，包含有关实验的信息。
        """
        trace_id = exp._trace_id
        # 创建一个新的实验定义对象
        new_ew = ExperimentDefinition()
        # 使用提供的跟踪ID从数据库加载实验定义
        new_ew.load(db_obj, trace_id)
        # 检查实验的工作状态是否为"simulation_done"
        self.assertEqual(new_ew._work_state, "simulation_done")

        # 创建一个结果跟踪对象
        result_trace = ResultTrace()
        # 使用提供的跟踪ID从数据库加载结果跟踪数据
        result_trace.load_trace(db_obj, trace_id)
        # 确认有提交列表存在，即实验确实提交过任务
        self.assertTrue(result_trace._lists_submit)

        # 检查最早的任务提交时间戳是否在实验开始前的预加载时间范围内
        self.assertLessEqual(result_trace._lists_submit["time_submit"][0],
                             exp.get_start_epoch() - exp._preload_time_s + 60)
        # 检查最晚的任务提交时间戳是否接近实验结束时间
        self.assertGreaterEqual(result_trace._lists_submit["time_submit"][-1],
                                exp.get_end_epoch() - 60)

    def _check_results_are_there(self, db_obj, exp, wf=False, manifest_list=[],
                                 job_fields=None):
        """
           检查实验结果是否存在于数据库中。

           该方法旨在验证给定实验（exp）在数据库（db_obj）中的分析结果是否存在。
           它会检查作业字段（job_fields）和（如果指定）工作流字段是否已存在结果。
           对于工作流结果，它还会检查每个指定在manifest_list中的清单是否有结果。

           参数:
           - db_obj: 数据库对象，用于访问实验数据。
           - exp: 实验对象，包含实验的详细信息。
           - wf: 布尔值，指示是否需要检查工作流结果。
           - manifest_list: 清单列表，用于指定需要检查的工作流清单。
           - job_fields: 作业字段列表，用于指定需要检查的作业结果字段。

           返回值:
           无返回值。该方法主要用于断言结果是否存在，并在测试环境中可能引发断言错误。
           """

        # 默认的作业字段列表，用于检查作业结果
        if job_fields is None:
            job_fields = ["jobs_runtime_cdf", "jobs_runtime_stats",
                          "jobs_waittime_cdf",
                          "jobs_waittime_stats", "jobs_turnaround_cdf",
                          "jobs_turnaround_stats", "jobs_requested_wc_cdf",
                          "jobs_requested_wc_stats", "jobs_cpus_alloc_cdf",
                          "jobs_cpus_alloc_stats"]
        # 获取实验的trace_id
        trace_id = exp._trace_id

        # 创建一个新的ExperimentDefinition对象并加载实验定义
        new_ew = ExperimentDefinition()
        new_ew.load(db_obj, trace_id)
        # 断言实验的工作状态是“analysis_done”
        self.assertEqual(new_ew._work_state, "analysis_done")
        # 创建并加载ResultTrace对象
        result_trace = ResultTrace()
        result_trace.load_analysis(db_obj, trace_id)

        # 打印作业结果和调试信息
        print
        "KK", result_trace.jobs_results
        print
        "I AN HERE"

        # 遍历作业字段列表，断言每个字段的结果都非空
        for field in job_fields:
            self.assertNotEqual(result_trace.jobs_results[field], None)
        # 如果wf参数为真，也检查工作流结
        if (wf):
            results = result_trace.workflow_results
            results_per_wf = result_trace.workflow_results_per_manifest
            # 打印工作流结果和每个清单的结
            print
            results
            print
            results_per_wf
            # 遍历工作流字段列表，断言每个字段的结果都非空
            for field in ["wf_runtime_cdf", "wf_runtime_stats",
                          "wf_waittime_cdf",
                          "wf_waittime_stats", "wf_turnaround_cdf",
                          "wf_turnaround_stats", "wf_stretch_factor_cdf",
                          "wf_stretch_factor_stats", "wf_jobs_runtime_cdf",
                          "wf_jobs_runtime_stats", "wf_jobs_cores_cdf",
                          "wf_jobs_cores_stats"]:
                self.assertNotEqual(results[field], None)
                # 对于每个清单，断言指定字段的结果都非空
                for manifest in manifest_list:
                    this_manifest_result = results_per_wf[manifest]
                    self.assertNotEqual(
                        this_manifest_result["m_" + manifest + "_" + field], None)
