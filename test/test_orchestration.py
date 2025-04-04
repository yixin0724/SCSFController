"""UNIT TESTS

 python -m unittest test_orchestration
 已成功
 
"""

import datetime
import os
import unittest

from commonLib.DBManager import DB
from commonLib.nerscUtilization import UtilizationEngine
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
from stats.compare import WorkflowDeltas
from stats.trace import ResultTrace


class TestOrchestration(unittest.TestCase):
    def setUp(self):  # 每个测试方法执行前都会执行这个进行初始化，self参数是这个类实例化的第一个对象
        # 初始化数据库连接，使用环境变量来配置数据库的主机、名称、用户和密码
        self._db = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                      os.getenv("TEST_DB_NAME", "test"),
                      os.getenv("TEST_DB_USER", "root"),
                      os.getenv("TEST_DB_PASS", ""))
        # 初始化虚拟机IP地址，使用环境变量配置，默认为192.168.56.24
        self._vm_ip = os.getenv("TEST_VM_HOST", "192.168.56.24")

        # 创建实验定义表，并注册清理方法删除该表
        ExperimentDefinition().create_table(self._db)
        self.addCleanup(self._del_table, ExperimentDefinition()._table_name)  # 注册清理时删除表的方法

        # 创建直方图数据表，并注册清理方法删除该表
        ht = Histogram()
        ht.create_table(self._db)
        self.addCleanup(self._del_table, ht._table_name)  # 注册清理时删除表的方法

        # 创建数值统计表，并注册清理方法删除该表
        ns = NumericStats()
        ns.create_table(self._db)
        self.addCleanup(self._del_table, ns._table_name)

        # 创建数值列表（包含利用率、浪费和校正利用率）的表，并注册清理方法删除该表
        us = NumericList("usage_values", ["utilization", "waste"
                                                         "corrected_utilization"])
        us.create_table(self._db)
        self.addCleanup(self._del_table, "usage_values")  # 直接使用表名注册清理方法

        # 初始化结果追踪对象，但注意这里先注册了清理方法，然后才创建表，顺序有误
        rt = ResultTrace()
        self.addCleanup(self._del_table, "traces")
        rt.create_trace_table(self._db, "traces")

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
            drain_time=0)

    def _del_table(self, table_name):
        """
        删除数据库中的指定表格。

        本函数通过执行SQL语句'drop table'来删除数据库中的指定表格。
        它调用了_db对象的doUpdate方法来执行这一操作，并检查操作是否成功。

        参数:
        - table_name (str): 要删除的表格名称。

        返回值:
        无返回值，但会使用assertTrue方法来验证表格是否已成功删除。
        """
        # 执行SQL语句删除指定表格，并获取操作结果
        ok = self._db.doUpdate("drop table " + table_name + "")
        # 验证表格是否已成功删除
        self.assertTrue(ok, "Table was not created!")

    def _del_exp(self, exp_def, db_obj):
        """
        删除实验相关数据。

        本函数负责调用实验定义对象上的方法来删除实验的结果、跟踪信息和实验本身的数据。

        参数:
        - exp_def: 实验定义对象，包含删除实验数据所需的方法。
        - db_obj: 数据库对象，用于实验数据的删除操作。

        返回值:
        无返回值。
        """
        # 删除实验结果
        exp_def.del_results(db_obj)
        # 删除实验跟踪信息
        exp_def.del_trace(db_obj)
        # 删除实验定义
        exp_def.del_exp(db_obj)

    def test_single_no_wf_create_sim_analysis(self):
        # 测试方法必须以test_开头，unittest会依次执行这些方法
        # 测试方法，用于验证在没有工作流的情况下，单个模拟实验的分析过程是否能正确创建并分析结果。

        # 获取数据库对象（已在setUp方法中初始化，用的是scsftest数据库）
        db_obj = self._db

        # 创建一个实验定义对象，设置各种参数
        exp = ExperimentDefinition(
            seed="AAAAA",
            machine="edison",
            trace_type="single",
            manifest_list=[],
            workflow_policy="no",
            workflow_period_s=0,
            workflow_handling="single",
            preload_time_s=0,
            workload_duration_s=3600 * 1)
        # 注册清理方法，用于在测试完成后，无论失败还是成功都会删除该实验对象
        self.addCleanup(self._del_exp, exp, db_obj)

        # 将实验定义存储到数据库中
        exp.store(db_obj)

        # 获取模拟数据库的连接对象（即工作器的数据库）
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
        self._check_results_are_there(db_obj, exp)

    def test_single_with_wf_create_sim_analysis(self):
        """
        测试创建模拟分析实验的功能。

        该方法定义了一个实验参数，创建了一个实验定义对象，
        并通过模拟调度数据库对象执行实验和分析工作。
        """
        # 初始化数据库对象
        db_obj = self._db
        # 创建实验定义对象，配置实验参数
        exp = ExperimentDefinition(
            seed="AAAAA",
            machine="edison",
            trace_type="single",
            manifest_list=[{"share": 1.0,
                            "manifest": "manifestsim.json"}],
            workflow_policy="period",
            workflow_period_s=60,
            workflow_handling="single",
            preload_time_s=0,
            workload_duration_s=3600 * 1)
        # 实验结束后清理资源
        self.addCleanup(self._del_exp, exp, db_obj)
        # 存储实验定义到数据库
        exp.store(db_obj)
        # 获取模拟调度数据库对象
        sched_db_obj = get_sim_db(self._vm_ip)
        # 创建并执行实验工作
        ew = ExperimentWorker()
        ew.do_work(db_obj, sched_db_obj)
        # 检查实验跟踪数据是否存在
        self._check_trace_is_there(db_obj, exp)
        # 创建并执行分析工作
        ew = AnalysisWorker()
        ew.do_work_single(db_obj)
        # 检查分析结果是否存在
        self._check_results_are_there(db_obj, exp, True,
                                      ["manifestsim.json"])

    def test_multi_with_wf_create_sim_analysis(self):
        """
        测试在使用工作流创建模拟分析时，多任务处理的正确性。

        这个方法主要目的是验证在模拟环境中，按照指定的实验定义进行工作流处理的正确性。
        它通过创建一个实验定义，使用数据库对象进行存储，并利用实验工作器和分析工作器
        来执行和分析实验，最后检查实验结果是否存在。
        """
        db_obj = self._db
        # 创建ExperimentDefinition实例，配置实验参数
        exp = ExperimentDefinition(
            seed="AAAAA",
            machine="edison",
            trace_type="single",
            manifest_list=[{"share": 1.0,
                            "manifest": "manifestsim.json"}],
            workflow_policy="period",
            workflow_period_s=60,
            workflow_handling="multi",
            preload_time_s=0,
            workload_duration_s=3600 * 1)
        # 实验结束后清理环境
        self.addCleanup(self._del_exp, exp, db_obj)
        # 存储实验定义到数据
        exp.store(db_obj)
        # 获取调度数据库对象
        sched_db_obj = get_sim_db(self._vm_ip)
        # 实验工作器执行任务
        ew = ExperimentWorker()
        ew.do_work(db_obj, sched_db_obj)
        # 检查实验痕迹是否存在
        self._check_trace_is_there(db_obj, exp)
        # 分析工作器执行任务
        ew = AnalysisWorker()
        ew.do_work_single(db_obj)
        # 检查分析结果是否存在
        self._check_results_are_there(db_obj, exp, True,
                                      ["manifestsim.json"])

    def test_manifest_with_wf_create_sim_analysis(self):
        db_obj = self._db
        # 创建ExperimentDefinition实例，配置实验参数
        exp = ExperimentDefinition(
            seed="AAAAA",
            machine="edison",
            trace_type="single",
            manifest_list=[{"share": 1.0,
                            "manifest": "manifestsim.json"}],
            workflow_policy="period",
            workflow_period_s=60,
            workflow_handling="manifest",
            preload_time_s=0,
            workload_duration_s=3600 * 1)
        # 实验结束后清理环境
        self.addCleanup(self._del_exp, exp, db_obj)
        # 存储实验定义到数据
        exp.store(db_obj)
        # 获取调度数据库对象
        sched_db_obj = get_sim_db(self._vm_ip)
        # 实验工作器执行任务
        ew = ExperimentWorker()
        ew.do_work(db_obj, sched_db_obj)
        # 检查实验痕迹是否存在
        self._check_trace_is_there(db_obj, exp)
        # 分析工作器执行任务
        ew = AnalysisWorker()
        ew.do_work_single(db_obj)
        # 检查分析结果是否存在
        self._check_results_are_there(db_obj, exp, True,
                                      ["manifestsim.json"])

    def test_delta_exp(self):
        """
        测试DeltaExperimentDefinition的功能。

        该测试函数旨在验证DeltaExperimentDefinition类以及相关功能的正确性，
        包括实验定义的存储、实验工作的执行、以及实验结果的分析和比较。
        """
        # 创建第一个实验定义
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
        # 将实验定义存储到数据库中，并获取实验ID
        id1 = exp1.store(self._db)
        # 创建第二个实验定义，参数与第一个实验定义相同
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
        # 创建Delta实验定义，基于前面两个实验定义
        exp3 = DeltaExperimentDefinition(subtraces=[id1, id2])
        # 存储Delta实验定义到数据库中，并获取实验ID
        id3 = exp3.store(self._db)
        # 获取调度数据库对象
        sched_db_obj = get_sim_db(self._vm_ip)
        # 创建并执行实验工作
        ew = ExperimentWorker()
        ew.do_work(self._db, sched_db_obj)

        # 创建并执行分析工作，重复定义ExperimentWorker
        ew = AnalysisWorker()
        ew.do_work_delta(self._db)
        # 加载和比较实验结果
        trace_comparer = WorkflowDeltas()
        results = trace_comparer.load_delta_results(self._db, exp3._trace_id)
        # 验证实验结果中包含所有预期的统计指标，并检查其类型
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

    def test_grouped_exp(self):
        """
        测试分组实验定义的处理和分析流程。

        此函数创建两个单独的实验定义，然后将它们组合成一个分组实验定义，
        并通过模拟数据库和实验工作流处理这些实验定义。最后，检查实验结果是否正确生成。
        """
        # 创建第一个实验定义
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
        # 将实验定义存储到数据库中，并获取实验ID
        id1 = exp1.store(self._db)
        # 创建第二个实验定义，配置与第一个相同
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

        # 创建分组实验定义，包含之前两个单独实验的ID
        exp3 = GroupExperimentDefinition(subtraces=[id1, id2])
        # 存储分组实验定义并获取其ID
        id3 = exp3.store(self._db)
        # 获取模拟数据库对象
        sched_db_obj = get_sim_db(self._vm_ip)

        # 创建并运行实验工作流处理单独和分组的实验定义
        ew = ExperimentWorker()
        ew.do_work(self._db, sched_db_obj)

        # 创建并运行分析工作流处理单独的实验定义
        aw = AnalysisWorker()
        aw.do_work_single(self._db)
        # 运行分析工作流处理分组的实验定义
        aw.do_work_grouped(self._db)
        # 检查分组实验定义的结果是否正确生成
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
