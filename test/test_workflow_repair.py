
"""
需自测
"""



from commonLib.DBManager import DB
from orchestration.running import ExperimentRunner
from stats.trace import ResultTrace
from stats.workflow_repair import StartTimeCorrector

import numpy as np
import os
import unittest
from orchestration.definition import ExperimentDefinition

class TestWorkflowRepair(unittest.TestCase):
    def setUp(self):
        ExperimentRunner.configure(manifest_folder="manifests")
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                   os.getenv("TEST_DB_NAME", "test"),
                   os.getenv("TEST_DB_USER", "root"),
                   os.getenv("TEST_DB_PASS", ""))
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table `"+table_name+"`")
        self.assertTrue(ok, "Table was not created!")
    def _create_tables(self):
        rt = ResultTrace()
        self.addCleanup(self._del_table,"import_table" )
        rt.create_import_table(self._db, "import_table")
        self.addCleanup(self._del_table,"traces" )
        rt.create_trace_table(self._db, "traces")
        self.addCleanup(self._del_table,"experiment" )
        exp = ExperimentDefinition()
        exp.create_table(self._db)
        
    
    def test_get_workflow_info(self):
        """
        测试获取工作流信息的功能

        本测试用例旨在验证StartTimeCorrector类的get_workflow_info方法是否能正确解析工作流信息
        并检查解析的信息是否与预期相符

        :return: 无返回值，但会断言工作流信息中的核心数、运行时间和任务键是否符合预期
        """
        # 创建StartTimeCorrector实例
        stc = StartTimeCorrector()
        # 调用get_workflow_info方法获取工作流信息
        info=stc.get_workflow_info("synthLongWide.json")

        self.assertEqual(info["cores"],480)
        self.assertEqual(info["runtime"],18000)
        # 断言工作流信息中的任务键是否为预期的集合{"S0", "S1"}
        self.assertEqual(set(info["tasks"].keys()), set(["S0", "S1"]))
        
    def test_get_time_start(self):
        """
        测试get_time_start方法以确保它返回正确的时间修正值。

        该测试方法会根据不同条件调整开始时间，并验证调整结果是否符合预期。
        它通过创建StartTimeCorrector实例并调用其get_time_start方法来执行测试。
        """
        stc = StartTimeCorrector()

        # 测试multi类型调整开始时间，预期结果是减去14340
        new_start_time = stc.get_time_start("wf_synthLongWide.json-1_S0",
                                      100000, "multi")
        self.assertEqual(new_start_time, 100000-14340)

        # 测试multi类型调整开始时间，预期结果是减去3540
        new_start_time = stc.get_time_start("wf_synthLongWide.json-1_S1_dS0",
                                      100000, "multi")
        self.assertEqual(new_start_time, 100000-3540)

        # 测试manifest类型调整开始时间，预期结果是减去3540
        new_start_time = stc.get_time_start("wf_synthLongWide.json-1_S1_dS0",
                                      100000, "manifest")
        self.assertEqual(new_start_time, 100000-3540)

        # 测试manifest类型调整开始时间，预期结果是不作调整
        new_start_time = stc.get_time_start("wf_synthLongWide.json-1",
                                      100000, "manifest")
        self.assertEqual(new_start_time, 100000)

        # 测试single类型调整开始时间，预期结果是减去18000
        new_start_time = stc.get_time_start("wf_synthLongWide.json-1",
                                      100000, "single")
        self.assertEqual(new_start_time, 100000-18000)

        # 测试当类型为multi但不满足调整条件时，确保抛出SystemError异常
        self.assertRaises(SystemError,
                          stc.get_time_start, "wf_synthLongWide.json-1",
                                      100000, "multi")
    
    def test_get_corrected_start_times(self):
        """
        测试获取修正后的开始时间。

        此方法主要用于测试在特定的实验条件下，根据提交和开始时间等信息，
        计算和验证修正后的开始时间是否符合预期。
        """
        # 创建测试所需的数据库表结构
        self._create_tables()

        # 初始化ResultTrace对象，并填充模拟的作业提交和开始时间等数据
        rt = ResultTrace()
        rt._lists_submit = {
             "job_db_inx":[1,2,3],
             "account": ["account1", "account2", "a3"],
             "cpus_req": [48, 96, 96],
             "cpus_alloc": [48, 96, 96],
             "job_name":["wf_synthLongWide.json-1_S0", 
                         "wf_synthLongWide.json-1_S1_dS0",
                         "wf_synthLongWide.json-2_S1_dS0"],
             "id_job": [1,2,3],
             "id_qos": [2,3,3],
             "id_resv": [3,4,5],
             "id_user": [4,5,6],
             "nodes_alloc": [2,4,4],
             "partition": ["partition1", "partition2", "partition2"],
             "priority": [99, 199, 210],
             "state": [3,3, 3],
             "timelimit": [100,200, 300],
             "time_submit": [3000,3003, 3004],
             "time_start": [0,20000, 0],
             "time_end": [20000,25000, 30000]                      
             }

        # 存储模拟的跟踪数据到数据库中，为后续的测试做准备
        trace_id=1
        rt.store_trace(self._db, trace_id)

        # 初始化StartTimeCorrector对象，并设置实验定义，加载跟踪数据
        stc = StartTimeCorrector()
        stc._experiment = ExperimentDefinition()
        stc._experiment._trace_id=trace_id
        stc._trace=ResultTrace()
        stc._trace.load_trace(self._db, trace_id)

        # 计算修正后的开始时间，并验证其是否与预期的结果一致
        new_times = stc.get_corrected_start_times("multi")
        self.assertEqual(new_times, {1:20000-14340, 3:30000-3540})
    
    def test_apply_new_times(self):
        """
        测试应用新的开始时间是否正确更新了数据库中的作业开始时间。

        此方法首先创建测试所需的表，然后创建并填充一个ResultTrace对象。
        通过调用store_trace方法将数据存储到数据库中。接着，创建一个StartTimeCorrector对象，
        并通过它来应用新的开始时间。最后，验证数据库中的开始时间是否已正确更新。
        """

        self._create_tables()

        rt = ResultTrace()
        rt._lists_submit = {
             "job_db_inx":[1,2,3],
             "account": ["account1", "account2", "a3"],
             "cpus_req": [48, 96, 96],
             "cpus_alloc": [48, 96, 96],
             "job_name":["wf_synthLongWide.json-1_S0", 
                         "wf_synthLongWide.json-1_S1_dS0",
                         "wf_synthLongWide.json-2_S1_dS0"],
             "id_job": [1,2,3],
             "id_qos": [2,3,3],
             "id_resv": [3,4,5],
             "id_user": [4,5,6],
             "nodes_alloc": [2,4,4],
             "partition": ["partition1", "partition2", "partition2"],
             "priority": [99, 199, 210],
             "state": [3,3, 3],
             "timelimit": [100,200, 300],
             "time_submit": [3000,3003, 3004],
             "time_start": [0,20000, 0],
             "time_end": [20000,25000, 30000]                      
             }

        trace_id=1
        trace_id_orig=2

        # 将数据存储到数据库中
        rt.store_trace(self._db, trace_id)
        rt.store_trace(self._db, trace_id_orig)

        # 创建并配置StartTimeCorrector对象
        stc = StartTimeCorrector()
        stc._experiment = ExperimentDefinition()
        stc._experiment._trace_id=trace_id

        # 应用新的开始时间
        stc.apply_new_times(self._db,{1:20000-14340, 3:30000-3540})

        # 加载并验证更新后的数据
        new_rt=ResultTrace()
        new_rt.load_trace(self._db, trace_id)
        self.assertEqual(new_rt._lists_submit["time_start"],
                         [20000-14340, 20000, 30000-3540])

        # 加载并验证原始数据未发生变化
        old_rt=ResultTrace()
        old_rt.load_trace(self._db, trace_id_orig)
        self.assertEqual(old_rt._lists_submit["time_start"],
                         [0,20000, 0])
        
    def test_correct_times(self):
        """
        测试并校正时间字段。

        该方法主要用于测试在特定场景下，时间相关的字段是否被正确处理。
        它首先创建必要的表结构，然后通过存储实验定义和结果追踪数据来设置测试环境。
        最后，它使用StartTimeCorrector类来校正时间字段，并验证校正结果是否符合预期。
        """

        self._create_tables()

        # 创建一个实验定义实例，并指定其处理方式为"manifest"
        exp = ExperimentDefinition(workflow_handling="manifest")

        # 在数据库中存储实验定义，返回一个trace_id
        trace_id=exp.store(self._db)

        # 创建一个结果追踪实例
        rt=ResultTrace()

        # 初始化结果追踪实例的时间提交字段，包含多个作业的详细信息
        rt._lists_submit = {
             "job_db_inx":[1,2,3],
             "account": ["account1", "account2", "a3"],
             "cpus_req": [48, 96, 96],
             "cpus_alloc": [48, 96, 96],
             "job_name":["wf_synthLongWide.json-1_S0", 
                         "wf_synthLongWide.json-1_S1_dS0",
                         "wf_synthLongWide.json-2"],
             "id_job": [1,2,3],
             "id_qos": [2,3,3],
             "id_resv": [3,4,5],
             "id_user": [4,5,6],
             "nodes_alloc": [2,4,4],
             "partition": ["partition1", "partition2", "partition2"],
             "priority": [99, 199, 210],
             "state": [3,3, 3],
             "timelimit": [100,200, 300],
             "time_submit": [3000,3003, 3004],
             "time_start": [0,20000, 0],
             "time_end": [20000,25000, 30000]                      
             }
        # 将结果追踪数据存储到数据库中
        rt.store_trace(self._db, trace_id)
        rt.store_trace(self._db, trace_id+1)

        # 创建一个开始时间校正器实例
        stc = StartTimeCorrector()
        # 使用开始时间校正器校正数据库中的时间字段
        stc.correct_times(self._db, trace_id)

        # 创建一个新的结果追踪实例来加载校正后的数据
        new_rt=ResultTrace()
        new_rt.load_trace(self._db, trace_id)
        # 验证校正后的时间开始字段是否符合预期
        self.assertEqual(new_rt._lists_submit["time_start"],
                         [20000-14340, 20000, 30000])

        # 创建另一个结果追踪实例来加载原始数据
        original_rt=ResultTrace()
        original_rt.load_trace(self._db, trace_id+1)
        # 验证原始的时间开始字段是否保持不变
        self.assertEqual(original_rt._lists_submit["time_start"],
                         [0, 20000, 0])
        
        
        
        
        