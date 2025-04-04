"""
 python -m unittest test_definition
 已测试成功
 

"""
from commonLib.DBManager import DB
from orchestration.definition import (ExperimentDefinition,
                                        GroupExperimentDefinition,
                                        DeltaExperimentDefinition)
import datetime
import os
import subprocess
import time
import unittest



class TestExperimentDefinition(unittest.TestCase):
    def setUp(self):
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                   os.getenv("TEST_DB_NAME", "test"),
                   os.getenv("TEST_DB_USER", "root"),
                   os.getenv("TEST_DB_PASS", ""))

    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table "+table_name+"")
        self.assertTrue(ok, "Table was not created!")

    def test_create_table(self):
        """
        测试创建实验定义表。

        本测试方法验证是否能够成功创建实验定义表。
        首先，创建一个ExperimentDefinition实例。
        然后，使用addCleanup方法确保测试结束后删除创建的表，以清理数据库环境。
        最后，调用create_table方法尝试创建表。
        """
        ed = ExperimentDefinition()
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)
    def test_constructor(self):
        """
        测试ExperimentDefinition类的构造函数是否正确工作。

        此方法创建了一个ExperimentDefinition实例，并验证了其各种属性是否按预期设置。
        它还检查了实例的字符串表示是否符合预期的格式。
        """
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="machine",
                 trace_type="double",
                 manifest_list=[{"share": 0.2, "manifest": "man1.json"},
                                {"share":0.8,  "manifest": "man2.json"}],
                 workflow_policy="period",
                 workflow_period_s=20,
                 workflow_share=30.0,
                 workflow_handling="manifest",
                 subtraces = [100002, 10003],
                 preload_time_s = 3600*24*3,
                 workload_duration_s = 3600*24*8,
                 work_state = "fresher",
                 analysis_state = "1",
                 overload_target=2.0,
                 conf_file="my.conf")

        # 验证ExperimentDefinition实例的字符串表示
        self.assertEqual(ed._experiment_set, "machine-double-m[0.2|man1.json,"
                         "0.8|man2.json]-period-p20-%30.0-manifest-"
                         "t[100002,10003]"
                         "-3d-8d-O2.0-my.conf")
        self.assertEqual(ed._name, "machine-double-m[0.2|man1.json,"
                         "0.8|man2.json]"
                         "-period-p20-%30.0-manifest-t[100002,10003]-3d-8d-O2.0"
                         "-my.conf-s[seeeed]")
        # 验证实例属性
        self.assertEqual(ed._seed, "seeeed")
        self.assertEqual(ed._machine, "machine")
        self.assertEqual(ed._trace_type, "double")
        self.assertEqual(ed._manifest_list, [dict(share=0.2,
                                                   manifest="man1.json"),
                                              dict(share=0.8,
                                                   manifest="man2.json")])
        self.assertEqual(ed._workflow_policy, "period")
        self.assertEqual(ed._workflow_period_s, 20)
        self.assertEqual(ed._workflow_share, 30.0)
        self.assertEqual(ed._workflow_handling, "manifest")
        self.assertEqual(ed._subtraces, [100002, 10003])
        self.assertEqual(ed._preload_time_s, 3*24*3600)
        self.assertEqual(ed._workload_duration_s, 8*24*3600)
        self.assertEqual(ed._work_state, "fresher")
        self.assertEqual(ed._analysis_state,  "1")
        self.assertEqual(ed._table_name, "experiment")
        self.assertEqual(ed._overload_target,2.0)
        self.assertEqual(ed._conf_file, "my.conf")


    def test_store_load(self):
        """
        测试ExperimentDefinition的存储和加载功能。

        1. 创建一个具有特定参数的ExperimentDefinition实例（ed_old）。
        2. 创建一个空的ExperimentDefinition实例（ed）。
        3. 在测试结束时删除"experiment"表，以清理环境。
        4. 使用ed_old生成并存储实验定义到数据库。
        5. 使用ed从数据库加载刚刚存储的实验定义。
        6. 验证加载的实验定义是否与原始定义完全一致。
        """
        ed_old = ExperimentDefinition(
                 seed="seeeed",
                 machine="machine",
                 trace_type="double",
                 manifest_list=[{"share": 0.2, "manifest": "man1.json"},
                                {"share":0.8,  "manifest": "man2.json"}],
                 workflow_policy="period",
                 workflow_period_s=20,
                 workflow_share=30.0,
                 workflow_handling="manifest",
                 subtraces = [100002, 10003],
                 preload_time_s = 3600*24*3,
                 workload_duration_s = 3600*24*8,
                 work_state = "fresher",
                 analysis_state = "1",
                 overload_target=2.0,
                 conf_file="my.conf")

        ed = ExperimentDefinition()
        # 在测试结束时删除"experiment"表，以清理环境
        self.addCleanup(self._del_table, "experiment")
        # 使用ed_old生成并存储实验定义到数据库
        ed.create_table(self._db)
        # 获取存储后的trace_id
        trace_id = ed_old.store(self._db)
        # 使用ed从数据库加载刚刚存储的实验定义
        ed.load(self._db, trace_id)
        # 验证加载的实验定义是否与原始定义完全一致
        self.assertEqual(ed._experiment_set, "machine-double-m[0.2|man1.json,"
                         "0.8|man2.json]-period-p20-%30.0-manifest-"
                         "t[100002,10003]"
                         "-3d-8d-O2.0-my.conf")
        self.assertEqual(ed._name, "machine-double-m[0.2|man1.json,"
                         "0.8|man2.json]"
                         "-period-p20-%30.0-manifest-t[100002,10003]-3d-8d-O2.0"
                         "-my.conf-s[seeeed]")
        self.assertEqual(ed._seed, "seeeed")
        self.assertEqual(ed._machine, "machine")
        self.assertEqual(ed._trace_type, "double")
        self.assertEqual(ed._manifest_list, [dict(share=0.2,
                                                   manifest="man1.json"),
                                              dict(share=0.8,
                                                   manifest="man2.json")])
        self.assertEqual(ed._workflow_policy, "period")
        self.assertEqual(ed._workflow_period_s, 20)
        self.assertEqual(ed._workflow_share, 30.0)
        self.assertEqual(ed._workflow_handling, "manifest")
        self.assertEqual(ed._subtraces, [100002, 10003])
        self.assertEqual(ed._preload_time_s, 3*24*3600)
        self.assertEqual(ed._workload_duration_s, 8*24*3600)
        self.assertEqual(ed._work_state, "fresher")
        self.assertEqual(ed._analysis_state,  "1")
        self.assertEqual(ed._table_name, "experiment")
        self.assertEqual(ed._overload_target,2.0)
        self.assertEqual(ed._conf_file, "my.conf")



    def test_get_file_names(self):
        """
        测试ExperimentDefinition类中生成文件名的函数。

        本测试用例旨在验证ExperimentDefinition类对象能够根据其属性
        正确生成对应的trace、qos和users文件名。
        """
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="machine",
                 trace_type="double",
                 manifest_list=[{"share": 0.2, "manifest": "man1.json"},
                                {"share":0.8,  "manifest": "man2.json"}],
                 workflow_policy="period",
                 workflow_period_s=20,
                 workflow_share=30.0,
                 workflow_handling="manifest",
                 subtraces = [100002, 10003],
                 preload_time_s = 3600*24*3,
                 workload_duration_s = 3600*24*8,
                 work_state = "fresher",
                 analysis_state = "1")
        # 验证get_trace_file_name方法是否返回正确的文件名
        self.assertEqual(ed.get_trace_file_name(),
                         "machine-double-m0.2man1.json"
                         "0.8man2.json"
                         "-period-p20-30.0-manifest-t10000210003-3d-8d-O0.0"
                         "-sseeeed.trace")
        # 验证get_qos_file_name方法是否返回正确的文件名
        self.assertEqual(ed.get_qos_file_name(),
                         "machine-double-m0.2man1.json"
                         "0.8man2.json"
                         "-period-p20-30.0-manifest-t10000210003-3d-8d-O0.0"
                         "-sseeeed.qos")
        # 验证get_users_file_name方法是否返回正确的文件名
        self.assertEqual(ed.get_users_file_name(),
                         "machine-double-m0.2man1.json"
                         "0.8man2.json"
                         "-period-p20-30.0-manifest-t10000210003-3d-8d-O0.0"
                         "-sseeeed.users")

    def test_get_fresh(self):
        """
        测试获取新的实验定义的功能。

        此测试用例的目的是验证在存储新的实验定义后，能够正确地加载并获取到指定的实验定义。
        """
        ed = ExperimentDefinition()
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)
        ed.store(self._db)

        ed_2 = ExperimentDefinition()
        ed_2.store(self._db)

        # 创建并加载最新的实验定义
        ed_f = ExperimentDefinition()
        ed_f.load_fresh(self._db)
        # 验证加载的实验定义的追踪ID是否为1
        self.assertEqual(ed_f._trace_id, 1)

        ed_f_2 = ExperimentDefinition()
        ed_f_2.load_fresh(self._db)
        self.assertEqual(ed_f_2._trace_id, 2)

    # 测试获取新鲜的待处理实验定义功能
    def test_get_fresh_pending(self):
        # 添加清理操作，测试结束后删除"experiment"表
        self.addCleanup(self._del_table, "experiment")
        # 创建实验定义表
        ExperimentDefinition().create_table(self._db)

        # 创建并存储第一个实验定义
        ed_1 = ExperimentDefinition(start_date=datetime.datetime(2019,1,1))
        trace_id_1=ed_1.store(self._db)

        # 创建并存储第二个实验定义
        ed_2 = ExperimentDefinition()
        trace_id_2=ed_2.store(self._db)

        # 创建并配置第一个组实验定义，包含两个子实验
        ed_g1= GroupExperimentDefinition(machine="kkk")
        ed_g1.add_sub_trace(trace_id_1)
        ed_g1.add_sub_trace(trace_id_2)
        ed_g1.store(self._db)

        # 创建并配置第二个组实验定义，仅包含第一个子实验
        ed_g2 = GroupExperimentDefinition()
        print ed_g2._subtraces
        ed_g2.add_sub_trace(trace_id_1)
        ed_g2.store(self._db)

        # 创建一个新的组实验定义对象，测试加载待处理的组实验定义
        one_g=GroupExperimentDefinition()
        self.assertTrue(one_g.load_pending(self._db))
        self.assertNotEqual(one_g._work_state, "pre_analyzing")

        # 更新第一个实验的状态，测试对组实验定义状态的影响
        ed_1.upate_state(self._db, "analysis_done")
        self.assertTrue(one_g.load_pending(self._db))
        self.assertEqual(one_g._work_state, "pre_analyzing")
        self.assertEqual(one_g._trace_id, ed_g2._trace_id)

        # 重新创建一个新的组实验定义对象，再次测试加载待处理的组实验定义
        one_g=GroupExperimentDefinition()
        self.assertTrue(one_g.load_pending(self._db))

        # 更新第二个实验的状态，测试对组实验定义状态的影响
        ed_2.upate_state(self._db, "analysis_done")
        self.assertTrue(one_g.load_pending(self._db))
        self.assertEqual(one_g._work_state, "pre_analyzing")
        self.assertEqual(one_g._trace_id, ed_g1._trace_id)


    def test_is_it_ready_to_process(self):
        ed = ExperimentDefinition()
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)
        t1 = ExperimentDefinition()
        id1=t1.store(self._db)
        t2 = ExperimentDefinition()
        id2=t2.store(self._db)

        t3 = GroupExperimentDefinition(subtraces=[id1, id2])
        t3.store(self._db)
        self.assertFalse(t3.is_it_ready_to_process(self._db), "The subtraces"
                         " are still pending, it should not be possible to"
                         " process it.")

        t1.mark_simulation_done(self._db)
        self.assertFalse(t3.is_it_ready_to_process(self._db), "One subtrace"
                         " is still pending, it should not be possible to"
                         " process it.")
        t2.mark_simulation_done(self._db)

        self.assertFalse(t3.is_it_ready_to_process(self._db), "Subtraces "
                         "have to be analyzed for this the grouped to be "
                         "ready")
        t1.mark_analysis_done(self._db)
        t2.mark_analysis_done(self._db)

        self.assertTrue(t3.is_it_ready_to_process(self._db), "Subtraces "
                         "are analyzed. It should be ready")

    def test_is_it_ready_to_process_delta(self):
        ed = ExperimentDefinition()
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)
        t1 = ExperimentDefinition()
        id1=t1.store(self._db)
        t2 = ExperimentDefinition()
        id2=t2.store(self._db)

        t3 = DeltaExperimentDefinition(subtraces=[id1, id2])
        t3.store(self._db)
        self.assertFalse(t3.is_it_ready_to_process(self._db), "The subtraces"
                         " are still pending, it should not be possible to"
                         " process it.")

        t1.mark_simulation_done(self._db)
        self.assertFalse(t3.is_it_ready_to_process(self._db), "One subtrace"
                         " is still pending, it should not be possible to"
                         " process it.")
        t2.mark_simulation_done(self._db)

        self.assertTrue(t3.is_it_ready_to_process(self._db), "Subtraces "
                         "are genreated, t3, should be ready to run.")



    def test_get_fresh_concurrent(self):
        """测试并发获取新增实验数据的正确性

        验证主进程和子进程并发读取时：
        1. 能正确获取新增数据
        2. 两个进程获取的数据ID不存在重叠
        通过检查两个进程获取的ID列表完全互斥来确认并发安全性

        参数: 无
        返回值: 无
        """
        ed = ExperimentDefinition()
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)
        for i in range(200):
            ed.store(self._db)

        if os.path.exists("./out.file"):
            os.remove("./out.file")
        out = open("./out.file", "w")
        p = subprocess.Popen(["python", "./fresh_reader.py"], stdout=out)

        count = 0
        there_are_more=True
        ids=[]

        while there_are_more:
            ed_f = ExperimentDefinition()
            there_are_more = ed_f.load_fresh(self._db)
            if there_are_more:
                ids.append(ed_f._trace_id)
        time.sleep(5)
        out.flush()
        out.close()

        out = open("./out.file", "r")
        lines = out.readlines()
        other_ids=[]

        for line in lines:
            if "END2" in line:
                print ""
                text_list=line.split("END2: [")[1]
                text_list=text_list.split("]")[0]
                other_ids = [int(x) for x in text_list.split(",")]
        self.assertGreater(len(ids), 0)
        self.assertGreater(len(other_ids), 0)
        for id in ids:
            self.assertNotIn(id, other_ids)
        print ("IDs", ids, other_ids)

    def test_mark_simulating(self):
        ed = ExperimentDefinition()
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)
        my_id=ed.store(self._db)

        ed.mark_simulating(self._db, "MyWorker")
        now_time=datetime.datetime.now()
        new_ed = ExperimentDefinition()
        new_ed.load(self._db, my_id)

        self.assertEqual(new_ed._work_state, "simulating")
        self.assertEqual(new_ed._worker, "MyWorker")
        self.assertLess(now_time-new_ed._simulating_start,
                        datetime.timedelta(10))

    def test_mark_simulation_done(self):
        ed = ExperimentDefinition()
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)
        my_id=ed.store(self._db)

        ed.mark_simulation_done(self._db)
        now_time=datetime.datetime.now()
        new_ed = ExperimentDefinition()
        new_ed.load(self._db, my_id)

        self.assertEqual(new_ed._work_state, "simulation_done")
        self.assertLess(now_time-new_ed._simulating_end,
                        datetime.timedelta(10))

    def test_mark_simulation_failed(self):
        ed = ExperimentDefinition()
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)
        my_id=ed.store(self._db)

        ed.mark_simulation_failed(self._db)
        now_time=datetime.datetime.now()
        new_ed = ExperimentDefinition()
        new_ed.load(self._db, my_id)

        self.assertEqual(new_ed._work_state, "simulation_failed")
        self.assertLess(now_time-new_ed._simulating_end,
                        datetime.timedelta(10))

    def test_reset_simulating_time(self):
        ed = ExperimentDefinition()
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)
        my_id=ed.store(self._db)
        ed.update_simulating_start(self._db)
        ed.update_simulating_end(self._db)
        new_ed = ExperimentDefinition()
        new_ed.load(self._db, my_id)
        self.assertNotEqual(new_ed._simulating_end, None)
        self.assertNotEqual(new_ed._simulating_start, None)
        ed.reset_simulating_time(self._db)
        new_ed.load(self._db, my_id)

        self.assertEqual(new_ed._simulating_end, None)
        self.assertEqual(new_ed._simulating_start,None)

    def test_load_next_ready_for_pass(self):
        ed = ExperimentDefinition()
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)
        ed_1=ExperimentDefinition()
        ed_2=ExperimentDefinition()
        ed_3=ExperimentDefinition()
        ed_4=ExperimentDefinition()
        ed_1._workflow_handling="manifest"
        ed_1._work_state="analysis_done"
        ed_2._workflow_handling="single"
        ed_2._work_state="analysis_done"
        ed_3._workflow_handling="multi"
        ed_3._work_state="analysis_done"
        target_trace_id=ed_1.store(self._db)
        ed_2.store(self._db)
        ed_3.store(self._db)
        #ed_4 should be skipped.
        ed_4.store(self._db)

        ed_1b=ExperimentDefinition()
        ed_2b=ExperimentDefinition()
        ed_3b=ExperimentDefinition()
        ed_1b._workflow_handling="manifest"
        ed_1b._work_state="analysis_done"
        ed_2b._workflow_handling="single"
        ed_2b._work_state="analysis_done"
        ed_3b._workflow_handling="multi"
        ed_3b._work_state="analysis_done"
        target_trace_id_b=ed_1b.store(self._db)
        ed_2b.store(self._db)
        ed_3b.store(self._db)


        ed.load_next_ready_for_pass(self._db)
        self.assertEqual(target_trace_id, ed._trace_id)

        ed.load_next_ready_for_pass(self._db)
        self.assertEqual(target_trace_id_b, ed._trace_id)

    def test_load_next_ready_for_pass_error(self):
        ed = ExperimentDefinition()
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)
        ed_1=ExperimentDefinition()
        ed_2=ExperimentDefinition()
        ed_3=ExperimentDefinition()
        ed_4=ExperimentDefinition()
        ed_1._workflow_handling="manifest"
        ed_1._work_state="analysis_done"
        ed_2._workflow_handling="multi"
        ed_2._work_state="analysis_done"
        ed_3._workflow_handling="multi"
        ed_3._work_state="analysis_done"
        target_trace_id=ed_1.store(self._db)
        ed_2.store(self._db)
        ed_3.store(self._db)
        ed_4.store(self._db)
        #ed_1 to ed_4 should be skipped.
        ed_1b=ExperimentDefinition()
        ed_2b=ExperimentDefinition()
        ed_3b=ExperimentDefinition()
        ed_1b._workflow_handling="manifest"
        ed_1b._work_state="analysis_done"
        ed_2b._workflow_handling="single"
        ed_2b._work_state="analysis_done"
        ed_3b._workflow_handling="multi"
        ed_3b._work_state="analysis_done"
        target_trace_id_b=ed_1b.store(self._db)
        ed_2b.store(self._db)
        ed_3b.store(self._db)


        ed.load_next_ready_for_pass(self._db)
        self.assertEqual(target_trace_id_b, ed._trace_id)

    def test_load_next_grouped_ready_for_pass(self):
        ed = GroupExperimentDefinition()
        self.addCleanup(self._del_table, "experiment")
        ed.create_table(self._db)

        other=ExperimentDefinition()
        other.store(self._db)

        subids_1=[]
        for i in range(5):
            subt_1=ExperimentDefinition()
            subt_1._workflow_handling="manifest"
            subt_1._work_state="analysis_done"
            subids_1.append(subt_1.store(self._db))

        subids_2=[]
        for i in range(5):
            subt_1=ExperimentDefinition()
            subt_1._workflow_handling="single"
            subt_1._work_state="analysis_done"
            subids_2.append(subt_1.store(self._db))


        subids_3=[]
        for i in range(5):
            subt_1=ExperimentDefinition()
            subt_1._workflow_handling="single"
            subt_1._work_state="analysis_done"
            subids_3.append(subt_1.store(self._db))



        ed_1=GroupExperimentDefinition()
        ed_2=GroupExperimentDefinition()
        ed_3=GroupExperimentDefinition()
        ed_4=GroupExperimentDefinition()
        ed_1._workflow_handling="manifest"
        ed_1._work_state="analysis_done"
        ed_1._subtraces=subids_1
        ed_2._workflow_handling="single"
        ed_2._work_state="analysis_done"
        ed_2._subtraces=subids_2
        ed_3._workflow_handling="multi"
        ed_3._work_state="analysis_done"
        ed_3._subtraces=subids_3
        target_trace_id=ed_1.store(self._db)
        ed_2.store(self._db)
        ed_3.store(self._db)
        #ed_4 should be skipped.
        ed_4.store(self._db)





        subids_1=[]
        for i in range(5):
            subt_1=ExperimentDefinition()
            subt_1._workflow_handling="manifest"
            subt_1._work_state="analysis_done"
            subids_1.append(subt_1.store(self._db))

        subids_2=[]
        for i in range(5):
            subt_1=ExperimentDefinition()
            subt_1._workflow_handling="single"
            subt_1._work_state="analysis_done"
            subids_2.append(subt_1.store(self._db))


        subids_3=[]
        for i in range(5):
            subt_1=ExperimentDefinition()
            subt_1._workflow_handling="single"
            subt_1._work_state="fresh"
            subids_3.append(subt_1.store(self._db))

        ed_1=GroupExperimentDefinition()
        ed_2=GroupExperimentDefinition()
        ed_3=GroupExperimentDefinition()
        ed_4=GroupExperimentDefinition()
        ed_1._workflow_handling="manifest"
        ed_1._work_state="analysis_done"
        ed_1._subtraces=subids_1
        ed_2._workflow_handling="single"
        ed_2._work_state="analysis_done"
        ed_2._subtraces=subids_2
        ed_3._workflow_handling="multi"
        ed_3._work_state="analysis_done"
        ed_3._subtraces=subids_3
        ed_1.store(self._db)
        ed_2.store(self._db)
        ed_3.store(self._db)
        #ed_4 should be skipped.
        ed_4.store(self._db)

        ed.load_next_ready_for_pass(self._db)
        self.assertEqual(target_trace_id, ed._trace_id)
        ed._work_state="second_pass_done"
        ed.store(self._db)

        newEd=GroupExperimentDefinition()
        self.assertRaises(ValueError, newEd.load_next_ready_for_pass, self._db)





