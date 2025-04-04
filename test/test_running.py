"""
 python -m unittest test_running
 只差test_is_it_running方法

"""
from datetime import datetime
import os
import unittest

from commonLib.DBManager import DB
from commonLib.filemanager import ensureDir
from generate import TimeController
from machines import Edison2015
from orchestration.definition import ExperimentDefinition
from orchestration.running import ExperimentRunner
import slurm.trace_gen as trace_gen
from stats.trace import ResultTrace


class TestExperimentRunner(unittest.TestCase):
    def setUp(self):
        self._db  = DB(os.getenv("TEST_DB_HOST", "127.0.0.1"),
                   os.getenv("TEST_DB_NAME", "test"),
                   os.getenv("TEST_DB_USER", "root"),
                   os.getenv("TEST_DB_PASS", ""))
        # 确保./tmp目录存在，如果不存在则创建该目录
        ensureDir("./tmp")
        self._vm_ip = os.getenv("TEST_VM_HOST", "192.168.56.24")
    
    def _del_table(self, table_name):
        ok = self._db.doUpdate("drop table "+table_name+"")
        self.assertTrue(ok, "Table was not created!")
        
    def test_conf(self):
        """
        测试配置实验运行器的配置参数是否正确设置。

        此方法通过调用ExperimentRunner的configure方法来设置各种配置参数，
        并使用assertEqual方法来验证这些参数是否被正确设置。
        """
        ExperimentRunner.configure(
                                   "tmp/trace_folder",
                                   "tmp", 
                                   True,
                                   "myhost", "myUser",
                                   local_conf_dir="local_file",
                                   scheduler_conf_dir="sched_conf_dir",
                                   scheduler_conf_file_base="conf.file",
                                   scheduler_folder="folder",
                                   scheduler_script="script",
                                   manifest_folder="man_folder")
        
       
        self.assertEqual(ExperimentRunner._trace_folder,  "tmp/trace_folder")
        self.assertEqual(ExperimentRunner._trace_generation_folder, "tmp")
        self.assertEqual(ExperimentRunner._local, True)
        self.assertEqual(ExperimentRunner._run_hostname, "myhost")
        self.assertEqual(ExperimentRunner._run_user, "myUser")
        self.assertEqual(ExperimentRunner._local_conf_dir, "local_file")
        self.assertEqual(ExperimentRunner._scheduler_conf_dir, "sched_conf_dir")
        self.assertEqual(ExperimentRunner._scheduler_conf_file_base, 
                         "conf.file")
        self.assertEqual(ExperimentRunner._scheduler_folder, "folder")
        self.assertEqual(ExperimentRunner._scheduler_script, "script")
        self.assertEqual(ExperimentRunner._manifest_folder, "man_folder")
        
    
    def test_generate_trace_files(self):
        """
            测试生成跟踪文件的功能是否正常工作。
            此测试用例配置了实验运行器，创建了实验定义，并生成了跟踪文件。
            最后，它验证生成的跟踪文件是否符合预期的格式和内容。
            """
        # 配置实验运行器的参数，为生成跟踪文件做准备
        ExperimentRunner.configure(
                                   "tmp/trace_folder",
                                   "tmp", 
                                   True,
                                   "myhost", "myUser",
                                   drain_time=0)
        # 确认配置参数是否正确设置
        self.assertEqual(ExperimentRunner._trace_folder,  "tmp/trace_folder")
        self.assertEqual(ExperimentRunner._trace_generation_folder, "tmp")
        self.assertEqual(ExperimentRunner._local, True)

        # 创建一个实验定义实例，用于生成跟踪文件
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 400)
        # 创建实验运行器实例，并传入实验定义
        er = ExperimentRunner(ed)
        # 生成跟踪文件
        er._generate_trace_files(ed)
        # 验证生成的跟踪文件是否存在
        self.assertTrue(os.path.exists("tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O0.0"
                         "-sseeeed.trace"))
        self.assertTrue(os.path.exists("tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O0.0"
                         "-sseeeed.qos"))
        self.assertTrue(os.path.exists("tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O0.0"
                         "-sseeeed.users"))
        # 提取跟踪文件中的记录，以便进一步验证
        records=trace_gen.extract_records(file_name=
                         "tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O0.0"
                         "-sseeeed.trace",
                                list_trace_location="../bin/list_trace")
        man_count=0
        # 验证提交时间的范围
        self.assertGreater(int(records[-1]["SUBMIT"])-
                                int(records[0]["SUBMIT"]), 320)
        self.assertLess(int(records[-1]["SUBMIT"])-
                                int(records[0]["SUBMIT"]), 1500)
        # 计算manifest的数量
        for rec in records:
            if rec["WF"].split("-")[0]=="manifestSim.json":
                man_count+=1
        # 验证manifest的数量是否在预期范围内
        self.assertGreaterEqual(man_count, 64, "There should be at least 80"
                           " workflows in the "
                           "trace, found: {0}".format(man_count))
        self.assertLessEqual(man_count, 104, "There should be at least 80"
                           " workflows in the "
                           "trace, found: {0}".format(man_count))
    
    def test_generate_trace_files_first_job(self):
        """
          测试生成跟踪文件的第一个作业
        """
        # 配置实验运行器，设置跟踪文件夹、生成文件夹、本地模式、主机和用户信息
        ExperimentRunner.configure(
                                   "tmp/trace_folder",
                                   "tmp", 
                                   True,
                                   "myhost", "myUser",
                                   drain_time=0)
        # 断言配置是否正确
        self.assertEqual(ExperimentRunner._trace_folder,  "tmp/trace_folder")
        self.assertEqual(ExperimentRunner._trace_generation_folder, "tmp")
        self.assertEqual(ExperimentRunner._local, True)

        # 定义实验参数，包括种子、机器、跟踪类型、清单列表、工作流策略等
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 400,
                 overload_target=3600000)
        # 创建实验运行器实例并生成跟踪文件
        er = ExperimentRunner(ed)
        er._generate_trace_files(ed)
        # 断言跟踪文件、QoS文件和用户文件是否已生成
        self.assertTrue(os.path.exists("tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O3600000"
                         "-sseeeed.trace"))
        self.assertTrue(os.path.exists("tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O3600000"
                         "-sseeeed.qos"))
        self.assertTrue(os.path.exists("tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O3600000"
                         "-sseeeed.users"))
        # 提取跟踪记录并进行验证
        records=trace_gen.extract_records(file_name=
                         "tmp/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O3600000"
                         "-sseeeed.trace",
                                list_trace_location="../bin/list_trace")
        man_count=0
        # 验证提交时间间隔
        self.assertGreater(int(records[-1]["SUBMIT"])-
                                int(records[0]["SUBMIT"]), 320)
        self.assertLess(int(records[-1]["SUBMIT"])-
                                int(records[0]["SUBMIT"]), 1500+3720)
        # 统计特定清单的工作流数量
        for rec in records:
            if rec["WF"].split("-")[0]=="manifestSim.json":
                man_count+=1
        # 断言工作流数量范围
        self.assertGreaterEqual(man_count, 64, "There should be at least 80"
                           " workflows in the "
                           "trace, found: {0}".format(man_count))
        self.assertLessEqual(man_count, 104, "There should be at least 80"
                           " workflows in the "
                           "trace, found: {0}".format(man_count))
        # 计算第一个提交时间
        first_submit=TimeController.get_epoch(datetime(2016,1,1))-20-3600-120
        # 验证前360个记录的详细内容
        for i in range(360):
            self.assertEqual(int(records[i]["NUM_TASKS"]),
                             16*24)
            self.assertEqual(int(records[i]["DURATION"]), 7320)
            self.assertEqual(int(records[i]["WCLIMIT"]), 123)
            self.assertEqual(int(records[i]["SUBMIT"]), first_submit)
            first_submit+=10
        # 验证最后一个记录的提交时间和持续时间
        self.assertGreaterEqual(int(records[360]["SUBMIT"]),
                                TimeController.get_epoch(datetime(2016,1,1))-20)
        self.assertNotEqual(int(records[360]["DURATION"]), 3600)
        
    
    def test_generate_trace_files_special(self):
        """
        测试在特殊条件下生成跟踪文件的功能。
        此测试验证了是否能够根据给定的实验定义生成跟踪文件，并且生成的记录是否符合预期。
        """
        # 配置实验运行器，设置跟踪文件夹路径、临时文件路径、是否使用主机、主机名和用户名
        ExperimentRunner.configure(
                                       "tmp/trace_folder",
                                       "tmp", 
                                       True,
                                       "myhost", "myUser")
        # 创建一个实验定义实例，设置了一系列实验参数，如种子、机器类型、跟踪类型等
        ed = ExperimentDefinition(
                     seed="AAAA",
                     machine="edison",
                     trace_type="single",
                     manifest_list=[],
                     workflow_policy="sp-sat-p2-c24-r36000-t4-b100",
                     workflow_period_s=0,
                     workflow_handling="single",
                     preload_time_s = 0,
                     start_date = datetime(2016,1,1),
                     workload_duration_s = 120,
                     overload_target=1.2
                     )
        # 创建一个实验运行器实例，并使用之前定义的实验定义进行初始化
        er = ExperimentRunner(ed)
        # 调用生成跟踪文件的方法
        er._generate_trace_files(ed)

        # 构建跟踪文件的路径
        trace_file_route=("tmp/{0}".format(ed.get_trace_file_name()))
        # 验证跟踪文件是否已生成
        self.assertTrue(os.path.exists(trace_file_route))

        # 从生成的跟踪文件中提取记录
        records=trace_gen.extract_records(file_name=trace_file_route,
                                    list_trace_location="../bin/list_trace")
        # 验证记录的数量是否符合预期
        self.assertEqual(len(records), 8)
        # 设置任务提交时间列表
        submit_times=[0, 2,4,6, 100, 102, 104, 106]
        # 获取第一个任务的提交时间，并将其作为基准时间
        first_submit = int(records[0]["SUBMIT"])
        # 将提交时间列表中的每个时间加上第一个任务的提交时间
        submit_times = [x+first_submit for x in submit_times]

        # 遍历每个记录和对应的提交时间，验证记录的细节是否符合预期
        for (rec, submit_time) in zip(records, submit_times):
            self.assertEqual(int(rec["SUBMIT"]), submit_time)
            self.assertEqual(int(rec["NUM_TASKS"]) * 
                             int(rec["CORES_PER_TASK"]), 
                             24)
            self.assertEqual(int(rec["DURATION"]), 36000)
            self.assertEqual(int(rec["WCLIMIT"]), 601)
        
    
    def test_generate_trace_files_overload(self):
        """
        测试在不同种子字符串和配置下，trace文件是否正确生成和运行。
        此函数检查ExperimentRunner配置、trace文件生成路径、以及生成的trace文件中的数据是否符合预期。
        """
        # 遍历不同的种子字符串，用于生成不同的trace文件
        for seed_string in ["seeeed", "asdsa", "asdasdasd", "asdasdasdas",
                            "asdasdlkjlkjl", "eworiuwioejrewk", "asjdlkasdlas"]:
            # 配置ExperimentRunner，设置trace文件夹路径、生成文件夹路径、本地模式、主机信息和用户信息
            ExperimentRunner.configure(
                                       "tmp/trace_folder",
                                       "tmp", 
                                       True,
                                       "myhost", "myUser")
            # 确保ExperimentRunner的配置属性被正确设置
            self.assertEqual(ExperimentRunner._trace_folder,"tmp/trace_folder")
            self.assertEqual(ExperimentRunner._trace_generation_folder, "tmp")
            self.assertEqual(ExperimentRunner._local, True)
            # 设置工作负载持续时间为4小时
            workload_duration=4*3600
            # 初始化机器模型，并获取总核心数
            m=Edison2015()
            total_cores=m.get_total_cores()
            # 创建实验定义，包括种子字符串、机器类型、trace类型等配置
            ed = ExperimentDefinition(
                     seed=seed_string,
                     machine="edison",
                     trace_type="single",
                     manifest_list=[],
                     workflow_policy="no",
                     workflow_period_s=0,
                     workflow_handling="single",
                     preload_time_s = 0,
                     start_date = datetime(2016,1,1),
                     workload_duration_s = workload_duration,
                     overload_target=1.2
                     )

            # 创建ExperimentRunner实例，并传入实验定义
            er = ExperimentRunner(ed)
            # 调用_generate_trace_files方法生成trace文件
            er._generate_trace_files(ed)
            # 构造trace文件的路径
            trace_file_route=("tmp/{0}".format(ed.get_trace_file_name()))
            # 确保trace文件已成功生成
            self.assertTrue(os.path.exists(trace_file_route))
            # 从生成的trace文件中提取记录
            records=trace_gen.extract_records(file_name=trace_file_route,
                                    list_trace_location="../bin/list_trace")
            # 初始化累计核心小时数为0
            acc_core_hours=0
            # 遍历记录，计算累计核心小时数
            for rec in records:
                acc_core_hours+=(int(rec["NUM_TASKS"]) 
                                  * int(rec["CORES_PER_TASK"])
                                  * int(rec["DURATION"]))

            # 打印压力指数，即累计核心小时数与总核心小时数的比率
            print "pressure Index:", (float(acc_core_hours) / 
                                      float(total_cores*workload_duration))
            # 确保累计核心小时数在预期的范围内
            self.assertGreater(acc_core_hours,
                                  1.1*total_cores*workload_duration)
            self.assertLess(acc_core_hours, 
                                   1.5*total_cores*workload_duration)
        
    def test_place_trace_files_local_and_clean(self):
        """
        测试本地配置和清理追踪文件的功能

        此测试函数旨在验证ExperimentRunner类在本地模式下正确配置追踪文件夹，
        生成、放置和清理追踪文件的能力它还确保了正确的文件被创建和删除
        """
        # 配置ExperimentRunner的参数，用于指定追踪文件夹、配置本地模式等
        ExperimentRunner.configure(
                                   "tmp/dest",
                                   "tmp/orig", 
                                   True,
                                   "myhost", "myUser",
                                   scheduler_folder="./tmp/sched",
                                   scheduler_conf_dir="./tmp/conf",
                                   manifest_folder="manifests")
        # 确保ExperimentRunner的内部变量被正确设置
        self.assertEqual(ExperimentRunner._trace_folder,  "tmp/dest")
        self.assertEqual(ExperimentRunner._trace_generation_folder, "tmp/orig")
        self.assertEqual(ExperimentRunner._local, True)
        # 确保所需的目录结构被创
        ensureDir("./tmp/orig")
        ensureDir("./tmp/dest")
        ensureDir("./tmp/sched")
        ensureDir("./tmp/conf")
        # 创建一个ExperimentDefinition实例，定义实验的参数
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 41,
                 overload_target=1.1)
        # 创建并配置一个ExperimentRunner实例
        er = ExperimentRunner(ed)
        # 生成追踪文件，并获取文件名
        filenames=er._generate_trace_files(ed)
        # 将生成的追踪文件和用户文件放置到目标文件夹
        er._place_trace_file(filenames[0])
        er._place_users_file(filenames[2])
        # 验证追踪文件和用户文件是否被正确放置
        self.assertTrue(os.path.exists(
                         "tmp/dest/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.trace"))
        self.assertTrue(os.path.exists(
                        "tmp/conf/users.sim"))
        # 确保原始文件夹中不存在追踪文件和用户文件
        self.assertFalse(os.path.exists(
                         "tmp/orig/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.trace"))
        
        self.assertFalse(os.path.exists(
                        "tmp/orig/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.users"))
        # 清理追踪文件，并验证文件是否被删除
        er.clean_trace_file()
        self.assertFalse(os.path.exists(
                         "tmp/dest/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.trace"))
        self.assertFalse(os.path.exists(
                        "tmp/dest/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.users"))
    def test_place_trace_files_remote_and_clean(self):
        """
        测试将追踪文件放置到远程位置并清理功能。

        此测试用例旨在验证ExperimentRunner类的配置、追踪文件生成、放置以及清理功能。
        它首先配置ExperimentRunner，然后创建实验定义和实验运行器对象，生成追踪文件，
        并验证这些文件是否被正确放置和随后被正确清理。
        """
        # 配置ExperimentRunner的参数，为测试环境设置特定的目录和参数。
        ExperimentRunner.configure(
                                   "/tmp/tests/tmp/dest",
                                   "/tmp/tests/tmp/orig", 
                                   True,
                                   "locahost", None,
                                   scheduler_folder="/tmp/tests/tmp/sched",
                                   scheduler_conf_dir="/tmp/tests/tmp/conf",
                                   manifest_folder="manifests")
        # 确保ExperimentRunner的追踪文件目标目录和生成目录符合预期。
        self.assertEqual(ExperimentRunner._trace_folder,
                                        "/tmp/tests/tmp/dest")
        self.assertEqual(ExperimentRunner._trace_generation_folder,
                                        "/tmp/tests/tmp/orig")
        # 确保实验是否设置为本地模式。
        self.assertEqual(ExperimentRunner._local, True)
        # 确保所有必要的目录存在。
        ensureDir("/tmp/tests/tmp/dest")
        ensureDir("/tmp/tests/tmp/orig")
        ensureDir("/tmp/tests/tmp/sched")
        ensureDir("/tmp/tests/tmp/conf")
        # 创建一个实验定义对象，用于生成追踪文件。
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 41,
                 overload_target=1.1)
        # 创建一个实验运行器对象，并基于实验定义生成追踪文件。
        er = ExperimentRunner(ed)
        filenames=er._generate_trace_files(ed)
        # 将生成的追踪文件和用户文件放置到目标目录。
        er._place_trace_file(filenames[0])
        er._place_users_file(filenames[2])

        # 验证追踪文件和用户文件是否被正确放置。
        self.assertTrue(os.path.exists(
                         "/tmp/tests/tmp/dest/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.trace"))
        self.assertTrue(os.path.exists(
                        "/tmp/tests/tmp/conf/users.sim"))
        # 确保追踪文件没有被错误地放置到生成目录。
        self.assertFalse(os.path.exists(
                         "/tmp/tests/tmp/orig/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.trace"))

        # 清理之前放置的追踪文件，并验证文件是否已被删除。
        er.clean_trace_file()
        self.assertFalse(os.path.exists(
                         "/tmp/tests/tmp/dest/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.trace"))

        # 验证用户文件也被正确清理。
        self.assertFalse(os.path.exists(
                        "/tmp/tests/tmp/dest/edison-single-m1.0manifestSim.json"
                         "-period-p5-0.0-single-t-0d-0d-O1.1"
                         "-sseeeed.users"))
    def test_configure_slurm(self):
        """
        测试配置Slurm调度系统的过程。

        该测试用例主要用于验证ExperimentRunner类中的configure方法和_experimentDefinition实例的
        _configure_slurm方法是否按预期工作。通过设置特定的配置目录、机器类型和其他参数，确保Slurm
        配置文件被正确生成和处理。
        """
        # 配置实验运行器的测试目录和参数，开启测试模式，并指定调度配置目录和本地配置目录。
        ExperimentRunner.configure("/tmp/tests/tmp/dest",
                                   "/tmp/tests/tmp/orig", 
                                   True,
                                   "locahost", None,
                                   scheduler_conf_dir="tmp/conf",
                                   local_conf_dir="tmp/conf_orig")
        ensureDir("tmp/conf")
        ensureDir("tmp/conf_orig")

        # 如果存在旧的Slurm配置文件，则删除，以确保测试的干净环境。
        if os.path.exists("tmp/conf/slurm.conf"):
            os.remove("tmp/conf/slurm.conf")

        # 创建并写入原始的Slurm配置文件，用于测试不同的配置场景。
        orig=open("tmp/conf_orig/slurm.conf.edison.regular", "w")
        orig.write("regular")
        orig.close()

        # 创建并写入另一种配置场景的Slurm配置文件。
        orig=open("tmp/conf_orig/slurm.conf.edsion.wfaware", "w")
        orig.write("aware")
        orig.close()

        # 创建一个实验定义实例，指定各种实验参数，如种子、机器类型、跟踪类型等。
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 41)
        # 创建一个实验运行器实例，并使用之前定义的实验定义实例对其进行初始化。
        er = ExperimentRunner(ed)
        # 调用实验运行器实例的方法来配置Slurm。
        er._configure_slurm()

        # 打开生成的Slurm配置文件，读取第一行，并断言其内容与预期相符。
        final=open("tmp/conf/slurm.conf")
        line=final.readline()
        self.assertEqual("regular", line)
        final.close()
    
    def test_is_it_running(self):
        """
        测试ExperimentRunner的is_it_running方法是否能正确判断进程是否在运行。

        此方法首先配置实验运行器，然后定义一个实验，接着创建ExperimentRunner实例，
        并使用is_it_running方法检查给定名称的进程是否在运行。
        """
        # 配置实验运行器的参数，包括目录、主机、调度和本地配置文件目录等。
        ExperimentRunner.configure(
                                   "/tmp/tests/tmp/dest",
                                   "/tmp/tests/tmp/orig", 
                                   True,
                                   "locahost", None,
                                   scheduler_conf_dir="tmp/conf",
                                   local_conf_dir="tmp/conf_orig")
        # 定义一个实验，包括种子、机器、跟踪类型、清单列表、工作流策略等参数
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 41)
        # 创建ExperimentRunner实例，传入前面定义的实验。
        er = ExperimentRunner(ed)
        # 检查名为"python"的进程是否在运行，断言其为真。
        self.assertTrue(er.is_it_running("python"))
        # 检查名为"pythondd"的进程是否在运行，断言其为假，因为这个进程名不存在。
        self.assertFalse(er.is_it_running("pythondd"))
    
    def test_is_it_running_failed_comms(self):
        """
        测试'is_it_running'方法在通信失败时是否正确抛出异常。

        此函数通过配置ExperimentRunner并使用特定的参数初始化ExperimentDefinition来模拟实验环境。
        然后，它尝试调用'is_it_running'方法，并期望该方法在通信失败时抛出SystemError异常。
        """
        ExperimentRunner.configure(
                                   "/tmp/tests/tmp/dest",
                                   "/tmp/tests/tmp/orig", 
                                   False,
                                   "fakehost.fake.com", "aUSer",
                                   scheduler_conf_dir="tmp/conf",
                                   local_conf_dir="tmp/conf_orig")
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 20,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 41)
        er = ExperimentRunner(ed)
        self.assertRaises(SystemError, er.is_it_running, "python")
        
    def test_run_simulation(self):
        """
        测试运行模拟的功能。

        本函数配置实验运行环境，定义实验参数，并调用方法来运行模拟，以验证模拟运行的正确性。
        """
        # 配置实验运行所需的各种参数和路径
        ExperimentRunner.configure(trace_folder="/tmp/",
                                   trace_generation_folder="tmp", 
                                   local=False,
                                   run_hostname=self._vm_ip,
                                   run_user=None,
                                   scheduler_conf_dir="/scsf/slurm_conf",
                                   local_conf_dir="configs/",
                                   scheduler_folder="/scsf/",
                                   drain_time=100)
        # 确保trace生成文件夹存在
        ensureDir("tmp")
        # 定义实验参数
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 60,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 3600)
        # 创建并运行实验
        er = ExperimentRunner(ed)
        er.create_trace_file()
        er._run_simulation()
        # 停止模拟并验证模拟是否完成
        er.stop_simulation()
        self.assertTrue(er.is_simulation_done())
        
    def test_do_full_run(self):
        """
        测试完整运行实验的过程，包括数据库连接、实验定义、配置实验运行环境、
        以及执行实验运行的各个步骤。
        """
        # 创建数据库对象，用于连接和操作Slurm会计数据库
        sched_db_obj = DB(self._vm_ip,
                          "slurm_acct_db",
                          os.getenv("SLURMDB_USER", None),
                          os.getenv("SLURMDB_PASS", None))
        # 创建结果追踪对象，用于记录实验结果
        trace = ResultTrace()
        # 确保测试完成后删除创建的trace表
        self.addCleanup(self._del_table, "traces")
        # 创建用于存储实验结果的trace表
        trace.create_trace_table(self._db, "traces")
        # 配置实验运行环境，包括设置必要的目录和参数
        ExperimentRunner.configure(trace_folder="/tmp/",
                                   trace_generation_folder="tmp", 
                                   local=False,
                                   run_hostname=self._vm_ip,
                                   run_user=None,
                                   scheduler_conf_dir="/scsf/slurm_conf",
                                   local_conf_dir="configs/",
                                   scheduler_folder="/scsf/",
                                   drain_time=100)
        # 确保trace_generation_folder目录存在
        ensureDir("tmp")
        # 定义实验参数，包括实验的种子、机器、追踪类型等
        ed = ExperimentDefinition(
                 seed="seeeed",
                 machine="edison",
                 trace_type="single",
                 manifest_list=[{"share": 1.0, "manifest": "manifestSim.json"}],
                 workflow_policy="period",
                 workflow_period_s=5,
                 workflow_handling="single",
                 preload_time_s = 60,
                 start_date = datetime(2016,1,1),
                 workload_duration_s = 1800)
        # 确保测试完成后删除创建的experiment表
        self.addCleanup(self._del_table, "experiment")
        # 创建用于存储实验定义的表
        ed.create_table(self._db)
        # 存储实验定义到数据库
        ed.store(self._db)
        # 创建实验运行对象，并执行实验
        er = ExperimentRunner(ed)
        # 断言实验能够成功完成整个运行过程
        self.assertTrue(er.do_full_run(sched_db_obj, self._db))
        