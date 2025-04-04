from generate import WorkloadGenerator, TimeController
from generate.pattern import (WorkflowGeneratorSingleJob, RepeatingAlarmTimer,
                              WorkflowGeneratorMultijobs, PatternGenerator)
from generate.special import SpecialGenerators
from generate.special.machine_filler import filler
from slurm.trace_gen import TraceGenerator
import os
import random_control
import shutil
import subprocess

from os import path
from datetime import timedelta
from stats.trace import ResultTrace
from tools.ssh import SSH
from time import sleep
from generate.overload import OverloadTimeController

class ExperimentRunner(object):
    """工作器类，能够接受实验定义、生成其工作负载、运行实验并导入以存储结果。
    """
    @classmethod
    def configure(cld,
                  trace_folder="/TBD/", #/TBD/ 是一种占位符，通常代表 "To Be Determined"（待定）
                  trace_generation_folder="/TBD/",
                  local=True, run_hostname=None,
                  run_user=None,
                  local_conf_dir="/TBD",
                  scheduler_conf_dir="/TBD/",
                  scheduler_conf_file_base="slurm.conf",
                  scheduler_folder="/TBD",
                  scheduler_script="run_sim.sh",
                  stop_sim_script = "stop_sim.sh",
                  manifest_folder="manifests",
                  scheduler_acc_table="perfdevel_job_table",
                  drain_time=6*3600):
        """ T他的类方法配置所有experimentunner实例所需的运行时参数。
        Args:
        - trace_folder: 将存储工作负载文件的最终文件系统目的地（本地或远程）.
        - trace_generation_folder: 可以临时生成工作负载文件的本地文件系统文件夹.
        - local: 如果为false，则所有复制文件或执行命令的操作都将在执行该代码的主机上执行。
        如果为True，这些操作将通过ssh对“run_hostname”执行。远程模式需要对run_user进行无密码的ssh访问。
        - local_conf_dir: local file system location where ExperimentRunner
            will find configuration model files to configure the experiment
            scheduler.
        - scheduler_conf_dir: local or remote route to the experiment's
            scheduler configuration folder.
        - scheduler_conf_file_base: name of the experiment's
            scheduler configuration file.
        - scheduler_folder: local or remote folder for the experiment's
            scheduler scheduler_script and stop_sim_sript script files.
        - scheduler_script: name of the script starting the scheduler 
            simulation.
        - stop_sim_script: name of the script capable of stoping the scheduler
            simulation.
        - manifest_folder: 在这个本地文件夹中，实验者可以找到要在模拟中使用的manifest.
        - scheduler_acc_table: 调度程序数据库中用于存储作业记帐信息的表的名称.
        """
        cld._trace_folder= trace_folder
        cld._trace_generation_folder = trace_generation_folder
        cld._local = local
        cld._run_hostname = run_hostname
        cld._run_user = run_user
        cld._scheduler_conf_dir=scheduler_conf_dir
        cld._scheduler_conf_file_base=scheduler_conf_file_base
        cld._local_conf_dir=local_conf_dir
        cld._scheduler_folder=scheduler_folder
        cld._scheduler_script=scheduler_script
        cld._stop_sim_script=stop_sim_script
        cld._manifest_folder=manifest_folder
        cld._scheduler_acc_table = scheduler_acc_table
        cld._drain_time = drain_time
    
    @classmethod
    def get_manifest_folder(cld):
        if hasattr(cld, "_manifest_folder"):
            return cld._manifest_folder
        return "./"
        
    def __init__(self, definition):
        """Constructor.
        Args:
        - definition: 定义对象，其中包含由该对象运行的实验的配置.
        """
        self._definition=definition
    
    
    def do_full_run(self, scheduler_db_obj, store_db_obj):
        """ 创建工作负载跟踪并根据self._definition设置调度器配置。
            然后在调度程序中播放它们，运行模拟，导入结果跟踪，并将其存储在数据库中。它也相应地改变了实验的状态。
        Args:
        - scheduler_db_obj: DBManager对象，配置为连接到调度程序的数据库。
        - store_db_obj: DBManager对象，配置为连接到应该存储结果跟踪的数据库。
            如果模拟生成了有效的跟踪，则返回True。假,否则.
        """
        # 准备机器状态以进行全新的模拟运行
        self._refresh_machine()
        # 创建跟踪文件，用于记录模拟过程中的数据
        self.create_trace_file()
        # 执行模拟过程
        self.do_simulation()
        # 标记实验状态为正在模拟，并记录执行模拟的主机名
        self._definition.mark_simulating(store_db_obj,
                                     worker_host=ExperimentRunner._run_hostname)
        # 等待模拟结束
        self.wait_for_sim_to_end()
        # 检查跟踪数据有效性并存储，如果有效则标记模拟完成，否则标记模拟失败
        if self.check_trace_and_store(scheduler_db_obj, store_db_obj):
            self._definition.mark_simulation_done(store_db_obj)
            self.clean_trace_file()
            return True
        else:
            self._definition.mark_simulation_failed(store_db_obj)
            return False

    def check_trace_and_store(self, scheduler_db_obj, store_db_obj):
        """从调度器数据库导入实验结果跟踪数据并存储到中央数据库
        Args:
            scheduler_db_obj (DBManager): 连接到调度器数据库的数据库管理器对象
                - 用于获取实验生成的原始跟踪数据
            store_db_obj (DBManager): 连接到结果存储数据库的数据库管理器对象
                - 用于持久化存储处理后的跟踪数据
        Returns:
            bool: 返回模拟有效性状态
                - True 表示模拟产生了有效跟踪数据
                - False 表示存在数据异常或未完成模拟
        处理流程:
            1. 从调度器数据库导入原始跟踪数据
            2. 验证跟踪数据的完整性和有效性
            3. 将验证后的跟踪数据存入结果存储库
        """
        # 初始化结果跟踪对象并导入调度器数据库数据
        result_trace = ResultTrace()
        result_trace.import_from_db(scheduler_db_obj,
                                    ExperimentRunner._scheduler_acc_table)

        status = True
        # 获取实验定义的结束时间阈值
        end_time = self._definition.get_end_epoch()

        # 检查是否存在空模拟结果
        if len(result_trace._lists_start["time_end"]) == 0:
            print
            "Error: No simulated jobs"
            return False

        # 验证最后作业的提交时间是否满足实验持续时间要求
        last_job_end_time = result_trace._lists_submit["time_submit"][-1]
        if last_job_end_time < (end_time - 600):
            print
            "Simulation ended too soon: {0} vs. expected {1}.".format(
                last_job_end_time,
                end_time)
            status = False

        # 持久化存储已验证的跟踪数据
        result_trace.store_trace(store_db_obj, self._definition._trace_id)
        return status

    def create_trace_file(self):
        """根据实验定义创建工作负载文件，并将其存储在调度器中。
            这些文件由作业提交列表和有效用户列表组成。
        """
        # 生成工作负载文件，包括作业提交列表和有效用户列表
        file_names = self._generate_trace_files(self._definition)
        # 将作业提交列表文件放置到调度器中
        self._place_trace_file(file_names[0])
        # 将有效用户列表文件放置到调度器中
        self._place_users_file(file_names[2])

    def _generate_trace_files(self, definition, trace_generator=None):
        """
        根据实验定义生成工作负载跟踪文件，并保存到指定路径。
        Args:
            definition (ExperimentDefinition): 包含实验配置参数的定义对象，用于获取种子、机器配置、时间范围等关键参数。
            trace_generator (TraceGenerator, optional): 跟踪生成器实例。若未提供，将自动创建新实例。
        Returns:
            list[str]: 包含生成的三个文件名的列表，依次为：
                - 跟踪文件名（trace_file_name）
                - QoS文件名（qos_file_name）
                - 用户文件名（users_file_name）
        处理流程：
        1. 初始化生成器组件
        2. 配置工作负载生成参数
        3. 生成跟踪数据
        4. 计算性能指标
        5. 持久化生成结果
        """
        # 初始化trace生成器
        if trace_generator is None:
            trace_generator = TraceGenerator()

        # 设置全局随机种子，保证实验可重复性
        print
        "This is the seed to be used:", definition._seed
        random_control.set_global_random_gen(seed=definition._seed)

        # 获取机器配置中的核心数、运行时限制等过滤参数
        machine = definition.get_machine()
        (filter_cores, filter_runtime,
         filter_core_hours) = machine.get_filter_values()

        # 初始化核心工作负载生成器，组合各类配置参数
        wg = WorkloadGenerator(machine=definition.get_machine(),
                               trace_generator=trace_generator,
                               user_list=definition.get_user_list(),
                               qos_list=definition.get_qos_list(),
                               partition_list=definition.get_partition_list(),
                               account_list=definition.get_account_list())

        # 工作流策略处理分支
        if definition._workflow_policy.split("-")[0] == "sp":
            # 特殊策略处理：注册专用生成器到定时器
            special_gen = SpecialGenerators.get_generator(
                definition._workflow_policy,
                wg,
                register_datetime=(definition._start_date -
                                   timedelta(0, definition._preload_time_s)))
            wg.register_pattern_generator_timer(special_gen)
        else:
            # 常规策略处理流程
            # 配置基础过滤策略和最大任务间隔时间
            wg.config_filter_func(machine.job_can_be_submitted)
            wg.set_max_interarrival(machine.get_max_interarrival())

            # 验证实验类型与trace生成的兼容性
            if definition._trace_type != "single":
                raise ValueError("Only 'single' experiments require trace "
                                 "generation")

            # 过载控制模块配置
            if definition.get_overload_factor() > 0.0:
                print
                "doing overload:", definition.get_overload_factor()
                max_cores = machine.get_total_cores()
                single_job_gen = PatternGenerator(wg)
                overload_time = OverloadTimeController(
                    single_job_gen,
                    register_datetime=(definition._start_date -
                                       timedelta(0, definition._preload_time_s)))
                overload_time.configure_overload(
                    trace_generator,
                    max_cores,
                    overload_target=definition.get_overload_factor())
                print
                "about to register", wg, overload_time
                wg.register_pattern_generator_timer(overload_time)

            # 根据工作流处理方式初始化对应生成器
            manifest_list = [m["manifest"] for m in definition._manifest_list]
            share_list = [m["share"] for m in definition._manifest_list]
            if (definition._workflow_handling == "single" or
                    definition._workflow_handling == "manifest"):
                flow = WorkflowGeneratorSingleJob(manifest_list, share_list, wg)
            else:
                flow = WorkflowGeneratorMultijobs(manifest_list, share_list, wg)

            # 配置工作流触发策略：周期触发或比例触发
            if definition._workflow_policy == "period":
                alarm = RepeatingAlarmTimer(flow,
                                            register_datetime=definition._start_date)
                alarm.set_alarm_period(definition._workflow_period_s)
                wg.register_pattern_generator_timer(alarm)
            elif definition._workflow_policy == "percentage":
                wg.register_pattern_generator_share(flow,
                                                    definition._workflow_share / 100)

        # 初始化等待时间填充处理
        target_wait = definition.get_forced_initial_wait()
        if target_wait:
            default_job_separation = 10
            separation = int(os.getenv("FW_JOB_SEPARATION",
                                       default_job_separation))
            filler(wg,
                   start_time=TimeController.get_epoch((definition._start_date -
                                                        timedelta(0, definition._preload_time_s))),
                   target_wait=target_wait,
                   max_cores=machine.get_total_cores(),
                   cores_per_node=machine._cores_per_node,
                   job_separation=separation)
            trace_generator.reset_work()

        # 核心trace生成阶段
        wg.generate_trace((definition._start_date -
                           timedelta(0, definition._preload_time_s)),
                          (definition._preload_time_s +
                           definition._workload_duration_s))

        # 计算作业压力指标（系统负载率）
        max_cores = machine.get_total_cores()
        total_submitted_core_s = trace_generator.get_total_submitted_core_s()
        job_pressure = (float(total_submitted_core_s)
                        /
                        float((definition._preload_time_s +
                               definition._workload_duration_s) * max_cores)
                        )
        print("Observed job pressure (bound): {0}".format(
            job_pressure))

        # 持久化生成结果到文件系统
        trace_generator.dump_trace(path.join(
            ExperimentRunner._trace_generation_folder,
            definition.get_trace_file_name()))
        trace_generator.dump_qos(path.join(
            ExperimentRunner._trace_generation_folder,
            definition.get_qos_file_name()))
        trace_generator.dump_users(path.join(
            ExperimentRunner._trace_generation_folder,
            definition.get_users_file_name()),
            extra_users=definition.get_system_user_list()
        )
        trace_generator.free_mem()

        # 返回生成的文件名列表
        return [definition.get_trace_file_name(),
                definition.get_qos_file_name(),
                definition.get_users_file_name()]

    def _place_trace_file(self, filename):
        """将工作负载文件放入调度器：作业提交列表和用户列表中。从ExperimentRunner._local_trace_files中读取它们
        Args:
        - filename: string with the name of the workload files.
        """
        source = path.join(
                        ExperimentRunner._trace_generation_folder, filename)
        dest =  path.join(
                        ExperimentRunner._trace_folder, filename)
        self._copy_file(source, dest, move=True)
        
        for manifest in self._definition._manifest_list:
            man_name=manifest["manifest"]
            man_route_orig=path.join(ExperimentRunner._manifest_folder, 
                                     man_name)
            man_route_dest=path.join(ExperimentRunner._scheduler_folder, 
                                     man_name)
            self._copy_file(man_route_orig, man_route_dest)
    
    def _place_users_file(self, filename):
        """Places the users list in the scheduler configuration folder. It
        is read from the local trace generation folder.
        Args:
        - filename: string with the name of the users files. 

        """
        source = path.join(
                        ExperimentRunner._trace_generation_folder, filename)
        dest =  path.join(
                        ExperimentRunner._scheduler_conf_dir, "users.sim")
        self._copy_file(source, dest, move=True)
      

    def clean_trace_file(self):
        """Removes the trace file placed in the scheduler.
        """
        filenames = [self._definition.get_trace_file_name()]
        for filename in filenames:
            dest =  path.join(
                        ExperimentRunner._trace_folder, filename)
            self._del_file_dest(dest)
            
    def do_simulation(self):
        """根据实验工作流处理配置配置调度程序。运行模拟。
             Trace必须已经放好了
        """
        # 配置Slurm调度系统
        self._configure_slurm()
        # 执行模拟
        self._run_simulation()
    
    def _refresh_machine(self):
        """重新启动工作线程并等待，直到它准备好."""
        command=["sudo", "/sbin/shutdown", "-r", "now"] 
        print "About to reboot the machine, waiting 60s"
        self._exec_dest(command)
        sleep(60)
        while not "hola" in self._exec_dest(["/bin/echo", "hola"]):
            print "Machine is not ready yet, waiting 30s more..."
            sleep(30)
        print "Wait done, machine should be ready."

    def stop_simulation(self):
        """Stops the scheduler and simulator. Non graceful stop."""
        self._exec_dest([path.join(ExperimentRunner._scheduler_folder,
                               ExperimentRunner._stop_sim_script)])
    
    def _run_simulation(self):
        """
        启动模拟器并检查其运行状态。

        本函数首先构造模拟器的执行命令，然后尝试启动模拟器。
        启动后，它会多次检查模拟器是否正在运行，以确认模拟器成功启动。
        如果模拟器在指定时间内未启动，将抛出异常。
        """
        # 构造追踪文件的完整路径
        trace_file=path.join(
                        ExperimentRunner._trace_folder,
                        self._definition.get_trace_file_name())
        # 构造调度脚本的完整路径
        script_route=path.join(ExperimentRunner._scheduler_folder,
                               ExperimentRunner._scheduler_script)

        # 构造启动模拟器的命令列表
        command=[script_route, str(self._definition.get_end_epoch()+
                                   ExperimentRunner._drain_time),
                        path.join(
                        ExperimentRunner._trace_folder, trace_file), 
                        "./sim_mgr.log"]
        # 打印即将执行的命令
        print "About to run simulation: \n{0}".format(" ".join(command))
        # 执行命令并使模拟器在后台运行
        self._exec_dest(command, background=True)
        running=False
        # 检查模拟器运行状态，最多检查60次，每次间隔10秒
        for i in range(6*10):
            sleep(10)
            try:
                # 尝试检查模拟器是否正在运行
                running = self.is_sim_running()
            except SystemError:
                # 如果通信失败，打印错误信息并继续尝试
                print "Failed comms to machine, keep trying."
            # 打印模拟器运行状态
            print "Is Simulation running?", running
            if running:
                break
        # 如果模拟器正在运行，退出循环
        if not running:
            raise Exception("Error Starting simulation!!!!")
        
        
          
 
    def _configure_slurm(self):
        """设置调度器的配置文件，选择特定的工作流调度策略。
            本函数根据工作流处理方式和机器定义来选择合适的配置文件。
            如果定义了自定义配置文件，则使用该配置文件。否则，根据机器类型和
            工作流处理方式构建配置文件路径，并将此配置文件复制到预定位置。
        """
        # 默认配置为常规调度策略
        cad="regular"
        # 如果工作流处理方式为manifest，则使用wfaware策略
        if self._definition._workflow_handling=="manifest":
            cad="wfaware"
        # 构建原始配置文件路径
        orig_conf_file = "{2}.{0}.{1}".format(
                                    self._definition._machine,
                                    cad,
                                    ExperimentRunner._scheduler_conf_file_base)
        # 如果定义了自定义配置文件，则使用之
        if self._definition._conf_file:
            orig_conf_file = self._definition._conf_file
        # 目标配置文件路径
        dest_conf_file=ExperimentRunner._scheduler_conf_file_base

        # 构建完整的原始配置文件路径
        orig=path.join(ExperimentRunner._local_conf_dir, orig_conf_file)
        # 构建目标配置文件路径
        dest=path.join(ExperimentRunner._scheduler_conf_dir, dest_conf_file)
        # 复制配置文件到目标位置
        self._copy_file(orig, dest)
    
    def _copy_file(self, orig, dest, move=False):
        if ExperimentRunner._local:
            shutil.copy(orig, dest)
        else:
            ssh = SSH(ExperimentRunner._run_hostname,
                          ExperimentRunner._run_user)
            ssh.push_file(orig, dest)
        if move:
            os.remove(orig)
    def _del_file_dest(self, dest):
        if ExperimentRunner._local:
                os.remove(dest)
        else:
            ssh = SSH(ExperimentRunner._run_hostname,
                      ExperimentRunner._run_user)
            ssh.delete_file(dest)
    def _exec_dest(self, command, background=False):
        """
        根据实验运行的环境（本地或远程）执行给定的命令。

        参数:
        - command: 要执行的命令，可以是字符串或命令列表。
        - background: 布尔值，指示命令是否应该在后台执行。

        返回:
        - output: 命令的输出结果。
        """
        # 根据ExperimentRunner的_local属性判断是否在本地运行
        if ExperimentRunner._local:
            # 在本地环境下执行命令
            if not background:
                # 如果不是在后台执行，使用subprocess.Popen运行命令并捕获输出
                p = subprocess.Popen(command, stdout=subprocess.PIPE)
                output, err = p.communicate()
                # 如果没有错误信息，则将其置为空字符串
                if err is None:
                    err=""
            else:
                # 如果在后台执行，仅启动进程而不捕获输出
                output=""
                p = subprocess.Popen(command)
            
        else:
            # 在远程环境下执行命令
            # 创建SSH对象以连接到远程主机
            ssh = SSH(ExperimentRunner._run_hostname,
                      ExperimentRunner._run_user)
            # 使用SSH对象执行远程命令
            output, err, rc= ssh.execute_command(command[0], command[1:],
            # 返回命令的输出结果                                         background=background)
        return output
    def is_simulation_done(self):
        """如果模拟引擎不再运行，则返回True"""
        # 仅通过检查 sim_mgr 进程是否终止来判断模拟是否完成。
        return not self.is_sim_running()
    
    def wait_for_sim_to_end(self):
        """阻塞，直到sim_mgr进程停止运行。
        该方法通过循环不断检查 sim_mgr 管理的模拟是否结束。如果模拟仍在运行，则等待一段时间后再次检查。
        如果在检查过程中发生异常，它会捕获异常并在等待一段时间后重试。
    """
        # 初始化计数器
        count = 0
        wait_time = 10
        failed_comms_count=0
        # 持续检查模拟是否结束
        while True:
            try:
                # 如果模拟已结束，退出循环
                if self.is_simulation_done():
                    break
                # 重置失败通信计数
                failed_comms_count=0
            except:
                # 如果发生异常，增加失败通信计数
                failed_comms_count+=1
                print("Failed commons while checking is sim was done, failed"
                      " count: {0}".format(failed_comms_count))
            # 计算总等待时间
            total_time=count*wait_time
            # 打印模拟未结束的时间
            print "Simulation has not ended. Wait time: {0}:{1}:{2}".format(
                                      total_time/3600, (total_time/60)%60,
                                      total_time%60  )
            # 增加计数以准备下一次等待周期
            count+=1
            # 等待一段时间后再进行下一次检查
            sleep(wait_time)
        
        
    
    def is_sim_running(self):
        """
        检查模拟环境中的所有关键进程是否都在运行。

        该方法首先检查"sim_mgr"进程是否在运行，然后检查"slurmctld"和"slurmd"进程。
        如果所有这些进程都在运行，则认为模拟环境正在运行。

        如果无法检查进程状态，将捕获SystemError异常，并输出错误信息。

        Returns:
            bool: 如果模拟环境中的所有关键进程都在运行，则返回True，否则返回False。
        """
        try:
            # 检查"sim_mgr"进程是否在运行
            if not self.is_it_running("sim_mgr"):
                return False
            # 检查"slurmctld"进程是否在运行
            if not self.is_it_running("slurmctld"):
                return False
            # 检查"slurmd"进程是否在运行
            if not self.is_it_running("slurmd"):
                return False
            # 如果所有关键进程都在运行，返回True
            return True
        except SystemError:
            # 如果检查进程状态时发生错误，输出错误信息并抛出SystemError异常
            print("Error communicating to check remote processes")
            raise SystemError
            # 即使在发生异常的情况下，也认为模拟环境应该是在运行状态，返回True
            return True
    def is_it_running(self, proc):
        """检查名为proc的进程是否正在本地运行或正在远程执行。
        Args:
        - proc: 包含要检查的进程名称的字符串。
        """
        # 执行命令以获取当前正在运行的进程列表
        output=self._exec_dest(["/bin/ps", "-eo comm,state"])
        # 初始化计数器
        count = 0
        total_count=0
        # 遍历进程输出的每一行
        for line in output.split("\n"):
            total_count+=1
            if proc in line and not "Z" in line:
                count+=1
        # 如果找到至少一个匹配且非僵尸状态的进程，返回True
        if count>0:
            return True
        # 如果进程总数小于5，抛出系统错误异常
        if total_count<5:
            raise SystemError()
        # 默认返回False，表示未找到运行中的进程
        return False