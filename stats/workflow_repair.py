from stats.workflow import TaskTracker
from generate.pattern import WorkflowGeneratorMultijobs
from os import path
from orchestration.definition import ExperimentDefinition
from stats.trace import ResultTrace

class StartTimeCorrector(object):
    
    def __init__(self):
        self.manifest_dics = {}
        
    def correct_times(self, db_obj, trace_id):
        """修正实验跟踪记录中的任务时间戳
        根据实验工作流处理类型重新计算任务正确开始时间，并将修正后的时间应用到数据库
        Args:
            db_obj: 数据库连接对象，用于执行数据加载和更新操作
            trace_id: 需要修正的跟踪记录唯一标识符
        """
        self._experiment = ExperimentDefinition()
        self._experiment.load(db_obj, trace_id)
        
        self._trace = ResultTrace()
        print "Loading trace {0}".format(trace_id)
        self._trace.load_trace(db_obj, trace_id)
        trace_type = self._experiment._workflow_handling
        print "Calculating corrected start times for trace {0}".format(trace_id)
        modified_start_times = self.get_corrected_start_times(trace_type)
        print ("Found {0} jobs which start time was 0, but had ended.".format(
                                            len(modified_start_times)))
        print ("About to update times")
        self.apply_new_times(db_obj, modified_start_times)    
    
    def apply_new_times(self, db_obj, modified_start_times):
        trace_id=self._experiment._trace_id
        for id_job in modified_start_times.keys():
            time_start=modified_start_times[id_job]
            print ("updating trace_id({0}), id_job({1}) with time_start: {2}"
                   "".format(trace_id, id_job, time_start))
            self.update_time_start(db_obj,trace_id, id_job, time_start)
    
    def update_time_start(self, db_obj, trace_id, id_job, time_start):
        """
        query =("update traces set time_start={0} where trace_id={1} and "
                " id_job={2}",format(time_start, trace_id, id_job))
        """
        db_obj.setFieldOnTable(
                               "traces",
                               "time_start", str(time_start),
                               "id_job", str(id_job), 
                    extra_cond="and trace_id={0}".format(trace_id),
                    no_commas=True)
    def get_corrected_start_times(self, trace_type):
        modified_start_times = {}
        
        for (id_job, job_name, time_submit, time_start, time_end) in zip(
                    self._trace._lists_submit["id_job"],
                    self._trace._lists_submit["job_name"],
                    self._trace._lists_submit["time_submit"],
                    self._trace._lists_submit["time_start"],
                    self._trace._lists_submit["time_end"]):
            if time_start==0 and time_end!=0 and "wf" == job_name[:2]:
                modified_start_times[id_job]=self.get_time_start(job_name,
                                                              time_end,
                                                              trace_type)

        return modified_start_times

        
    def get_workflow_info(self, workflow_file):
        """获取指定工作流文件的资源信息并进行缓存
        从类实例的manifest_dics缓存字典中获取工作流文件信息，若不存在则解析计算后存储到缓存，
        最后返回该工作流文件对应的资源信息字典
        Args:
            workflow_file (str): 工作流配置文件名(需存在于manifest文件夹中)
        Returns:
            dict: 包含三个键值的字典：
                - cores (int): 工作流所需总核心数
                - runtime (int): 工作流预估总运行时间
                - tasks (int): 工作流包含的任务总数
        """
        if not workflow_file in self.manifest_dics.keys():
            from orchestration.running import ExperimentRunner
            manifest_route = path.join(ExperimentRunner.get_manifest_folder(),
                                      workflow_file)
            cores, runtime, tasks =  WorkflowGeneratorMultijobs.parse_all_jobs(
                                                                manifest_route)
            self.manifest_dics[workflow_file] = {"cores": cores, 
                                            "runtime":runtime, 
                                            "tasks":tasks}
        
        return self.manifest_dics[workflow_file]
        
            
        
    def get_time_start(self, job_name, time_end, trace_type="manifest"):
        name, stage_id, deps = TaskTracker.extract_wf_name(job_name)
        workflow_file="-".join(name.split("-")[:-1])
        from orchestration import ExperimentRunner
        manifest_info = self.get_workflow_info(workflow_file)
        (cores, runtime, tasks) =  (manifest_info["cores"],
                                    manifest_info["runtime"],
                                    manifest_info["tasks"])
        if stage_id is "":
            if trace_type == "multi":
                raise SystemError("Found a bounding job ({0}) in a "
                                  "dependencies type trace.".format(job_name))
            if trace_type == "manifest":
                return time_end
            else:
                return time_end-runtime 
        else:
            return time_end-int(tasks[stage_id]["runtime_sim"])
        
    @classmethod
    def get_traces_with_bad_time_starts(cls, db_obj):
        query = """
                SELECT traces.trace_id tid, name, count(*) cc
                FROM traces, experiment
                WHERE traces.trace_id=experiment.trace_id 
                     AND time_start=0 AND time_end!=0 
                     AND job_name!="sim_job"
                group by traces.trace_id
        """
        result_list=db_obj.doQueryDic(query)
        trace_id_list = [res["tid"] for res in result_list]
        return trace_id_list
    