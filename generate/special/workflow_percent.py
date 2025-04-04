"""
模式生成器，它将核心工时分配给工作流，并将1-share分配给常规工作。
支持upper_cap，限制工作负载允许的核心工作时间超过生产工作时间。

它是随机生成器的一个版本，其中返回的生成器的讨论不是随机的，而是由工作负载中存在的常规和工作流核心小时数控制的。
"""

from generate import RandomSelector

class WorkflowPercent(RandomSelector):
    """
    继承自RandomSelector的类，用于根据工作流和常规工作的核心小时数
    来生成随机对象，并支持通过upper_cap限制核心工作时间。
    """
    
    def __init__(self, random_gen, trace_gen, time_controller, total_cores,
                 max_pressure=None):
        """
            参数:
            - random_gen: 随机生成器对象，具有返回两个浮点数之间随机浮点数的uniform方法。
            - trace_gen: 用于跟踪工作负载生成器生成的作业和工作流核心小时数的跟踪生成器。
            - time_controller: 由工作负载生成器使用的计时控制器，提供工作负载生成时的运行时间。
            - total_cores: 系统中可用的核心数。
            - max_pressure: 如果设置，工作负载中的核心小时数将限制为max_pressure*total_cores*runtime。
        """
        super(WorkflowPercent, self).__init__(random_gen)
        self._trace_gen = trace_gen
        self._time_controller=time_controller
        self._total_cores=total_cores
        if max_pressure is None:
            max_pressure = 1.1
        self._max_pressure = 1.1
        
    
    def _get_pressure_index(self):
        """
        计算压力指数，即当前已生成的核心小时数与系统核心数和运行时间的比率。

        返回:
        - 压力指数，如果运行时间小于3600秒，则返回0。
        """
        total_runtime = self._time_controller.get_runtime()
        total_core_s =  self._trace_gen.get_total_actual_cores_s()
        pressure_index = 0
        if (total_runtime>=3600): 
            pressure_index = (float(total_core_s) /
                              float(self._total_cores*total_runtime))
        return pressure_index
        
    def get_random_obj(self):
        """
            根据配置的%share获取随机对象。
            如果工作流核心小时数小于配置的份额且压力指数未超过最大压力，则返回工作流对象；
            否则返回常规工作对象。
        """
        if len(self._prob_list)==2:
            # 获取工作流份额并检查工作流核心小时数
            if self._get_pressure_index() >self._max_pressure:
                return None
            workflow_share=self._prob_list[1]-self._prob_list[0]
            acc_wf_share=self._trace_gen.get_share_wfs()
            if (acc_wf_share is not None) and (acc_wf_share < workflow_share):
                return self._obj_list[1]
            else:
                return self._obj_list[0]
                    
        
        return super(WorkflowPercent, self).get_random_obj()
    
    def config_upper_cap(self, upper_cap):
        """
        配置最大压力值，用于限制工作负载中的核心小时数。

        参数:
        - upper_cap: 新的最大压力值。
        """
        self._max_pressure=upper_cap
    
    