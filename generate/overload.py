from pattern import PatternTimer


class OverloadTimeController(PatternTimer):
    """创建这个PatternTimer是为了使接收工作负载的系统过载。
        它将每秒提交一些作业，而提交的总核心小时数大约是overload_target乘以迄今为止提交作业时的系统容量。
        它配置一个10分钟的衰减窗口来观察过去的作业。它采用了一个滞后控制器，以5%为半径来避免振荡。.
    """
    
    def configure_overload(self, trace_generator, capacity_cores_s,
                           overload_target=0.0):
        """为过载配置特殊参数.
        Args:
        - trace_generator: TraceGenerator object used by the wrapping generator.
        - capacity_core_s: int representing the cores per second produced by the
            target system of the workload.
        - jobs_per_second: jobs to produce per second.
        - overload_target: how many times the capacity of the system in a period
            of time should be submitted.
        """
        self._trace_generator=trace_generator
        self._capacity_cores_s=capacity_cores_s
        self._overload_target=overload_target
        
        self._decay_window_size=3600
        self._hysteresis_radius=0.05
        
        self._trace_generator.set_submitted_cores_decay(self._decay_window_size)
        self._hysteresis_trend=0
        
    
    def is_it_time(self, current_timestamp):
                
        super(OverloadTimeController, self).is_it_time(current_timestamp)
        
        submitted_core_s, runtime = self._trace_generator.get_submitted_core_s()
        
        pressure_index=(float(submitted_core_s) / 
                      float(self._capacity_cores_s*runtime))
        
        if pressure_index<self._overload_target:
            return 1
        return 0
    
    def can_be_purged(self):
        return False
    
    def do_reentry(self):
        return True
    
    
    