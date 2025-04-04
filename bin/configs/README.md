slurm.conf.edison.regular：是slurm.conf，也就是slurm的配置文件


wfaware和regular的配置文件差异的地方：
    SlurmctldDebug=2
    SlurmdDebug=2
    SchedulerType=sched/wfbackfill
    SchedulerParameters=bf_interval=30,max_job_bf=50,wf_max_depth=50