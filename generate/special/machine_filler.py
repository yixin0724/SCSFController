from math import ceil


def filler(workload_gen, start_time, target_wait, max_cores, cores_per_node, 
           job_separation=10, final_gap=120):
    """
    生成填充作业以模拟工作负载。

    根据目标等待时间和最大核心数等参数，为给定的工作负载生成器生成一系列填充作业。
    这些作业旨在填充测试环境以评估系统性能。

    参数:
    - workload_gen: 工作负载生成器对象，用于生成新的作业。
    - start_time: 开始时间，用于计算时间戳。
    - target_wait: 目标等待时间，用于计算作业生成的起始时间戳和作业运行时间。
    - max_cores: 最大核心数，限制每个作业可以使用的最大核心数。
    - cores_per_node: 每个节点的核心数，用于计算每个作业应使用的核心数。
    - job_separation: 作业间隔时间，默认为10秒。
    - final_gap: 最后一个作业和开始时间之间的间隔，默认为120秒。
    """
    # 计算第一个作业生成的时间戳
    time_stamp=start_time-target_wait-final_gap
    # 计算需要生成的作业数量
    num_jobs = int(target_wait/job_separation)
    # 计算每个作业应分配的核心数
    cores_per_job=int(ceil(float(max_cores/cores_per_node)/float(num_jobs)) *
                      cores_per_node)

    # 生成所有作业
    for i in range(num_jobs):
        # 为每个作业调用_generate_new_job方法
        workload_gen._generate_new_job(
                            time_stamp,
                            cores=cores_per_job,
                            run_time=2*target_wait+final_gap,
                            wc_limit=(2*target_wait+final_gap)/60+1,
                            override_filter=True)
        # 更新下一个作业的生成时间
        time_stamp+=job_separation
    
    