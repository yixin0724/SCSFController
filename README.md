# SCSF

ScSF调度模拟框架包含两部分

​	ScSFController-0.1.tar.gz：ScSF 控制器，即控制器虚拟机。该控制器用于调用ScSF Worker去执行作业，并将结果拿过来进行分析。

​	ScSFWorkerForSlurm-0.1.tar.gz ：ScSF Worker虚拟机。这个安装包是为该虚拟机安装slurm并为它赋予woas技术的环境，模拟工作节点，可以拥有多个。该包包括用于安装、修补和配置基于 Slurm 14.3.8.1 的模拟 Worker 的自动脚本。它在 Ubuntu Server 16.04.3 上进行了全面测试。

ScSF的典型设置包括在Linux/Unix系统上运行的**控制器**和在Linux VM/Host中运行的至少一个**工作器**实例(要实现ssh免密)。

控制器：

​	实验在控制器中定义。它里面有个实验运行器的概念，也就是定义实验运行的配置等等。

​	控制器管理工作器实例、部署实验设置、运行模拟和收集结果。

​	控制器中还具有数据分析和绘图功能。



ScSF是一个调度仿真框架，它包含一组工具启用调度研究。


ScSF是一个完整的框架，其功能包括：

- 模拟工作负载。
- 根据模型生成工作负载。
- 通过调度器模拟器运行这些工作负载。
- 检索仿真结果并进行分析。
- 协调多个模拟的并发执行。
- 分析和比较仿真结果。
- 用工作流和不同的工作流提交策略运行实验。

这个read-me包括安装、配置和操作ScSF的信息。

python所需环境(python2.7)

​	MySQL-python(也就是MySQLdb，但是MySQL-python只支持python2.x)

​	numpy

​	scipy

​	matplotlib

​	pygraphviz

环境：

​	mysql5.7(密码不做要求)

​	virtualenv

