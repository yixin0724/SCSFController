# 前言

这个scsf框架是由论文R. Gonzalo P., E. Elmroth, P-O. Ostberg, and L. Ramakrishnan, ScSF: a Scheduling Simulation Framework', in Proceedings of the 21th Workshop on Job Scheduling Strategies for Parallel Processing, 2017 ( [link](http://www.jsspp.org/papers17/paper_2.pdf) )团队发布者制作的，这里仅仅为框架的使用作帮助文档。

------

ScSF调度模拟框架包含两部分

​	ScSFController-0.1.tar.gz：ScSF 控制器，即控制器虚拟机。该控制器用于调用ScSF Worker去执行作业，并将结果拿过来进行分析。

​	ScSFWorkerForSlurm-0.1.tar.gz ：ScSF Worker虚拟机。这个安装包是为该虚拟机安装slurm并为它赋予woas技术的环境，模拟工作节点，可以拥有多个。该包包括用于安装、修补和配置基于 Slurm 14.3.8.1 的模拟 Worker 的自动脚本。它在 Ubuntu Server 16.04.3 上进行了全面测试。

ScSF的典型设置包括在Linux/Unix系统上运行的**控制器**和在Linux VM/Host中运行的至少一个**工作器**实例(要实现ssh免密)。

控制器：

​	实验在控制器中定义。它里面有个实验运行器的概念，也就是定义实验运行的配置等等。

​	控制器管理工作器实例、部署实验设置、运行模拟和收集结果。

​	控制器中还具有数据分析和绘图功能。

ScSF是一个完整的框架，其功能包括：

- 模拟工作负载。
- 根据模型生成工作负载。
- 通过调度器模拟器运行这些工作负载。
- 检索仿真结果并进行分析。
- 协调多个模拟的并发执行。
- 分析和比较仿真结果。
- 用工作流和不同的工作流提交策略运行实验。

这个read-me包括安装、配置和操作ScSF的信息。





# 第一步 Worker的安装

## 前置

```
Slurm模拟器模拟的Slurm版本为14.3.8.1，主机名为“simulatorvm”。
实现两个虚拟机的ssh免密
Worker端要设置用户的无密码sudo
用于自动安装的脚本将在/scsf中安装和编译Slurm和模拟器。虽然模拟器可以在多种版本的Linux上工作，但脚本已经在Ubuntu Server 16.04.3上进行了广泛的测试。安装之后，可以使用根目录下的"/ scsf /"通过ScSF控制器控制slurm模拟器。



# 要确保Worker的安装包安装在根目录下的/somefolder，解压后的名字为slurmsimdeploy
cd /
sudo mkdir /somefolder
sudo chown -R yixin:yixin /somefolder
cd somefolder

# 将ScSF Worker安装包解压到根目录下的/somefolder
tar -zxvf ScSFWorkerForSlurm-0.1.tar.gz
mv ScSFWorkerForSlurm-0.1 slurmsimdeploy



开启ssh免密功能的用户
sudo apt-get install openssh-server
sudo apt-get install openssh-client
①进入当前用户的home目录，生成本机秘钥。
	cd ~
	ssh-keygen -t rsa -P ""
②将公钥追加到 authorized_keys 文件中。
	cat .ssh/id_rsa.pub >> .ssh/authorized_keys
③然后赋予authorized_keys 文件权限。
	chmod 600 .ssh/authorized_keys
④查看ssh是否配置成(第一次需要先yes)
	ssh localhost
	exit ssh
	ssh localhost
	
实现Worker端和controller的免密(root也要做)：
	vim /etc/ssh/sshd_config
	修改prohibit-password yes
	sudo systemctl restart sshd
	①先让两台服务器免密自己，上面的步骤
	②让服务器A免密登录服务器B(互相做一遍)
		scp id_rsa.pub 192.168.xxx.xxx:/tmp(在A中执行)
		cd ~/.ssh(在B中执行)
 		cat /tmp/id_rsa.pub >> authorized_keys(在B中执行)
 		ssh 192.168.xxx.xxx(在A中执行)
 		
 		scp id_rsa.pub 192.168.xxx.xxx:/tmp(在B中执行)
 		cd ~/.ssh(在A中执行)
 		cat /tmp/id_rsa.pub >> authorized_keys(在A中执行)
 		ssh 192.168.xxx.xxx(在B中执行)


使得当前用户sudo无密码
	修改 visudo 配置文件。
		①sudo visudo
		②在visudo界面中，使用快捷键 Ctrl + End 滚动至文件底部。在文件的末尾添加以下行，xxx 为您的实际用户名。
			yixin ALL=(ALL) NOPASSWD: ALL
		③验证：sudo apt update
```

## 安装

### 第一步

```
步骤：
sudo apt-get install aptitude
sudo aptitude install libboost-all-dev(出现3次选择，n，y，y)
sudo aptitude install libglib2.0-dev
sudo aptitude install libgtk2.0-dev
sudo chmod 755 /var/log

# 查看munge服务 systemctl status munge
# 若没有，则启动 systemctl start munge
# 若报错，则查看 sudo systemctl status munge.service

#执行脚本
./1-prepare_linux.sh


报错1，关于一些依赖的报错
	情况一：不一致的版本正要被安装
		如： vim : 依赖: vim-common (= 2:7.4.826-1ubuntu1) 但是 2:7.4.1689-3ubuntu1 正要被安装
		解决：sudo apt-get remove vim-common
报错2，关于munge报错
	问题1：/var/log 目录的权限不够
		sudo chmod 755 /var/log
```

安装Slurm和模拟器所需的库和应用程序。重要提示:

- 安装MySQL需要为数据库root用户提供一个新密码。请记下此密码，因为在后续步骤中将需要它。

- 完成这一步后，编辑Mysql配置以启用外部连接是很重要的。通常在/etc/mysql/mysql.conf.d/mysqld.cnf中替换

```
sudo vim /etc/mysql/mysql.conf.d/mysqld.cnf

bind-address            = 127.0.0.1
```

为

```
bind-address            = 0.0.0.0
```

#重启服务
sudo systemctl restart mysql



### 第二步

创建数据库并配置所需的用户。它将三次询问数据库的根密码。

```
#执行脚本
./2-create_slurm_db.sh
```

### 第三步

```
# 该脚本：下载Slurm 14.3.8.1并使用模拟器代码对其进行修补。随后，编译和安装代码。

# 可选，修改dns，增加下载速度
sudo vim /etc/hosts
#添加20.205.243.166 github.com	


vim compile_and_install_slurm.sh
#修改munge指向位置为munge的根目录

# 安装依赖环境
sudo aptitude install libpam0g-dev
sudo aptitude install libhdf5-dev hdf5-tools
sudo aptitude install libssl-dev	
sudo apt-get -y install libcr0 freeipmi-tools libfreeipmi-dev rrdtool librrd-dev libncurses5-dev libncursesw5-dev


# 提前将环境变量设置好
sudo vim /etc/profile

# Set environment variables for simulation
export SIM_DAEMONS_PATH="/somefolder/slurmsimdeploy/slurm_install/slurm_programs/sbin"
export SIM_DIR="/somefolder/slurmsimdeploy/slurm_install"
export SIM_LIBRARY_PATH="/somefolder/slurmsimdeploy/slurm_install/slurm/src/simulation_lib/.libs"
export LD_LIBRARY_PATH="/somefolder/slurmsimdeploy/slurm_install/slurm_varios/lib"
 
# Add directories to PATH
export PATH=$PATH:/somefolder/slurmsimdeploy/slurm_install/slurm_programs/bin
export PATH=$PATH:/somefolder/slurmsimdeploy/slurm_install/slurm_programs/sbin
 
# Set additional variables
export LIBS=-lrt
export CFLAGS="-D SLURM_SIMULATOR -D WF_API"

#刷新
source /etc/profile

# signal变量总是为0
# 修改slurmsim.git.patch文件
vim +1723 slurmsim.git.patch
添加 signal = signal + 1;

#执行脚本
./3-install_slurm_sim.sh

# 在slurm/slurm_configure.log中观察错误信息
cat slurm/slurm_configure.log

# 查看error信息
grep -i "error" config.log > error_config.log
```

### 第四步

该步骤为创建一个从/scsf/到slurm模拟器位置的软连接。/scsf/slurm_programs/sbin是命令的二进制文件。

```
./4-create_sim_links.sh
```

### 第五步

使用名为perfdevel的集群所需的模式填充slurm数据库。

```
sudo su root

#添加环境变量
vim /root/.bashrc
# Set environment variables for simulation
export SIM_DAEMONS_PATH="/somefolder/slurmsimdeploy/slurm_install/slurm_programs/sbin"
export SIM_DIR="/somefolder/slurmsimdeploy/slurm_install"
export SIM_LIBRARY_PATH="/somefolder/slurmsimdeploy/slurm_install/slurm/src/simulation_lib/.libs"
export LD_LIBRARY_PATH="/somefolder/slurmsimdeploy/slurm_install/slurm_varios/lib"
 
# Add directories to PATH
export PATH=$PATH:/somefolder/slurmsimdeploy/slurm_install/slurm_programs/bin
export PATH=$PATH:/somefolder/slurmsimdeploy/slurm_install/slurm_programs/sbin
 
# Set additional variables
export LIBS=-lrt
export CFLAGS="-D SLURM_SIMULATOR -D WF_API"

#刷新
source /root/.bashrc


./5-fill_slurm_db.sh

#查看进程
ps -ef|grep slurm

#查看已经拥有的集群(需要启动相关进程，root权限)
sacctmgr list clusters


```





# 第二步 Controller的安装

安装好Worker后，可以直接克隆worker的虚拟机，然后修改为Controller。

## 前置

python2.7

​	MySQL-python(也就是MySQLdb，但是MySQL-python只支持python2.x)

①修改主机名：

```
①控制端虚拟机修改主机名为controllervm
sudo vim /etc/hostname
sudo vim /etc/hosts
	127.0.1.1   controllervm
sudo reboot
②虚拟机root用户的互相免密配置
```

②配置MySQL以接受大型SQL查询。在/etc/mysql/my.cnf:

```
sudo apt install mysql-server(若没有mysql)
#密码随便设置

sudo vim /etc/mysql/my.cnf
#添加
[mysqld]
max_allowed_packet=128M
sql_mode=NO_ENGINE_SUBSTITUTION

#重启mysql
sudo systemctl restart mysql

#添加Worker的映射(可选)
sudo vim /etc/hosts
#添加192.168.217.91 simulatorvm
```

③在虚拟环境中安装。

```
sudo apt -y install python-pip python-virtualenv
	pip install pip==9.0.1(如果需要更换版本，在这里不需要)

# 如果不需要虚拟环境，则省略创建虚拟环境的代码块(建议创建虚拟环境)。
cd ScSFController-0.1
virtualenv env(创建名为env的虚拟环境，在当前目录下创建一个名为env的文件夹，其中包含虚拟环境的所有文件)
source env/bin/activate(激活虚拟环境，而deactivate是退出命令)

#环境安装
sudo apt-get -y install aptitude graphviz libgraphviz-dev pkg-config python-tk
sudo aptitude install libmysqlclient-dev
sudo aptitude install  python2.7-dev
pip install "setuptools<45" -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install MySQL-python numpy scipy matplotlib pygraphviz -i https://pypi.tuna.tsinghua.edu.cn/simple

#构建库，并保存到虚拟环境的site-packages中，这样能访问自定义的第三方库
python setup.py install(从源代码安装 Python 包)
```

注意：在OS X中安装pygraphviz可能会产生文件。在这种情况下，安装pygraphviz运行 ([More info](http://www.alexandrejoseph.com/blog/2016-02-10-install-pygraphviz-mac-osx.html):

```
pip install pygraphzviz \
--install-option="--include-path=/usr/local/include/graphviz/" \
--install-option="--library-path=/usr/local/lib/graphviz"
```

④创建数据库和默认用户：

```
bin/sql_createdb.sh

# 创建了scsf和scsftest用户和对应的数据库，密码分别为scsf-pass和testscsf-pass
# 该脚本使用root用户访问数据库，并请求其密码两次。
```

⑤创建数据库模式（假设默认数据库用户）

```
cd bin
vim sql_conf_env.sh(这里要修改Worker对应的ip地址)

# 配置ScSF读取的env var来配置其数据库访问。
source sql_conf_env.sh

# 在scsf数据库中创建对应的5个表，为experiment、histograms、numericStats、traces、usage_values
python sql_populate_db.py
```

⑥编译list_trace命令

```
# 这一步将list_trace 编译为独立脚本的脚本，需要Slurm Worker包及其Slurm代码的下载和补丁。
# 假设Slurm Worker包的根目录为“/somefolder/slurmsimdeploy”，执行命令：
./compile_list_trace.sh /somefolder/slurmsimdeploy

所执行的工作
# 复制Worker包中的list_trace.c" "sim_trace.c" "sim_trace.h文件到当前目录
# 将 sim_trace.patch 应用于当前目录下的 sim_trace.c 文件。
# 编译
```

## 安装后的基本测试

```
#注意在test_ManifestMaker.py中第442行，有一个双引号可以去除，因为预期结果没有这个双引号(猜测：作者的失误)
vim test_ManifestMaker.py

cd 控制器文件的根目录
source bin/sql_conf_env.sh
cd test
#这些测试使用testscsf数据库。确保存在本地Testscsf数据库并配置了正确的env变量（请阅读bin/sql_conf_env.sh）
./test_all.sh

#或者依次测试python -m unittest test_ManifestMaker
#测试完会在当前目录生成测试结果文件
    test_jobAnalysis.test_result
    test_Machine.test_result
    test_ProbabilityMap.test_result
    test_TimeController.test_result
    test_WorkloadGenerator.test_result
    test_trace_gen.test_result
    test_PatternGenerator.test_result
    test_RandomSelector.test_result
    test_Result.test_result
    test_ResultTrace.test_result
    test_WorkflowTracker.test_result
    test_WorkflowDeltas.test_result
    test_definition.test_result
    test_ManifestMaker.test_result: FAILED. Re-run: 
    test_SpecialGenerator.test_result
```



## #ScSF调用worker的测试

```
#进到控制器根目录
sudo su root
source env/bin/activate(使用虚拟环境)
source bin/sql_conf_env.sh(若没有加入系统环境变量，则每次测试时，都需要先一次性添加一下)

cd ~
vim .bashrc

# Set environment variables for simulation
export SIM_DAEMONS_PATH="/somefolder/slurmsimdeploy/slurm_install/slurm_programs/sbin"
export SIM_DIR="/somefolder/slurmsimdeploy/slurm_install"
export SIM_LIBRARY_PATH="/somefolder/slurmsimdeploy/slurm_install/slurm/src/simulation_lib/.libs"
export LD_LIBRARY_PATH="/somefolder/slurmsimdeploy/slurm_install/slurm_varios/lib"

# Add directories to PATH
export PATH=$PATH:/somefolder/slurmsimdeploy/slurm_install/slurm_programs/bin
export PATH=$PATH:/somefolder/slurmsimdeploy/slurm_install/slurm_programs/sbin

# Set additional variables
export LIBS=-lrt
export CFLAGS="-D SLURM_SIMULATOR -D WF_API"

export ANALYSIS_DB_HOST="localhost"
export ANALYSIS_DB_NAME="scsf"
export ANALYSIS_DB_USER="scsf"
export ANALYSIS_DB_PASS="scsf-pass"
export SLURMDB_USER="scsf_remote"
export SLURMDB_PASS="scsf_remote-pass"
export NERSCDB_USER="nersc_example"
export NERSCDB_PASS="nersc_example-pass"
export TEST_DB_NAME="scsftest"
export TEST_DB_USER="testscsf"
export TEST_DB_PASS="testscsf-pass"
export TEST_VM_HOST="192.168.217.91"	#注意ip修改

source .bashrc

cd test
./test_all_vm.sh(执行的测试太多了，不建议，建议依次执行各个方法)

source env/bin/activate

#因为会重启Worker(可以去源代码中去掉)，手动启动Worker的munge服务
#在Worker端
sudo chmod 755 /var/log
systemctl start munge


#在workder中的/scsf/sim_mgr.log查看日志

作者的错误：
    #文件test_orchestration.py中的setUp函数中的us = NumericList，列表中少一个逗号
	#文件test_orchestration.py中的test_single_with_wf_create_sim_analysis函数中文件manifestsim.json写错了应该为manifestSim.json
	可以修改orchestration/running.py下的_refresh_machine方法，注释掉重启命令
```





## 使用

```
#在Worker端
sudo chmod 755 /var/log
systemctl status munge
systemctl start munge

#在控制器端的根目录下执行
sudo su root
source env/bin/activate

# 到bin目录下执行自己所编写的实验方法
cd bin
python -m unittest run_experiment.TestExperiment.test_single_with_wf_experiment

# 将工作流的xml文件转换为实验的json文件
python xml2json.py xml/Montage_100.xml xml/Montage_100.json

#在worker查看执行日志
cd /scsf
cat sim_mgr.log

# 生成图表
python ./plot_exp_profile.py id 工作流名称		#在“./out”文件夹中
python ./plot_exp_utilization.py out/ id		# out目录
python ./plot_exp_waittime_in_time.py id		#在waittime目录

```

