#!/bin/bash
# 根据参数获取主进程ID
# $1: 参数用于标识特定的进程
getpid () {
	echo `ps aux | grep "python ./run_sim_exp.py $1" | grep -v "grep" | tr -s " " | cut -d\   -f2`
}
# 根据参数获取子进程ID
# $1: 参数用于标识特定的进程
getpid_subw() {
	echo `ps aux | grep "run_sim.sh" | grep "$1" | grep -v "grep" | tr -s " " | cut -d\   -f2`
}
# 定义临时文件路径，用于记录正在运行的进程信息
TMP_FILE=/tmp/running.log
HOSTS_FILE="./hosts.list"
RUN_CMD="python ./run_sim_exp.py"
LOG_FOLDER="./log"
# 定义停止模拟的脚本命令
STOP_COMMAND="/home/gonzalo/cscs14038bscVIII/stop_sim.sh"

# 获取所有运行中的模拟进程的用户ID，并将其写入临时文件
ps aux | grep "python ./run_sim_exp.py" | grep -v "grep" | tr -s " " | cut -d\  -f13  > $TMP_FILE

# 读取主机列表文件，检查每个工作节点的运行状态
cat "$HOSTS_FILE" | grep -v "!" | while IFS='' read -r worker || [[ -n "$worker" ]]; do
	echo "Checking... $worker"
	if [ "$worker" = "" ]; then
		continue
	fi
	found=0
	# 检查当前工作节点是否在运行
	while IFS='' read -r client || [[ -n "$client" ]]; do
		if [ "$client" = "$worker" ]; then
			echo "Worker $worker running."
			found=1
			break
		fi
	done < "$TMP_FILE"
	if [ $found = 1 ]; then
		worker_pid=`getpid $worker`
		# 获取当前工作节点的进程ID并终止该进程
		echo "Killing worker $worker process $worker_pid."
		kill -9 $worker_pid
		# 在工作节点上远程执行停止模拟的脚本
		ssh -A -t $worker $STOP_COMMAND &> /dev/null < /dev/null &
	fi
		# 获取并终止当前工作节点的子进程
	subw_pid=`getpid_subw $worker`
	if [ "$subw_pid" != "" ]; then
		echo "Subworker for $worker is still alive with pid(s) $subw_pid"
		echo "Killing $subw_pid"
		kill -9 $subw_pid
	fi
done
