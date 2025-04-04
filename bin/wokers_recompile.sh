#!/bin/bash

HOSTS_FILE="./hosts.list"
LOG_FOLDER="./log"
# 定义需要编译的分支名称
BRANCH_NAME="oss-clean"
# 定义临时文件路径，用于存储处理后的主机列表
TMP_FILE="/tmp/compile_temp.txt"
# 初始化数组，用于存储每个编译进程的PI
MYA=()
# 初始化关联数组，用于存储每个PID对应的工作者主机名
WORKERS_PID=()
# 从主机列表文件中读取主机名，忽略以"!"开头的行，并将结果存入临时文件
cat "$HOSTS_FILE" | grep -v "!" > "$TMP_FILE"
# 遍历临时文件中的每个工作者主机名
while IFS='' read -r worker || [[ -n "$worker" ]]; do
   # 输出编译开始的信息
	echo "Recompiling $worker, branch $BRANCH_NAME"
	# 调用编译脚本，重定向输出到日志文件，并将进程放入后台
	./do_recompile.sh $worker $BRANCH_NAME &> "${LOG_FOLDER}/compile.${worker}.log" &
	# 获取后台进程的PID
	pid=$!
	# 将PID添加到数组中
	MYA+=("$pid")
	# 在关联数组中存储PID对应的工作者主机名
	WORKERS_PID["$pid"]="$worker"
done < "$TMP_FILE"
# 初始化退出状态码为0
exit_status=0
# 遍历所有编译进程的PID，等待它们结束，并检查它们的退出状态
for pid in "${MYA[@]}"; do
	# 输出等待编译进程结束的信息
	echo "Waiting for compilation running on $pid to end..."
	wait $pid
 	status=$?
        # 如果编译进程的退出状态码不为0，则输出警告信息，并将退出状态码设置为-1
        if [ $status -ne 0 ]; then
		echo "Warning compilation on ${WORKERS_PID[$pid]} failed, check log" 
		exit_status=-1
        fi      
done
# 如果所有编译进程的退出状态码都为0，则输出所有工作者主机编译成功的消息
if [ $exit_status -eq 0 ]; then
	echo "All workers compiled correctly"
fi
# 退出脚本，返回所有编译进程中的最大退出状态码
exit $exit_status
