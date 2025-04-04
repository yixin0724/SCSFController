# 定义临时文件路径，用于存储当前运行的进程信息
TMP_FILE=/tmp/running.log
# 定义主机列表文件路径，包含需要检查和启动进程的主机名
HOSTS_FILE="./hosts.list"
# 定义需要在远程主机上运行的命令
RUN_CMD="python ./run_sim_exp.py"
# 定义日志文件夹路径，用于存储远程主机的输出日志
LOG_FOLDER="./log"
# 设置环境变量以禁用Python的缓冲输出，确保日志实时更新
export PYTHONUNBUFFERED=1
# 获取当前运行的进程信息，并过滤出我们需要的进程，将其PID保存到临时文件中
ps aux | grep "$RUN_CMD" | grep -v "grep" | tr -s " " | cut -d\  -f13 > $TMP_FILE

# 读取主机列表文件，忽略以"!"开头的注释行，对每个主机执行检查和启动进程的操作
cat "$HOSTS_FILE" | grep -v "!" | while IFS='' read -r worker || [[ -n "$worker" ]]; do
	# 输出当前检查的主机名
	echo "Checking... $worker"
		# 初始化标志变量，用于指示是否在临时文件中找到了当前主机
	found=0
	# 读取临时文件中的每个进程信息，与当前主机名进行比较
	while IFS='' read -r client || [[ -n "$client" ]]; do
	  # 如果找到匹配的主机名，则输出提示信息并设置标志变量，然后跳出循环
		if [ "$client" = "$worker" ]; then
			echo "Worker $worker already running, skipping."
			found=1
			break
		fi
	done < "$TMP_FILE"
	# 如果没有找到匹配的主机名，则输出启动进程的信息，并在后台启动进程，将其输出重定向到日志文件
	if [ $found = 0 ]; then
		echo "Launching worker on $worker..."
		$RUN_CMD $worker &> "${LOG_FOLDER}/worker.log.$worker" &
	fi
done
