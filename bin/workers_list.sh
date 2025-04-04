TMP_FILE=/tmp/running.log
# 定义主机列表文件路径，包含所有需要检查的主机名
HOSTS_FILE="./hosts.list"
# 定义需要检查的命令，这里是运行模拟实验的Python脚本
RUN_CMD="python ./run_sim_exp.py"
# 定义日志文件夹路径，未在本段代码中使用，可能是其他部分的配置
LOG_FOLDER="./log"
# 通过ps和grep命令查找正在运行的目标命令，并将结果保存到临时文件中
# 这里使用tr和cut命令对输出进行格式化，以提取出需要的列
ps aux | grep "$RUN_CMD" | grep -v "grep" | tr -s " " | cut -d\  -f13 > $TMP_FILE

# 读取主机列表文件，逐行检查每个主机是否在运行目标命令
while IFS='' read -r worker || [[ -n "$worker" ]]; do
	# 初始化标志变量，用于指示是否找到匹配的进程
	found=0
	# 读取临时文件中的进程信息，检查当前主机是否在运行目标命令
	while IFS='' read -r client || [[ -n "$client" ]]; do
		 # 如果找到匹配的进程，设置标志变量并跳出循环
		if [ "$client" = "$worker" ]; then
			found=1
			break
		fi
	done < "$TMP_FILE"
	# 如果找到匹配的进程，输出主机名
	if [ $found = 1 ]; then
		echo "$worker"
	fi
done < "$HOSTS_FILE"
