HOSTS_FILE="./hosts.list"

# 从指定的主机文件中读取每一行作为主机名，检查主机的连通性
while IFS='' read -r worker || [[ -n "$worker" ]]; do
	 # 尝试通过SSH连接到主机，重定向输出和错误到/dev/null，输入从/dev/null读取
	ssh -t $worker exit  &> /dev/null < /dev/null
	# 检查SSH命令的退出状态码
	if [ $? = 0 ]; then
	  # 如果状态码为0，表示主机可达
		echo "$worker Up"
	else
	  # 如果状态码非0，表示主机不可达
		echo "$worker Down"
	fi
done < "$HOSTS_FILE"
