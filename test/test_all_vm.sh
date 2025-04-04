#!/bin/bash
# Script to launch all the tests

# 提示用户确保本地存在测试数据库，并且环境变量配置正确。
# 这些信息对于正确运行测试至关重要。
echo "These tests use a test database. Make sure that a local Test database"\
" exists and correct env vars are configured (read bin/sql_conf_env.sh)"

# 提醒用户测试需要一个特定 IP 地址（192.168.56.24）的 Slurm worker，
# 或者可以通过 TEST_VM_HOST 环境变量指定的 IP 地址。
echo "Tests require a Slurm worker with IP 192.168.56.24 or at the "\
"TEST_VM_HOST IP"


# 定义一个包含所有测试模块名称的数组。每个元素代表一个需要执行的 Python 测试脚本。
test_list=("test_running" "test_running" "test_orchestration")

# 初始化通过和失败的测试计数器为0。
passed_count=0
failed_count=0



eval "test_list=\${$test_list[@]}"

# 打印正在测试的模块列表，使用大括号展开数组内容。
echo ".......Testing ${test_list[@]}}"

# 遍历 test_list 数组中的每一个测试模块。
for test_name in ${test_list[@]}; do
	# 构建当前测试的输出文件名，格式为 <test_name>.test_result。
	test_output_file="${test_name}.test_result"
	# 构建运行测试的命令字符串，使用 unittest 模块来执行测试。
	command="python -m unittest ${test_name}"
	# 打印当前测试的名称，以跟踪进度。
	printf "Test ${test_output_file}: "
	# 默认设置结果为失败，并提供重新运行命令的信息。
	result="FAILED. Re-run: $command"

	# 执行测试并将标准输出和标准错误重定向到指定的输出文件。
	$command &> "${test_output_file}"

	# 检查上一条命令的退出状态码。如果为0，则表示测试通过；否则表示失败。
	if [ $? -eq 0 ]; then
		# 更新结果为通过，并增加通过计数器。
		result="PASSED"
		passed_count=$[ ${passed_count}+1 ]
	else
		failed_count=$[ ${failed_count}+1 ]
	fi
	echo "${result}"
done

# 计算总测试数量，即通过和失败的测试之和。
total_tests=$[ ${passed_count}+${failed_count} ]
echo ".......Summary......."
echo "Total tests: ${total_tests}."
echo "Tests PASSED: ${passed_count}"
echo "Tests FAILED: ${failed_count}"

# 根据是否有失败的测试决定最终退出状态。如果没有失败，则返回0表示成功；如果有失败，则返回1表示有错误。
if [ $failed_count -eq 0 ]; then
	echo "ALL TESTS PASSED!! OLÉ!!!"
	exit 0
else
	echo "SOME TESTS FAILED!! Keep trying..."
	exit 1
fi
