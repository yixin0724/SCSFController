#!/bin/bash
# 用于启动所有测试的脚本

echo "这些测试使用 test 数据库。确保本地 Test 数据库存在，并且配置了正确的环境变量（读取 bin/sql_conf_env.sh）"

# 创建一个符号链接 'data' 指向 '../bin/data' 目录，以便测试可以访问所需的数据文件
ln -s ../bin/data data

# 创建一个名为 'tmp' 的临时目录，用于存放临时文件或测试输出。
mkdir tmp

# 定义一个包含所有测试模块名称的数组。每个元素代表一个需要执行的 Python 测试脚本。
test_list=("test_jobAnalysis" "test_jobAnalysis" "test_Machine" "test_ProbabilityMap" \
		 "test_TimeController" "test_WorkloadGenerator" "test_trace_gen" \
		 "test_PatternGenerator" "test_RandomSelector" "test_Result" \
		 "test_ResultTrace" "test_WorkflowTracker" "test_WorkflowDeltas" \
		 "test_definition" "test_ManifestMaker" "test_SpecialGenerator")

# 初始化计数器
passed_count=0
failed_count=0




# 下面这行原本尝试重新定义 test_list 数组，但实际上没有必要，因此保持原样但加注释说明其意图。
eval "test_list=\${$test_list[@]}"
# # 打印正在测试的模块列表，使用大括号展开数组内容
echo ".......Testing ${test_list[@]}}"

# 遍历 test_list 数组中的每一个测试模块
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
		# 增加失败计数器。
		failed_count=$[ ${failed_count}+1 ]
	fi
	# 输出当前测试的结果（通过或失败）。
	echo "${result}"
done

# 汇总测试结果
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
