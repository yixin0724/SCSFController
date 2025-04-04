#/bin/bash
#
# 将 list_trace 编译为独立脚本的脚本。

# 检查命令行参数的数量是否为1
if [ "$#" -ne 1 ]; then
    echo "Missing argument: route to the Slurm Worker package code"
    exit -1
fi

# 定义补丁文件名和源目录路径
patch_file="sim_trace.patch"
source_dir="$1/slurm/contribs/simulator"

# 定义需要复制的文件列表
file_list=( "list_trace.c" "sim_trace.c" "sim_trace.h" )

# 输出提示信息，表示开始复制文件
echo "Copying files from ${source_dir}"

# 遍历文件列表，逐个复制文件到当前工作目录
for file_name in "${file_list[@]}"
do
	echo "${source_dir}/${file_name}"	# 输出正在复制的文件路径
	cp "${source_dir}/${file_name}" .		# 将文件复制到当前目录
done	

# 使用 patch 命令对 sim_trace.c 文件应用补
echo "Patching sim_trace.c"
patch sim_trace.c "$patch_file"


# 执行 make 命令进行编译
echo "Compiling"
make
