from workflows import convert_xml_wf_to_json_manifest
import sys

"""
examples:
python xml2json.py xml/mont-degree8.xml xml/mont-degree8.json 0 mBackground mProjectPP

python xml2json.py xml/mont-degree8.xml xml/mont-degree8.json 480 mBackground mProjectPP

该脚本将DAX工作流定义转换为JSON清单格式。
它可以对属于具有相同名称的同一阶段的任务进行分组。
可以设置每个阶段的总体或每个阶段的核心限制（适用于分组任务）。

"""
# 定义脚本使用说明，包括脚本名、输入输出文件路径及可选参数
usage = ("python xml2json input_xml_file output_json_file [max_cores_per_task]"
         " [task_name_0] [task_name_1:max_cores_for_this_task]... [task_name_n]")
# 检查命令行参数数量，确保至少提供了输入XML和输出JSON文件路径
if len(sys.argv) < 3:
    print
    "Usage: ", usage
    raise ValueError("Missing input xml and/our output json files");

# 从命令行参数中提取输入XML和输出JSON文件路径
xml_file = sys.argv[1]
json_file = sys.argv[2]
# 初始化任务组列表和最大核心数变量
grouped_jobs = []
max_cores = 0
# 检查命令行参数数量，确保max_cores参数使用正确
if (len(sys.argv) == 4):
    print
    "Usage:", usage
    raise ValueError("Cannot provide max_cores value wihtout a list of job"
                     " names to fuse.")
# 如果提供了额外参数，提取最大核心数和任务组列表
if len(sys.argv) > 3:
    max_cores = int(sys.argv[3])
    grouped_jobs = sys.argv[4:]

# 调用函数将XML工作流转换为JSON清单，传入文件路径、最大核心数和任务组列表
convert_xml_wf_to_json_manifest(xml_file, json_file,
                                max_cores=max_cores,
                                grouped_jobs=grouped_jobs)
