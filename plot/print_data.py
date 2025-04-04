from plot import extract_usage, calculate_diffs


def get_diff_results(db_obj, trace_id_rows, mean=False,
                     field = "corrected_utilization"):
    """
    计算两组使用率数据的差异结果

    通过提取指定跟踪ID的使用数据，计算基准组与其他组的数值差异。适用于对比实验组与对照组场景。

    Args:
        db_obj: 数据库连接对象，用于数据提取
        trace_id_rows: 包含跟踪ID的行数据，用于标识需要处理的数据集
        mean: 是否返回均值结果，默认为False返回原始数据
        field: 要计算差异的字段名称，默认为"corrected_utilization"

    Returns:
        list: 包含差异计算结果的二维列表，结构为[group1_diffs, group2_diffs,...]
    """
    # 提取使用率数据，factor参数将数值转换为百分比格式
    usage_rows=extract_usage(db_obj, trace_id_rows, factor=100.0, mean=mean)
#     print [[dd._get("corrected_utilization") for dd in row] 
#            for row in usage_rows]
#     
#     print usage_rows

    # 基于第一组作为基准（base_index=1），计算两组间的差异
    # group_count=2表示每次对比两个数据组，percent=False返回绝对差值
    diffs_results = calculate_diffs(usage_rows, base_index=1, 
                                    group_count=2, percent=False,
                                    field=field)
    return diffs_results

def print_results(time_labels, manifest_label,diffs_results,
                  num_decimals=2):
    """
    生成并打印LaTeX格式的对比结果表格

    Args:
        time_labels: list[str], 表格列头的时间标签列表
        manifest_label: list[str], 每行对应的方法/配置标签列表
        diffs_results: list[list[float]], 二维数组存储的数值对比结果
        num_decimals: int, 可选，结果数值保留小数位数（默认2位）

    Returns:
        None: 直接打印结果，不返回数值
    """
    # 生成表格列头行
    print " & ".join(time_labels) + "\\\\"
    print "\\hline"
    # 遍历每个方法及其对应的结果行
    for (man,row) in zip(manifest_label,diffs_results):
        # 格式化数值为LaTeX数学模式字符串，保留指定小数位数
        print (" & ".join([man]+
                      [("${0:."+str(num_decimals)+"f}$").format(the_value) 
                        for the_value in row]) +
               "\\\\")
    print "\\hline"