"""
使用nerscsplot库生成特定的绘图。
"""
from commonLib.nerscPlot import (paintHistogramMulti, paintBoxPlotGeneral,
                                 paintBarsHistogram)
import getopt
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import sys
from array import array
from numpy import ndarray, arange, asarray
from stats.trace import ResultTrace
from orchestration.definition import ExperimentDefinition
from stats import  NumericStats
from matplotlib.cbook import unique


def get_args(default_trace_id=1, lim=False):
    """
    解析命令行参数并返回处理结果
    参数:
        default_trace_id (int): 默认跟踪ID，当未提供 -i 参数时使用该值，默认为1
        lim (bool): 默认限制标志，当未提供参数时使用该值，默认为False
    返回:
        tuple: 包含两个元素的元组
            - int: 解析后的跟踪ID
            - bool: 最终限制标志状态
    异常:
        当参数解析失败时打印帮助信息并退出程序
    """
    try:
        # 尝试解析命令行参数
        opts, args = getopt.getopt(sys.argv[1:], "i:ln", ["id=", "lim", "nolim"])
    except getopt.GetoptError:
        # 参数解析错误处理
        print
        'test.py [-i <trace_id>] [-l]'
        sys.exit(2)

    # 处理解析到的所有选项参数
    for opt, arg in opts:
        if opt in ("-i", "--id"):
            # 更新跟踪ID参数
            default_trace_id = int(arg)
        elif opt in ("-l", "--lim"):
            # 启用限制标志
            lim = True
        elif opt in ("-n", "--nolim"):
            # 禁用限制标志
            lim = False

    return default_trace_id, lim


def profile(data, name, file_name, x_axis_label, x_log_scale=False):
    """
    生成包含直方图和累积分布图（CDF）的可视化图表并保存为png文件。

    Args:
        data (list): 待分析的数值数据集，以列表形式提供
        name (str): 图表的主标题文本，用于说明图表内容
        file_name (str): 输出文件的保存路径（无需包含扩展名，自动添加.png后缀）
        x_axis_label (str): 直方图x轴的标签说明文本
        x_log_scale (bool, optional): 是否对x轴使用对数刻度，默认为线性刻度

    Returns:
        None: 该函数没有返回值，直接生成图像文件到指定路径

    实现说明：
        1. 将数据封装为字典结构以适应绘图接口要求
        2. 调用底层绘图函数生成双y轴图表（左侧直方图计数，右侧CDF百分比）
        3. 自动固定分箱数为100个以保持一致性
    """
    # 将数据封装为字典结构，键名为图表标题，用于适配多数据集绘图接口
    data_dic = {name: data}

    # 调用多数据直方图绘制函数，固定右侧y轴为CDF显示，设置坐标轴标签等参数
    paintHistogramMulti(name, data_dic, bins=100,
                        graphFileName=file_name,
                        labelX=x_axis_label,
                        xLogScale=x_log_scale,
                        labelY="Number Of Jobs")


def profile_compare(log_data, trace_data, name, file_name, x_axis_label,
                    x_log_scale=False, filterCut=0):
    """
    生成直方图和箱线图，对比真实工作负载与合成工作负载的随机变量分布
    Args:
        log_data (list): 真实工作负载的随机变量数值列表
        trace_data (list): 合成工作负载的随机变量数值列表
        name (str): 图表标题文本
        file_name (str): 输出文件路径（不含扩展名，自动追加.png），直方图和箱线图分别用
                         file_name.png 和 file_name-boxplot.png 保存
        x_axis_label (str): 直方图x轴标签文本
        x_log_scale (bool): 是否对直方图x轴使用对数刻度，默认为False
        filterCut (int): 直方图x轴显示范围的上限阈值，设为0时表示不限制
    Returns:
        None: 无返回值，直接生成并保存图表文件
    """

    # 合并原始数据与合成数据，构建绘图数据结构
    data_dic = {"original jobs": log_data,
                "synthetic jobs": trace_data}

    # 根据对数刻度参数选择不同的直方图绘制方式
    if x_log_scale:
        paintHistogramMulti(name, data_dic, bins=100,
                            graphFileName=file_name,
                            labelX=x_axis_label,
                            xLogScale=x_log_scale,
                            labelY="Number Of Jobs",
                            xLim=filterCut)
    else:
        # 当不使用对数刻度时，使用filterCut参数控制显示范围
        paintHistogramMulti(name, data_dic, bins=100,
                            graphFileName=file_name,
                            labelX=x_axis_label,
                            xLogScale=x_log_scale,
                            labelY="Number Of Jobs",
                            filterCut=filterCut)

    # 生成带对数刻度的箱线图，文件名追加"-boxplot"后缀
    paintBoxPlotGeneral(name, data_dic, labelY=x_axis_label,
                        yLogScale=True,
                        graphFileName=file_name + "-boxplot")


def histogram_cdf(edges, hist, name, file_name, x_axis_label,
                  y_axis_label, target_folder="",
                  hists_order=None,
                  do_cdf=False,
                  x_log_scale=False,
                  y_log_scale=False,
                  cdf_y_log_scale=False,
                  min_max=None,
                  cdf_min_max=None):
    """绘制直方图及其累积分布函数（CDF），并将图表保存为PNG文件

    所有输入的直方图必须具有相同的边界划分。支持对数坐标轴和自定义数值范围设置。

    Args:
        edges (list[float]): 所有直方图的边界值列表
        hist (dict|list): 直方图数据。字典形式时，键为直方图名称，值为各区间计数值列表
        name (str): 图表标题名称
        file_name (str): 输出文件名（自动添加.png后缀）
        x_axis_label (str): X轴标签文本
        y_axis_label (str): Y轴标签文本
        target_folder (str, optional): 文件保存路径，默认为当前目录
        hists_order (list[str], optional): 直方图绘制顺序，需与hist字典键匹配
        do_cdf (bool, optional): 是否绘制CDF曲线，默认False
        x_log_scale (bool, optional): X轴是否使用对数刻度，默认False
        y_log_scale (bool, optional): 直方图Y轴是否使用对数刻度，默认False
        cdf_y_log_scale (bool, optional): CDF的Y轴是否使用对数刻度，默认False
        min_max (tuple, optional): 直方图Y轴范围控制，格式为(min, max)，None表示自动
        cdf_min_max (tuple, optional): CDF的Y轴范围控制，格式同上

    Raises:
        ValueError: 当hists_order与hist字典的键不匹配时抛出

    Notes:
        - 当hist参数为列表时，会自动转换为匿名直方图（键名为空字符串）
        - 未指定hists_order时，默认按字典键的字母顺序排列
        - 对数刻度参数可独立控制直方图和CDF的坐标轴显示方式
    """

    # 统一输入格式：将单一直方图转换为字典形式
    if type(hist) is not dict:
        hist = {"": hist}

    # 验证并设置直方图绘制顺序
    if hists_order is not None:
        if set(hists_order) != set(hist.keys()):
            raise ValueError("hists_order必须与hist字典的键完全匹配")
    else:
        hists_order = sorted(hist.keys())

    # 调用底层绘图函数生成图表
    paintBarsHistogram(
        name, hists_order, edges, hist,
        target_folder=target_folder,
        file_name=file_name,
        labelX=x_axis_label, labelY=y_axis_label,
        cdf=do_cdf,
        x_log_scale=x_log_scale,
        y_log_scale=y_log_scale,
        cdf_y_log_scale=cdf_y_log_scale,
        min_max=min_max,
        cdf_min_max=cdf_min_max
    )


def create_legend(ax, legend):
    """在一幅图的指定坐标轴上创建横向分布的图例
    图例默认放置在坐标轴顶端，当坐标轴高度较小时会自动微调垂直位置。
    支持通过hatch参数添加填充图案，允许自定义图例项颜色、边框和图案样式。

    Args:
        ax (matplotlib.axes.Axes): 要添加图例的matplotlib坐标轴对象
        legend (list[tuple]): 图例项定义列表，每个元素为包含以下内容的元组：
            - 第0位 str: 图例文本标签
            - 第1位 str: 颜色名称或HEX值
            - [可选]第3位 str: hatch图案标识符（如'/'，'\\'等）
    Returns:
        None: 直接修改传入的ax对象，无返回值
    """
    handles = []
    # 遍历图例定义生成图形元素
    # 每个元素处理顺序：提取颜色、可选hatch参数，创建矩形色块
    for key in legend:
        hatch = None
        if len(key) > 3:  # 当元素包含hatch参数时
            hatch = key[3]
        handles.append(mpatches.Patch(facecolor=key[1], label=key[0],
                                      edgecolor="black",
                                      hatch=hatch))

    # 根据坐标轴高度调整图例垂直位置
    bbox = ax.get_window_extent()
    correct = 0
    if bbox.height < 100:  # 小尺寸坐标轴需要微调
        correct = 0.1

        # 创建横向分布的无边框图例
    ax.legend(handles=handles, fontsize=10,
              bbox_to_anchor=(0.0, 1.00 - correct, 1., 0.00), loc=3,
              ncol=len(legend), mode="expand", borderaxespad=0.0,
              frameon=False)


def do_list_like(the_list, ref_list, force=False):
    """根据输入列表结构返回标准化处理的嵌套列表
    当检测到输入不是嵌套列表时（或强制模式），返回与参照列表等长的重复元素列表。
    主要用于将单列表参数统一为与参照列表结构匹配的多列表格式。
    Args:
        the_list (list): 待处理的输入列表，可以是普通列表或嵌套列表
        ref_list (list): 参照列表，用于确定最终输出的列表长度
        force (bool, optional): 是否强制转换为嵌套列表格式，默认False
    Returns:
        list: 处理后的嵌套列表。当the_list不是嵌套列表或force=True时，
              返回包含len(ref_list)个the_list副本的新列表；否则返回原列表
    """
    # 判断需要展开为嵌套列表的条件：
    # 1. 强制模式激活 或
    # 2. 输入列表非空且首元素不是列表类型
    if force or (the_list is not None and not type(the_list[0]) is list):
        # 生成与参照列表等长的重复元素嵌套列表
        return [the_list for x in range(len(ref_list))]
    else:
        # 直接返回符合要求的原嵌套列表
        return the_list


def join_rows(row_list_1, row_list_2):
    """
    将两个二维列表按行连接，返回新的二维列表
    对两个输入列表中对应位置的子列表进行连接操作，当输入列表长度不一致时，
    仅处理到较短列表的长度（与zip函数特性一致）。若任一输入列表为空，则直接返回非空列表。
    Args:
        row_list_1 (list of list): 第一个二维列表，每个元素为一行数据
        row_list_2 (list of list): 第二个二维列表，每个元素为一行数据
    Returns:
        list of list: 新的二维列表，每个元素是输入列表对应行连接后的结果。
            特殊情况处理：
            - 当row_list_1为空时，直接返回row_list_2
            - 当row_list_2为空时，直接返回row_list_1
    """
    # 处理空输入的特殊情况
    if not row_list_1:
        return row_list_2
    if not row_list_2:
        return row_list_1

    # 使用zip配对两个列表，逐行连接对应行内容
    row_list = []
    for (row1, row2) in zip(row_list_1, row_list_2):
        row_list.append(row1 + row2)
    return row_list


def calculate_diffs(result_list, base_index=0, group_count=3, percent=True,
                    groups=None, speedup=False, field="median"):
    """计算各组数据与基准值的绝对/相对差异
    将result_list中的每组结果按指定分组策略（固定分组或自定义分组）进行划分，
    计算每组内各元素与基准元素的差异值。支持绝对差值或相对百分比差值计算，
    支持加速比计算模式。
    Args:
        result_list: 二维列表，包含多个NumericStats对象的结果行
        base_index: 每组中作为基准值的元素索引，默认为0
        group_count: 固定分组模式下的每组元素数量，默认为3
        percent: 是否计算百分比差值，True返回相对值，False返回绝对值
        groups: 自定义分组模式下的分组配置列表，如[2,3]表示第一组2元素、第二组3元素
        speedup: 加速比模式开关，True时以非基准元素为分母计算相对值
        field: 用于计算的统计量字段名，默认为"median"

    Returns:
        list: 二维差异值列表，每行对应输入的一行结果，元素为计算后的差异值

    """
    diffs = []
    # 处理自定义分组模式
    if groups:
        for row in result_list:
            index = 0
            diffs_row = []
            diffs.append(diffs_row)
            # 遍历每个自定义分组
            for group in groups:
                # 获取基准元素的统计值
                base_res = row[index + base_index]
                base_median = base_res._get(field)
                # 遍历组内所有元素
                for j in range(group):
                    res_median = row[index + j]._get(field)
                    # 加速比模式计算逻辑
                    if speedup:
                        if base_median == 0:
                            diff_value = 0
                        else:
                            diff_value = res_median / base_median
                    else:
                        # 常规差值计算逻辑
                        diff_value = res_median - base_median
                        if percent and base_res != 0:
                            if base_median == 0:
                                base_median = 1
                            diff_value = float(diff_value) / float(base_median)
                    # 跳过基准元素自身比较
                    if j != base_index:
                        diffs_row.append(diff_value)
                index += group
                # 处理固定分组模式
    else:
        for row in result_list:
            diffs_row = []
            diffs.append(diffs_row)
            # 按固定分组步长遍历
            for i in range(0, len(row), group_count):
                # 获取当前分组的基准值
                base_res = row[i + base_index]
                base_median = base_res._get(field)
                # 遍历分组内元素
                for j in range(group_count):
                    res_median = row[i + j]._get(field)
                    diff_value = res_median - base_median
                    # 加速比模式特殊处理
                    if speedup:
                        if base_median == 0:
                            diff_value = 0
                        else:
                            diff_value = res_median / base_median
                    else:
                        # 百分比模式转换
                        if percent and base_res != 0:
                            if diff_value != 0:
                                if base_median == 0:
                                    base_median = 1
                                diff_value = float(diff_value) / float(base_median)
                    # 排除基准元素自身
                    if j != base_index:
                        diffs_row.append(diff_value)

    return diffs


def adjust_number_ticks(ax, tick_count, log_scale=False, extra=None):
    """
    调整坐标轴y轴的刻度标签数量
    参数:
        ax (matplotlib.axes.Axes): 需要调整刻度的matplotlib坐标轴对象
        tick_count (int): 期望显示的刻度标签总数
        log_scale (bool, optional): 是否使用对数刻度模式，默认线性刻度
        extra (float, optional): 需要强制添加的额外刻度值，默认不添加
    返回值:
        None: 直接修改传入的ax对象，不返回新值
    """
    # 获取当前坐标轴刻度并确定范围
    my_t = ax.get_yticks()
    y_lim = (float(str(my_t[0])), float(str(my_t[-1])))
    print "INTERNAL", y_lim

    # 计算等间距刻度步长并生成刻度数组
    step = float(max(y_lim) - min(y_lim)) / (tick_count - 1)
    step = float(str(step))
    upper_limit = float(str(max(y_lim) + step))
    lower_limit = float(str(min(y_lim)))

    ticks = arange(lower_limit, upper_limit, step)

    # 合并额外刻度并保持排序
    if extra is not None:
        ticks = sorted(list(ticks) + [float(extra)])

    # 处理对数刻度转换
    if log_scale:
        ticks = _log_down(ticks)

    # 应用新刻度到坐标轴
    ax.set_yticks(ticks)


def _log_down(num):
    """
    计算输入数值或数组中各元素的10的幂次向下取整结果，返回排序后的唯一幂次列表。
    参数:
        num (int/float或array-like): 输入数值/数组，支持标量或numpy数组结构。函数将对每个元素计算log10后处理。
    返回值:
        ndarray: 由10的整数幂次组成的数组，其中每个整数是对应元素log10值的floor结果，
                 最终结果经过唯一化处理和升序排列。
    实现说明:
        1. 计算输入元素的log10值
        2. 对log10结果向下取整得到数量级整数
        3. 通过集合操作去重后生成有序整数列表
        4. 将整数列表转换为10的对应幂次数值
    """
    from numpy import log10, power, floor
    # 计算输入元素的对数值
    power_l = log10(num)
    # 对数量级取整、去重、排序后生成最终幂次结果
    return power(10, sorted(list(set(floor(power_l)))))


def remove_ids(list_of_rows, group_size=3, list_of_pos_to_remove=[0]):
    """
    根据分组规则过滤二维列表中的元素
    遍历每个子列表，将元素按指定分组大小划分，过滤掉每个分组中指定位置的元素。
    例如group_size=3时，每3个元素为一组，移除组内索引符合list_of_pos_to_remove的元素。
    参数：
        list_of_rows (list): 二维输入列表，每个元素为一个可迭代对象
        group_size (int, 可选): 元素分组的大小，默认3个元素为一组
        list_of_pos_to_remove (list, 可选): 每组中要排除的元素位置索引，默认为[0]
    返回值：
        list: 新二维列表，包含过滤后的元素集合，保持原有子列表结构
    """
    new_list_of_rows = []
    # 处理每个原始子列表
    for row in list_of_rows:
        new_row = []
        new_list_of_rows.append(new_row)
        index = 0  # 初始化索引计数器（实际在循环中被覆盖）

        # 遍历元素及其绝对索引
        for (index, elem) in zip(range(len(row)), row):
            # 判断当前元素是否在需要保留的位置：根据分组内相对位置判断
            if not index % group_size in list_of_pos_to_remove:
                new_row.append(elem)
    return new_list_of_rows


def replace(trace_id_rows, original, replacement):
    """
    替换二维列表中指定的原始元素为对应的新元素
    参数:
        trace_id_rows (list[list]): 二维列表，包含需要处理的原始数据行
        original (list): 需要被替换的原始元素列表
        replacement (list): 替换后的新元素列表，与original列表一一对应
    返回值:
        list[list]: 处理后的新二维列表，所有原始元素已被替换为对应的新元素
    """
    new_trace_id_rows = []
    # 遍历处理每一行数据
    for row in trace_id_rows:
        new_row = []
        new_trace_id_rows.append(new_row)
        # 替换当前行中的每个元素
        for item in row:
            # 当元素存在于原始替换列表时，执行索引查找和替换
            if item in original:
                index = original.index(item)
                new_row.append(replacement[index])
            else:
                new_row.append(item)

    return new_trace_id_rows


def gen_trace_ids_exps(base_id, base_exp=None, group_size=3, group_count=5,
                       block_count=6, group_jump=18, inverse=False,
                       base_exp_group=None, skip=0):
    """
    生成分层结构的trace_id列表集合，用于加载和绘制实验数据
    参数结构按block-group-experiment三级组织，每个block包含多个group，
    每个group包含多个实验trace_id。支持在多个层级添加基准实验。

    Args:
        base_id (int): 基础实验ID，第一个block的第一个group的第一个trace_id
        base_exp (int, optional): 基准实验ID，会添加到每个block列表的开头
        group_size (int): 每个group包含的实验数量，默认3
        group_count (int): 每个block包含的group数量，默认5
        block_count (int): 生成的block总数，默认6
        group_jump (int): 同block内不同group之间的ID间隔，默认18
        inverse (bool): 是否反转group顺序，True时第一个group在末尾，默认False
        base_exp_group (int, optional): 组基准实验ID，会添加到每个group的开头
        skip (int): 不同block之间的ID间隔，默认0

    Returns:
        list: 二维列表结构，外层列表包含block_count个block，每个block列表结构：
            - 开头可能包含base_exp（如果设置）
            - 包含group_count个group，每个group结构：
                * 可能包含base_exp_group（如果设置）
                * 包含group_size个连续生成的trace_id

    生成逻辑：
    - trace_id = base_id + (group_size+skip)*block_i + group_jump*group_i + exp_i
    - block间增量：(group_size+skip) * block索引
    - group间增量：group_jump * group索引
    - 实验间增量：exp索引(0到group_size-1)
    """
    trace_id_rows_colors = []
    # 生成每个block的实验ID集合
    for block_i in range(block_count):
        trace_id_row = []
        trace_id_rows_colors.append(trace_id_row)

        # 添加block级别的基准实验
        if base_exp is not None:
            trace_id_row.append(base_exp)

        # 生成group索引列表（考虑正序/逆序）
        group_index_list = range(group_count)
        if inverse:
            group_index_list = reversed(group_index_list)

        # 处理每个group的实验ID生成
        for group_i in group_index_list:
            # 添加group级别的基准实验
            if base_exp_group is not None:
                trace_id_row.append(base_exp_group)

            # 生成当前group内的所有实验ID
            for exp_i in range(group_size):
                trace_id = (
                        base_id
                        + (group_size + skip) * block_i  # block间的偏移量
                        + group_jump * group_i  # group间的偏移量
                        + exp_i  # 实验间的偏移量
                )
                trace_id_row.append(trace_id)

    return trace_id_rows_colors


def plot_multi_exp_boxplot(name, file_name, title,
                           exp_rows,
                           y_axis_labels,
                           x_axis_labels,
                           y_axis_general_label=None,
                           grouping=None,
                           colors=None,
                           hatches=None,
                           aspect_ratio=None,
                           y_limits=None,
                           y_log_scale=False,
                           legend=None,
                           percent_diff=False,
                           base_diff=0,
                           group_count=3,
                           grouping_alt=None,
                           precalc_diffs=None,
                           y_tick_count=None,
                           y_tick_count_alt=None,
                           y_axis_label_alt=None):
    """
    绘制多实验组的矩阵组合箱线图，支持差异百分比柱状图叠加

    Args:
        name (str): matplotlib图形对象的名称标识
        file_name (str): 输出图像文件的存储路径
        title (str): 图表主标题
        exp_rows (list[list[NumericStats]]): 二维实验数据矩阵，每行表示一个实验组，每个元素为数值统计对象
        y_axis_labels (list[str]): 左侧y轴标签列表，每个标签对应一行的组合箱线图
        x_axis_labels (list[str]): 底部x轴标签列表，每个标签对应一列的箱线图组
        y_axis_general_label (str, optional): 全局y轴标签（左侧主标签）
        grouping (list[int], optional): 箱线图分组布局配置，0表示插入间隔，非零值表示连续箱线图数量
        colors (list, optional): 箱线图颜色配置，支持单层列表或嵌套列表结构
        hatches (list, optional): 箱线图填充图案配置，支持单层列表或嵌套列表结构
        aspect_ratio (float, optional): 图表宽高比（width/height）
        y_limits (list[tuple], optional): y轴范围限制列表，每个元组对应一行的(min,max)
        y_log_scale (bool, optional): 是否使用对数y轴
        legend (list[tuple], optional): 图例配置列表，格式为[("系列名", "颜色名"), ...]
        percent_diff (bool, optional): 是否显示百分比差异柱状图
        base_diff (int, optional): 差异计算的基准组索引
        group_count (int, optional): 差异比较组的大小
        grouping_alt (list, optional): 替代分组配置，用于差异计算
        precalc_diffs (list[list[float]], optional): 预计算的差异值矩阵
        y_tick_count (int, optional): 左侧y轴刻度数量
        y_tick_count_alt (int, optional): 右侧y轴刻度数量（用于差异百分比轴）
        y_axis_label_alt (str, optional): 右侧y轴标签（差异百分比轴）

    Returns:
        None: 直接保存图像文件，无返回值
    """
    num_rows = len(exp_rows)
    # 初始化子图矩阵，每行数据对应一个子图
    fig, axes = plt.subplots(nrows=num_rows, ncols=1)
    # 统一处理单行子图的情况
    if not (type(axes) is ndarray):
        axes = [axes]

        # 预处理颜色和图案参数为嵌套结构
    colors = do_list_like(colors, exp_rows)
    hatches = do_list_like(hatches, exp_rows)

    # 差异百分比计算分支
    if percent_diff:
        diffs_results = precalc_diffs if precalc_diffs else calculate_diffs(
            exp_rows, base_index=base_diff,
            group_count=group_count,
            groups=grouping_alt
        )
    else:
        diffs_results = do_list_like([0], exp_rows)

    # 配置差异图的额外间隔
    extra_spacing = 0
    if percent_diff:
        extra_spacing = group_count - 1

    # 主绘图循环：逐行处理实验数据
    label_ax = axes[len(axes) / 2]
    for (ax, results_row, y_axis_label, color_item, hatches_item, diffs) in zip(
            axes,
            exp_rows,
            y_axis_labels,
            colors,
            hatches,
            diffs_results):
        # 提取箱线图统计数据
        median, p25, p75, min_val, max_val = _get_boxplot_data(results_row)

        # 首行设置标题，末行设置x轴标签和图例
        if ax == axes[0]:
            ax.set_title(title)
        the_labels = None
        if ax == axes[-1]:
            the_labels = x_axis_labels
            if legend:
                create_legend(ax, legend)
        else:
            the_labels = ["" for x in results_row]

        # 配置复合y轴标签
        if y_axis_general_label:
            if ax == label_ax:
                y_axis_label = "{0}\n{1}".format(y_axis_general_label,
                                                 y_axis_label)
            else:
                y_axis_label = "{0}".format(y_axis_label)
            ax.get_yaxis().set_label_coords(-0.06, 0.5)

        # 绘制主箱线图并获取布局信息
        positions, widths, alt_positions, alt_width = (
            _add_precalc_boxplot(ax, median, p25, p75, min_val, max_val,
                                 grouping=grouping,
                                 colors=color_item,
                                 hatches=hatches_item,
                                 labels=the_labels,
                                 y_axis_label=y_axis_label,
                                 y_limits=y_limits,
                                 y_log_scale=y_log_scale,
                                 extra_spacing=extra_spacing))

        # 差异百分比柱状图叠加逻辑
        if grouping and percent_diff:
            the_y_label_alt = None
            if ax == label_ax:
                the_y_label_alt = y_axis_label_alt

            if grouping[0] != group_count:
                color_item = color_item[1:]
                hatches_item = hatches_item[1:]
            _add_diffs(ax, diffs, alt_positions, alt_width,
                       colors=_extract_pos(color_item, base_diff,
                                           extra_spacing + 1),
                       hatches=_extract_pos(hatches_item, base_diff,
                                            extra_spacing + 1),
                       y_tick_count=y_tick_count_alt,
                       y_label=the_y_label_alt)

        # 调整y轴刻度密度
        if y_tick_count:
            adjust_number_ticks(ax, y_tick_count, y_log_scale)

    # 后处理：设置宽高比和保存图像
    if aspect_ratio:
        plt.axes().set_aspect(aspect_ratio)
    if title:
        axes[0].set_title(title)

    fig.savefig(file_name, bbox_inches='tight')


def flatten_list(the_list):
    """ Takes a lists of lists and puts all their elements in a list."""
    return [item for sublist in the_list for item in sublist]


def extract_type(data_list, type_list, select_type):
    """根据类型列表筛选数据列表中对应位置的元素，返回符合指定类型的子列表

    遍历类型列表，当元素类型与指定类型匹配时，将数据列表对应位置的元素加入返回列表。
    类型列表会先进行扁平化处理以支持嵌套结构。

    Args:
        data_list (list): 需要被筛选的原始数据列表，包含任意类型的元素
        type_list (list): 类型标签列表，必须与data_list长度相同，元素为字符串类型
        select_type (str): 要筛选的目标类型标识符

    Returns:
        list: 包含所有符合select_type条件的data_list元素的新列表，保持原始顺序

    Important Notes:
        - 该函数会先对type_list执行flatten_list操作以处理嵌套结构
        - 要求data_list和type_list必须长度一致，否则可能丢失数据
    """
    new_data = []

    # 扁平化类型列表以处理可能的嵌套结构
    # 例如将[[A,B],C]转换为[A,B,C]，确保与数据列表位置对齐
    type_list = flatten_list(type_list)

    # 并行遍历数据和类型列表
    # 当类型匹配时，将对应数据加入返回列表
    for (the_data, the_type) in zip(data_list, type_list):
        if the_type == select_type:
            new_data.append(the_data)

    return new_data


def plot_multi_boxplot_bars(name, file_name, title, 
                   exp_rows,
                   type_rows,
                   y_axis_labels,
                   x_axis_labels,
                   y_axis_general_label=None,
                   colors=None,
                   hatches=None,
                   aspect_ratio=None,
                   y_limits=None,
                   y_log_scale=False,
                   legend=None,
                   y_tick_count=None,
                   y_tick_count_alt=None,
                   y_axis_label_alt=None):
    """
    绘制多行组合图表（箱线图+柱状图），支持不同可视化类型的混合展示

    参数：
    name: (str) 图表名称（未使用，可能为保留参数）
    file_name: (str) 输出图像文件名
    title: (str) 图表主标题
    exp_rows: (list[list[numericStats]]) 二维数值统计列表，每行代表一组实验结果
    type_rows: (list[list[str]]) 二维字符串列表，指定每行中元素的展示类型（"box"箱线图/"bar"柱状图）
    y_axis_labels: (list[str]) 每行图表的Y轴标签列表
    x_axis_labels: (list[str]) X轴标签列表
    y_axis_general_label: (str, optional) 全局Y轴标签
    colors: (list, optional) 颜色配置列表，支持逐行/逐元素配置
    hatches: (list, optional) 填充图案配置列表
    aspect_ratio: (float, optional) 图表宽高比
    y_limits: (tuple, optional) Y轴范围限制 (min, max)
    y_log_scale: (bool) 是否使用对数Y轴
    legend: (dict, optional) 图例配置
    y_tick_count: (int, optional) 主Y轴刻度数量
    y_tick_count_alt: (int, optional) 辅助Y轴刻度数量
    y_axis_label_alt: (str, optional) 辅助Y轴标签

    返回值：
    无，直接保存图表到文件
    """
    num_rows=len(exp_rows)
    fig, axes = plt.subplots(nrows=num_rows, ncols=1)
    if not (type(axes) is ndarray):
        axes=[axes] 
    colors=do_list_like(colors, exp_rows)
    hatches=do_list_like(hatches, exp_rows)
    type_rows=do_list_like(type_rows, exp_rows, force=True)
    
    label_ax=axes[len(axes)/2]
    
    for (ax,results_row, type_grouping, y_axis_label, color_item, 
         hatches_item) in zip(axes,
                             exp_rows,
                             type_rows,
                             y_axis_labels,
                             colors,
                             hatches):
        if ax==axes[0]:
            ax.set_title(title)
        
        if y_axis_general_label: 
            if ax==label_ax:
                y_axis_label="{0}\n{1}".format(y_axis_general_label,
                                               y_axis_label)
            else:
                y_axis_label="{0}".format(y_axis_label)
            ax.get_yaxis().set_label_coords(-0.06,0.5)
        boxplot_results=extract_type(results_row, type_grouping, "box")
        bar_results=extract_type(results_row, type_grouping, "bar")
        positions_dic, widths = _cal_positions_hybrid(type_grouping)
        if boxplot_results:
            the_labels=None
            if ax==axes[-1]:
                the_labels=extract_type(x_axis_labels, type_grouping,"box")
                if legend:
                    create_legend(ax,legend)
            else:
                the_labels=["" for x in boxplot_results]
            median, p25, p75, min_val, max_val = _get_boxplot_data(
                                                                boxplot_results)
            _add_precalc_boxplot(ax,median, p25, p75, min_val, max_val,
                            x_position=positions_dic["box"],
                            x_widths=widths,
                        colors=extract_type(color_item, type_grouping,"box"),
                        hatches=extract_type(hatches_item, type_grouping,"box"),
                            labels=the_labels,
                            y_axis_label=y_axis_label,
                            y_limits=y_limits,
                            y_log_scale=y_log_scale)
        if bar_results:
            the_y_label_alt=None
            if ax==label_ax:
                the_y_label_alt=y_axis_label_alt
            if ax==axes[-1]:
                the_labels=extract_type(x_axis_labels, type_grouping,"bar")
            else:
                the_labels=["" for x in bar_results]
            
            _add_diffs(ax, bar_results, positions_dic["bar"], widths,
                       colors=extract_type(color_item, type_grouping,"bar"),
                       hatches=extract_type(hatches_item, type_grouping,"bar"),
                       y_tick_count=y_tick_count_alt,
                       y_label=the_y_label_alt,
                       x_labels=the_labels)
        if y_tick_count:
            adjust_number_ticks(ax, y_tick_count, y_log_scale)

    if aspect_ratio:
        plt.axes().set_aspect(aspect_ratio)
    if title:
        axes[0].set_title(title)
    
    fig.savefig(file_name, bbox_inches='tight')
    
def plot_multi_bars(name, file_name, title, 
                   exp_rows,
                   type_rows,
                   y_axis_labels,
                   x_axis_labels,
                   y_axis_general_label=None,
                   colors=None,
                   hatches=None,
                   aspect_ratio=None,
                   y_limits=None,
                   y_log_scale=False,
                   legend=None,
                   y_tick_count=None,
                   y_tick_count_alt=None,
                   y_axis_label_alt=None,
                   ncols=1,
                   subtitle=None,
                   ref_line=None,
                   do_auto_label=True):
    """绘制分组柱状图，支持多行子图布局

    参数说明：
    name: (已弃用) 保留参数，未实际使用
    file_name: 输出图像的文件路径
    title: 主标题文本
    exp_rows: 二维结果数据列表，每个子列表对应一个子图的数据集（绘制中位数）
    type_rows: 二维字符串列表，定义每个子图的柱状分组方式（必须包含"bar"标记）
    y_axis_labels: 每个子图的Y轴标签列表
    x_axis_labels: X轴刻度标签列表
    y_axis_general_label: 公共Y轴总标签（默认None）
    colors: 颜色配置列表，每个子图对应一组颜色（默认None自动分配）
    hatches: 填充图案配置列表（默认None）
    aspect_ratio: 子图宽高比（默认None自动计算）
    y_limits: Y轴范围[ymin, ymax]（默认None自动调整）
    y_log_scale: 是否使用对数Y轴（默认False）
    legend: 图例配置参数（默认None不显示）
    y_tick_count: Y轴主刻度数量（默认None自动计算）
    y_tick_count_alt: 备用Y轴刻度数量（默认None）
    y_axis_label_alt: 备用Y轴标签（默认None）
    ncols: 子图列数（默认1）
    subtitle: 副标题文本（默认None）
    ref_line: 横向参考线的y坐标值（默认None不绘制）
    do_auto_label: 是否自动添加数值标签（默认True）

    返回值：
    无，直接保存图像到指定文件
    """
    num_rows=len(exp_rows)
    fig, axes = plt.subplots(nrows=num_rows/ncols, ncols=ncols)
    print axes
    if ncols>1:
        axes=asarray(flatten_list(axes))
    print axes
    if not (type(axes) is ndarray):
        axes=[axes] 
    colors=do_list_like(colors, exp_rows)
    hatches=do_list_like(hatches, exp_rows)
    type_rows=do_list_like(type_rows, exp_rows, force=True)
    
    label_ax=axes[len(axes)/2-(ncols-1)]
    if aspect_ratio:
        plt.axes().set_aspect(aspect_ratio)
    if ncols>1:
        plt.tight_layout(pad=0)
        
    for (ax,results_row, type_grouping, y_axis_label, color_item, 
         hatches_item) in zip(axes,
                             exp_rows,
                             type_rows,
                             y_axis_labels,
                             colors,
                             hatches):

        if ax==axes[0]:
            ax.set_title(title)
        if len(axes)==1 or ax==axes[1] and ncols>1 and subtitle:
            ax.set_title(subtitle)
        if ref_line:
            ax.axhline(ref_line, linestyle="--")
        
        if y_axis_general_label: 
            if ax==label_ax:
                y_axis_label="{0}\n{1}".format(y_axis_general_label,
                                               y_axis_label)
            else:
                y_axis_label="{0}".format(y_axis_label)
            #ax.get_yaxis().set_label_coords(-0.07*ncols,0.5)
        # 计算柱状图位置参数
        bar_results=results_row
        positions_dic, widths = _cal_positions_hybrid(type_grouping)

        if ax==axes[-1] or ax==axes[-ncols]:
            the_labels=x_axis_labels
            if legend and ax==axes[-1]:
                create_legend(ax,legend)
        else:
            the_labels=["" for x in bar_results]
    

        _add_diffs(ax, bar_results, positions_dic["bar"], widths,
                   colors=extract_type(color_item, type_grouping,"bar"),
                   hatches=extract_type(hatches_item, type_grouping,"bar"),
                   y_tick_count=None,
                   y_label=y_axis_label,
                   x_labels=the_labels,
                   main_axis=True,
                   bigger_numbers=True,
                   do_auto_label=do_auto_label,
                   y_log_scale=y_log_scale,
                   y_limits=y_limits)
        if y_limits:
            ax.set_ylim(y_limits[0],y_limits[1])
        if y_tick_count:
            adjust_number_ticks(ax, y_tick_count, y_log_scale, extra=ref_line)


    fig.savefig(file_name, bbox_inches='tight')

def _extract_pos(items, pos, size):
    new_items=[]
    for i in range(len(items)):
        if i%size!=pos:
            new_items.append(items[i])
    return new_items


def _get_boxplot_data(numeric_results_list):
    values=[[],[],[],[],[]]
    for result in numeric_results_list:
        a_value = result.get_values_boxplot()
        for (target, src) in zip(values, a_value):
            target.append(src)
    return values[0], values[1], values[2], values[3], values[4]  
    
def _autolabel(ax, rects, values,bigger_numbers=False, background=True,
               y_limits=None):
    """在条形图条柱上方自动添加数值标签

    Args:
        ax: matplotlib的Axes对象，用于绘制标签的坐标系
        rects: 条形图的矩形对象集合（BarContainer或列表）
        values: 需要显示的数值列表，与rects一一对应
        bigger_numbers: 是否使用更大的字号显示数值（默认False）
        background: 是否在标签后添加半透明背景（默认True）
        y_limits: 可选参数，元组形式(y_min, y_max)，限制标签的垂直位置范围

    Returns:
        None: 直接修改Axes对象，无返回值
    """
    extra_cad=""
    max_value=max(values)
    min_value=min(values)
    y_lims=ax.get_ylim()
    max_value=min(max_value, y_lims[1])
    min_value=max(min_value, y_lims[0])
    if y_limits is not None:
        if y_limits[0] is not None:
            min_value=max(min_value, y_limits[0])
        if y_limits[1] is not None:
            max_value=min(max_value, y_limits[1])
    distance = max_value-min_value
    mid_point=min_value+distance/2.0
    print "values", min_value, max_value, distance, mid_point
    va="bottom"
    margin=0.05
    h_margin=0.0
    for (rect, value) in zip(rects, values):
        if value<0.3 and value>-0.3:
            if mid_point>=0:
                height=distance*margin
            else:
                height=-distance*margin
                va="top"
        elif value>0:
            if abs(value)>distance/2:
                height=distance*margin
            else:
                height = value+distance*margin
        elif value<0:
            if abs(value)>distance/2: 
                height=-distance*margin
                va="top"
            else:
                height=value-distance*margin
        horiz_position=rect.get_x() + (rect.get_width()/2)*(1+h_margin)
        font_size="smaller"
        if bigger_numbers:
            font_size="large"
        bbox=None
        extraText=""
        if y_limits is not None:
            if y_limits[0] is not None:
                height=max(height, y_limits[0])
            if y_limits[1] is not None:
                height=min(height, y_limits[1])
        if background:
            bbox=dict(facecolor='lightgrey', pad=0,
                      edgecolor="lightgrey", alpha=0.5)
            extraText=" "
        myt=ax.text(horiz_position, 1.01*height,
                extraText+"{0:.2f}{1}".format(float(value), extra_cad),
                ha='center', va=va, rotation="vertical",
                fontsize=font_size,
                bbox=bbox)



def precalc_boxplot(name,file_name, median, p25, p75, min_val, max_val,
                    grouping=None,
                    aspect_ratio=None,
                    colors=None,
                    hatches=None,
                    labels=None,
                    y_axis_label=None,
                    title=None):
    """根据预计算的统计值绘制箱线图（无需原始数据），支持多组箱线绘制

    参数：
    name: str - 图形窗口名称
    file_name: str - 保存图像的文件路径
    median: list - 中位数列表（每个箱线对应一个）
    p25: list - 下四分位数列表（第25百分位）
    p75: list - 上四分位数列表（第75百分位）
    min_val: list - 最小值列表（下须）
    max_val: list - 最大值列表（上须）
    grouping: list, optional - 分组结构，用于多级分组箱线图
    aspect_ratio: float, optional - 图形纵横比例
    colors: list, optional - 自定义颜色列表（每个箱线对应一个）
    hatches: list, optional - 自定义填充图案列表
    labels: list, optional - 箱线标签列表
    y_axis_label: str, optional - Y轴标签文本
    title: str, optional - 图形标题

    返回值：
    None - 结果直接保存为图像文件
    """
    fig = plt.figure(name)
    ax = fig.add_subplot(111)
    _add_precalc_boxplot(ax,median, p25, p75, min_val, max_val,
                        grouping=grouping,
                        colors=colors,
                        hatches=hatches,
                        labels=labels,
                        y_axis_label=y_axis_label)
    if aspect_ratio:
        plt.axes().set_aspect(aspect_ratio)
    if title:
        ax.set_title(title)
    fig.savefig(file_name, bbox_inches='tight')

def _add_diffs(ax, diff_values, positions, width,
               colors=None, hatches=None,
               y_tick_count=None, y_label=None,
               x_labels=None,
               main_axis=False,
               bigger_numbers=False,
               do_auto_label=True,
               y_log_scale=False,
               y_limits=None):
    """
    在指定坐标轴上绘制差值条形图，支持多种自定义选项

    参数：
    ax: matplotlib.axes.Axes - 主坐标轴对象
    diff_values: array-like - 要显示的差值数据数组
    positions: array-like - 每个条形在x轴上的位置
    width: float - 条形的宽度
    colors: list[str] or None - 每个条形的填充颜色列表（与条形数量一致）
    hatches: list[str] or None - 每个条形的填充图案列表（与条形数量一致）
    y_tick_count: int or None - y轴主刻度数量控制参数
    y_label: str or None - 右侧y轴的标签文本
    x_labels: list[str] or None - x轴刻度标签列表
    main_axis: bool - 是否使用主坐标轴绘制（False时创建副坐标轴）
    bigger_numbers: bool - 是否启用大数字格式化标签
    do_auto_label: bool - 是否在条形顶部自动显示数值标签
    y_log_scale: bool - 是否使用对数刻度y轴
    y_limits: tuple[float, float] or None - 强制设置的y轴范围

    返回值：
    None
    """
    if main_axis:
        ax_alt=ax
    else:
        ax_alt=ax.twinx()
    bplot = ax_alt.bar(positions, diff_values, width=width,
                       tick_label=x_labels,
                       log=y_log_scale)
    if y_label:
        ax_alt.set_ylabel(y_label)
    if colors:        
        for patch, color in zip(bplot, colors):
            if color:
                patch.set_facecolor(color)
    if hatches:
        for patch, hatch in zip(bplot, hatches):
            if hatch:
                patch.set_hatch(hatch)
    
    if y_tick_count:
        adjust_number_ticks(ax_alt, y_tick_count)
    if do_auto_label:
        _autolabel(ax_alt, bplot, diff_values,bigger_numbers=bigger_numbers,
                   y_limits=y_limits)
    

def _adjust_y_limits_margin(ax, values, margin=0.3):
    """
    调整坐标轴Y轴的上下限，增加动态边距以改善数据可视化效果
    参数：
        ax (matplotlib.axes.Axes): 需要调整的matplotlib坐标轴对象
        values (iterable): 用于计算Y轴范围的基础数据值
        margin (float, optional): 边距比例，基于数据范围计算上下边距。默认为0.3
    返回值：
        None: 直接修改传入的ax对象的Y轴范围
    """
    max_value=float(max(values))
    min_value=float(min(values))

    if max_value>0:
        max_value+=(max_value-min_value)*margin
    if min_value<0:
        min_value-=(max_value-min_value)*margin
    ax.set_ylim((min_value, max_value))
    
    
    


def _add_precalc_boxplot(ax, median, p25, p75, min_val, max_val,
                        x_position=None,
                        x_widths=None,
                        grouping=None,
                        colors=None,
                        hatches=None,
                        labels=None,
                        y_axis_label=None,
                        y_limits=None,
                        y_log_scale=False,
                        extra_spacing=0,
                        alt_grouping=None):
    """在matplotlib坐标轴上添加基于预计算统计值的箱线图

    Args:
        ax (matplotlib.axes.Axes): 目标坐标轴对象
        median (list): 各箱线中位数列表
        p25 (list): 各箱线下四分位数列表(25th percentile)
        p75 (list): 各箱线上四分位数列表(75th percentile)
        min_val (list): 各箱线最小值列表
        max_val (list): 各箱线最大值列表
        x_position (list, optional): 自定义箱线中心位置列表
        x_widths (list, optional): 自定义箱线宽度列表
        grouping (list, optional): 分组结构配置(用于自动计算位置)
        colors (list, optional): 箱线填充颜色列表
        hatches (list, optional): 箱线填充图案列表
        labels (list, optional): X轴刻度标签列表
        y_axis_label (str, optional): Y轴标签文本
        y_limits (tuple, optional): Y轴范围限制(min, max)
        y_log_scale (bool): 是否启用Y轴对数刻度
        extra_spacing (float): 组间额外间距
        alt_grouping (list, optional): 替代分组配置(用于二级定位)

    Returns:
        tuple: 包含四个元素的元组
            - positions: 使用的箱线中心位置列表
            - widths: 使用的箱线宽度列表
            - alt_positions: 替代位置列表(存在分组时)
            - alt_width: 替代宽度值(存在分组时)

    实现要点:
        - 支持手动指定位置或自动分组布局
        - 通过生成模拟数据绕过原始数据限制
        - 提供丰富的样式自定义选项(颜色/填充/标签等)
        - 支持双分组布局配置
    """
    
    positions=None
    alt_positions=None
    widths=0.5
    alt_width=0.25

    if x_position and widths:
        positions=x_position
        widths = x_widths
    elif grouping:
        positions, widths, alt_positions, alt_width=_cal_positions_widths(
                                                grouping, 
                                                extra_spacing=extra_spacing,
                                                alt_grouping=alt_grouping)
    fake_data=_create_fake_data(median, p25,p75, min_val, max_val)
    bplot = ax.boxplot(fake_data, positions=positions, 
               widths=widths,patch_artist=True,
               labels=labels,
               whis=9999999999999)
    if colors:        
        for patch, color in zip(bplot['boxes'], colors):
            if color:
                patch.set_facecolor(color)
    if hatches:
        for patch, hatch in zip(bplot['boxes'], hatches):
            if hatch:
                patch.set_hatch(hatch)
    
    if y_axis_label:
        ax.set_ylabel(y_axis_label)
    if y_limits:
        ax.set_ylim(y_limits)
    if y_log_scale:
        ax.set_yscale("log")
    
    return positions, widths, alt_positions, alt_width

    
    
def _create_fake_data(median, p25, p75, min_val, max_val):
    """
    根据统计参数生成模拟数据点或嵌套结构
    参数可以是单个数值或同长度列表。当参数为列表时，递归生成嵌套的模拟数据结构。
    生成的单个数据点格式为五数概括[min, p25, median, p75, max]
    Args:
        median: 中位数值/列表
        p25: 25%分位数值/列表（同时作为是否批量处理的标志）
        p75: 75%分位数值/列表
        min_val: 最小值/列表
        max_val: 最大值/列表

    Returns:
        list: 单个数据点列表或嵌套结构列表。当输入参数为列表时，
              返回结构为各参数元素递归生成的嵌套列表

    Note:
        当p25参数为列表类型时，其他参数必须为相同长度的列表
    """
    if (type(p25) is list):
        fake_data=[]
        for (median_i, p25_i, p75_i,min_val_i, max_val_i) in zip(
            median, p25, p75, min_val, max_val):
            fake_data.append(
                _create_fake_data(median_i, p25_i, p75_i,min_val_i, max_val_i))
        return fake_data 
    else:
        return [min_val, p25,median,p75,max_val]
    
def _cal_positions_widths(grouping,extra_spacing=0, alt_grouping=None):
    """
    计算分组元素的布局位置和宽度参数
    Args:
        grouping: list[int] | None, 主分组方案，每个元素表示该组的基准单位数量
        extra_spacing: int, 每组后需要添加的额外间距单位数（默认0）
        alt_grouping: list[int] | None, 备用分组方案，用于计算额外间距位置（默认同主分组）

    Returns:
        tuple: 包含四个元素的元组
            - positions: list[float], 主分组中每个基准单位的中心位置坐标
            - widths: float, 每个基准单位块的归一化宽度
            - alt_positions: list[float], 额外间距标记的位置坐标
            - space_width: float, 间距单位的归一化宽度

    注意：所有位置坐标和宽度参数均在归一化的数值空间内计算
    """
    if grouping is None:
        return None, 0.5
    if alt_grouping is None:
        alt_grouping=grouping
    total_bp=sum(grouping)
    total_blocks=total_bp+len(grouping)+1
    if extra_spacing:
        total_blocks+=len(grouping)*extra_spacing
    
    widths=total_blocks/float(total_blocks)
    space_width=float(widths)/2.0
    
    current_pos=1.0
    positions = []
    alt_positions=[]
    for (bp_group, alt_group) in zip(grouping, alt_grouping):
        for bp in range(bp_group):
            positions.append(current_pos)
            current_pos+=widths
        for i in range(min(extra_spacing, alt_group-1)):
            alt_positions.append(current_pos-space_width)
            current_pos+=space_width
        current_pos+=space_width
    return positions, widths, alt_positions, space_width

def _cal_positions_hybrid(grouping):
    flat_grouping = flatten_list(grouping)
    if grouping is None:
        return None, 0.5
    uniq_types=list(set(flat_grouping))
    positions_dic = {}
    for ut in uniq_types:
        positions_dic[ut] = []
   
#     total_bp=len(flat_grouping)
#     total_blocks=float(total_bp)+(float(len(grouping))-1)*0.5
# 
#     widths=total_blocks/float(total_blocks)
#     space_width=float(widths)/2
#     
#     if float(len(grouping))%2==0:
#         widths*=2
#         space_width*=2
#         current_pos=widths
#     else:
#         current_pos=0.0
    widths=1.0
    space_width=widths
    current_pos=0


    for (bp_group) in grouping:
        for bp in bp_group:
            positions_dic[bp].append(current_pos)
            current_pos+=widths
        current_pos+=space_width
    return positions_dic, widths
            
    

def extract_grouped_results(db_obj, trace_id_rows_colors, edges, result_type):
    """
    从数据库提取分组实验结果并组织为层级结构

    参数：
    db_obj: DBManager
        已连接数据库的DBManager对象，用于提取实验结果
    trace_id_rows_colors: List[List[int]]
        二维列表，包含实验的跟踪ID，每个子列表代表一组实验
    edges: List[str]
        边标识列表。当包含非空元素时，会生成组合键（格式："g{edge}_str{result_type}"）来提取结果
    result_type: str
        结果类型标识符，对应数据库中NumericStats存储的类型

    返回值：
    Dict[str, List[List[NumericStats]]]
        按edges索引的字典，每个值是与trace_id_rows_colors维度相同的二维列表，
        包含对应组件的NumericStats统计结果对象

    处理逻辑：
    1. 初始化按edges分组的空结果容器
    2. 遍历每个实验行和跟踪ID：
        - 加载实验定义
        - 对每个edge生成结果键
        - 从数据库加载或创建默认统计结果
    3. 将结果按层级结构组织返回
    """
    exp_rows={}
    for edge in edges:
        exp_rows[edge]=extract_results(db_obj, trace_id_rows_colors,
                                       ResultTrace.get_result_type_edge(edge,
                                                               result_type))
    return exp_rows
    
    
    exp_rows={}
    for edge in edges:
        exp_rows[edge]=[]
    for row in trace_id_rows_colors:
        these_rows={}
        for edge in edges:
            these_rows[edge]=[]
            exp_rows[edge].append(these_rows[edge])
        for trace_id in row:
            exp=ExperimentDefinition()
            exp.load(db_obj, trace_id)
            for edge in edges:
                result=None
                if exp.is_it_ready_to_process():
                    if edge=="":
                        key = ResultTrace.get_result_type_edge(edge,
                                                               result_type)
                    else:
                        key=result_type
                    key+="_stats"
                    result = NumericStats()
                    result.load(db_obj, trace_id, key)
                else:
                    result = NumericStats()
                    result.calculate([0, 0, 0])
                these_rows[edge].append(result)
    return exp_rows

def get_list_rows(rows, field_list):
    new_rows=[]
    for row in rows:
        new_row = []
        new_rows.append(new_row)
        
        for (index,res) in zip(range(len(row)),row):
            field=field_list[index%len(field_list)]
            new_row.append(res._get(field))
    return new_rows

def extract_usage(db_obj, trace_id_rows, fill_none=True, factor=1.0, 
                  mean=False):
    """从数据库提取实验使用率结果，按原始结构组织成结果对象矩阵
    Args:
        db_obj (DBManager): 已连接的数据库管理器对象，用于执行数据查询
        trace_id_rows (list[list]): 二维列表结构，每个元素为trace_id标识符
        fill_none (bool): [未使用的参数] 保留参数，用于未来扩展占位符处理
        factor (float): 结果数值的缩放系数，默认1.0表示原始值
        mean (bool): True时提取usage_mean结果，False时提取usage结果
    Returns:
        list[list]: 二维结果矩阵，每个元素为ResultTrace对象，结构与输入trace_id_rows对应
    处理逻辑：
        1. 根据mean参数确定结果类型(res_type)
        2. 遍历二维结构中的每个trace_id
        3. 对每个实验加载定义并尝试获取结果
        4. 若实验未完成分析则初始化空结果
        5. 应用缩放系数后保存结果对象
    """
    exp_rows=[]  
    my=ResultTrace()
    res_type="usage"
    if mean:
        res_type="usage_mean"
    for row in trace_id_rows:
        new_row=[]
        exp_rows.append(new_row)
        for trace_id in row:
            exp=ExperimentDefinition()
            exp.load(db_obj, trace_id)
            result = my._get_utilization_result()           
            if exp.is_analysis_done():
                result.load(db_obj, trace_id,res_type)
            else:
                result._set("utilization", 0)
                result._set("waste", 0)
                result._set("corrected_utilization", 0)
            result.apply_factor(factor)
            new_row.append(result)
    return exp_rows


def extract_results(db_obj, trace_id_rows_colors, result_type, factor=None,
                    fill_none=True, second_pass=False):
    """从实验跟踪ID矩阵中提取对应类型的结果矩阵

    遍历包含实验跟踪ID的二维矩阵，从数据库查询每个ID对应的实验结果，根据条件处理空值和应用缩放因子，
    最终返回与输入矩阵维度相同的NumericStats对象矩阵

    Args:
        db_obj (DBManager): 已建立连接的数据库管理器对象，用于执行数据加载操作
        trace_id_rows_colors (list[list[int]]): 二维矩阵，每个元素代表实验的跟踪ID
        result_type (str): 结果类型标识符，对应数据库中NumericStats存储的类型名称
        factor (float, optional): 结果缩放因子，如果提供则会对结果应用该因子. Defaults to None
        fill_none (bool, optional): 是否用空统计对象填充缺失值. Defaults to True
        second_pass (bool, optional): 是否检查二次分析结果. Defaults to False

    Returns:
        list[list[NumericStats]]: 与输入矩阵同维度的结果矩阵，每个元素为对应的统计对象
    """
    exp_rows=[]  
    for row in trace_id_rows_colors:
        new_row=[]
        exp_rows.append(new_row)
        for trace_id in row:
            exp=ExperimentDefinition()
            exp.load(db_obj, trace_id)
            
            if exp.is_analysis_done(second_pass=second_pass):
                key=result_type+"_stats"
                result = NumericStats()
                result.load(db_obj, trace_id, key)
                if factor:
                    result.apply_factor(factor)
            else:
                result = NumericStats()
                result.calculate([0, 0, 0])
            if fill_none and result._get("median") is None:
                result = NumericStats()
                result.calculate([0, 0, 0])
            new_row.append(result)
    return exp_rows

def get_dic_val(dic, val):
    if val in dic.keys():
        return dic[val]
    return dic[""]

def produce_plot_config(db_obj, trace_id_rows_colors):
    """
    生成矩阵样式图表所需的颜色配置和阴影模式配置

    根据实验使用的调度算法类型，从数据库获取实验结果并为每个子图生成对应的
    颜色矩阵和阴影模式矩阵，同时生成图例说明

    Args:
        db_obj (DBManager): 已连接的数据库管理对象，用于获取实验数据
        trace_id_rows_colors (list of list of int): 二维列表，每个元素代表一个
            实验的trace_id，用于组织子图矩阵结构

    Returns:
        color_rows (list of list of str): 二维颜色值列表，每个元素对应子图的
            matplotlib颜色值
        hatches_rows (list of list of str): 二维阴影模式列表，每个元素对应子图
            的matplotlib阴影符号
        new_legend (list of tuple): 过滤后的图例列表，格式为(系列名称, 颜色值,
            处理方式标识, 阴影符号)，仅包含实验中实际存在的调度算法类型
    """
    colors_dic = {"no":"white", "manifest":"lightgreen", "single":"lightblue",
                  "multi":"pink", "":"white"}
    
    hatches_dic = {"no":None, "manifest": "-", "single":"\\",
                  "multi":"/", "":None}
    
    detected_handling={}
    
    color_rows = []
    hatches_rows = []
    for row in trace_id_rows_colors:
        this_color_row=[]
        color_rows.append(this_color_row)
        this_hatches_row=[]
        hatches_rows.append(this_hatches_row)
        for trace_id in row:
            exp = ExperimentDefinition()
            exp.load(db_obj, trace_id)
            handling=exp.get_true_workflow_handling()
            detected_handling[handling]=1
            this_color_row.append(get_dic_val(colors_dic,
                                              handling))
            this_hatches_row.append(get_dic_val(hatches_dic,
                                                handling))
        
    legend=[("n/a","white", "no", None),
                ("aware","lightgreen", "manifest","-"),
                ("waste","lightblue", "single", "\\"),
                ("wait","pink", "multi", "/")]
    new_legend=[]
    for item in legend:
        if item[2] in detected_handling.keys():
            new_legend.append(item)
    
    return color_rows, hatches_rows, new_legend
            
     
     
        
    
    
    