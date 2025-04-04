
import matplotlib.dates as mdates
import numpy as np
import scipy as sp
from scipy.stats import scoreatpercentile                                                                         
import math
import matplotlib
from sys import platform
if platform == "darwin":
    matplotlib.use('TkAgg')
from matplotlib import pyplot

from pylab import *
import time
from matplotlib.font_manager import FontProperties

import matplotlib.pyplot as plt
import gc
import os
# from scipy.spatial import Voronoi, voronoi_plot_2d

from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt

from commonLib import starLib
import scipy.cluster.vq as vq

def clearMem():
    plt.close('all')
    plt.clf()
    

showGraphs = True
def disableShow():
	global showGraphs
	matplotlib.use('Agg')
	showGraphs = False
	
def showFig(fig, ax):
	if (showGraphs):
		fig.show()
	else:
		plt.close('all')
		plt.clf()
		del fig
		del ax
	
# 	gc.collect()
def showAll():
	plt.show()
	plt.close('all')
	plt.clf()
	gc.collect()
	
def saveGraph(fig, dir, graphFileName):
    """
    将 matplotlib 图形对象保存为 PNG 文件
    参数:
    fig (matplotlib.figure.Figure): 需要保存的 matplotlib 图形对象
    dir (str): 文件保存目录路径。如果为空字符串，表示当前目录
    graphFileName (str): 要保存的文件名（不含扩展名）。如果为空字符串则不执行保存操作
    返回值:
    None: 该函数没有返回值
    """
	if (graphFileName != ""): 
		if (dir != ""):
			dir += "/"
		fullRoute = dir + graphFileName + ".png"

		fig.savefig(fullRoute)
                
def cleanFileName(fileName):
    words = fileName.split(" ")
    nWords = []
    for word in words:
        nWords.append("".join(c.upper() if i == 0 else c for i, c in enumerate(word)))
    return "".join(nWords)
    
	
def adjustXTickers(ax, ticks="", rotation="vertical", fontsize=None,):
    """
    调整指定坐标轴对象的x轴刻度显示属性
    Args:
        ax: matplotlib Axes对象
            需要被调整的坐标轴实例
        ticks: list of float, optional, default=""
            指定刻度位置的列表，空字符串表示不修改刻度位置
        rotation: str/float, optional, default="vertical"
            刻度标签旋转角度，支持字符串('vertical')或数值角度
        fontsize: int/None, optional, default=None
            刻度标签字体大小，None表示保持原样

    Returns:
        None: 直接修改传入的Axes对象，无返回值
    """
    if (ticks != ""):
        print "do"
        ax.set_xticks(ticks)
    tickers = ax.xaxis.get_major_ticks()
    for tick in tickers:
        if (fontsize != None):
            tick.label.set_fontsize(fontsize=fontsize)
        if (rotation != None):
            tick.label.set_rotation(rotation)

def adjustYTickers(ax, ticks="", fontsize=13, fontsize2=13):
	if (ticks != ""):
		ax.set_yticks(ticks)
		ax.set_ylim(ticks[0], ticks[-1])
	tickers = ax.yaxis.get_major_ticks()
	for tick in tickers:
		tick.label.set_fontsize(fontsize) 
        tick.label2.set_fontsize(fontsize2)

def setTitle(fig, title, fontsize=14):
  title = title.replace("edison", "Edison")
  title = title.replace("hopper", "Hopper")
  title = title.replace("carver", "Carver")
  fig.suptitle(title, fontsize=fontsize, fontweight='bold')

def adjustLabels(ax, labelX, labelY, labelZ="", fontsizeX=13, fontsizeY=13, fontsizeZ=13):
  if labelX != "":
	   ax.set_xlabel(labelX, fontsize=fontsizeX)
  if labelY != "":
	   ax.set_ylabel(labelY, fontsize=fontsizeY)
  if (labelZ != ""):
	   ax.set_zlabel(labelZ, fontsize=fontsizeZ)
		
def adjustMargin(fig, top="", bottom="", right="", left="", \
                 hspace="", vspace=""):
    if top == "":
        top = 0.9
    if bottom == "":
        bottom = 0.2
    if top != "":
        fig.subplots_adjust(top=top)
    if bottom != "":
        fig.subplots_adjust(bottom=bottom)
    
	if (right != ""):
		fig.subplots_adjust(right=right)
	if (left != ""):
		fig.subplots_adjust(left=left)
    
    if (hspace != ""):
        subplots_adjust(hspace=hspace)
    if (vspace != ""):
        subplots_adjust(vspace=vspace)
                
	
	
def addHLines(ax, ticks=""):
	if (ticks == ""):
		ticks = ax.get_yticks()
	for l in ticks:
		ax.axhline(y=l)

		
def yAxisNormalize(ax):
	yAxis = ax.get_yaxis()

def splitDic(dic, series=""):
    if (series == ""):
        series = sort(dic.keys())
    values = []
    for k in series:
        values.append(dic[k])
    return series, values


#on dataList：每个系列有一行…每一列是对应变量的序列值（行是序列，列是变量）。
def paintVoronov(name, vor, dir="", graphFileName="", logScale=False):
    """
    绘制并保存Voronoi图
    根据输入的Voronoi对象生成二维可视化图形，并进行图像保存操作
    Args:
        name (str): 图形窗口的标题名称
        vor (scipy.spatial.Voronoi): Voronoi图计算对象
        dir (str, optional): 图形保存目录路径，默认为当前目录
        graphFileName (str, optional): 保存文件的名称，空字符串时使用默认命名
        logScale (bool, optional): 是否使用对数刻度（当前实现中未使用该参数）

    Returns:
        None: 该函数没有返回值
    """
    fig = plt.figure(name)
    setTitle(fig, name)
    adjustMargin(fig)
    ax = fig.add_subplot(111)
    fig = voronoi_plot_2d(vor)
    # plt.show()
    # showAll()
    saveGraph(fig, dir, graphFileName)

def paintStarGraph(name, dataList, varsNames, seriesNames, dir="", graphFileName="", logScale=False):
    theta = starLib.radar_factory(len(varsNames), frame="polygon")
    
    fig = plt.figure(name)
    setTitle(fig, name)
    adjustMargin(fig)
    ax = fig.add_subplot(111, projection='radar')
    # ax = fig.add_subplot(2, 2, n+1, projection='radar')
    cmap = matplotlib.cm.ScalarMappable(cmap="Accent")

    colors = ['b', 'r', 'g', 'm', 'y', 'r', 'g', 'm', 'y']
    
    cN = range(1, len(seriesNames))
    colors = cmap.to_rgba(cN)
    
    plt.rgrids([0.2, 0.4, 0.6, 0.8])
    
    for d, color in zip(dataList, colors):
        ax.plot(theta, d, color=color)
        ax.fill(theta, d, facecolor=color, alpha=0.25)
    ax.set_varlabels(varsNames)
    legend = plt.legend((seriesNames), loc=(0.97, 0.00), labelspacing=0.1)
    plt.setp(legend.get_texts(), fontsize='small')
    
    saveGraph(fig, dir, graphFileName)
    
    
    
def makeLabelsEqual(labels):
    size = max([len(x) for x in labels])
    return [x.rjust(size) for x in labels]
    

def paintBoxPlot(name, dic, dir="", graphFileName="", labelX="Series", labelY="Value", logScale=False, inSeries="", fontsizeX=12, fontsizeY=13):
    series, values = splitDic(dic)
    return paintBoxPlotSimple(name, series, values, dir, graphFileName, labelX, labelY, logScale, inSeries, fontsizeX, fontsizeY)
    
def paintBoxPlotSeries(name, seriesNames, dic, dir="", graphFileName="", labelX="Series", labelY="Value", logScale=False, inSeries="", fontsizeX=12, fontsizeY=13):
    valuesDic = {}
    for s in seriesNames:
        series, values = splitDic(dic[s])
        valuesDic[s] = values
    return paintBoxPlotSimpleSeries(name, seriesNames, series, valuesDic, dir, graphFileName, labelX, labelY, logScale, inSeries, fontsizeX, fontsizeY)


def cleanLabels(labels, count=1, dates=False):
    newLabels = []
    for (label, i) in zip(labels, range(len(labels))):
        if (dates and count == "month"):
            if (time.strftime('%d', time.localtime(label)) == 1):
                 label = time.strftime('%Y-%m-%d', time.localtime(label))
            else:
                label = ""
        else:
            if (i % count == 0):
                label == ""
            elif (dates):
                label = time.strftime('%Y-%m-%d', time.localtime(label))
        
        newLabels.append(label)
    return newLabels
        
            


def paintBoxPlotSimpleMultiSplitGraph(name, seriesDic, valuesDic, dir="", \
                    graphFileName="", labelX="Series", labelY="Value", logScale=False, \
                    inSeries="", fontsizeX=12, fontsizeY=13, filterUp=None, \
                    limitY=None, seriesNames=None, tickFrequence=None, \
                        allGraphsLabels=False, yLim=None, xTicksRotation=None, \
                            interPlotSpace=None):
    
    graphNames = seriesDic.keys()
    if seriesNames != None:
        graphNames = seriesNames

    if (inSeries != ""):
        series = inSeries
    
    fig, axes = plt.subplots(nrows=len(graphNames))
    

   
    setTitle(fig, name, fontsize=fontsizeX + 2)
    adjustMargin(fig, top=0.92, bottom=0.22)
    outputDic = {}
    
    numberGraphs = len(graphNames)
    labelYIndex = numberGraphs / 2
    ticksXIndex = numberGraphs - 1
    
    if (numberGraphs == 1):
        axes = [axes]
    
    for ax, graphName, index in zip(axes, graphNames, range(numberGraphs)):
        series = seriesDic[graphName]
        values = valuesDic[graphName]    
        if xTicksRotation != None:
            
            adjustXTickers(ax, rotation=xTicksRotation, fontsize=fontsizeX)
        if (logScale):

            ax.set_yscale('log')
        outputDic[graphName] = ax.boxplot(values)
        addHLines(ax)
        
        tempLabelX = labelX
        tempLabelY = labelY + "\n" + graphName
        
        if (ticksXIndex != index):
            tempLabelX = ""
        if (labelYIndex != index):
            tempLabelY = graphName
        
        adjustLabels(ax, tempLabelX, tempLabelY, fontsizeX=fontsizeX, fontsizeY=fontsizeY)

        if index == ticksXIndex or allGraphsLabels:
            x = series
            if (tickFrequence != None):
                x, xMin, xMax = tansformEpochToDates(series, -1, 0) 
                xAxisDates(ax, xMin, xMax, tickFrequence) 
            
            xtickNames = plt.setp(ax, xticklabels=x)
            # plt.setp(xtickNames, rotation=45, fontsize=fontsizeX, horizontalalignment="right")
            # plt.setp(xtickNames, rotation=45, fontsize=fontsizeX, horizontalalignment="right")
        else:
             xtickNames = plt.setp(ax, xticklabels=["" for s in series])
        
        if (limitY != None):
            print "SEEEEETTTING"
            ax.set_ylim(limitY)
        
        if (yLim != None):
            ax.set_ylim(yLim)
        adjustYTickers(ax, fontsize=fontsizeY)
    if (interPlotSpace != None):
        plt.subplots_adjust(hspace=interPlotSpace)
        
    saveGraph(fig, dir, graphFileName)
    # plt.close()
    return outputDic


def paintBoxPlotSimpleSeries(name, seriesNames, subSeriesNames, valuesDic, dir="", graphFileName="", labelX="Series",
                             labelY="Value", logScale=False, inSeries="", fontsizeX=12, fontsizeY=13):
    
    print "sub series:", subSeriesNames
    factor = 0.9 / len(seriesNames)
    colors = cm.rainbow(np.linspace(0, 1, len(seriesNames)))
    
    if (inSeries != ""):
        series = inSeries
    
    # series=makeLabelsEqual(series)
    # print series
    # print values
    fig = plt.figure(name)
    setTitle(fig, name, fontsize=fontsizeX + 2)
    adjustMargin(fig, top=0.92, bottom=0.22)
    ax = fig.add_subplot(111)
    if (logScale):
        ax.set_yscale('log')
  
    addHLines(ax)
    adjustLabels(ax, labelX, labelY, fontsizeX=fontsizeX, fontsizeY=fontsizeY)
    # xtickNames = plt.setp(ax, xticklabels=subSeriesNames)
    # plt.setp(xtickNames, rotation=45, fontsize=fontsizeX, horizontalalignment="right")
    
    edges = np.arange(len(subSeriesNames))
    
    acumm = 0
    done = False
    for (series, c) in zip(seriesNames, colors):
        
        values = valuesDic[series]
        print "postions", edges + acumm
        acumm += factor
        # plt.boxplot(values, color=c)
        plt.boxplot(values, positions=edges + factor, widths=factor)
    
    # plt.setp(series, rotation=45, fontsize=fontsizeX)
    adjustYTickers(ax, fontsize=fontsizeY)
    
    saveGraph(fig, dir, graphFileName)
    # plt.close()
    return True
    
def paintScatter(name, x, y, dir="", graphFileName="", labelX="Series", labelY="Value", logScale=False, xLogScale=False):
    # print values
    fig = plt.figure(name)
    setTitle(fig, name)
    adjustMargin(fig)
    ax = fig.add_subplot(111)
    if (logScale):
        ax.set_yscale('log')
    if (xLogScale):
        ax.set_xscale('log')
    plt.scatter(x, y, alpha=0.03)
    addHLines(ax)
    adjustLabels(ax, labelX, labelY)
    # ax.set_xticks(edges+width/2.0)
    adjustXTickers(ax)
    
    saveGraph(fig, dir, graphFileName)

def coWhiten(dic, queueNames):
    """
    对指定队列数据进行协同白化处理，消除各维度相关性并标准化
    参数:
        dic (dict): 输入数据字典，键为队列名称，值为对应的二维数据数组(变量按行排列)
        queueNames (list): 需要处理的队列名称列表，用于筛选字典键值
    返回值:
        dict: 处理后的数据字典，结构与输入字典一致，值为白化后的数据数组
    处理流程:
        1. 将多个队列数据按变量维度纵向拼接
        2. 对合并后的数据集进行白化处理
        3. 将处理结果按原始队列拆分回各自队列
    """
    # each row on DicList is one of the variables... they have to become a comlum.
    
    outDic = {}
    subL = []
    elementCount = {}

    count = 0
    for q in queueNames:
        subL += dic[q]
        elementCount[q] = len(dic[q])
    subL = np.transpose(vq.whiten(np.transpose(np.array(subL, dtype=float))))
    counter = 0
    for q in queueNames:
        outDic[q] = subL[counter:counter + elementCount[q]]
        counter += elementCount[q]
    return outDic


    # outDic[queue]=np.transpose(vq.whiten(np.transpose(np.array(dic[queue], dtype=float))))

    


def paintScatterSeries(name, xDic, yDic, seriesNames=None, dir="", graphFileName="", labelX="Series", labelY="Value", \
                        logScale=False, xLogScale=False, centroids="", whiten=False, bounds="", \
                            fontsizeX=12, fontsizeY=13, fontsizeY2=13, displace=0, alpha=0.25, dotSize=2.0, \
                                tickFrequence=None, boundsUnrelated=None, doTight=False, showLegendBoxes=True):
    # print values
    
    # seriesNames=sort(seriesNames)
    # fig = plt.figure(name,figsize=(11, 6))
    fig = plt.figure(name)
    setTitle(fig, name, fontsize=fontsizeX + 2)
    adjustMargin(fig)
    # adjustMargin(fig, top=0.9, bottom=0.1, right=0.7, left=0.0)
    ax = fig.add_subplot(111)
    # plt.axes().set_aspect('equal', 'datalim')
    if seriesNames == None:
        seriesNames = xDic.keys()
    
    if (logScale):
        ax.set_yscale('log')
        labelY += " (Log Scale)"
    if (xLogScale):
        ax.set_xscale('log')
    
   
    colors = cm.rainbow(np.linspace(0, 1, len(seriesNames)))
    # NO se puede hacer as+i! hayq eu untasr antes de whiten
    if (whiten):
        xDic = coWhiten(xDic, seriesNames)
        yDic = coWhiten(yDic, seriesNames)
   
    minXValue = -1
    maxXValue = 0
    for serie, c in zip(seriesNames, colors):
        x = xDic[serie]
        y = yDic[serie]
        if tickFrequence != None:
            x, minXValue, maxXValue = tansformEpochToDates(x, minXValue, maxXValue)
        print "minmax", minXValue, maxXValue
        plt.scatter(x, y, alpha=alpha, color=c, s=dotSize, lw=dotSize)
       
        # if (dateLabels):
        #    plt.plot_date(x,y)
        
   
    

    if(centroids != ""):
        print centroids[:, 0], centroids[:, 1]
        plt.scatter(centroids[:, 0], centroids[:, 1], color="black")
        
        
    if (bounds != ""):
        for serie, c in zip(seriesNames, colors):
            l = bounds[serie]
            # print l
            l.append(l[0])
            l = np.array(l)
            # print l[:,0], l[:,1]
            plt.plot(l[:, 0], l[:, 1], color=c, linewidth=2.0)
            
  
#            circle=boundsUnrelated[cluster]
#            center=circle["center"]
#            r=circle["r"]
            
#            pltCircle=plt.Circle(center, r, color=color, fill=False)
#            ax.add_artist(pltCircle)
            
#            #print l
#            l.append(l[0])
#            l=np.array(l)
#            #print l[:,0], l[:,1]
#            plt.plot(l[:,0], l[:,1], color="black", linewidth=2.0)
            
            
           
        # plt.scatter(bounds[:,0], bounds[:,1],color="black", marker='+')
    if (doTight):
        plt.axis('tight')
#    legend = plt.legend(sort(seriesNames), loc=(0.97, 0.00), labelspacing=0.1)
    # legend = plt.legend(seriesNames, bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    legend = plt.legend(seriesNames, bbox_to_anchor=(0.95 + displace, 1), loc=2, borderaxespad=0., labelspacing=0.1, prop={'size':fontsizeX - 2})

    if (boundsUnrelated != None):
        print boundsUnrelated
        colorsUn = cm.rainbow(np.linspace(0, 1, len(boundsUnrelated.keys())))
        plotLines = []
        for (cluster, color) in zip(boundsUnrelated.keys(), colorsUn):

            l = boundsUnrelated[cluster]
            # print l
            l.append(l[0])
            l = np.array(l)
            # print l[:,0], l[:,1]
            plotLines.append(plt.plot(l[:, 0], l[:, 1], color=color, linewidth=2.0, label=str(cluster)))
        
        if (showLegendBoxes):
            legend2 = pyplot.legend(sorted([str(x) for x in boundsUnrelated.keys()]), \
                                    bbox_to_anchor=(1.2 + displace, 0), loc=4, borderaxespad=0., \
                                    labelspacing=0.1, prop={'size':fontsizeX - 2})
            pyplot.gca().add_artist(legend)



    addHLines(ax)
   
    adjustLabels(ax, labelX, labelY, fontsizeX=fontsizeX, fontsizeY=fontsizeY)
    # ax.set_xticks(edges+width/2.0)
    adjustXTickers(ax, rotation=45, fontsize=fontsizeY)
    adjustYTickers(ax, fontsize=fontsizeY)
    
#    if (doTight):
#        fig.tight_layout()
    if tickFrequence != None:
        xAxisDates(ax, minXValue, maxXValue, tickFrequence) 
    
    saveGraph(fig, dir, graphFileName)

def tansformEpochToDates(x, minXValue, maxXValue):
    dateconv = np.vectorize(datetime.datetime.fromtimestamp)
    if minXValue == -1:
        minXValue = min(x)
    else:
        minXValue = min([minXValue] + x)
    maxXValue = max([minXValue] + x)
   # print x
    x = dateconv(x)
    return x, minXValue, maxXValue
    
def xAxisDates(ax, minXValue, maxXValue, tickFrequence):

    
    minLocator = None
    dateFormatter = mdates.DateFormatter('%Y-%m-%d')
    if (tickFrequence == "year"):
        locator = mdates.YearLocator()  # every year
        minLocator = mdates.MonthLocator() 
    elif (tickFrequence == "month"):
        locator = mdates.MonthLocator()  # every month
        # minLocator = mdates.WeekdayLocator()
    elif (tickFrequence == "week"):
        locator = mdates.WeekdayLocator()
        minLocator = mdates.DayLocator()
    elif (tickFrequence == "day"):    
        locator = mdates.DayLocator()    
        minLocator = mdates.HourLocator()
        print "day"
    elif (tickFrequence == "hour"):   
        locator = mdates.HourLocator()   
        dateFormatter = mdates.DateFormatter('%H')
        print "hour"
    else:
        locator = mdates.AutoDateLocator()   
   
    if (tickFrequence != "none"):
        ax.xaxis.set_major_formatter(dateFormatter)
        ax.xaxis.set_major_locator(locator)
        if (minLocator != None):
            ax.xaxis.set_minor_locator(minLocator)

    # datemin = time.gmtime(minXValue)
    # datemax = time.gmtime(maxXValue)
    # datemin = datetime.date(time.gmtime(minXValue).tm_year, 1, 1)
    # datemax = datetime.date(time.gmtime(maxXValue).tm_year+1, 1, 1)

    # ax.format_xdata = dateFormatter
    # fig.autofmt_xdate()
    # plt.tight_layout()
    print datetime.datetime.fromtimestamp(minXValue)
    print datetime.datetime.fromtimestamp(maxXValue)
    print "bounds", minXValue, maxXValue
    # ax.set_xlim([datetime.datetime.fromtimestamp(minXValue), datetime.datetime.fromtimestamp(maxXValue)])
    ax.set_xlim(date2num(datetime.datetime.fromtimestamp(minXValue)), date2num(datetime.datetime.fromtimestamp(maxXValue)))
    ax.grid(True)




def calculateBins(values):
    """
    根据Freedman-Diaconis规则计算直方图的最佳箱数
    参数:
        values (array-like): 输入数据值列表/数组，将基于这些值计算箱数
    返回值:
        float: 建议的直方图分箱数量，通过数据范围除以Freedman-Diaconis公式计算的箱宽得到
    参考:
        Freedman-Diaconis规则文献:
        http://stats.stackexchange.com/questions/798/calculating-optimal-number-of-bins-in-a-histogram-for-n-where-n-ranges-from-30
        http://comments.gmane.org/gmane.comp.python.scientific.user/19755
    """
    X = np.sort(values) 
    max = X[-1]
    min = X[0]

    # 计算四分位距(IQR)
    upperQuartile = scoreatpercentile(X, .75)                                                                      
    lowerQuartile = scoreatpercentile(X, .25)                                                                      
    IQR = upperQuartile - lowerQuartile
    h = 2.*IQR / len(X) ** (1. / 3.)  
    return (max - min) / h

def highFilter(list, val):
    return [x for x in list if x <= val ]
    
def getPertentiles(list, percentiles):
    print "percent", percentiles
    return np.percentile(np.array(list), percentiles)
    
	
def paintHistogram(name, values, bins=0, dir="", graphFileName="", labelX="Series", labelY="Value", \
                   logScale=False, special="", fontsizeX=12, fontsizeY=13, fontsizeY2=13, \
                    normed=False, cumulative=False, cumulativePlot=True,
                    filterCut=0):
    """
    绘制带有可选累积分布曲线的直方图
    参数：
        name (str): 图表标题
        values (array-like): 输入数据数组
        bins (int): 直方图柱子数量，0表示自动计算（默认0）
        dir (str): 图表保存目录（默认当前目录）
        graphFileName (str): 保存文件名（空则不保存）
        labelX (str): X轴标签（默认"Series"）
        labelY (str): Y轴标签（默认"Value"）
        logScale (bool): 是否使用对数Y轴（默认False）
        special (str): 特殊绘图参数，用于绘制额外曲线（默认空）
        fontsizeX (int): X轴标签字体大小（默认12）
        fontsizeY (int): Y轴标签字体大小（默认13）
        fontsizeY2 (int): 右侧Y轴标签字体大小（默认13）
        normed (bool): 是否归一化为百分比（默认False）
        cumulative (bool): 是否绘制累积直方图（默认False）
        cumulativePlot (bool): 是否绘制累积分布曲线（默认True）
        filterCut (float): 数据过滤阈值，0表示不过滤（默认0）
    返回值：
        tuple: (频数数组, 柱子边界数组, 柱子对象列表)
    """
# 	print exp.edges
# 	print exp.hist
        if (bins == 0):
            bins = calculateBins(values)
        
        
        if normed and labelY == "Value":
            labelY = "%"
        
        # print "Doing histogram, number of bins:"+str(bins)
    #    hist, edges=np.histogram(values, bins=bins, range=None, normed=False, weights=None, density=None)
        
        # numpy.histogram(a, bins=bins, range=None, normed=False, weights=None, density=None)[source]

	fig = plt.figure(name)
	setTitle(fig, name, fontsize=fontsizeX + 2)
	adjustMargin(fig, top=0.88, bottom=0.18)
        
     
	
	ax = fig.add_subplot(111)

	
	# if (size(edges)<64):
	# 		plt.ylim(ymin=0)

	
	# ax.bar(edges[0:-1], hist, width=(edges[1]-edges[0]))
        # ax.bar(edges[0:-1], hist)
        if (logScale):
            ax.set_yscale('log')
            
        # n, edges, patches  =ax.hist(values, bins=bins, log=logScale, normed=normed)
        weights = None
        total = sum(values)
       
        if (filterCut != 0):
            filterValues = highFilter(values, filterCut)
            # values=filterValues
        else:
            filterValues = values
        if (normed):
            weights = np.zeros_like(filterValues) + 100. / len(values)

            
        n, edges, patches = ax.hist(filterValues, bins=bins, log=logScale, weights=weights, cumulative=cumulative)
        
#        if (normed):
#            print np.sum(n)
#            formatter = FuncFormatter(to_percent)
#            plt.gca().yaxis.set_major_formatter(formatter)
        
        # print edges
        ax.set_ylim(min(n), max(n))
        
        if cumulativePlot:
            ax2 = ax.twinx()
            ax2.set_yscale("linear")
            
    #        print n
    #        print hist
    #        print edges
    #        print "Lenght", len(edges[:-1]), np.cumsum(n)

            cumulativeArray = np.cumsum(n, dtype=float)
            cumulativeArray /= cumulativeArray[-1]
           # print cumulativeArray
            ax2.plot(edges[:-1], cumulativeArray, color="red")
            adjustLabels(ax2, "", "Cumulative %", fontsizeX=fontsizeX, fontsizeY=fontsizeY)
            adjustYTickers(ax2, fontsize=fontsizeY, fontsize2=fontsizeY2)
            legend = plt.legend((["Cumulative %"]))
            if (normed):
                ax2.set_ylym = [0, 1]
        
        
        if (special != ""):
            # ax3 = ax.twinx()
#            ax3.set_yscale("linear")
            # print "here we go"
#            print special
#            cumulativeArray=np.cumsum(special, dtype=float)
#            print "Total CPU Seconds: ", cumulativeArray[-1]
#            cumulativeArray/=cumulativeArray[-1]
#            print cumulativeArray
            special[1] = [x for x in special[1]]
#            print special[0]
#            print special[1]
#            print special[1]-n
            ax.plot(special[0], special[1], color="green")
#            ax3.set_xlim(min(edges), max(edges))
#            if (normed):
#                ax3.set_ylim(min(n),max(n))
#                ax.set_ylim(min(n),max(n))
                
#            if (logScale):
#                ax3.set_yscale("log")
   
            
        
            
        
	adjustLabels(ax, labelX, labelY, fontsizeX=fontsizeX, fontsizeY=fontsizeY)
	adjustXTickers(ax, rotation=45, fontsize=fontsizeY)
	adjustYTickers(ax, fontsize=fontsizeY)
	addHLines(ax)

	if graphFileName != None:
            saveGraph(fig, dir, graphFileName)

# 	plt.close('all')
# 	plt.clf()
# 	del fig
# 	del ax
	
# 	gc.collect()
	# showFig(fig, ax)
        return n, edges, patches
    
# def highFilter(list, val):
#    return [x for x in list if x<=val ]

    
def paintHistogramMulti(name, valuesDic, bins=0, dir="", graphFileName="", labelX="Series", labelY="Value", \
                   logScale=False, special="", fontsizeX=12, fontsizeY=13, fontsizeY2=13, \
                    normed=False, cumulative=False, cumulativePlot=True,
                    filterCut=0, multiFilter=None, excludeTag=None, xLogScale=False, \
                        xLim=None, onlyCumulative=False, xLogTicks=None, xLogMin=None):
    """
    绘制多组数据的直方图及累积分布图
    参数：
    name (str): 图表标题
    valuesDic (dict): 数据字典，格式为{标签: 值列表}
    bins (int): 直方图分箱数，0表示自动计算
    dir (str): 图表保存目录
    graphFileName (str): 图表文件名
    labelX (str): X轴标签
    labelY (str): Y轴标签
    logScale (bool): 是否使用对数Y轴
    fontsizeX (int): X轴标签字体大小
    fontsizeY (int): Y轴标签字体大小
    fontsizeY2 (int): 次Y轴标签字体大小
    normed (bool): 是否标准化为百分比
    cumulative (bool): 是否绘制累积直方图
    cumulativePlot (bool): 是否绘制累积分布曲线
    filterCut (int): 数值过滤阈值
    multiFilter (float): 多组数据统一过滤阈值
    excludeTag (str): 需要排除的标签关键字
    xLogScale (bool): 是否使用对数X轴
    xLim (float): X轴显示范围
    onlyCumulative (bool): 是否仅显示累积分布
    xLogTicks (list): 对数X轴刻度值
    xLogMin (float): 对数X轴最小值
    返回值：
    tuple: (直方图计数数组, 分箱边界数组, 图形对象列表)
    """

    if multiFilter != None:
        for key in valuesDic.keys():
#            for (limit, index) in zip(multiFilter, range(len(multiFilter))):
#                if (limit!=0):
                     valuesDic[key] = [x for x in valuesDic[key] if x <= multiFilter]
                    
               
 #   print valuesDic
                
        

    if (bins == 0):
        bins = calculateBins(values)


    if normed and labelY == "Value":
        labelY = "%"
    fig = plt.figure(name)
    setTitle(fig, name, fontsize=fontsizeX + 2)
    adjustMargin(fig, top=0.88, bottom=0.18)

    ax = fig.add_subplot(111)
    
    if (logScale):
        ax.set_yscale('log')
    if (xLogScale):
        ax.set_xscale('log')
        if (xLogTicks!=None):
            ax.set_xticks(xLogTicks)
            ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    
    if cumulativePlot:
      if (onlyCumulative):
        ax2 = ax
      else:
        ax2 = ax.twinx()
        
      ax2.set_yscale("linear")

    m = 0
    mMin = None
    superMax = 0
    for key in valuesDic.keys():
        superMax = max(m, max(valuesDic[key],))
        if (filterCut != 0):
            m = max(m, max(highFilter(valuesDic[key], filterCut)))
            minTemp = min(highFilter(valuesDic[key], filterCut))
        else:
            m = max(m, max(valuesDic[key]))
            minTemp = min(valuesDic[key])
        if mMin == None:
            mMin = minTemp
        else:
            mMin = min(minTemp, mMin)
    numBins=bins
    if not xLogScale:
        bins = np.linspace(0, m, bins)
    else:
        print "LOGSPACE", mMin, m, bins
        #bins = np.logspace(0.1, log(float(m)), bins)
        if xLogMin == None:
            xLogMin = 1
        bins = np.logspace(log(xLogMin), log(float(m)), bins, endpoint=False)
        #bins = np.logspace(0.1, 1.0, bins)

    #print "BINS", bins
    # n, edges, patches  =ax.hist(values, bins=bins, log=logScale, normed=normed)
    
    minCum = None
    maxCum = 0
    line_styles = ["-","--","-.",":", 'steps']
    marker_styles = [ '+' , ',' , '.' , '1' , '2' , '3' , '4' ]

    for (key, line_style, marker) in zip(valuesDic.keys(),
                                         line_styles, marker_styles):
        values = valuesDic[key]
        weights = None
        total = sum(values)

        if (filterCut != 0):
            filterValues = highFilter(values, filterCut)
            # values=filterValues
        else:
            filterValues = values
        if (normed):
            weights = np.zeros_like(filterValues) + 100. / len(values)

        if (excludeTag == None or excludeTag not in key): 
          if not onlyCumulative:   
            n, edges, patches = ax.hist(filterValues, bins=bins,
                                        log=logScale, weights=weights,
                                        cumulative=cumulative, alpha=0.5,
                                        label=key)
          else:
            patches = []
        # n, edges, patches  =pyplot.hist(filterValues, bins=bins, alpha=0.5, label=key,weights=weights, cumulative=cumulative, log=logScale)

        # ax.set_ylim(min(n),max(n))
            
        if cumulativePlot:
            nBins = list(bins)
            if (superMax>nBins[-1]):
                nBins.append(superMax)
            if xLogScale:
                nBins.insert(0, 0)
            print "BINS2", nBins
#             if (not xLogScale):
#                 nBins.append(superMax)
#             else:
#                 nBins=numBins
            n, edges = np.histogram(valuesDic[key], nBins)
            print "last", n[-1]
            cumulativeArray = np.cumsum(n, dtype=float)
            cumulativeArray /= cumulativeArray[-1]
            minTemp = min(cumulativeArray)
            maxTemp = max(cumulativeArray)
            if (minCum == None):
                minCum = minTemp
            else:
                minUCum = min(minCum, minTemp)
            
            maxCum = max(maxCum, maxTemp)
            
           # print cumulativeArray
            ax2.plot(edges[:-2], cumulativeArray[:-1], label="CDF " + key, 
                     ls = line_style, marker = marker)
    
    

    
    
    if (cumulativePlot):
        adjustLabels(ax2, "", "Cumulative %", fontsizeX=fontsizeX, fontsizeY=fontsizeY)
        adjustYTickers(ax2, fontsize=fontsizeY, fontsize2=fontsizeY2)
        legend = plt.legend((["Cumulative %"]))
        ax2.set_ylim(minCum, maxCum)
        # if (normed):
         #   ax2.set_ylym=[0,1]
    if (xLim != None):
        ax.set_xlim(0, xLim)
        if cumulativePlot:
            ax2.set_xlim(0, xLim)
    if onlyCumulative:
      labelY=""
    adjustLabels(ax, labelX, labelY, fontsizeX=fontsizeX, fontsizeY=fontsizeY)
    adjustXTickers(ax, rotation=45, fontsize=fontsizeY)
    adjustYTickers(ax, fontsize=fontsizeY)
    # ax.set_ylim(0, m)
    fontP = FontProperties()
    fontP.set_size('small')
    if not onlyCumulative:
      legend = ax.legend(loc=1, prop=fontP)
    legen2 = ax2.legend(loc=4, prop=fontP)
    addHLines(ax)
    # if (logScale):
    #    print "MIN, MAX:", mMin, m
    #    ax.set_ylim=[mMin, m]

    if graphFileName != None:
        saveGraph(fig, dir, graphFileName)


    return n, edges, patches

def to_percent(y, position):
    # Ignore the passed in position. This has the effect of scaling the default
    # tick locations.
    s = str(100 * y)

    # The percent symbol needs escaping in latex
    if matplotlib.rcParams['text.usetex'] == True:
        return s + r'$\%$'
    else:
        return s + '%'

def createDicFromLists(keys, values):
    dic = {}
    for (k, v) in zip(keys, values):
        dic[k] = v
    return dic
def r_squared(actual, ideal):
    actual_mean = np.mean(actual)
    ideal_dev = np.sum([(val - actual_mean) ** 2 for val in ideal])
    actual_dev = np.sum([(val - actual_mean) ** 2 for val in actual])

    return ideal_dev / actual_dev
def doTrendLine(series, values):
    xValues = np.array(range(1, len(values) + 1))
    values = np.array(values)
    print len(values)
    print len(xValues)
    slope, intercept = np.polyfit(xValues, values, 1)
    ideal_values = intercept + (slope * xValues)
    r_sq = r_squared(values, ideal_values)

    fit_label = 'Linear fit ({0:.2f})'.format(slope)
    print len(xValues), len(ideal_values)
    plt.plot(xValues, ideal_values, color='red', linestyle='--', label=fit_label)
    plt.annotate('r^2 = {0:.2f}'.format(r_sq), (0.05, 0.9), xycoords='axes fraction')
    plt.legend(loc='lower right')
	

def paintBars(name, dic, dir="", graphFileName="", labelX="Series", labelY="Value", logScale=False, trend=False):
        series, values = splitDic(dic)
        paintBarsSimple(name, series, values, dir=dir, graphFileName=graphFileName, labelX=labelX, labelY=labelY, logScale=logScale, trend=trend)
        
def paintBarsSeries(name, seriesNames, dic, dir="", graphFileName="", labelX="Series", labelY="Value", logScale=False, trend=False, \
                    rotateXLabels=False, fontsizeX=12):
        valuesDic = {}
        for s in seriesNames:
            series, values = splitDic(dic[s])
            valuesDic[s] = values
        
            
#        aintBarsSimpleSeries(name, seriesNames, subSeriesNames, valuesDic, dir="", graphFileName="", labelX="Series", labelY="Value", logScale=False, trend=False):
        paintBarsSimpleSeries(name, seriesNames, series, valuesDic, dir=dir, graphFileName=graphFileName, labelX=labelX, labelY=labelY, \
                            logScale=logScale, trend=trend, rotateXLabels=rotateXLabels, fontsizeX=fontsizeX)
        

def paintBarsSimple(name, series, values, dir="", graphFileName="", labelX="Series", labelY="Value", logScale=False, trend=False):
      


     #   print "Doing Bars, Series:"+str((series))
	
	fig = plt.figure(name)
	setTitle(fig, name)
	adjustMargin(fig)
		

	ax = fig.add_subplot(111)
        
        edges = np.arange(len(values))
        width = 1.0 / float(len(edges))
        if (logScale):
            ax.set_yscale('log')
        print edges, values, len(edges), len(values)
        ax.bar(edges, values, log=logScale)
	adjustLabels(ax, labelX, labelY)
        ax.set_xticks(edges + 0.5)
        ax.set_xticklabels(series)
        # plt.xticks(series)
        adjustXTickers(ax)
	# adjustYTickers(ax)
	addHLines(ax)

	if trend:
            doTrendLine(series, values)

	
       # plt.close()
	saveGraph(fig, dir, graphFileName)



def reDoTags(wordList):
    return ["\n".join(word.split("_")) for word in wordList]

def paintBarsSimpleMultiSplitGraph(name, seriesDic, valuesDic, dir="", graphFileName="", \
                    labelX="Series", labelY="Value", logScale=False, trend=False,
                    seriesNames=None, rotateXLabels=False, fontsizeX=12, interPlotSpace=None, fontsizeY=12,
                    sortingFunctionSubSeries=None, showLegend=True,
                    legendFontSize=8):
      


     #   print "Doing Bars, Series:"+str((series))

    allGraphManes = seriesDic.keys()
    if (seriesNames != None):
        allGraphManes = seriesNames

    fig, axes = plt.subplots(nrows=len(allGraphManes))
    setTitle(fig, name)
    adjustMargin(fig)

    
    print "PAINT ordering:", allGraphManes

    for (ax, graphName, graphCount) in zip(axes, allGraphManes, range(len(allGraphManes))):
        seriesNames = (seriesDic[graphName])
        factor = 0.9 / len(seriesNames)
        colors = cm.rainbow(np.linspace(0, 1, len(seriesNames)))
        
        subSeriesNames = sorted(valuesDic[graphName][seriesNames[0]].keys())
        if sortingFunctionSubSeries!=None:
            subSeriesNames = [x.replace("\\n", "_") for x in subSeriesNames]
            print subSeriesNames
            subSeriesNames = sortingFunctionSubSeries(subSeriesNames, graphName)
          #  subSeriesNames = [x.replace("_", "\n") for x in subSeriesNames]
            print subSeriesNames
            
        
        edges = np.arange(len(subSeriesNames))
        ax.set_xticklabels(reDoTags(subSeriesNames))
        ax.set_xticks(edges + 0.5)
        tempLabel = graphName
        if (graphCount == 1):
            tempLabel = labelY + "\n" + graphName    
        adjustLabels(ax, labelX, tempLabel)
        if (rotateXLabels):
            adjustXTickers(ax, rotation=45, fontsize=fontsizeX)
        else:
            adjustXTickers(ax, fontsize=fontsizeX)
        adjustYTickers(ax, fontsize=fontsizeY)

        if (logScale):
            ax.set_yscale('log')
        

        # width=1.0/float(len(edges))
        width = factor
        displace = 0.0
       
        for (series, c) in zip(seriesNames, colors):

            values = [valuesDic[graphName][series][x] for x in subSeriesNames]
            print series, edges, width, values
            ax.bar(edges + displace, values, log=logScale, width=width, color=c, label=series)
            displace += width
#            if trend:
#                doTrendLine(subSeriesNames, values)
        
       # legend = plt.legend(seriesNames, bbox_to_anchor=(0.9+displace, 1), loc=2, borderaxespad=0., labelspacing=0.1, prop={'size':fontsizeX-2})
        if (showLegend):
            legend = ax.legend(prop={"size":legendFontSize})
        
        # addHLines(ax)
    if (interPlotSpace != None):
        plt.subplots_adjust(hspace=interPlotSpace)
    # plt.close()
    saveGraph(fig, dir, graphFileName)
        
def paintBarsSimpleSeries(name, seriesNames,
                          subSeriesNames,
                          valuesDic,
                          dir="", graphFileName="",
                          labelX="Series", 
                          labelY="Value", 
                          logScale=False, trend=False,
                          rotateXLabels=False,
                          fontsizeX=12):
      

    print seriesNames
    print subSeriesNames
    factor = 0.9 / len(seriesNames)
    colors = cm.rainbow(np.linspace(0, 1, len(seriesNames)))
     #   print "Doing Bars, Series:"+str((series))
    
    fig = plt.figure(name)
    setTitle(fig, name)
    adjustMargin(fig)
        
        
    
    ax = fig.add_subplot(111)
        
    edges = np.arange(len(subSeriesNames))
    ax.set_xticklabels(subSeriesNames)
    ax.set_xticks(edges + 0.5)
    adjustLabels(ax, labelX, labelY)
    if (rotateXLabels):
        adjustXTickers(ax, rotation=45, fontsize=fontsizeX)
    else:
        adjustXTickers(ax, fontsize=fontsizeX)
    if (logScale):
        ax.set_yscale('log')
    

    # width=1.0/float(len(edges))
    width = factor
    displace = 0
   
    for (series, c) in zip(seriesNames, colors):
        values = valuesDic[series]
    
        ax.bar(edges + displace, values, log=logScale, 
               width=width, color=c, label=series)
        displace += width
#            if trend:
#                doTrendLine(subSeriesNames, values)
    
    # legend = plt.legend(seriesNames, bbox_to_anchor=(0.9+displace, 1), loc=2, borderaxespad=0., labelspacing=0.1, prop={'size':fontsizeX-2})
    legend = ax.legend()
    addHLines(ax)
        
    saveGraph(fig, dir, graphFileName)
    
def paintBarsHistogram(name,
                      series_names,
                      edges,
                      histograms_value_dictionary,
                      target_folder="",
                      file_name="",
                      labelX="Values", 
                      labelY="", 
                      x_log_scale=False,
                      y_log_scale=False,
                      cdf_y_log_scale=False,
                      cdf=False, only_cdf=False,
                      rotateXLabels=False,
                      fontsizeX=12,
                      min_max=None,
                      cdf_min_max=None):
    """绘制带有可选累积分布函数(CDF)的直方图，并保存为PNG文件
    所有直方图必须具有相同的边界。支持对数坐标轴和自定义坐标范围，
    可配置多系列数据的颜色、线型和标记样式。
    Args:
        name (str): 图表标题名称
        series_names (list[str]): 数据系列名称列表，决定绘制顺序
        edges (list[float]): 所有直方图共用的边界值列表
        histograms_value_dictionary (dict[list]): 直方图数据字典，键为系列名，值为对应bin的计数值列表
        target_folder (str, optional): 输出文件存储路径，默认当前目录
        file_name (str, optional): 输出文件名(自动追加.png后缀)
        labelX (str, optional): X轴标签，默认"Values"
        labelY (str, optional): Y轴标签
        x_log_scale (bool, optional): X轴是否使用对数刻度
        y_log_scale (bool, optional): 直方图Y轴是否使用对数刻度
        cdf_y_log_scale (bool, optional): CDF的Y轴是否使用对数刻度
        cdf (bool, optional): 是否绘制累积分布函数
        only_cdf (bool, optional): 是否仅绘制CDF不显示直方图
        rotateXLabels (bool, optional): 是否旋转X轴标签
        fontsizeX (int, optional): X轴标签字体大小，默认12
        min_max (tuple, optional): 直方图Y轴范围控制(None,None)/(min,None)/(None,max)/(min,max)
        cdf_min_max (tuple, optional): CDF的Y轴范围控制，格式同min_max
    Returns:
        None: 无返回值，直接生成并保存图表文件
    """
    # 转换边界值为numpy数组并计算数据系列数量
    edges=np.array(edges)
    num_series= len(series_names)
    
    colors = cm.rainbow(np.linspace(0, 1,num_series))
    line_styles = ["-","--","-.",":", 'steps']
    marker_styles = [ '+' , ',' , '.' , '1' , '2' , '3' , '4' ]
    
    """General figure configuration"""
    fig = plt.figure(name)
    setTitle(fig, name)
    adjustMargin(fig)
    ax = fig.add_subplot(111)
    
    """Configure x axis ticks"""
    ax.set_xticklabels([str(e) for e in edges])
    ax.set_xticks(edges)
    
    """Bar width for the case in which the edges are not uniform"""
    bars_width = [(float(x2-edges[0])-float(x1-edges[0])) 
                              for (x1, x2) in zip(edges[:-1], edges[1:])]
    bars_width=np.array(bars_width)
    bars_width/=num_series
    
    """Adjust Labels"""
    adjustLabels(ax, labelX, labelY)
    if (rotateXLabels):
        adjustXTickers(ax, rotation=45, fontsize=fontsizeX)
    else:
        adjustXTickers(ax, fontsize=fontsizeX)
        
    if (x_log_scale):
        ax.set_xscale('log')
    if (y_log_scale):
        ax.set_yscale('log')

    displace = 0
    if cdf:
        if (only_cdf):
            ax2 = ax        
        else:
            ax2 = ax.twinx()        
    min_value=0
    for (series, c, line_style, marker_styles) in zip(series_names, colors,
                                                      line_styles, marker_styles
                                                      ):
        values = histograms_value_dictionary[series]
        ax.bar(edges[:-1]+displace*bars_width, values, log=y_log_scale,
               width=bars_width,
               color=c, label=series)
        min_value=min(min_value, min(values))
        if cdf:
            cumulative_values = np.cumsum(values, dtype=float)/sum(values)
            ax2.plot(edges[:-1]+((bars_width)/2)*num_series,
                     cumulative_values, label="CDF " + series, 
                     ls = line_style, marker = marker_styles)
            min_value=min(min_value, min(cumulative_values))
        displace += 1
    
    if cdf and cdf_y_log_scale:
            ax2.set_yscale('log')
    if y_log_scale:
        ax.set_yscale('log')
    
    if min_max is not None:
        if min_max[0] is not None:
            ax.set_ylim(bottom=min_max[0])
        if min_max[1] is not None:
            ax.set_ylim(top=min_max[1])
    else:
        if y_log_scale:
            ax.set_ylim(min_value)
            ax.set_yscale('log')
        else:
            ax.set_ylim(0)
    if cdf and cdf_min_max is not None:
        if cdf_min_max[0] is not None:
            ax2.set_ylim(bottom=cdf_min_max[0])
        if cdf_min_max[1] is not None:
            ax2.set_ylim(top=cdf_min_max[1])
    elif cdf:
        if cdf_y_log_scale:
            ax.set_ylim(min_value)
        else:
            ax2.set_ylim(0)
    if num_series>1:
        ax.legend()
    addHLines(ax)    
    saveGraph(fig, target_folder, file_name)
        
def addHRefLines(lines):
    for line in lines:
        axhline(line)
        
        
        
        
def paintPlotMulti(name, series, valuesDic, dir="", graphFileName="", labelX="Series", \
                   labelY="Value", logScale=False, trend=False, \
                    tickFrequence=None, hLines=[], xLim=None):
      


     #   print "Doing Bars, Series:"+str((series))
	
	fig = plt.figure(name)
	setTitle(fig, name)
	adjustMargin(fig)
		
	
	ax = fig.add_subplot(111)
        
        # edges=np.arange(len(values))
        # width=1.0/float(len(edges))
        if (logScale):
            ax.set_yscale('log')
        
       
        
        if tickFrequence != None:
            series, minXValue, maxXValue = tansformEpochToDates(series, -1, 0)
            xAxisDates(ax, minXValue, maxXValue, tickFrequence)

       
        # if (dateLabels):
        #    plt.plot_date(x,y)
        
    
           
        addHRefLines(hLines)
        
        
        for valuesKey in valuesDic.keys():
            
            
  
            ax.plot(series, valuesDic[valuesKey], label=valuesKey)

        ax.legend()
        if (xLim != None):
            ax.set_xlim(xLim)
	adjustLabels(ax, labelX, labelY)
        
        # ax.set_xticklabels(series)
        # plt.xticks(series)
        adjustXTickers(ax)
	# adjustYTickers(ax)
	# addHLines(ax)
	

	saveGraph(fig, dir, graphFileName)
    
# def paintPlotMultiV2(name, seriesDic, valuesDic, dir="", graphFileName="", labelX="Series", \
#                   labelY="Value", logScale=False, trend=False, \
#                    tickFrequence=None, hLines=[], xLim=None):
      


#     #   print "Doing Bars, Series:"+str((series))
	
# 	fig = plt.figure(name)
# 	setTitle(fig, name)
# 	adjustMargin(fig)
		
	
# 	ax = fig.add_subplot(111)
        
#        #edges=np.arange(len(values))
#        #width=1.0/float(len(edges))
#        if (logScale):
#            ax.set_yscale('log')
        
       
        

       
#        #if (dateLabels):
#        #    plt.plot_date(x,y)
        
    
           
#        addHRefLines(hLines)
        
        
#        for (valuesKey) in valuesDic.keys():
#            series=seriesDic[valuesKey]
            
#            if tickFrequence!=None:
#                series, minXValue, maxXValue= tansformEpochToDates(series, -1, 0)
#                xAxisDates(ax, minXValue, maxXValue, tickFrequence)
#            print len(series), len(valuesDic[valuesKey])
#            ax.plot(series, valuesDic[valuesKey], label=valuesKey)

#        ax.legend()
#        if (xLim!=None):
#            ax.set_xlim(xLim)
# 	adjustLabels(ax, labelX, labelY)
        
#        #ax.set_xticklabels(series)
#        #plt.xticks(series)
#        adjustXTickers(ax)
# 	#adjustYTickers(ax)
# 	#addHLines(ax)
	

# 	saveGraph(fig, dir, graphFileName)
        
def paintPlotMultiV2(name, seriesDic, valuesDic, dir="", graphFileName="", xLim=None, \
                      labelX="Series", labelY="Value", \
                      logScale=False, trend=False, tickFrequence=None, hLines=[], \
                          xLogScale=False, alpha=1.0, legendLoc=0, \
                              fontSizeX=12, yLim=None):
      
   
    fig = plt.figure(name)
    ax = fig.add_subplot(111)
   

   
    setTitle(fig, name)
    adjustMargin(fig)
    
        
    if (logScale):
        ax.set_yscale('log')

    if (xLogScale):
        ax.set_xscale('log')
        
    
    addHRefLines(hLines)

    minXValue = None
    maxXValue = 0

    
    for valuesKey in sorted(valuesDic.keys()):
        series = seriesDic[valuesKey]
        if tickFrequence != None:
            series = seriesDic[valuesKey]
            series, minXValueTemp, maxXValueTemp = tansformEpochToDates(series, -1, 0)
            if minXValue == None:
                minXValue = minXValueTemp
            else:
                minXValue = min(minXValue, minXValueTemp)
            maxXValue = max(maxXValue, maxXValueTemp)
   
        #print series, valuesDic[valuesKey]
        ax.plot(series, valuesDic[valuesKey], label=valuesKey, alpha=alpha)
   
    if tickFrequence != None:
        xAxisDates(ax, minXValue, maxXValue, tickFrequence)
        
    if (xLim != None):
        ax.set_xlim(xLim)
    if (yLim!=None):
        ax.set_ylim(yLim)
    ax.legend(loc=legendLoc, prop={"size":fontSizeX})
    adjustLabels(ax, labelX, labelY)
    
   

    # ax.set_xticklabels(series)
    # plt.xticks(series)
    adjustXTickers(ax)
    # adjustYTickers(ax)
    # addHLines(ax)




    
    saveGraph(fig, dir, graphFileName)

       
def paintPlotMultiV2_axis(name, seriesDic, valuesDic, dir="", graphFileName="", xLim=None, \
                      labelX="Series", labelY="Value", \
                      logScale=False, trend=False, tickFrequence=None, hLines=[], \
                          xLogScale=False, alpha=1.0, legendLoc=0, \
                              fontSizeX=12,
                         second_axis_series_dic=None,
                         second_axis_values_dic=None):
      
   
    fig = plt.figure(name)
    ax = fig.add_subplot(111)
   

   
    setTitle(fig, name)
    adjustMargin(fig)
    
        
    if (logScale):
        ax.set_yscale('log')

    if (xLogScale):
        ax.set_xscale('log')
        
    
    addHRefLines(hLines)

    minXValue = None
    maxXValue = 0

    
    for valuesKey in sorted(valuesDic.keys()):
        series = seriesDic[valuesKey]
        if tickFrequence != None:
            series = seriesDic[valuesKey]
            series, minXValueTemp, maxXValueTemp = tansformEpochToDates(series, -1, 0)
            if minXValue == None:
                minXValue = minXValueTemp
            else:
                minXValue = min(minXValue, minXValueTemp)
            maxXValue = max(maxXValue, maxXValueTemp)
   
        #print series, valuesDic[valuesKey]
        ax.plot(series, valuesDic[valuesKey], label=valuesKey, alpha=alpha)
   
    if tickFrequence != None:
        xAxisDates(ax, minXValue, maxXValue, tickFrequence)
        
    if (xLim != None):
        ax.set_xlim(xLim)
    ax.legend(loc=legendLoc, prop={"size":fontSizeX})
       
    adjustLabels(ax, labelX, labelY)
    
    if second_axis_series_dic and second_axis_values_dic:
        ax2 = ax.twinx()
        ax2.set_yscale("linear")
        if (xLogScale):
            ax2.set_xscale('log')
        if (xLim != None):
            ax2.set_xlim(xLim)
        for valuesKey in sorted(second_axis_series_dic.keys()):
            series = second_axis_series_dic[valuesKey]
            values = second_axis_values_dic[valuesKey]
            ax2.plot(series, values, label=valuesKey, alpha=alpha, marker="+",
                     ls=":")
        ax2.legend(loc=2)
   

    # ax.set_xticklabels(series)
    # plt.xticks(series)
    adjustXTickers(ax)
    # adjustYTickers(ax)
    # addHLines(ax)




    
    saveGraph(fig, dir, graphFileName)


def isInSeriesOwnGraphs(key, list):
    """
    检查目标字符串是否包含列表中任意一个子字符串
    参数:
        key (str): 待检查的目标字符串
        list (list[str]|None): 包含候选子字符串的列表，允许传入空值

    返回值:
        bool: 如果列表中任意一个子字符串存在于目标字符串中返回True，否则返回False
    """
    if (list == None):
        return False
    for l in list:
        if l in key:
            return True
    return False

def getSubAxe(key, list):
    for (l, i) in zip(list, range(len(list))):
        if l in key:
            return i
    return 0

def paintPlotMultiV3(name, seriesDic, valuesDic, dir="", graphFileName="", xLim=None, labelX="Series", labelY="Value", \
                      logScale=False, trend=False, tickFrequence=None, hLines=[], \
                          xLogScale=False, alpha=1.0, seriesOwnGraphs=None, \
                        ownGraphsLabels=None, legendLocation=1, yLim=None):
      
    subAxes = None
    if (seriesOwnGraphs != None and len(seriesOwnGraphs) > 0):
        print "HEY", len(seriesOwnGraphs), len(valuesDic.keys())
        if all([isInSeriesOwnGraphs(theKey, seriesOwnGraphs) for theKey in seriesDic.keys()]):
            fig, axes = plt.subplots(nrows=len(seriesOwnGraphs) )
            subAxes = list(axes)[0:]
            ax = list(axes)[0]
        else: 
            fig, axes = plt.subplots(nrows=len(seriesOwnGraphs) + 1)
            ax = list(axes)[0]
            subAxes = list(axes)[1:]
    else:
        fig = plt.figure(name)
        ax = fig.add_subplot(111)
   

   
    setTitle(fig, name)
    adjustMargin(fig)
    
        
    if (logScale):
        ax.set_yscale('log')

    if (xLogScale):
        ax.set_xscale('log')
        
    
    addHRefLines(hLines)

    minXValue = None
    maxXValue = 0
    # subAx=0
    
    for valuesKey in sort(valuesDic.keys()):
        series = seriesDic[valuesKey]
        if tickFrequence != None:
            series = seriesDic[valuesKey]
            series, minXValueTemp, maxXValueTemp = tansformEpochToDates(series, -1, 0)
        if isInSeriesOwnGraphs(valuesKey, seriesOwnGraphs):
            subAx = getSubAxe(valuesKey, seriesOwnGraphs)
        # if (valuesKey in seriesOwnGraphs):
            subAxes[subAx].plot(series, valuesDic[valuesKey], label=valuesKey, alpha=alpha)
            if (logScale):
                subAxes[subAx].set_yscale('log')
            if tickFrequence != None:
                xAxisDates(subAxes[subAx], minXValueTemp, maxXValueTemp, tickFrequence)
            tL = labelX
            if isInSeriesOwnGraphs(valuesKey, seriesOwnGraphs):
            # if ownGraphsLabels!=None and valuesKey in ownGraphsLabels.keys():
                tL = ownGraphsLabels[seriesOwnGraphs[subAx]]
            adjustLabels(subAxes[subAx], tL, labelY, fontsizeX=10)
            # subAx+=1
            
        else:
            if tickFrequence != None:    
                if minXValue == None:
                    minXValue = minXValueTemp
                else:
                    minXValue = min(minXValue, minXValueTemp)
                maxXValue = max(maxXValue, maxXValueTemp)
            ax.plot(series, valuesDic[valuesKey], label=valuesKey, alpha=alpha)
            ax.legend(loc=legendLocation, prop={'size':8})
            adjustLabels(ax, labelX, labelY, fontsizeX=10)
   
    if tickFrequence != None and minXValue != None:
        
        xAxisDates(ax, minXValue, maxXValue, tickFrequence)
        
    if (xLim != None):
        ax.set_xlim(xLim)
    if (yLim != None):
        ax.set_ylim(yLim)


    
   

    # ax.set_xticklabels(series)
    # plt.xticks(series)
    adjustXTickers(ax)
    # adjustYTickers(ax)
    # addHLines(ax)
    if (subAxes != None):

        for ax in subAxes:

            if (xLim != None):
                ax.set_xlim(xLim)
            ax.legend(loc=legendLocation, prop={'size':8})
            if (yLim != None):
                ax.set_ylim(yLim)
            adjustXTickers(ax)
    
    saveGraph(fig, dir, graphFileName)
    
    
    
    
    
    
    
    
    
    
    
def getValueForGraph(field, graph):
    if (field == None):
        return field
    if (type(field) is dict and graph in field.keys()):
        return field[graph]
    elif (type(field) is list):
        outF = []
        for f in field:
            outF.append(getValueForGraph(f, graph))
        return outF
    return field

def unpackValues(fieldList, graph):

    outFields = []
    for field in fieldList:
#        print "UP", field, graph
#        exit()
        outFields.append(getValueForGraph(field, graph))
    return tuple(outFields)

def createAxesDic(axes, keys):
    dic = {}
    for (ax, key) in zip(axes, keys):
        dic[key] = ax
    return dic

def createLegend(ax, loc=1, fontSize=9):
    return ax.legend(loc=loc, prop={'size':fontSize})


def doPlotSubGraphList(ax, seriesDicList, seriesOrder, tickFrequence=None, \
                   xLogScale=False, yLogScale=False, alpha=1.0, \
                    xLim=None, yLim=None):
        
        xSeriesDic = seriesDicList[0]
        ySeriesDic = seriesDicList[1]
        minXValue = None
        maxXValue = 0
        
        plotDic = {}
        for seriesName in seriesOrder:
            xValues = xSeriesDic[seriesName]
            yValues = ySeriesDic[seriesName]
            
           
            if tickFrequence != None:             
                xValuesTransform, minXValueTemp, maxXValueTemp = tansformEpochToDates(xValues, -1, 0)
                if(minXValue == None):
                    minXValue = minXValueTemp
                else:
                    minXValue = min(minXValue, minXValueTemp)
                maxXValue = max(maxXValue, maxXValueTemp)
            else:
                xValuesTransform = xValues
            # ax.xaxis.set_major_locator(MaxNLocator(5))
            plotDic[seriesName] = ax.plot(xValuesTransform, yValues, label=seriesName, alpha=alpha)
           
        if tickFrequence != None:
       
            xAxisDates(ax, minXValue, maxXValue, tickFrequence)
        
            
        if (xLim != None):
                ax.set_xlim(xLim)
        
        if (yLim != None):
                ax.set_ylim(yLim)
        return plotDic

# def doPlotSubGraph(ax, xSeriesDic, ySeriesDic, seriesOrder, tickFrequence=None, \
#                   xLogScale=False,  yLogScale=False, alpha=1.0, \
#                    xLim=None, yLim=None):
                    
#        minXValue=None
#        maxXValue=0
        
#        plotDic={}
#        for seriesName in seriesOrder:
#            xValues=xSeriesDic[seriesName]
#            yValues=ySeriesDic[seriesName]
            
           
#            if tickFrequence!=None:             
#                xValuesTransform, minXValueTemp, maxXValueTemp= tansformEpochToDates(xValues, -1, 0)
#                if(minXValue==None):
#                    minXValue=minXValueTemp
#                else:
#                    minXValue=min(minXValue, minXValueTemp)
#                maxXValue=max(maxXValue, maxXValueTemp)
#            else:
#                xValuesTransform=xValues
#          #  print xValuesTransform, yValues
#            plotDic[seriesName]=ax.plot(xValuesTransform, yValues, label=seriesName, alpha=alpha)
            
#        if tickFrequence!=None:
#            xAxisDates(ax, minXValue, maxXValue, tickFrequence)
            
#        if (xLim!=None):
#                ax.set_xlim(xLim)
        
#        if (yLim!=None):
#                ax.set_ylim(yLim)
#        return plotDic

def doBoxPlotSubGraphList(ax, seriesDicList, seriesOrder, tickFrequence=None, \
                   xLogScale=False, yLogScale=False, alpha=1.0, \
                    xLim=None, yLim=None):
        
        seriesDic = seriesDicList[0]
                    
        minXValue = None
        maxXValue = 0
        
        plotDic = {}
        boxPlotValues = []
       
        for seriesName in seriesOrder:
            boxPlotValues.append(seriesDic[seriesName])
            
           
          #  print xValuesTransform, yValues
        plotDic[seriesName] = ax.boxplot(boxPlotValues)
        xtickNames = plt.setp(ax, xticklabels=seriesOrder)
#        if tickFrequence!=None:
#            xAxisDates(ax, minXValue, maxXValue, tickFrequence)
            
#        if (xLim!=None):
#                ax.set_xlim(xLim)
        
        if (yLim != None):
                ax.set_ylim(yLim)
        return plotDic

# def doBoxPlotSubGraph(ax, seriesDic, seriesOrder, tickFrequence=None, \
#                   xLogScale=False,  yLogScale=False, alpha=1.0, \
#                    xLim=None, yLim=None):
                    
#        minXValue=None
#        maxXValue=0
        
#        plotDic={}
#        boxPlotValues=[]
       
#        for seriesName in seriesOrder:
#            boxPlotValues.append(seriesDic[seriesName])
            
           
#          #  print xValuesTransform, yValues
#        plotDic[seriesName]=ax.boxplot(boxPlotValues)
#        xtickNames = plt.setp(ax, xticklabels=seriesOrder)
# #        if tickFrequence!=None:
# #            xAxisDates(ax, minXValue, maxXValue, tickFrequence)
            
# #        if (xLim!=None):
# #                ax.set_xlim(xLim)
        
#        if (yLim!=None):
#                ax.set_ylim(yLim)
#        return plotDic

def doAxisAdjustments(ax, labelXSeries, labelYSeries, xFontSize, yFontSize, \
                      xLogScaleSeries=False, yLogScaleSeries=False, \
                      rotation="vertical", graphName="", \
                          xScalar=True, yScalar=True, \
                            scientificX=False, scientificY=False):
        
        if (xLogScaleSeries):
            ax.set_xscale('log')
        if (yLogScaleSeries):
            ax.set_yscale('log')
        if (xScalar):
            form = matplotlib.ticker.ScalarFormatter()
            ax.get_xaxis().set_major_formatter(form)
        ax.get_xaxis().get_major_formatter().set_useOffset(scientificX)
        if (yScalar):
            form = matplotlib.ticker.ScalarFormatter()
            ax.get_yaxis().set_major_formatter(form)
        ax.get_yaxis().get_major_formatter().set_useOffset(scientificY)
        if graphName != "":
            labelYSeries += "\n" + graphName

        adjustLabels(ax, labelXSeries, labelYSeries, \
                     fontsizeX=xFontSize, fontsizeY=yFontSize)
        adjustXTickers(ax, rotation=rotation, fontsize=xFontSize)
        
def adjustMarginDic(fig, margins, numGraphs):
    if margins == None:
        margins = {}

    for a in ["top", "bottom", "left", "right", "hspace", "vspace"]:
        if (not a in margins.keys()):
            margins[a] = ""
    if (numGraphs > 1 and  margins["hspace"] == ""):
         margins["hspace"] = 0.5
    adjustMargin(fig, margins["top"], margins["bottom"], \
                     margins["right"], margins["left"], \
                    margins["hspace"], margins["vspace"])
                     

def doFigOperatios(name, numGraphs, graphOrder, margins=None):
    fig = plt.figure(name)
    
    

    fig, axesList = plt.subplots(numGraphs)
    if not (type(axesList) is np.ndarray):
        axesList = [axesList]
    else:
        axesList = axesList.tolist()
#    if not type(axesList) is list:
#        axesList=[axesList]
#    else:
#        axiesList=list(axesList)
    axesDic = createAxesDic(axesList, graphOrder)
    setTitle(fig, name)
    adjustMarginDic(fig, margins, numGraphs=numGraphs)
    
    return fig, axesDic
    

def preProcessInputGraphs(dicOfDicX, dicOfDicY, \
                    graphOrder=None, dicOfListSeriesOrder=None):
    defaultKey = ""
    
    if not type (dicOfDicX.values()[0]) is dict:
        dicOfDicX = {defaultKey: dicOfDicX}
        dicOfDicY = {defaultKey: dicOfDicY}
        if (dicOfListSeriesOrder != None):
            dicOfListSeriesOrder = {defaultKey: dicOfListSeriesOrder}
    
        numGraphs = 1
        
    else:
        if defaultKey in dicOfDicX.keys():
            defaultKey = "N/A"
        numGraphs = len(dicOfDicX.keys())
    
    
    if (graphOrder == None):
        graphOrder = sorted(listOfDicX.keys())
        
    if (dicOfListSeriesOrder == None):
        dicOfListSeriesOrder = {}
        
        for key in dicOfDicX.keys():
            dicOfListSeriesOrder[key] = sorted(dicOfDicX[key].keys())
    
    return dicOfDicX, dicOfDicY, numGraphs, defaultKey, graphOrder, dicOfListSeriesOrder

def preProcessInputGraphsList(dicList, \
                    graphOrder=None, dicOfListSeriesOrder=None):
    defaultKey = ""
    
    if (len(dicList) == 0):
        return [], 0, "", [], []
    
    dicOfDicX = dicList[0]
    numDimensions = len(dicList)
    if not type (dicOfDicX.values()[0]) is dict:
        for i in range (numDimensions):
            dicList[i] = {defaultKey: dicList[i]}

        if (dicOfListSeriesOrder != None):
            dicOfListSeriesOrder = {defaultKey: dicOfListSeriesOrder}
        dicOfDicX = dicList[0]
        numGraphs = 1
        
    else:
        if defaultKey in dicOfDicX.keys():
            defaultKey = "N/A"
        numGraphs = len(dicOfDicX.keys())
    
    
    if (graphOrder == None):
        graphOrder = sorted(dicOfDicX.keys())
        
    if (dicOfListSeriesOrder == None):
        dicOfListSeriesOrder = {}
        
        for key in dicOfDicX.keys():
            dicOfListSeriesOrder[key] = sorted(dicOfDicX[key].keys())
    
    return dicList, numGraphs, defaultKey, graphOrder, dicOfListSeriesOrder

def paintPlotGeneral(name, dicOfDicX, dicOfDicY, dicOfListSeriesOrder=None,
                     graphOrder=None, \
                     xLim=None, yLim=None, labelX="Series", labelY="Value", \
                    tickFrequence=None, xLogScale=False, yLogScale=False, alpha=1.0, \
                     dir="", graphFileName="", \
                    legendLocation=1, legendFontSize=9, xFontSize=10, yFontSize=10, \
                    rotation="vertical", margins=None, xScalar=True, yScalar=True, \
                        scientificX=False, scientificY=False):
# def paintPlotGeneral(name, seriesDic, valuesDic, dir="", graphFileName="", xLim=None, labelX="Series", labelY="Value", \
#                      logScale=False, trend=False, tickFrequence=None, hLines=[], \
#                          xLogScale=False, alpha=1.0, seriesOwnGraphs=None,  \
#                        ownGraphsLabels=None, legendLocation=1, yLim=None):
    # this function will support multiple graphs in one and multple series in the same graph
    # listofDicX can be either a dictionary of dictionaries or a dictionary with series (or just one inside). It contain Y values
    # listOfDicY same as the X one. But has to have the same structure
    # listOfListSeriesOrder, if we want to force the order of how the series are painted from each dictionar. If the previous
    #     ones, it will be a list ot lists.
    # xLim, yLim, labelX, labelY, tickFrequence, xLogScale, label Y, xLogScale, yLogScale and alpha
    #    can be either a value or a dicitioanry
    
    return paintGenericGeneral(doPlotSubGraphList, name, [dicOfDicX, dicOfDicY], dicOfListSeriesOrder,
                 graphOrder, \
                 xLim, yLim, labelX, labelY, \
                tickFrequence, xLogScale, yLogScale, alpha, \
                 dir, graphFileName, \
                legendLocation, legendFontSize, xFontSize, yFontSize, \
                rotation, margins, xScalar, yScalar, \
                    scientificX=scientificX, scientificY=scientificY)
    

        


def paintGenericGeneral(paintFunction, name, dicList, dicOfListSeriesOrder=None,
                     graphOrder=None, \
                     xLim=None, yLim=None, labelX="Series", labelY="Value", \
                    tickFrequence=None, xLogScale=False, yLogScale=False, alpha=1.0, \
                     dir="", graphFileName="", \
                    legendLocation=1, legendFontSize=9, xFontSize=10, yFontSize=10, \
                    rotation="vertical", margins=None, xScalar=True, yScalar=True, \
                        scientificX=False, scientificY=False):
# def paintPlotGeneral(name, seriesDic, valuesDic, dir="", graphFileName="", xLim=None, labelX="Series", labelY="Value", \
#                      logScale=False, trend=False, tickFrequence=None, hLines=[], \
#                          xLogScale=False, alpha=1.0, seriesOwnGraphs=None,  \
#                        ownGraphsLabels=None, legendLocation=1, yLim=None):
    # this function will support multiple graphs in one and multple series in the same graph
    # listofDicX can be either a dictionary of dictionaries or a dictionary with series (or just one inside). It contain Y values
    # listOfDicY same as the X one. But has to have the same structure
    # listOfListSeriesOrder, if we want to force the order of how the series are painted from each dictionar. If the previous
    #     ones, it will be a list ot lists.
    # xLim, yLim, labelX, labelY, tickFrequence, xLogScale, label Y, xLogScale, yLogScale and alpha
    #    can be either a value or a dicitioanry
    
    
    
    if (not type(dicList) is list):
        dicList = [dicList]
    
    (dicList, numGraphs, defaultKey, graphOrder, dicOfListSeriesOrder) = preProcessInputGraphsList(dicList, \
                    graphOrder=graphOrder, \
                    dicOfListSeriesOrder=dicOfListSeriesOrder)
    
        
    fig, axesDic = doFigOperatios(name, numGraphs, graphOrder, margins)
    
    fieldList = [dicList, dicOfListSeriesOrder, xLim, yLim, \
           labelX, labelY, tickFrequence, xLogScale, \
               yLogScale, alpha, \
                 xScalar, yScalar, axesDic, scientificX, scientificY]
    
    for graphName in graphOrder:
        dicList, seriesOrder, xLimSeries, yLimSeries, \
                   labelXSeries, labelYSeries, tickFrequenceSeries, xLogScaleSeries, \
                       yLogScaleSeries, alphaSeries, xScalarSeries, yScalarSeries, \
                        ax, scientificXSeries, scientificYSeries, \
 = unpackValues(fieldList, graphName)
        
       # print "AX", axesDic
        doAxisAdjustments(ax, labelXSeries, labelYSeries, xFontSize, yFontSize, \
                          xLogScaleSeries, yLogScaleSeries, \
                          rotation, graphName, xScalar=xScalarSeries, \
                          yScalar=yScalarSeries, \
                          scientificX=scientificXSeries, scientificY=scientificYSeries)
        plotDic = paintFunction(ax, dicList, seriesOrder, \
                                tickFrequence=tickFrequenceSeries, \
                                xLogScale=xLogScaleSeries, yLogScale=yLogScaleSeries, \
                                alpha=alphaSeries, xLim=xLimSeries, yLim=yLimSeries)
        

        
        createLegend(ax, loc=legendLocation, fontSize=legendFontSize)
        

    
    saveGraph(fig, dir, graphFileName)

def paintBoxPlotGeneral(name, dicOfSeries, dicOfListSeriesOrder=None,
                     graphOrder=None, \
                     xLim=None, yLim=None, labelX="Series", labelY="Value", \
                    tickFrequence=None, xLogScale=False, yLogScale=False, alpha=1.0, \
                     dir="", graphFileName="", \
                    legendLocation=1, legendFontSize=9, xFontSize=10, yFontSize=10, \
                    rotation="vertical", margins=None, xScalar=True, yScalar=True):
                    
    return paintGenericGeneral(doBoxPlotSubGraphList, name, dicOfSeries, dicOfListSeriesOrder,
                     graphOrder, \
                     xLim, yLim, labelX, labelY, \
                    tickFrequence, xLogScale, yLogScale, alpha, \
                     dir, graphFileName, \
                    legendLocation, legendFontSize, xFontSize, yFontSize, \
                    rotation, margins, xScalar, yScalar)
# def paintPlotGeneral(name, seriesDic, valuesDic, dir="", graphFileName="", xLim=None, labelX="Series", labelY="Value", \
#                      logScale=False, trend=False, tickFrequence=None, hLines=[], \
#                          xLogScale=False, alpha=1.0, seriesOwnGraphs=None,  \
#                        ownGraphsLabels=None, legendLocation=1, yLim=None):
    # this function will support multiple graphs in one and multple series in the same graph
    # listofDicX can be either a dictionary of dictionaries or a dictionary with series (or just one inside). It contain Y values
    # listOfDicY same as the X one. But has to have the same structure
    # listOfListSeriesOrder, if we want to force the order of how the series are painted from each dictionar. If the previous
    #     ones, it will be a list ot lists.
    # xLim, yLim, labelX, labelY, tickFrequence, xLogScale, label Y, xLogScale, yLogScale and alpha
    #    can be either a value or a dicitioanry
    
    
#    dicList=[dicOfSeries]
    
#    (dicList, numGraphs, defaultKey, graphOrder, dicOfListSeriesOrder) = preProcessInputGraphsList(dicList, \
#                    graphOrder=graphOrder,\
#                    dicOfListSeriesOrder=dicOfListSeriesOrder)
#    dicOfSeries=dicList[0]
        
#    fig, axesDic=doFigOperatios(name, numGraphs,graphOrder, margins)
    
#    fieldList=[dicOfSeries, dicOfListSeriesOrder,xLim,yLim,\
#           labelX, labelY,tickFrequence, xLogScale, \
#               yLogScale,alpha, \
#                 xScalar, yScalar, axesDic]
    
#    for graphName in graphOrder:
#        seriesDic, seriesOrder,xLimSeries,yLimSeries,\
#                   labelXSeries, labelYSeries,tickFrequenceSeries, xLogScaleSeries, \
#                       yLogScaleSeries,alphaSeries,xScalarSeries, yScalarSeries,\
#                        ax = unpackValues(fieldList, graphName)
        
#       #print "AX", axesDic
#        doAxisAdjustments(ax, labelXSeries,labelYSeries,xFontSize,yFontSize,\
#                          xLogScaleSeries, yLogScaleSeries, \
#                          rotation, graphName, xScalar=xScalarSeries, yScalar=yScalarSeries)
#        plotDic= doBoxPlotSubGraph(ax, seriesDic, seriesOrder, \
#                                tickFrequence=tickFrequenceSeries, \
#                                xLogScale=xLogScaleSeries,  yLogScale=yLogScaleSeries,\
#                                alpha=alphaSeries, xLim=xLimSeries, yLim=yLimSeries)
        

        
#        createLegend(ax, loc=legendLocation, fontSize=legendFontSize)
        

    
#    saveGraph(fig, dir, graphFileName)
        
def paintPlotSimple(name, series, values, dir="", graphFileName="", labelX="Series", labelY="Value", logScale=False, trend=False, tickFrequence=None, hLines=[]):
      


     #   print "Doing Bars, Series:"+str((series))
	
	fig = plt.figure(name)
	setTitle(fig, name)
	adjustMargin(fig)
		
	
	ax = fig.add_subplot(111)
        
        # edges=np.arange(len(values))
        # width=1.0/float(len(edges))
        if (logScale):
            ax.set_yscale('log')
        
        
        
        if tickFrequence != None:
            series, minXValue, maxXValue = tansformEpochToDates(series, -1, 0)
            xAxisDates(ax, minXValue, maxXValue, tickFrequence)

       
        # if (dateLabels):
        #    plt.plot_date(x,y)
        

        ax.legend()
        addHRefLines(hLines)
        
        
        
  
        ax.plot(series, values)

    
	adjustLabels(ax, labelX, labelY)
        
        # ax.set_xticklabels(series)
        # plt.xticks(series)
        adjustXTickers(ax)
	# adjustYTickers(ax)
	# addHLines(ax)
	

	saveGraph(fig, dir, graphFileName)


        
