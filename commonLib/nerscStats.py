import numpy as np


def cleanWallClocks(wc, req):
    """过滤并同步清洗wall clock时间和对应的请求数数据
    根据业务规则，仅保留wall clock时间和请求数都超过10的数据点
    Args:
        wc: list[float|int], wall clock时间数据列表
        req: list[float|int], 对应的请求数数据列表，长度需与wc保持一致
    Returns:
        tuple: 包含两个元素的元组，按顺序返回:
            - wcN: list, 过滤后的wall clock时间数据
            - reqN: list, 过滤后的请求数数据，与wcN保持索引对应关系
    """
    wcN=[]
    reqN=[]
    for (w,r) in zip(wc, req):
        if (w>10 and r >10 ):
            wcN.append(w)
            reqN.append(r)
    return wcN, reqN


def calculateAccuracy(wc, req):
    """
    计算每个任务的时间比例或准确率
    参数:
    wc (list): 每个任务的成功计数列表（如：单词正确数）
    req (list): 每个任务的总请求次数列表（如：总单词数）
    返回:
    list: 包含各任务百分比值的列表，计算方式为 (成功数/(总次数+1))*100，
          加1用于避免总次数为0时除零错误
    """
    taskTimeProp=[100*float(s)/float(w+1) for s,w in zip(wc, req)]
    return taskTimeProp
    

def normalize(varList, maxVal=None):
    """
    对输入列表进行归一化处理，返回各元素除以最大值的比例列表
    Args:
        varList (list): 需要归一化的数值列表，元素应为可转换为float的类型
        maxVal (float/int, optional): 指定的最大值参数。若未提供则自动取列表最大值
    Returns:
        list: 归一化后的新列表，所有元素取值范围为[0.0, 1.0]，元素类型为float
    处理逻辑：
        1. 当未指定maxVal时，自动获取列表最大值作为基准值
        2. 将基准值转换为浮点数类型保证除法精度
        3. 通过列表推导式生成各元素比例值
    """
    if maxVal==None:
        maxVal=max(varList)
    maxVal=float(maxVal)
    return [float(x)/maxVal for x in varList]

def calculatePerson(varList):
    """
    计算输入变量的相关系数矩阵
    参数:
    varList (list of float or array-like): 输入变量列表，支持一维列表或二维数组结构。
        每个元素应为数值类型，函数内部会转换为浮点数numpy数组。
    返回:
    numpy.ndarray: 二维相关系数矩阵，形状为(n_vars, n_vars)
        矩阵元素[i][j]表示第i个变量与第j个变量的皮尔逊相关系数
    """
   # print varList
    vec = np.array(varList, dtype=float)
    print vec.shape
   
    return np.corrcoef(vec)
    
def cleanTimeStamps(stamps, period=1):
    st0=stamps[0]
    return [float(st-st0)/float(period) for st in stamps]

def doKeyCalc(values, bins=100):
    """
    计算输入数据的直方图及累积分布函数(CDF)
    Args:
        values: 输入数据数组，用于计算直方图和CDF
        bins: 直方图的分箱数量，默认为100。可以是整数或定义分箱区间的序列
    Returns:
        edges: 分箱的左边界数组，长度等于bins
        hist: 每个分箱的计数值数组
        cdf: 归一化的累积分布函数数组，值范围[0,1]
    """
    hist, edges=np.histogram(values, bins=bins, normed=False)
    edges=edges[0:-1]
    cdf=np.cumsum(hist, dtype=float)
    cdf/=cdf[-1]
    return edges, hist, cdf


def genDateTag(elements):
    return "-".join([str(i) for i in elements])

def resKeys():
    return ["edges", "hist", "CDF"]

class ResultsStore:
    
    
    def __init__(self, hostnames=["host"]):
        self.resultsDic={}
        self.keyStore={}
        self.hostnames=hostnames
    
    def createResult(self, name, keys=["edges", "hist", "CDF"]):
        self.keyStore[name]=keys
        self.resultsDic[name]=ResultsStore.createBaseDic(keys, self.hostnames)
    
    def regResult(self,name, hostname, listR, dateKey="date"):
        if type(listR) is dict:
            self.regResultDic(name, hostname, listR, dateKey=dateKey)
        else:
            print 
            #print self.keyStore[name]
            #print listR
            for (key, res) in zip(self.keyStore[name], listR):
                self.resultsDic[name][key][hostname][dateKey]=res
    
    def regResultDic(self,name, hostname, dicData, dateKey="date"):
        #print "DICREgistered", name, dateKey, dicData
#        print listR
        for key in self.keyStore[name]:
            self.resultsDic[name][key][hostname][dateKey]=dicData[key]
            
            
    def getResult(self, name, key, flatten=False):
        if not flatten:
            return  self.resultsDic[name][key]
        else:
            dicD={}
            dicO=self.resultsDic[name][key]
            for host in dicO.keys():
                for date in dicO[host].keys():
                    dicD[host+" "+date]=dicO[host][date]
            return dicD
                

    def getResultHostDate(self, name, key, hostname, dateKey="date"):
        return  self.resultsDic[name][key][hostname][dateKey]
    
    def getResultHost(self, name, key, hostname, flatten=False):
        if not flatten:
            return  self.resultsDic[name][key][hostname]
        else:
            dicD={}
            dicO=resultsDic[name][key][hostname]
            for k in dicO.keys():
                dicD[hostname+" "+k]=dicO[k]
            return dicD
            
    
    def getKeys(self, name):
        return self.keyStore[name]
    @classmethod
    def createBaseDic(self, keys, hostnames):
        dic={}
        for k in keys:
            dic[k]={}
            for h in hostnames:
                dic[k][h]={}
        return dic
        
    