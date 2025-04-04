from collections import Counter
import gc
import operator
import os
import pickle
import socket
import sys
import time

from commonLib import nerscPlot as pLib
from commonLib import nerscRadar as nr
from commonLib import timeLib
from commonLib.DBManager import DB
from commonLib.TaskRecord import TaskRecord
import filemanager as fm
import numpy as np
import scipy as sc
import scipy.cluster.vq as vq


def groupValuesByKeys(keys, values, upper_edges_bins):
    """根据键的区间划分将对应的值分组
    将keys和values按照键所属的区间进行分组，区间边界由upper_edges_bins定义。
    每个区间的右边界作为字典键，对应的值列表包含所有属于该区间的原始值。
    Args:
        keys: 键列表，数值类型，与values一一对应
        values: 值列表，与keys一一对应
        upper_edges_bins: 有序的区间右边界列表，数值类型且必须递增
    Returns:
        dict: 分组结果字典，结构为：
            key: 区间右边界值(float)
            value: 属于该区间的原始值列表(list)
    """
    groups_dic = {}
    current_key = float(-1)
    current_key_index = -1
#    print keys, values, upper_edges_bins
    
    for (key, value) in sorted(zip(keys, values)): #.sort(key=lambda x: x[0]):
#        print key
        while (current_key==-1 or
            float(key) > current_key and
            (current_key_index < len(upper_edges_bins)-1)):
            current_key_index += 1;
            current_key = float(upper_edges_bins [current_key_index])
            groups_dic[current_key] = []
        if current_key_index == len(upper_edges_bins):
            break
        groups_dic[current_key].append(value)
        
    while current_key_index < len(upper_edges_bins)-1:
        current_key_index += 1
        current_key = float(upper_edges_bins [current_key_index])
        groups_dic[current_key] = []
    return groups_dic

def doMedianOnDic(in_dic):
    """
    对字典中的数值列表进行中位数计算，处理空列表及NaN情况
    参数：
        in_dic (dict): 输入字典，格式为 {key: [数值列表]}，数值列表可能为空
    返回值：
        dict: 输出字典，格式为 {key: 中位数/处理值}，处理规则：
            - 空列表返回-1
            - 数值列表计算中位数
            - NaN中位数值转为-1
    """
    out_dic = {}
    for (key, values) in in_dic.iteritems():
        if len(values) > 0:
            val = np.median(values)
            if (val!=val):
                val = -1
        else:
            val=-1
        out_dic[key] = val    
    return out_dic



def dumpRecordsMoab(taskRecords, fileRoute, reassignClusters=False, hostname=None, maxCount=20000):
    """
    将任务记录转换为Moab格式并写入文件
    参数：
    taskRecords: list[Task] - 需要处理的任务记录列表
    fileRoute: str - 输出文件路径
    reassignClusters: bool = False - 是否重新分配聚类中心
    hostname: str = None - 当需要重新分配聚类时使用的主机名
    maxCount: int = 20000 - 缓冲区最大行数（控制内存使用）
    流程说明：
    1. 当需要重新分配聚类时，从指定路径加载聚类中心数据
    2. 使用缓冲机制分批写入文件，避免内存溢出
    """
    centroids=None
    if (reassignClusters):
        route=nr.getCentroidsRoute(hostname)
        centroids=nr.getCentroids(route)
        print "Centroids loaded: "+str(centroids.shape[0])
        
        
    
    i=0
    fw=fm.openWriteFile(fileRoute)
    buffer=""
    for task in taskRecords:
        line=task.toMoabFormat(reassignClusters, centroids)
        buffer+=line+"\n"
        if i==maxCount:
            fw.write(buffer)
            buffer=""
            i=0
        else:
            i+=1
    if (buffer!=""):
        fw.write(buffer)
    fw.close()
            
        

def getDBInfo(forceLocal=False):
    hostname=socket.gethostname()
    user = os.getenv("NERSCDB_USER", "root")
    password = os.getenv("NERSCDB_PASS", "")
    
    if forceLocal:
        return "localhost", user, password, "3306"
    if not user or not password:
        print("Database connection requires env vars NERSCDB_USER and "
         "NERSCDB_PASS to be set... exiting!")
        exit(-1)
    if "underdog" in hostname:
        return "127.0.0.1", user, password, "3306"
    return "127.0.0.1", user, password, "5050"
    

def getEpoch(year, month=1, day=1):
    """
    将指定日期转换为UNIX时间戳(秒级)
    Args:
        year (int): 必需参数，表示年份(四位数格式)
        month (int): 可选参数，表示月份，默认为1
        day (int): 可选参数，表示日期，默认为1
    Returns:
        int: 对应日期零点时刻的UNIX时间戳(从1970-01-01起的秒数)
    """
    date_time = str(day)+"."+str(month)+"."+str(year)+" 00:00:00"
    pattern = '%d.%m.%Y %H:%M:%S'
    epoch = int(time.mktime(time.strptime(date_time, pattern)))
    return epoch

def getDB(host="localhost", dbName="nerc", userName="nersc", password="nersc", \
          port="3306", useTunnel=False):
    print dbName, port
    d= DB(host, dbName, userName, password, port=port, useTunnel=useTunnel)
    #d.connect()
    return d

# Functions around the summary table    
def parseFromSQL(dbName="nersc", hostname="", userName="nersc", password="nersc", dbHost="localhost", dbPort="3306", year=-1, month=-1, day=-1, \
                 endYear=-1, endMonth=-1, endDay=-1, timeAdjust=0, orderingField=None, filtered=False):
    """
    从SQL数据库获取并解析任务记录
    参数：
        dbName (str): 数据库名称，默认'nersc'
        hostname (str): 主机名筛选条件，默认空字符串
        userName (str): 数据库用户名，默认'nersc'
        password (str): 数据库密码，默认'nersc'
        dbHost (str): 数据库主机地址，默认'localhost'
        dbPort (str): 数据库端口，默认'3306'
        year (int): 起始年份，默认-1表示无限制
        month (int): 起始月份，默认-1表示无限制
        day (int): 起始日期，默认-1表示无限制
        endYear (int): 结束年份，默认-1表示无限制
        endMonth (int): 结束月份，默认-1表示无限制
        endDay (int): 结束日期，默认-1表示无限制
        timeAdjust (int): 时间调整值（秒），默认0
        orderingField (str): 排序字段，默认None
        filtered (bool): 是否过滤已过滤任务，默认False
    返回值：
        list[TaskRecord]: 解析后的任务记录对象列表
    """
    print "Loading Records"
    items=[]
    db=getDB(dbName=dbName, userName=userName, password=password, host=dbHost)

    m=1
    d=1
    if( month!=-1 and year!=-1):
        m=month
    if ( month!=-1 and day!=-1):
        d=day;
    condition="True"
    
    if (year!=-1):
        startEpoch=getEpoch(year, m, d)+timeAdjust
        condition="start>="+str(startEpoch)
        print "START:"+str(startEpoch)
   # print condition
   
    if (endYear!=-1):
        em=1
        ed=1
        if (endMonth!=-1):
            em=endMonth
        if (endDay!=-1):
            ed=endDay
        endEpoch=getEpoch(endYear, em, ed)
        condition+=" and start<="+str(endEpoch)
        print "STOP:"+str(endEpoch)
    if filtered:
        condition+=" and filtered=False"
        print "Filtered TASKS"
    to=timeLib.getTS()
    if (hostname==""):
        rows=db.getValuesDicList("summary", TaskRecord.getFields(), condition=condition, orderBy=orderingField)
    else:
        rows=db.getValuesDicList("summary", TaskRecord.getFields(), condition=condition+" and "+"hostname='"+hostname+"'", orderBy=orderingField)
    print "Time to Retrieve SQL Records:"+str(timeLib.getFinalT(to))
    print "Records Loaded"
    print "Parsing Records"
    to=timeLib.getTS()
    for row in rows:
        items.append(TaskRecord().parseSQL(row))
    print "Records parsed: "+str(len(items))
    print "Time to Parse SQL Records:"+str(timeLib.getFinalT(to))
    rows=None
    gc.collect()
    return items

def getTimeStamp(year, month, day, timeAdjust=0, shiftStart=0):
    m=1
    d=1
    if( month!=-1 and year!=-1):
        m=month
    if ( month!=-1 and day!=-1):
        d=day;
    startEpoch=getEpoch(year, m, d)+timeAdjust-shiftStart
    return startEpoch
    

def parseFromSQL_LowMem(dbName="nersc", hostname="", userName="nersc", password="nersc", dbHost="localhost", dbPort="3306", year=-1, month=-1, day=-1, startEpoch=-1,\
                 endYear=-1, endMonth=-1, endDay=-1, endEpoch=-1, timeAdjust=0, orderingField=None, filtered=False, fieldDate="start",
                 shiftStart=0, condition="True", useTunnel=False):
    """
    从SQL数据库低内存模式加载并解析任务记录
    参数：
    dbName (str): 数据库名称，默认为"nersc"
    hostname (str): 过滤的主机名，默认为空表示不过滤
    userName (str): 数据库用户名
    password (str): 数据库密码
    dbHost (str): 数据库主机地址
    dbPort (str): 数据库端口号
    year/month/day (int): 起始日期年月日（-1表示未设置）
    startEpoch (int): 直接指定起始时间戳（优先级高于年月日）
    endYear/endMonth/endDay (int): 结束日期年月日
    endEpoch (int): 直接指定结束时间戳（优先级高于结束年月日）
    timeAdjust (int): 时间戳调整值（秒）
    orderingField (str): 排序字段名称
    filtered (bool): 是否过滤任务标记
    fieldDate (str): 时间字段名称（start/end）
    shiftStart (int): 起始时间偏移量（秒）
    condition (str): 附加查询条件
    useTunnel (bool): 是否使用SSH隧道连接
    返回值：
    tuple: (items 解析后的任务记录列表, significantStart 有效起始时间戳)
    """
    print "Loading Records", dbPort
    items=[]
    db=getDB(dbName=dbName, userName=userName, password=password, host=dbHost, port=dbPort, useTunnel=useTunnel)
    significantStart=startEpoch
    
    if (startEpoch==-1):
        m=1
        d=1
        if( month!=-1 and year!=-1):
            m=month
        if ( month!=-1 and day!=-1):
            d=day;
        #condition="True"
        
        if (year!=-1):
            startEpoch=getEpoch(year, m, d)+timeAdjust-shiftStart
            significantStart=startEpoch+shiftStart
            
    
    if (startEpoch!=-1):
        condition+=" AND "+fieldDate+">="+str(startEpoch)
        print "START:"+str(startEpoch)
   # print condition
    if (endEpoch==-1):
        if (endYear!=-1):
            em=1
            ed=1
            if (endMonth!=-1):
                em=endMonth
            if (endDay!=-1):
                ed=endDay
            endEpoch=getEpoch(endYear, em, ed)+timeAdjust
    
    if endEpoch!=-1:
        
        condition+=" and "+fieldDate+"<="+str(endEpoch)
        print "STOP:"+str(endEpoch)
    if filtered:
        condition+=" and filtered=False"
        print "Filtered TASKS"
    to=timeLib.getTS()
    if (hostname==""):
        cur=db.getValuesDicList_LowMem("summary", TaskRecord.getFields(), condition=condition, orderBy=orderingField)
    else:
        cur=db.getValuesDicList_LowMem("summary", TaskRecord.getFields(), condition=condition+" and "+"hostname='"+hostname+"'", orderBy=orderingField)
    print "Time to Retrieve SQL Records:"+str(timeLib.getFinalT(to))
    print "Records Loaded"
    print "Parsing Records"
    
    to=timeLib.getTS()
    end=False
    maxCount=10000
    while not end:
        rows=[]
        count=1

        row=cur.fetchone()
        while row!=None:
            rows.append(row)
            if (count>=maxCount):
                break
            row=cur.fetchone()
       
            count+=1
        
        end=row==None
        for row in rows:
            items.append(TaskRecord().parseSQL(row))
        
            
        
   
    print "Records parsed: "+str(len(items))
    print "Time to Parse SQL Records:"+str(timeLib.getFinalT(to))
    rows=None
    
    db.close_LowMem(cur)
    gc.collect()
    return items, significantStart




def insertIntoDB(db, listRecords):
    print "inserting Records"

    for record in listRecords:
       # print "record", record
        db.insertValues("summary", record.keys(), record.values())

def insertIntoDBMany(db, listRecords):
    print "inserting Records"

    db.insertValuesMany("summary", listRecords)        
        
        
        

def readFileAndInsertDB(fileName, hostname, dbName="custom", moabFormat=False):
    print "Opening:"+fileName
    f=fm.openReadFile(fileName)
    lines=f.readlines()
    
    stepMax=1000
    
    dbHost, dbUser, dbPass, dbPort=getDBInfo()
    
    #db=getDB(dbName=dbName)
    
    db=getDB(host=dbHost, dbName=dbName, userName=dbUser, password=dbPass, port=dbPort)
    parsedRecords=[]
    for line in lines:
        record=TaskRecord()
        if record.parsePartialLog(hostname, line, moabFormat=moabFormat):
            parsedRecords.append(record.valuesDic)
            #print "New Record: "+record.valuesDic["stepid"]
        if (len(parsedRecords)==stepMax):
            #print "Dumping "+str(len(parsedRecords))+" records on DB "+dbName
            insertIntoDBMany(db, parsedRecords)
            parsedRecords=[]
    if len(parsedRecords)>0:
        #print "Dumping "+str(len(parsedRecords))+" records on DB "+dbName
        insertIntoDBMany(db, parsedRecords)
    print 
    print "Dump done"
    f.close()
            
            
                        
        
    


def GetTasksPerJobWithNoFailure(rows):
    """
    统计各作业的任务数量，失败作业标记为-1，正常作业统计任务总数
    遍历任务记录，若作业存在失败任务(status<0)则标记为-1，否则累计正常任务数。
    注意：一旦作业被标记失败，后续该作业的正常任务将不会改变计数状态。
    参数：
        rows (iterable): 任务记录集合，每条记录需包含以下字段：
            - jobname: 作业名称
            - status: 任务状态(数值类型，status<0表示失败)
    返回值：
        dict: 作业名为键的字典，值说明：
            - 若作业存在失败任务，值为-1
            - 否则为作业包含的正常任务总数
    """
    jobCount={}
    for row in rows:
        jobName=row.getVal("jobname")
        stepid=row.getVal("stepid")
        status=row.getVal("status")
        if (status<0):
            jobCount[jobName]=-1
        else:
            print jobName
            
            if jobName in jobCount:
                if (jobCount[jobName]>0):
                    jobCount[jobName]+=1
            else:
                jobCount[jobName]=1
#    for jobName in jobCount.keys():
#        if (jobCount[jobName]>1):
#            print jobName, jobCount[jobName]

            
    return jobCount



def getBin(duration, globalEdges):
    """
    根据持续时间确定对应的区间索引
    参数:
        duration (float/int): 需要分箱的时间长度
        globalEdges (list): 有序的边界值列表，用于划分时间区间

    返回值:
        int: 区间索引值。当duration超过最大边界时返回最后一位索引，
             如果总区间数量超过99则返回99
    """
    #print globalEdges, duration
    i=0
    for edge in globalEdges:
        if (i>99):
            return 99
        if duration<edge:

            return i
        i+=1
    return i-1


def createFieldDic(dataFields):
    outputDic={}
    for field in dataFields:
        outputDic[field]=[]
    return outputDic


def createDicDic(dataFields):
    outputDic={}
    for field in dataFields:
        outputDic[field]={}
    return outputDic

def createAccDic(dataFields):
    outputDic={}
    for field in dataFields:
        outputLists[field]=0
    return outputDic
    

def getValueFromRow(row, field):
    value=None
    if field=="duration":
        value=row.duration()
    elif field=="totalcores":
        if (row.getVal("hostname")=="hopper" or row.getVal("hostname")=="edison"):
            value=row.getVal("numnodes")*24#row.getVal("cores_per_node")
        else:
            value=row.getVal("numnodes")*row.getVal("cores_per_node")
    elif field=="totaltime":
        value=row.getVal("numnodes")*row.getVal("cores_per_node")*row.duration()
    elif field=="waittime":
        value=row.waitTime()
    else:
        value=row.getVal(field)
        
        
    
    return value

def getSelectedDataFromRows(rows, dataFields, queueFields=[], accFields=[]):
    """
    从数据行集合中提取并组织指定字段的数据
    参数：
    rows       - 待处理的数据行对象集合
    dataFields - 需要提取的基础数据字段列表
    queueFields - 需要按队列分类存储的字段列表（默认空列表）
    accFields  - 需要实时累加统计的字段列表（默认空列表）
    返回值：
    tuple包含7个元素：
    1. 处理行数
    2. outputDic - 基础字段数据字典，结构为{field: [values]}
    3. outputAcc - 累加字段数据字典，结构为{field: [accumulated_values]}
    4. queues    - 普通队列计数字典，结构为{queue_name: count}
    5. queueDic  - 普通队列字段数据字典，结构为{field: {queue: [values]}}
    6. queuesG   - 全局队列计数字典
    7. queueGDic - 全局队列字段数据字典（结构同queueDic）
    """
    print "Starting Data Selection"
    to=timeLib.getTS()
    
    outputDic=createFieldDic(dataFields)
    temporaryAcc=createAccDic(accFields)
    outputAcc=createFieldDic(accFields)
    queueDic=createDicDic(queueFields)
    queueGDic=createDicDic(queueFields)
    queues={}
    queuesG={}
    
    count=0
#    while (len(rows)>0):
#        count+=1
#        if (count==100000):
#            gc.collect()
#        row=rows.pop(0)
    for row in rows:
        currentQueue=row.getVal("class")
        currentGQueue=row.getVal("classG")
        if (not currentQueue in queues.keys()):
            queues[currentQueue]=0
        if (not currentGQueue in queuesG.keys()):
            queuesG[currentGQueue]=0
        queues[currentQueue]+=1
        queuesG[currentGQueue]+=1
 
        
        for field in dataFields:
            value=getValueFromRow(row, field)
            outputDic[field].append(value)
            if field in accFields:
                temporaryAcc[field]+=value
                outputAcc[field].append(value)
        
        for field in queueFields:
      
            fieldDic=queueDic[field]
            if (not currentQueue in fieldDic.keys()):
                fieldDic[currentQueue]=[]
            fieldDic[currentQueue].append(getValueFromRow(row, field))
            
          
            fieldDic=queueGDic[field]
            if (not currentGQueue in fieldDic.keys()):
                fieldDic[currentGQueue]=[]
            fieldDic[currentGQueue].append(getValueFromRow(row, field))
            
            
#        currentQueue=row.getVal("class")
#        if (not currentQueue in queueDic.keys()):
#            queueDic[currentQueue]=createFieldDic(queueFields)
#        for field in queueFields:
#            queueDic[currentQueue][field].append(getValueFromRow(row, field))
        
#        currentQueue=row.getVal("classG")
#        if (not currentQueue in queueGDic.keys()):
#            queueGDic[currentQueue]=createFieldDic(queueFields)
#        for field in queueFields:
#            queueGDic[currentQueue][field].append(getValueFromRow(row, field))
    print "Time to extract information:"+str(timeLib.getFinalT(to))
    return len(rows), outputDic, outputAcc, queues, queueDic, queuesG, queueGDic
            
                
                
          


def getDataFromRows(rows, globalEdges):
    """
    从给定的任务行数据中提取和统计多维度的任务特征数据
    参数:
        rows: 可迭代对象，包含多个任务数据行，每行需支持duration(), getVal()等方法
        globalEdges: 数组，用于CPU时间分布统计的分箱边界值
    返回值:
        包含22个元素的元组，按顺序返回以下数据:
        - 各任务持续时间列表
        - 任务使用的节点数列表
        - 任务使用的总核心数列表
        - 任务CPU时间(核心数*持续时间)列表
        - 任务墙上时间列表
        - 任务类别的计数统计
        - 按类别分类的任务持续时间字典
        - 按类别累计的CPU时间字典
        - 按类别记录的核心数使用列表字典
        - 按类别的CPU时间分布字典
        - 按时间分箱统计的特殊CPU时间累加器
        - 任务等待时间列表
        - 按类别的等待时间分布字典
        - 任务内存使用列表
        - 任务虚拟内存使用列表
        - 按类别的内存使用字典
        - 按类别的虚拟内存使用字典
        - 原始类别标签列表
    """
    taskSizes=[]
    taskNodes=[]
    #taskCorePerNode=[]
    taskCores=[]
    cpuTime=[]
    wallClock=[]
    classes=[]
    waitTime=[]
    
    classTaskDuration={}
    classTaskCPUTime={}
    classCores={}
    classTaskCPUTimeDistro={}
    classWaitTimeDistro={}
    
    taskMemory=[]
    taskVMemory=[]
    classTaskMemory={}
    classTaskVMemory={}
    
  
    
    specialCPUTimeAcumulator=np.zeros(100, dtype=float)
    
    classOfPoint=[]
    
    for row in rows:
        taskSizes.append(row.duration())
        taskNodes.append(row.getVal("numnodes"))
        
        if row.getVal("hostname") in ["hopper", "edison"]:
            numCoresPerNode=24
        else:
            numCoresPerNode=row.getVal("cores_per_node")
        taskCores.append(row.getVal("numnodes")*numCoresPerNode)
        cpuTime.append(row.getVal("numnodes")*numCoresPerNode*row.duration())
        wallClock.append(row.getVal("wallclock"))
        classes.append(row.getVal("class"))
        waitTime.append(row.waitTime())
        taskMemory.append(row.getVal("memory"))
        taskVMemory.append(row.getVal("vmemory"))
        
        specialCPUTimeAcumulator[getBin(row.duration(), globalEdges)]+=row.getVal("numnodes")*numCoresPerNode*row.duration()
        
        c=row.getVal("class")
        if not c in classTaskDuration.keys():
            classTaskDuration[c]=[]
            classTaskCPUTime[c]=0
            classCores[c]=[]
            classTaskCPUTimeDistro[c]=[]
            classWaitTimeDistro[c]=[]
            classTaskMemory[c]=[]
            classTaskVMemory[c]=[]
            
        classTaskDuration[c].append(row.duration())
        classTaskCPUTime[c]+=row.duration()
        classCores[c].append(row.getVal("numnodes")*numCoresPerNode)
        classTaskCPUTimeDistro[c].append(row.duration()*row.getVal("numnodes")*numCoresPerNode)
        classWaitTimeDistro[c].append(row.waitTime())
        classTaskMemory[c].append(row.getVal("memory"))
        classTaskVMemory[c].append(row.getVal("vmemory"))
        #taskCorePerNode.append(row.getVal("cores_per_node"))
        classOfPoint=classes
    classes=Counter(classes)
    return taskSizes,taskNodes, taskCores, cpuTime, wallClock, classes, classTaskDuration, classTaskCPUTime, classCores, classTaskCPUTimeDistro, specialCPUTimeAcumulator, \
        waitTime, classWaitTimeDistro, taskMemory, taskVMemory, classTaskMemory, classTaskVMemory, classOfPoint 

    
def getCompareDics(dbName, hostname, dbHost, userName, password, year, month=1, endYear=-1, endMonth=-1):
    """
    从指定数据库获取作业数据并提取关键指标
    Args:
        dbName: str - 数据库名称
        hostname: str - 数据库服务器主机名
        dbHost: str - 数据库实例地址
        userName: str - 数据库用户名
        password: str - 数据库密码
        year: int - 起始年份
        month: int - 起始月份（默认1月）
        endYear: int - 结束年份（-1表示不指定）
        endMonth: int - 结束月份（-1表示不指定）
    Returns:
        tuple: 包含两个列表的元组
            [0] list - 基础指标列表:
                wallClock: 任务墙上时间统计
                taskCores: 任务核心使用情况
                taskMemory: 任务内存使用指标
                classOfPoint: 任务分类标签
            [1] list - 分类详细指标列表:
                classCores: 分类维度的核心分配数据
                classTaskCPUTimeDistro: 分类CPU时间分布
                classTaskDuration: 分类任务持续时间统计
                classTaskMemory: 分类内存使用详情
                classTaskVMemory: 分类虚拟内存使用详情
    """
    rows=parseFromSQL(dbName=dbName, hostname=hostname, dbHost=dbHost, userName=userName, \
                      password=password, \
                    year=year, month=month, endYear=endYear, endMonth=endMonth)

    taskSizes,taskNodes, taskCores, cpuTime, wallClock, classes, classTaskDuration, \
        classTaskCPUTime, classCores, classTaskCPUTimeDistro, specialCPUTimeAcumulator, \
        waitTime, classWaitTimeDistro,taskMemory, taskVMemory, \
        classTaskMemory, classTaskVMemory, classOfPoint =getDataFromRows(rows, "")
    return [wallClock, taskCores, taskMemory, classOfPoint], [classCores, classTaskCPUTimeDistro, classTaskDuration, classTaskMemory, classTaskVMemory]
def fuseDics(old, new, prefix):
    for key in new.keys():
        old[prefix+"-"+key]=new[key]
    return old
def fuseLists(old, new, prefix):
    newL=[]
    for l1, l2 in zip(old, new):
        if len(l2)>0 and type(l2[0])==str:
            l=l1+[]
            for e in l2:
                l.append(prefix+"-"+e) 
            newL.append(l)
        else:
            newL.append(l1+l2)
    return newL

def getFusedDics(dbName, hostnames, dbHost, userName, password, year, month=1, endYear=-1, endMonth=-1):
    """
    融合多个主机名的比较数据，生成统一的字典和列表结构
    参数：
    dbName (str): 数据库名称
    hostnames (list): 主机名列表，用于生成前缀标识
    dbHost (str): 数据库主机地址
    userName (str): 数据库用户名
    password (str): 数据库密码
    year (int): 起始年份
    month (int): 起始月份，默认为1
    endYear (int): 结束年份，默认-1表示未设置
    endMonth (int): 结束月份，默认-1表示未设置
    返回值：
    tuple: 包含三个元素的元组
        - allLists: 融合后的统一列表结构
        - allDics: 融合后的字典结构列表，每个字典对应一个层级
        - keys: 第一个字典的键集合，用于展示字段名称
    """
    allDics=""
    allLists=""
    for hostname in hostnames:
        newLists, newDics=getCompareDics(dbName=dbName, hostname=hostname, dbHost=dbHost, \
                                         userName=userName, password=password, \
                                        year=year, month=month, endYear=endYear, endMonth=endMonth)
        if allLists=="":
            allLists=newLists
        else:
            allLists=fuseLists(allLists, newLists, hostname[0:2])
        
        
        finalDics=[]
        prefix=hostname[0:2]
        if allDics=="":
            allDics=[]
            for i in range(len(newDics)):
                allDics.append({})
        for (old, new) in zip(allDics, newDics):
            finalDics.append(fuseDics(old, new, prefix))
        allDics=finalDics
        
    return allLists, allDics, allDics[0].keys()

def dumpClusters(fileName, centroids, associations, points, queuePerPoint, classDuration, classCores):
    """将聚类结果数据序列化保存到多个文件中
    Args:
        fileName: str - 输出文件的基础路径/前缀名
        centroids: ndarray - 聚类中心坐标矩阵，形状为(K, D) K为聚类数，D为特征维度
        associations: ndarray - 各数据点所属聚类索引数组，形状为(N,)
        points: ndarray - 原始数据点坐标矩阵，形状为(N, D)
        queuePerPoint: ndarray - 每个数据点的队列关联数据，形状与业务逻辑相关
        classDuration: dict - 聚类类别持续时间字典，key为类别索引，value为持续时间
        classCores: dict - 聚类类别核心参数字典，key为类别索引，value为核心参数
    Returns:
        None
    """
    np.save(fileName+"-cent", centroids)
    np.save(fileName+"-asso", associations)
    np.save(fileName+"-points", points)
    np.save(fileName+"-qasso", queuePerPoint)
    
    save_obj(fileName+"-qDura.dict", classDuration)
    save_obj(fileName+"-qCores.dict", classCores)
    
def loadClusters(fileName):
    """
    从指定基础文件名加载聚类相关数据
    该函数加载由多个.npy文件和序列化字典组成的聚类数据集，包含质心、关联关系、原始点数据、
    队列分配信息以及类别的持续时间和核心属性数据。
    Args:
        fileName (str): 数据文件的基础路径名(不含后缀)
    Returns:
        tuple: 包含以下元素的元组:
        - centroids (np.ndarray): 聚类质心坐标数组
        - associations (np.ndarray): 点与质心的关联关系数组
        - points (np.ndarray): 原始数据点坐标集合
        - queuePerPoint (np.ndarray): 每个点的队列分配信息数组
        - classDuration (dict): 类别持续时间字典，键为类别ID
        - classCores (dict): 类别核心属性字典，键为类别ID
    """
    ext=".npy"
    centroids=np.load(fileName+"-cent"+ext)
    associations=np.load(fileName+"-asso"+ext)
    points=np.load(fileName+"-points"+ext)
    queuePerPoint=np.load(fileName+"-qasso"+ext)
    
    classDuration=load_obj(fileName+"-qDura.dict")
    classCores=load_obj(fileName+"-qCores.dict")
    
    return centroids, associations, points,queuePerPoint, classDuration, classCores

def save_obj(name, obj):
    with open(name, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open(name, 'r') as f:
        return pickle.load(f)

def getSeed(forced=None):
    if (forced==None):
        np.random.seed()
        return np.random.randint(0, sys.maxint)
    else:
        return forced
    

def initSeed(seed=None):
    np.random.seed(seed)
    
#print ge

#rows=parseFromSQL()

#taskSizes,taskNodes, taskCores, cpuTime, wallClock, classes, classTaskDuration, classTaskCPUTime, classCores, classTaskCPUTimeDistro, specialCPUTimeAcumulator, \
#    waitTime, classWaitTimeDistro =getDataFromRows(rows, ge)
#dir="Charts"
#pLib.paintHistogram("Task Duration distribution \N (#Jobs in 100 bins)", taskSizes, bins=100, dir=dir, graphFileName="DurationDistTask", labelX="Task Duration (s)", labelY="#Tasks", logScale=True, special=specialCPUTimeAcumulator)
#pLib.paintHistogram("Cores per Task distribution \N (#Jobs in 100 bins)", taskCores, bins=100, dir=dir, graphFileName="CoresDistTasks", labelX="#Cores/Task", labelY="#Tasks", logScale=True)
#pLib.paintHistogram("CPU TIME per Task distribution \N (#Jobs in 100 bins)", cpuTime, bins=100, dir=dir, graphFileName="CPUDistTasks", labelX="CPU Time(s)", labelY="#Tasks", logScale=True)
##print (classes)
#pLib.paintHistogram("WAIT TIME per Task distribution \N (#Jobs in 100 bins)", cpuTime, bins=100, dir=dir, graphFileName="WaitTimeTasks", labelX="Wait Time(s)", labelY="#Tasks", logScale=True)

#pLib.paintBars("Tasks per Class \N (#Jobs)",classes, dir=dir, graphFileName="TasksPerClass.png", labelX="Class", labelY="#Tasks", logScale=True)

#pLib.paintBoxPlot("Task Duration per Class", classTaskDuration, labelX="Class", labelY="Task Duration (s)", dir=dir, graphFileName="TaskDurationBoxPlotClass")
#pLib.paintBars("CPU Time per Class \N (#Jobs)",classTaskCPUTime, dir=dir, graphFileName="CPUTimePerClass", labelX="Class", labelY="CPUTime(s)", logScale=True)
#pLib.paintBoxPlot("#Cores per task in classes", classCores, labelX="Class", labelY="#cores", dir=dir, graphFileName="CoresPerTaskBoxPlotClass")
#pLib.paintBoxPlot("#CPUTime per task in classes", classTaskCPUTimeDistro, labelX="Class", labelY="CPU Time(s)", dir=dir, graphFileName="CPUTimePerTaskBoxPlotClass")

#pLib.paintBoxPlot("WaitTime per task in classes", classWaitTimeDistro, labelX="Class", labelY="Wait Time(s)", dir=dir, graphFileName="WaitTimePerTaskBoxPlotClass")


    
#pLib.showAll()
    
    

        
    