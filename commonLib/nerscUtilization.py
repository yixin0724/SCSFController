import numpy as np
import bisect
import time
from __builtin__ import True


def getCurrentActiveJobs(startEpoch, startNumbers, durationNumbers, coreNumbers, completionNumbers):
    """获取在指定时间点处于活跃状态的作业参数
    Args:
        startEpoch (int): 基准时间戳（epoch时间），用于判断作业是否活跃的时间点
        startNumbers (list): 作业开始时间戳列表
        durationNumbers (list): 作业持续时间列表（单位应与时间戳一致）
        coreNumbers (list): 作业所需计算核心数列表
        completionNumbers (list): 作业完成时间戳列表
    Returns:
        tuple: 包含三个列表的元组，按顺序返回：
            - 符合条件的作业开始时间列表
            - 对应作业的持续时间列表
            - 对应作业的核心需求列表
    Note:
        当前逻辑包含一个潜在问题：条件表达式中的`or True`会导致所有作业都被选中
        这可能是需要修复的逻辑错误（原意应为仅选择在基准时间点处于活跃的作业）
    """
    preStartNumbers=[]
    preDurationNumbers=[]
    preCoreNumbers=[]

    for (start, duration, cores, completion) in zip(startNumbers, durationNumbers, coreNumbers, completionNumbers):
        if start<startEpoch and completion > startEpoch or True:
            #print start, startEpoch, completion

            preStartNumbers.append(start)
            preDurationNumbers.append(duration)
            preCoreNumbers.append(cores)
            
    return preStartNumbers, preDurationNumbers, preCoreNumbers
    

def getMaxCores(hostname):
    if "hopper" in hostname:
        return 153216
    if ("carver" in hostname):
        return 9984
    if ("edison" in hostname):
        return 133824 
    
def cutSamples(inTimeStamps, sampleUse, shiftValue):
    """根据时间偏移值切割时间戳和对应样本数据
    通过给定的偏移值确定切割点，返回切割后时间戳和样本数据的后半部分
    Args:
        inTimeStamps (list): 原始时间戳列表，要求按升序排列
        sampleUse (list): 与时间戳对应的样本数据列表，长度需与inTimeStamps一致
        shiftValue (int/float): 时间偏移阈值，用于确定切割点的临界值
    Returns:
        tuple: 包含两个元素的元组：
            - list: 切割后的时间戳列表（保留>=shiftValue的部分）
            - list: 切割后的样本数据列表（保留对应索引的部分）
    """
    #timeStamps=[x-inTimeStamps[0] for x in inTimeStamps]
    timeStamps=inTimeStamps
    index=0
    for t in timeStamps:
        if t>=shiftValue:
            break
        index+=1
    return inTimeStamps[index:], sampleUse[index:]
    
    


class UtilizationEngine:
    def __init__(self):
        self.endingJobsTime=[]
        self.endingJobsUse=[]
    
        self.sampleTimeStamp=[]
        self.sampleUse=[]
        self.currentUse=0
        self._waste_stamps = None
        self._waste_deltas = None
    
    def getIntegralUsage(self, maxUse=None, shiftValue=None):
        """
        计算积分使用率（实际使用面积与目标使用面积的比率）
        参数:
        maxUse (int/None): 最大理论使用量。若为None则自动取样本最大值
        shiftValue (int/None): 时间偏移量，用于对齐时间序列。非零时会调用cutSamples处理
        返回:
        float: 积分使用率（实际使用面积 / (最大理论用量 × 总时间)）
        异常:
        ValueError: 当时间序列数据不足2个时抛出
        """
        #timeStamps=[x-self.sampleTimeStamp[0] for x in self.sampleTimeStamp]
        if len(self.sampleTimeStamp) < 2:
            raise ValueError("Integral usage cannot be processed with a single"
                             " time Step")
        timeStamps=self.sampleTimeStamp
        #print timeStamps
        maxTimeStamp=long(timeStamps[-1])
        lastTimeStamp=0
        accummSurface=0
        
        cutSampleUser=self.sampleUse
        if (shiftValue!=None and shiftValue!=0):
            timeStamps,cutSampleUser=cutSamples(timeStamps, cutSampleUser, shiftValue)
        if (maxUse==None):
            maxUse=long(max(cutSampleUser))
        ts=np.array(timeStamps[1:])-np.array(timeStamps[0:-1])
        u=np.transpose(np.array(cutSampleUser[0:-1]))
        accummSurface=np.dot(ts,u)
        print "Obtained Integrated Surface:", accummSurface               
        #for ts, u in zip(timeStamps[1:],self.sampleUse[0:-1]):
        #    accummSurface+=long(ts)*long(u)
        timePeriod=timeStamps[-1]-timeStamps[0]
        print "Time period", timePeriod
        targetSurface=maxUse*(timePeriod)
        print "Target surface", targetSurface
       
        return float(accummSurface)/float((targetSurface))
        
        
        
    
    def changeUse(self, timeStamp, useDelta, doRegister=True):
        """
        修改当前使用量并记录样本数据
        参数：
        timeStamp: int/float
            当前操作的时间戳，用于记录样本时间点
        useDelta: int/float
            使用量的变化值（可正可负），将被累加到当前使用量
        doRegister: bool, 可选
            是否记录样本数据，默认True表示记录时间戳和当前使用量到样本列表
        返回值：
        None
        """
        #print "Change USe", timeStamp, useDelta, self.currentUse
        self.currentUse+=useDelta
#        if self.currentUse > 153216:
#            print "over User:", self.currentUse, useDelta
        if doRegister:
            self.sampleTimeStamp.append(timeStamp)
            self.sampleUse.append(self.currentUse)
        
        if (self.currentUse<0):
            print "JODER"
        
    
    def procesEndingJobs(self, timeStamp, doRegister=True):
        """
        处理在指定时间戳前结束的作业，释放资源并更新记录
        Args:
            timeStamp: 截止时间戳，将处理所有在此时间戳之前结束的作业
            doRegister: 是否注册资源变更（默认True），控制是否触发资源变更回调

        Returns:
            int: 本次处理的作业数量
        """
        i=0
        for time, use in zip(self.endingJobsTime, self.endingJobsUse):
            if time<=timeStamp or timeStamp is None:
                #print "job dying", timeStamp
                self.changeUse(time, -use, doRegister=doRegister)
                i+=1
            else:
                break
        
        #print "removing", i
        self.endingJobsTime=self.endingJobsTime[i:]
        self.endingJobsUse=self.endingJobsUse[i:]
        return i
    
    def processStartingJob(self, timeStamp, duration, use, doRegister=True):
        """处理新启动的任务，更新资源使用情况
        参数说明：
        timeStamp (float): 任务开始时间戳
        duration (float): 任务预计持续时间
        use (float): 任务占用的资源量
        doRegister (bool, optional): 是否注册资源变更事件，默认为True
        返回值：
        None
        """
        # 使用二分查找确定插入位置，保持endingJobsTime列表有序
        #print "Job Starts", timeStamp, timeStamp+duration
        index=bisect.bisect_left(self.endingJobsTime, timeStamp+duration)
        self.endingJobsTime.insert(index, timeStamp+duration)
        self.endingJobsUse.insert(index, use)
        
        self.changeUse(timeStamp, use, doRegister=doRegister)
        
    def apply_waste_deltas(self, waste_stamps, waste_deltas, start_cut=None,
                         end_cut=None):
        #print ("apply_waste_deltas",  waste_stamps, waste_deltas, start_cut, 
        #       end_cut)
        if start_cut and waste_stamps:
            pos = bisect.bisect_left(waste_stamps, start_cut)
            waste_stamps=waste_stamps[pos:]
            waste_deltas=waste_deltas[pos:]
        if end_cut and waste_stamps:
            pos = bisect.bisect_right(waste_stamps, end_cut)
            waste_stamps=waste_stamps[:pos]
            waste_deltas=waste_deltas[:pos]
        #print ("apply_waste_deltas after",  waste_stamps, waste_deltas, start_cut, 
        #       end_cut)
        if waste_stamps:
            self.sampleTimeStamp, self.sampleUse = _apply_deltas_usage(
                     self.sampleTimeStamp, self.sampleUse,
                     waste_stamps, waste_deltas, neg=True)
        
        return self.sampleTimeStamp, self.sampleUse    
    
    def processUtilization(self, timeStamps, durations, resourceUse, 
                           startCut=None, endCut=None, 
                           preloadDone=False, doingPreload=False):
        """
        处理资源利用率数据并生成时间序列样本

        参数:
        timeStamps: list[float] - 事件触发时间戳列表
        durations: list[float] - 对应事件的持续时间列表
        resourceUse: list[float] - 对应事件的资源使用量列表
        startCut: float - 时间范围起始截断点(包含)
        endCut: float - 时间范围结束截断点(不包含)
        preloadDone: bool - 是否已完成预处理阶段的标志
        doingPreload: bool - 是否正在执行预处理的标志

        返回值:
        tuple(list, list) - 包含两个元素的元组：
            1. 处理后的时间戳样本列表
            2. 对应时间点的资源使用量样本列表
        """
        self.sampleTimeStamp=[]
        self.sampleUse=[]
            
        if (not preloadDone):
            self.endingJobsTime=[]
            self.endingJobsUse=[]

            self.currentUse=0

        first_stamp_registered=False
        steps=float(len(timeStamps))
        step=0.0
        for time, jobDuration, jobUse in zip(timeStamps, durations, 
                                             resourceUse):
            if (startCut!=None and time<startCut):
                continue
            if preloadDone and not first_stamp_registered:
                if startCut!=time:
                    self.changeUse(startCut, 0, True)
                first_stamp_registered=True
            if (endCut!=None and time>=endCut):
                break
            if (time<1):
                continue
            percent=(step/steps*100)
            if (percent%5.0==0.0):
                print "Progress: "+str( percent)+"%"
                
            
            time=long(time)
            jobDuration=long(jobDuration)
            jobUse=long(jobUse)
            
            
            
            self.procesEndingJobs(time, doRegister=not doingPreload)
            self.processStartingJob(time, jobDuration, jobUse, 
                                    doRegister=not doingPreload)
            step+=1.0
        if not doingPreload:
            self.procesEndingJobs(endCut)
            if endCut is not None and self.sampleTimeStamp[-1]!=endCut:
                self.changeUse(endCut, 0, True)
        return self.sampleTimeStamp, self.sampleUse

def _apply_deltas_usage(stamps_list, usage_list, stamps, usage, neg=False):
    """应用增量使用量数据到基础使用量序列
    将分段的时间戳区间和使用量增量，叠加到现有的连续时间戳使用量数据上。
    支持对增量值取负数操作，可用于反向修正场景。
    Args:
        stamps_list: list[float] - 现有时间戳序列（有序且连续的基础时间轴）
        usage_list: list[float] - 对应stamps_list各时间点的累计使用量
        stamps: list[float] - 增量区间的分界时间戳序列（需为连续递增序列）
        usage: list[float] - 对应stamps区间段的使用量增量值列表
        neg: bool - 是否对usage增量值取负数，默认为False
    Returns:
        tuple[list, list] - 更新后的(stamps_list, usage_list)元组
    Note:
        会直接修改输入的stamps_list和usage_list，返回值与输入为同一对象
    """
    """Applies a list of usage deltas over a list of absolute usage values."""
    if neg: 
        usage = [-x for x in usage]
    for (st_1, st_2, us) in zip(stamps[:-1], stamps[1:], usage):
        pos_init = bisect.bisect_left(stamps_list, st_1)
        pos_end = bisect.bisect_left(stamps_list, st_2)
        if _create_point_between(pos_init, st_1, stamps_list, usage_list):
            pos_end+=1
        if _create_point_between(pos_end, st_2, stamps_list, usage_list):
            pos_end+=1
        for pos in range(pos_init, pos_end):
            usage_list[pos]+=us       
    usage_list[pos_end-1]+=usage[-1]            
    return stamps_list, usage_list 

def _create_point_between(pos, key, key_list, value_list):
    """在有序键值列表的指定位置插入新节点，保持数值连续性
    当目标位置不存在相同键时，在key_list和value_list的pos位置插入新键值。
    新节点的值会继承前驱节点的值（若存在前驱节点则取前值，否则为0）
    Args:
        pos: int - 需要插入的候选位置索引
        key: Any - 需要插入的新键值
        key_list: List - 维护有序键值的列表（将被原地修改）
        value_list: List - 对应键的数值列表（将被原地修改）
    Returns:
        bool - 返回是否成功执行插入操作
    """
    if (pos==(len(key_list)) or key!=key_list[pos]):
        prev=0
        if pos>0:
            prev=value_list[pos-1]
        key_list.insert(pos, key)
        value_list.insert(pos, prev)
        return True
    return False



