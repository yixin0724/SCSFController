import re
import time
from commonLib import nerscRadar as nr

class TaskRecord:

#    stepid
#    jobname
#    owner
#    account
#    jobtype
#    cores_per_node
#    numnodes
#    class
#    status
#    dispatch
#    start
#    completion
#    queued
#    wallclock
#    mpp_secs
#    wait_secs
#    raw_secs
#    superclass
#    wallclock_requested
#    hostname
#    memory
#    created
#    refund
#    tasks_per_node

# 全局变量:
#   userNames (dict): 用户名字典，用于存储或映射用户信息（当前为空）
#   fieldNames (list): 目标字段名称列表，定义最终输出的字段结构
#   subfieldIndexes (list): 原始数据字段索引列表，对应fieldNames的数据来源字段名
#   timeStampFields (list): 时间戳类型字段列表，这些字段需要特殊时间格式处理
#   numericFields (list): 数值类型字段列表，需要转换为数值类型
#   simpleTextFields (list): 直接文本映射的目标字段列表
#   simpleTextIndexes (list): 简单文本字段在原始数据中的对应字段名
#   simpleTextIndexesMoab (list): Moab系统中简单文本字段的索引位置列表
#   simpleNumericFields (list): 直接数值映射的目标字段列表
#   simpleNumericIndexes (list): 数值字段在原始数据中的对应字段名
#   simpleNumericIndexesMoab (list): Moab系统中数值字段的索引位置列表
#   simpleNumericFieldsMoab (list): Moab系统中数值字段的目标字段名
#   specialFields (list): 需要特殊处理逻辑的字段列表
#   timeCountFields (list): 时间计量字段列表（最终输出用）
#   timeCountIndexes (list): 时间计量字段在原始数据中的对应字段名
#   memFields (list): 内存相关字段列表（最终输出用）
#   memIndexes (list): 内存相关字段在原始数据中的对应字段名
#   emptyFields (list): 保留但当前无数据源的字段列表
#   carverNodeField (str): Carver系统特有的节点字段名
#   valuesDic (dict): 数据存储字典，用于存放处理后的字段数据（当前为空）
    
    userNames={}

    # 定义最终输出的字段结构（约27个标准字段）
    fieldNames=["stepid","jobname","owner","account","jobtype","cores_per_node", \
        "numnodes","class","status","dispatch","start","completion","queued", \
        "wallclock","mpp_secs","wait_secs","raw_secs","superclass", \
        "wallclock_requested","hostname","memory","created","refund", \
        "tasks_per_node", "vmemory", "filtered", "classG"]
    
    # PBS系统原始字段名列表，与fieldNames顺序对应
    subfieldIndexes=["jobname","owner","account","jobtype","Resource_List.mppnppn", \
        "Resource_List.mppnodect","queue","Exit_status","etime","start","end","qtime", \
        "resources_used.walltime", \
        "Resource_List.walltime","resources_used.mem","ctime", \
        "resources_used.vmem"]

# 需要转换为时间戳的字段列表
    timeStampFields=["start","completion","queued","dispatch" \
        "created","refund"]

# 需要转换为数值类型的字段列表
    numericFields=["cores_per_node", \
        "numnodes",\
        "wallclock","mpp_secs","wait_secs","raw_secs", \
        "wallclock_requested", "memory", \
        "tasks_per_node", "refund"]

    # 直接w文本映射配置
    simpleTextFields=["jobname","owner","account","class", "status"]
    simpleTextIndexes=["jobname", "owner", "account", "queue", "Exit_status"]
    simpleTextIndexesMoab=[29,7,29,11, 51]

    # 直接数值映射配置
    simpleNumericFields=["cores_per_node", \
        "numnodes", "dispatch","start","completion","queued","created"]
    simpleNumericIndexes=["Resource_List.mppnppn", "Resource_List.mppnodect", \
                          "etime", "start", "end", "qtime", "ctime"]
    simpleNumericIndexesMoab=[\
        24, 13,14,15,12,12,9]
    
    simpleNumericFieldsMoab=[ \
        "numnodes", "dispatch","start","completion","queued","created", "wallclock_requested"]
    
    specialFields=["hostname", "superclass"]
    
    timeCountFields=["wallclock", "wallclock_requested"]
    timeCountIndexes=["resources_used.walltime", "Resource_List.walltime"]
    
    memFields=["memory", "vmemory"]
    memIndexes=["resources_used.mem","resources_used.vmem"]
    
    emptyFields=["jobtype", "mpp_secs", "wait_secs","raw_secs","superclass" ,"refund", \
        "tasks_per_node"]

    carverNodeField="Resource_List.nodes"
    
    valuesDic={}
    def __init(self):
        self.valuesDic={}
    
    def getUser(self, curated=False):
        user=self.getVal("owner")
        if (curated):
            user=TaskRecord.getAltUserName(user)
        return user
    
    @classmethod
    def getAltUserName(self, user):
        if not user in self.userNames.keys():
            self.userNames[user]=len(self.userNames.keys())
        return "FakeUser"+str(self.userNames[user])
        
    
    @classmethod
    def getFields(self):
        return self.fieldNames
    
    def duration(self):
        if "wallclock" in self.valuesDic.keys():
            return self.getVal("wallclock")
        else:
            return self.getVal("completion")-self.getVal("start")+1
        
    def waitTime(self):
        
        return self.getVal("start")-self.getVal("created")+1
    
    def getVal(self, field):
        return self.valuesDic[field]
    def setField(self, field, val):
        self.valuesDic[field]=val
    
    def parseSQL(self, dic):
        self.valuesDic=dic
#        for field in self.fieldNames:
#            setattr(self, field, dic[field])
        return self
    def isFieldDef(self, field):
        return field in self.valuesDic.keys()
    
    def is_long(self, s):
        try:
            long(s)
            return True
        except ValueError:
            return False
    
    def parsePartialLog(self, hostname, line, moabFormat=False):
        if not moabFormat:
            return self.parseNerscLog(hostname, line)
        else:
            return self.parseMoabLog(hostname, line)
    
    def parseMoabLog(self, hostname, line):
#        try:
            self.valuesDic={}
            mainFields=" ".join(line.split(" ")).split()
            self.setField("hostname", hostname)
            self.setField("stepid", "m-"+hostname+"-"+mainFields[3])
            print "Field:"+mainFields[3]
          #  print mainFields
            for (index, field) in zip(self.simpleTextIndexesMoab, self.simpleTextFields):
                self.setField(field, mainFields[index])
            
            for (index, field) in zip(self.simpleNumericIndexesMoab, self.simpleNumericFieldsMoab):
                print index, field, mainFields[index]
                self.setField(field, int(mainFields[index]))
                
            #tasksPerNode=int(mainFields[24])
            tasksPerNode=1
            procsPerTask=int(mainFields[34])
            self.setField("cores_per_node", tasksPerNode*procsPerTask)
            
            for field in self.timeCountFields:
                if field!="wallclock_requested":
                    self.setField(field, self.duration())
            
            
            for field in self.fieldNames:
                if (not self.isFieldDef(field)):
                    self.setField(field, 0)
            
            
            self.cutTextField("owner", 8)
            self.cutTextField("account", 8)
            self.cutTextField("jobname", 64)
            self.setField("filtered", int(self.getFiltered()))
            self.setField("classG", self.getGroupClass())
            #print self.valuesDic

           # exit()
            return True
    #   except:
            print "Error in MOAB  format / parsing: "+line
            return False

    
    def parseNerscLog(self, hostname, line):
       
       # First the id feilds 
        try:
            self.valuesDic={}
            #print line
            line=line.replace(":ppn=", " ppn=")
            #print line
            mainFields=line.split(";")
            if (mainFields[1]!="E"):
                return False;
            self.valuesDic={}
            self.setField("hostname", hostname)
            self.setField("stepid", mainFields[2])
            
            # We extract the fields with data
            #  We fill those that are empty not to have toruble later
            subFieldList=mainFields[3].split(" ")
            subFields={}
            #try:
            #print subFieldList
            for field in subFieldList:
                #print field
                words=field.split("=")
                subFields[words[0]]=words[1]
            #print subFields
            for field in self.subfieldIndexes:
                if (not field in subFields.keys()):
                    subFields[field]=0
            #print subFields
                
            #print "ONE"
            #print subFields
            # We take the fields by types and process them
            for (field, index) in zip(self.simpleTextFields, self.simpleTextIndexes):
                self.setField(field, str(subFields[index]))
            for (field, index) in zip(self.simpleNumericFields, self.simpleNumericIndexes):
                self.setField(field, long(subFields[index]))
            for (field, index) in zip(self.timeCountFields, self.timeCountIndexes):
                text=subFields[index]
                if (text==0):
                    text="00:00:00"
                num=long(0)
         #       print text
                words=text.split(":")
                num+=long(words[0])*3600
                num+=long(words[1])*60
                num+=long(words[2])
                self.setField(field, num)
            for (field, index) in zip(self.memFields, self.memIndexes):
          
                text=subFields[index]
                if (text==0):
                    text="0kb"
         #       print text
                num=long(text.split("kb")[0])
                self.setField(field, num)
            
            # If any fields are empty we put a 0
            for field in self.fieldNames:
                if (not self.isFieldDef(field)):
                    self.setField(field, 0)
                    
            #print "two"
            if ("carver" in hostname):
                nodes=1
                coresPerNode=1
                if (self.carverNodeField in subFields.keys()):
                    text=subFields[self.carverNodeField]
                    #print text
                    if self.is_long(text):
                        nodes=long(text)
                    else:
                        nodes=len(text.split("+"))
            
             
                    
                    if ("ppn" in subFields.keys()):
                        text=subFields["ppn"]
                        words=text.split(":")
                        if (len(words)>0):
                            coresPerNode=long(words[0])
                        nodeType=""
                        if (len(words)>1):
                            nodeType=words[1]
                        self.setField("nodetype", nodeType)
                    else:
                        self.setField("nodetype", "N/A")
                else:
                    self.setField("nodetype", "N/A")
                    
                self.setField("numnodes",nodes)
                self.setField("cores_per_node", coresPerNode)
                       
                
            
            # We cut some fields that may be longer than the ones in the DB
            self.setField("owner", self.getVal("owner").split("@")[0])
            self.cutTextField("owner", 8)
            self.cutTextField("account", 8)
            self.cutTextField("jobname", 64)

            self.setField("filtered", int(self.getFiltered()))
  
            self.setField("classG", self.getGroupClass())

            return True
        except:
            print "Error in format / parsing: "+line
            return False
    #except:
        #    print "Error parsing: "+line
        
    def getFiltered(self):
        queue=self.getVal("class")
        hostname=self.getVal("hostname")
        if "hopper" in hostname:
            return queue in ["bench", "scavenger","system","thruput","xfer"]
        if "carver" in hostname:
            return queue in ["scavenger","dirac_int","dirac_reg","dirac_small","dirac_special","special", "system"]
        if "edison" in hostname:
            return queue in ["bench","special", "system"]
    
    def getGroupClass(self):
       queue=self.getVal("class")
       #print queue
       if queue in ["reg_1hour","reg_big","reg_long","reg_med","reg_short","reg_small","reg_xbig","reg_xlong"]:
           return "regular"
       else:
           return queue
    
            
        
       
            
                    
        
    def cutTextField(self, field, length):
        
        self.setField(field, self.getVal(field)[:length])

    def toMoabFormat(self, reassignClusters=False, centroids=None):
        eventTimeEpoch=self.getVal("completion")
        eventTimeHMS=time.strftime('%H:%M:%S', time.localtime(eventTimeEpoch))
        wallClock=self.getVal("wallclock_requested")
        
        #print reassignClusters
        if reassignClusters:
            newClass=nr.getClusterOfPoint(self.duration(), self.getVal("cores_per_node")*self.getVal("numnodes"), centroids)
            classString="Cluster-"+str(newClass)
        else:
            classString=self.getVal("class")
            
        cad=" ".join([eventTimeHMS, str(eventTimeEpoch), "job", self.getVal("stepid"), \
                     #1-4
                     "JOBEND", str(self.getVal("numnodes")), str(self.getVal("numnodes")), self.getVal("owner"), \
                     #5-8
                     self.getVal("owner"),str(wallClock), "completed", classString, \
                     #9-12
                     str(self.getVal("created")), str(self.getVal("dispatch")), str(self.getVal("start")), str(self.getVal("completion")), \
                     #13-16 These are the times 
                     "-", "-", ">=", "0", \
                     #17-20
                     ">=", "0", "-", str(self.getVal("queued")), \
                     #21-24
                     str(self.getVal("numnodes")), "1", "-", "-", \
                     #25-28
                     #self.getVal("owner"), "executable.cmd", "-", "-1", \
                     self.getVal("owner"), "executable.cmd", "-", "0", \
                     #29-32 Bypass has to be 0 (32)
                     "0", "[DEFAULT]", str(self.getVal("cores_per_node")), "0", \
                     #33-36
                     "0", "0", "0", "0", \
                     #37-40
                     "-", "-", "-", "-", \
                     #41-44
                     "-", "-", "-", "0.0", \
                     #45-48
                     "-", "-", "-", "-", \
                     #49-52
                     #"-","-","-1","-1", \
                     "-","-","-","-", \
                     #53-56
                     "arguments"])
                     #57
                     
        return cad
        
    #def parseFields(fields): 
        # Fileds have to be tuples
        
    
     

    #def toString():
        #this strange Format was used before... maybe to feed GridSim
        #return taskid+' '+str(dispatch-base)+' 1 '+str(end-start)+' '+str(nodes*cores)+' -1 -1 '+str(nodes*cores)+' '+str(wall)+' -1 5 55 -1 -1 0 -1 -1 -1'
    
    def checkTime(self, time, timeRatio):
        t=time/timeRatio
        if t<1:
            t=1
        return t

    def checkRunTime(self, runTime, starTime, now, timeRatio):
        if (timeRatio==1):
            return runTime
        alreadyDone=(now-starTime)
        pending=(runTime-alreadyDone)
        return alreadyDone+self.checkTime(pending, timeRatio)
            
        

    def toSimulatorFormat(self, jobCount, baseTaskNumber, currentTime, \
            timeTransport=None, spoolDir="/opt/moab/spool/", jobNameBase="MoabS.", timeRatio=1, idleJob=False):
        """
        将作业数据转换为模拟器所需的格式字符串
        参数:
        jobCount (int): 作业计数器，用于生成唯一作业名
        baseTaskNumber (int): 任务编号的起始基准值
        currentTime (int): 当前时间戳，用于时间计算
        timeTransport (int, optional): 传输时间偏移量，若存在则用于计算时间偏移
        spoolDir (str): 作业执行目录的存储路径
        jobNameBase (str): 作业名称的基础前缀
        timeRatio (int): 时间缩放比例因子
        idleJob (bool): 标识是否生成空闲作业格式

        返回值:
        tuple: 包含格式字符串和任务数的元组 (格式字符串, 任务数)
               格式字符串为空表示无效作业，任务数为实际生成的任务数量
        """
        offset=0
        if (timeTransport!=None):
            offset=timeTransport-currentTime
        cad=""
        jobName=jobNameBase+str(jobCount)+"-"+self.getVal("stepid")
        start=(self.getVal("start"))
        endTime=(self.getVal("completion"))
        queueTime=(start-100)
        numNodes=self.getVal("numnodes")
        coresPerNode=self.getVal("cores_per_node")
        queue=self.getVal("class")
        user=self.getUser()
        
        createdTime=self.getVal("created")
   
        
        
        limitWC=self.getVal("wallclock_requested")
        numTasks=numNodes
        taskList=[]
        if start==0:
            return "", 0
        
       
        for node in range(baseTaskNumber, baseTaskNumber+numNodes):
            taskList+=[str(node)]*coresPerNode
        if not idleJob:
            cad+=" ".join([\
                jobName,\
                "COMMENT=\"SID=Moab?SJID="+jobName+"=?SRMJID="+jobName+"\"",\
                "EXEC="+spoolDir+"moab.job."+str(jobCount),\
                "GNAME="+user,\
                "IWD=/home/"+user,\
                "QUEUETIME="+str(queueTime+offset),\
                "RCLASS="+queue,\
                #"RUNTIME="+str(self.checkTime(endTime-start,timeRatio)),\
                "RUNTIME="+str(self.checkRunTime(endTime-start, start, currentTime, timeRatio)),\
                "STARTTIME="+str(start+offset),\
                "STATE=Running",\
                #"TASKLIST="+",".join([str(x+baseTaskNumber) for x in range(numTasks)]),\
                "TASKLIST="+",".join(taskList),\
                "TASKS="+str(numTasks*coresPerNode),\
                "TEMPLATE=DEFAULT",\
                "UNAME="+user,\
                "WCLIMIT="+str(self.checkRunTime(limitWC, start, currentTime, timeRatio))])
        else:
            cad+=" ".join([\
                jobName,\
                "COMMENT=\"SID=Moab?SJID="+jobName+"=?SRMJID="+jobName+"\"",\
                "EXEC="+spoolDir+"moab.job."+str(jobCount),\
                "GNAME="+user,\
                "IWD=/home/"+user,\
                "QUEUETIME="+str(createdTime+offset),\
                "RCLASS="+queue,\
                "RUNTIME="+str(self.checkTime(endTime-start,timeRatio)),\
                #"RUNTIME="+str(self.checkRunTime(endTime-start, start, currentTime, timeRatio)),\
                "STATE=Idle",\
                #"STATE=Queued",\
                #"TASKLIST="+",".join([str(x+baseTaskNumber) for x in range(numTasks)]),\
                #"TASKLIST="+",".join(taskList),\
                "TASKS="+str(numTasks*coresPerNode),\
                "TEMPLATE=DEFAULT",\
                "UNAME="+user,\
                "WCLIMIT="+str(self.checkTime(limitWC, timeRatio))])
        
        
        #cad+="\n"
        return cad, numTasks
            
        
        
        