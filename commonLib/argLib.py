import getopt
import sys

def getFileName(fileName, viewName):
    if (fileName=="auto"):
        fileName=viewName+"UtilizationPlot"
    return fileName
    
def getDir(dirBase, hostname):
    return dirBase+"-"+hostname
def getDirParams(dirBase, params):
    for par in params:
        if par!=-1:
            dirBase+="-"+str(par)
    return dirBase
def getViewName(name, hostname, startYear,startMonth, startDay, stopYear, stopMonth, stopDay):
    if (name=="auto"):
        viewName=hostname+"-"
        for a in [startYear,startMonth, startDay, stopYear, stopMonth, stopDay]:
            if a!="" and a!=-1:
                viewName+=str(a)+"-"
    else:
        viewName=name
    return viewName

def addScore(list, double=False):
    char="-"
    if double:
        char="--"
    
    return [char+x for x in list]
def addPost(list):
    
    char="="

    
    return [x+char for x in list]
def summaryParameters(outActions, outputValues, parameterActions):
    """
    生成参数字符串，将不同参数源合并为特定格式的字符串
    参数:
        outActions (dict): 需要添加双横线前缀的参数键值对字典
        outputValues (list): 普通参数值列表，与parameterActions配合使用
        parameterActions (list): 普通参数键列表，与outputValues配合使用
    返回:
        str: 格式为" --key1 val1, --key2 val2, key3 val3..."的参数字符串
    """
    cad=""
    for key in outActions.keys():
        if (cad!=""):
            cad+=", "
        cad+="--"+key+" "+str(outActions[key])
    for (key, val) in zip(parameterActions,outputValues):
        if (cad!=""):
            cad+=", "
        cad+=key+" "+str(val)
    return cad
        
        

# singleActions是一个不带参数的选项列表
# parameterActions是一个需要额外参数的选项列表
# defaultParameters是参数Actions的默认值列表
#  
def processParameters(argv, singleActions, singleLetters, parameterActions, parameterLetters, defaultParameters):
    """
    处理命令行参数并解析为结构化数据
    参数:
        argv: list               命令行参数列表(sys.argv)
        singleActions: list      不需要参数的完整形式参数名列表(如:['help'])
        singleLetters: list      不需要参数的简写参数名列表(如:['h'])
        parameterActions: list   需要参数的完整形式参数名列表(如:['output'])
        parameterLetters: list   需要参数的简写参数名列表(如:['o'])
        defaultParameters: list  带默认值的参数默认值列表(与parameterActions顺序对应)
    返回值:
        tuple: (outActions, outputValues)
        outActions: dict         布尔值字典，标记无参数选项是否被设置
        outputValues: list       带参数选项的实际值列表(保持与parameterActions相同顺序)
    """
    try:
        allLetters=":".join(parameterLetters)
        if (allLetters!=""):
            allLetters+=":"
        allLetters+="".join(singleLetters)
        #print allLetters
        allWords=addPost(parameterActions)+singleActions
        #print allLetters, allWords
        
        opts, args = getopt.getopt(sys.argv[1:], allLetters, \
                                   allWords)
                    
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        sys.exit(2)
    
    singleActions=addScore(singleActions, True)
    singleLetters=addScore(singleLetters)
    parameterActions=addScore(parameterActions, True)
    parameterLetters=addScore(parameterLetters)
    
    
    outActions= {}
    for key in singleActions:
        outActions[key[2:]]=False
    outputValues=defaultParameters
    
    #print opts
    #print args

    for o, a in opts:
        if o in singleActions or o in singleLetters:
            index=-1
            if o in singleActions:
                index=singleActions.index(o)
            elif o in singleLetters:
                index=singleLetters.index(o)
            nameAction=singleActions[index]
            outActions[nameAction[2:]]=True
        else:
            index=-1
            if o in parameterActions:
                index=parameterActions.index(o)
            elif o in parameterLetters:
                index=parameterLetters.index(o)
            #print "KK", index
            if (index==-1):
                print "unknown parameter: "+o
                exit(2)
            else:
                outputValues[index]=type(outputValues[index])(a)
    print "Input Args: "+summaryParameters(outActions, outputValues, parameterActions)
    return outActions, outputValues
            