import os

def openReadFile(file):
	try:
		f=open(file, "r")
		
		return f
	except:
		print "Can't open file"
		return 0

def openWriteFile(file):
	try:
		f=open(file, "w")
		
		return f
	except:
		print "Can't open file"
		return 0
	

	
def genCSV(list):
	"""
	将列表元素转换为CSV格式的字符串行
	参数:
	    list (iterable): 包含任意类型元素的可迭代对象，元素将被转换为字符串
	返回:
	    str: 由逗号分隔的CSV格式字符串，以换行符结尾
	"""
	cad=""
	first=True
	for word in list:
		if (not first):
			cad+=","
		else:
			first=False
		cad+=str(word)
	cad+="\n"
	return cad

def genCSVFile(list, fw):
	"""
	将列表数据写入CSV格式文件
	遍历输入列表元素，将其转换为逗号分隔值(CSV)格式并写入文件对象。
	每个列表元素会被转换为字符串类型，行末自动添加换行符。
	Args:
	    list (iterable): 需要写入CSV文件的可迭代对象，元素需支持str()转换
	    fw (file object): 已打开的文件写入对象，需具备write()方法
	Returns:
	    None: 无返回值
	"""
	first=True
	for word in list:
		cad=""
		if (not first):
			cad+=","
		else:
			first=False
		cad+=str(word)
		fw.write(cad)
	fw.write("\n")
	
		
def ensureDir(directory):
	if not os.path.exists(directory):
		os.makedirs(directory)