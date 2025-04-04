"""使用SSH作为通信通道的remote.api. clientchannel类的实现。它使用scp和直接ssh执行。"""



import subprocess
from getpass import getuser

class SSH(object):

    def __init__(self, hostname, username=None, password=None):
        self._hostname = hostname;
        self._username=username
        if self._username is None:
            self._username = getuser()
 
    def push_file(self, origin_route, dest_route):
        """
        通过SCP协议将本地文件推送到远程服务器
        参数:
        origin_route (str): 需要传输的本地源文件路径
        dest_route (str): 目标服务器上的存储路径(绝对路径)
        返回值:
        bool: 传输成功返回True，失败返回False
        """
        # 构造SCP命令行参数列表
        command_list =  ["scp", origin_route, self._username + "@" +
                         self._hostname + ":" + dest_route]
        #print command_list
        # 执行子进程并等待完成
        p = subprocess.Popen(command_list, stdout=subprocess.PIPE)
        output, err = p.communicate()
        rc = p.returncode
        # 处理非零返回码的错误情况
        if (rc!=0):
            print "File push operation error", output, err
        return rc == 0
    
    def retrieve_file(self, origin_route, dest_route):
        """
        通过SCP协议从远程服务器检索文件到本地
        参数:
        origin_route (str): 远程服务器上的源文件路径，格式应符合SCP路径规范
        dest_route (str): 本地目标路径，用于保存下载的文件
        返回值:
        bool: 操作是否成功，True表示成功，False表示失败
        """
        command_list =  ["scp", self._username + "@" +
                 self._hostname + ":" + origin_route,  dest_route]
        #print command_list
        p = subprocess.Popen(command_list, stdout=subprocess.PIPE)
        output, err = p.communicate()
        rc = p.returncode
        if (rc!=0):
            print "File retrieve operation error", output, err
        return rc == 0
    
    def delete_file(self, route):
        output, err, rc=self.execute_command("/bin/rm", [route])
        if rc!=0:
            print "File delete operation error", output, err
        return rc==0
        
        
    def execute_command(self, command, arg_list=[], keep_env=False,
                        background=False):
        """
        通过SSH远程执行命令并返回结果
        Args:
            command (str): 要执行的主命令
            arg_list (list): 命令参数列表，默认为空列表
            keep_env (bool): 是否继承当前环境变量（暂未实现）
            background (bool): 是否以后台模式运行命令
        Returns:
            tuple: 包含三个元素的元组:
                - output (bytes): 命令的标准输出内容
                - err (str): 错误信息字符串
                - rc (int): 命令的返回状态码
        注意:
            arg_list参数使用可变默认值，调用时应当注意潜在风险
        """
        output=None
        err=""
        if background:
            command_list = ["ssh", self._username+"@"+self._hostname, 
                        "nohup", command] + arg_list
            p = subprocess.Popen(command_list)
        else:
            command_list = ["ssh", self._username+"@"+self._hostname, 
                        command] + arg_list
                        
            p = subprocess.Popen(command_list, stdout=subprocess.PIPE)
            output, err = p.communicate()
        if err is None:
            err=""
        rc = p.returncode
        return output, err, rc
    
    def get_home_dir(self):
        return self._home_dir
    

