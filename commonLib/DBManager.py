import MySQLdb as mdb
from commonLib.Logging import *
import gc
from commonLib import tunnelLib
import datetime


class DB:
    hostName = "localhost"
    dbName = "nersc"
    userName = "nersc"
    password = "nersc"
    port = "3306"

    #	con=False

    def __init__(self, hostName, dbName, userName, password, port="3306", useTunnel=False):
        """
        初始化数据库连接对象
        Parameters:
        hostName (str): 数据库服务器主机名或IP地址
        dbName (str): 要连接的数据库名称
        userName (str): 数据库登录用户名
        password (str): 数据库登录密码
        port (str): 数据库服务器端口号，默认3306（MySQL默认端口）
        useTunnel (bool): 是否启用SSH隧道连接，默认False（直接连接）
        """
        self.con = False
        self.hostName = hostName;
        self.dbName = dbName;
        self.userName = userName;
        self.password = password;
        self.port = port
        self.useTunnel = useTunnel
        if (useTunnel):
            self.tunnel = tunnelLib.Tunnel()
        self._in_transaction = False
        self._cursor = None

    def start_transaction(self):
        """
        开启数据库事务
        方法说明:
        - 建立数据库事务环境，包括连接验证、关闭自动提交、获取游标对象
        - 当连接失败时会抛出包含错误信息的异常
        异常:
        - 当数据库连接不可用时抛出Exception异常
        流程:
        1. 检查数据库连接状态
        2. 关闭自动提交模式
        3. 创建数据库游标
        4. 设置事务状态标志
        """
        if not self.connect():
            raise Exception("Cannot connect at transaction start")
        self.con.autocommit(False)
        self.get_cursor()
        self._in_transaction = True

    def end_transaction(self):
        """
        结束当前数据库事务并释放相关资源。
        该方法执行以下操作：
        1. 尝试提交当前事务，若提交失败则打印错误信息（注意不会执行回滚操作）
        2. 更新内部事务状态标志为False
        3. 断开数据库连接
        4. 关闭游标对象并释放相关资源
        异常处理：
            - 捕获MySQL数据库错误(mdb.Error)并打印提交失败信息
        返回值：
            None
        """
        try:
            # self.con.rollback()
            self.con.commit()
        except mdb.Error as e:
            print("COMMIT FAILED!!", e)
        self._in_transaction = False
        self.disconnect()
        self._cursor.close()
        self._cursor = None

    def get_cursor(self):
        """获取数据库游标实例
        根据事务状态决定返回现有游标或创建新游标。当不处于事务中或游标未初始化时，
        会创建新的数据库游标实例；若已存在有效游标则直接返回
        Returns:
            Cursor: 数据库连接游标对象，用于执行SQL语句和获取查询结果
        """
        if not self._in_transaction or self._cursor == None:
            self._cursor = self.con.cursor()
        return self._cursor

    def connect(self):
        if self._in_transaction:
            return True
        # print "DB", self.port
        if (self.useTunnel):
            self.tunnel.connect()
        try:
            self.con = mdb.connect(self.hostName, self.userName, self.password, self.dbName, port=int(self.port))

            return True
        except mdb.Error as e:
            Log.log("Error %d: %s" % (e.args[0], e.args[1]))
            if (self.useTunnel):
                self.tunnel.disconnect()
            return False

    def disconnect(self):
        if (self.con == False or self._in_transaction):
            return
        self.con.close()
        self.con = False
        if (self.useTunnel):
            self.tunnel.disconnect()

    def date_to_mysql(self, my_date):
        return my_date.strftime('%Y-%m-%d %H:%M:%S')

    def date_from_mysql(self, my_date):
        return datetime.datetime.strptime(my_date, "%b %d %Y %H:%M")

    def doQuery(self, query):
        # 执行数据库查询并返回结果
        # print "QUERY:"+query
        rows = []
        if self.connect():
            try:
                cur = self.get_cursor()
                cur.execute(query)
                rows = cur.fetchall()
            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0], e.args[1]))
                rows = False
            self.disconnect()
        return rows

    # 同样的，但结果是一个字典列表
    def doQueryDic(self, query):

        rows = []
        # print "QUERY:"+query
        if self.connect():
            try:
                cur = self.con.cursor(mdb.cursors.DictCursor)
                cur.execute(query)
                rows = cur.fetchall()

            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0], e.args[1]))
                rows = False
            self.disconnect()
        return rows

    def delete_rows(self, table, id_field, id_value, like_field=None,
                    like_value=None):
        """
        根据条件删除数据库表中的记录
        Args:
            table: str 目标表名
            id_field: str 用于精确匹配的条件字段名
            id_value: int/str 精确匹配的字段值
            like_field: str, optional 用于模糊匹配的条件字段名
            like_value: str, optional 模糊匹配的字段值（需包含SQL通配符）
        Returns:
            int/None 数据库操作影响的行数，具体类型取决于doUpdate实现
        """
        query = "DELETE FROM `{0}` where `{1}`={2}".format(table, id_field,
                                                           id_value)
        if like_field is not None:
            query += """ and `{0}` like "{1}" """.format(like_field, like_value)
        return self.doUpdate(query)

    def doUpdate(self, update, get_insert_id=False):
        ok = True
        insert_id = None
        if self.connect():
            try:
                cur = self.get_cursor()

                res = cur.execute(update)
                if get_insert_id:
                    insert_id = self.con.insert_id()
                if not self._in_transaction:
                    self.con.commit()


            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0], e.args[1]))
                ok = False
            self.disconnect()
        return ok, insert_id

    def doUpdateMany(self, query, values):
        ok = True
        if self.connect():
            try:
                cur = self.get_cursor()

                res = cur.executemany(query, values)
                if not self._in_transaction:
                    self.con.commit()

            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0], e.args[1]))
                ok = False
            self.disconnect()
        return ok

    def doUpdateParams(self, update, params):
        ok = True
        if self.connect():
            try:
                cur = self.get_cursor()

                res = cur.execute(update, params)
                if res == 0:
                    ok = False
                if not self._in_transaction:
                    self.con.commit()
            except mdb.Error as e:
                print
                "EEEEERRRRRROOOOOOORRRR", e
                Log.log("Error %d: %s" % (e.args[0], e.args[1]))
                ok = False
            self.disconnect()
        return ok

    def q(self, cad):
        return "'" + (cad) + "'"

    def insertListValues(self, table, fields, valuesList):
        for values in valuesList:
            self.insertValues(table, fields, values)

    def concatFields(self, fields, isText=False, commas=False):
        """
        将字段列表拼接为字符串，支持字段转义和格式处理
        Args:
            fields: list 需要拼接的字段列表
            isText: bool 是否对字段值进行文本转义（调用self.q方法）
            commas: bool 是否用反引号包裹字段名
        Returns:
            str: 拼接后的查询字符串，字段间用逗号分隔
        """
        first = True
        query = ""
        for field in fields:
            if not first:
                query += ","
            first = False
            if (commas):
                field = "`" + str(field) + "`"
            if (isText):
                query += self.q(str(field))
            else:
                query += str(field)
        return query

    def cleanFields(self, fieldList, isText=False):
        newList = []
        for field in fieldList:
            f = ""
            if (isText):
                f = self.q(str(field))
            else:
                f = str(field)
            newList.append(f)
        return newList

    def doInsertQueryMany(self, table, fields, values):
        query = "INSERT INTO " + table + " ("

        query += self.concatFields(fields, commas=True)
        query += ") VALUES("

        query += ", ".join(["%s"] * len(values))
        query += ")"
        print
        "Q:" + query
        return query

    def insertValues(self, table, fields, values, get_insert_id=False):
        """
        向指定表中插入数据，并可选择是否获取插入数据的自增ID。
        参数:
        - table: 字符串，指定要插入数据的表名。
        - fields: 列表，包含所有字段名的字符串。
        - values: 列表，包含与fields参数中字段对应的值。
        - get_insert_id: 布尔值，指示是否需要获取插入数据的自增ID，默认为False。
        返回:
        - ok: 布尔值，表示插入操作是否成功。
        - insert_id: 如果get_insert_id为True，则返回插入数据的自增ID；否则返回None。
        """
        # 初始化SQL查询字符串
        query = "INSERT INTO `" + table + "` ("
        # 拼接字段名
        query += self.concatFields(fields)
        query += ") VALUES("

        # 拼接字段值，第二个参数指示是否需要添加引号
        query += self.concatFields(values, True)
        query += ")"
        # 执行更新操作，获取执行结果和插入ID
        ok, insert_id = self.doUpdate(query, get_insert_id=get_insert_id)
        return ok, insert_id

    def insertValuesColumns(self, table, columns_dic, fixedFields=None):
        queryList = []
        query = ""
        count = len(columns_dic.values()[0])
        column_keys = columns_dic.keys()
        keys = fixedFields.keys() + columns_dic.keys()
        for i in range(count):
            values = (fixedFields.values() +
                      [columns_dic[x][i] for x in column_keys])
            if (query == ""):
                query = self.doInsertQueryMany(table, keys, values)
            queryList.append(tuple(self.cleanFields(values, False)))
        self.doUpdateMany(query, queryList)

    def insertValuesMany(self, table, dicList):
        queryList = []
        query = ""
        #            maxLength=0
        for dic in dicList:
            #                print dic.keys()
            #                if (maxLength!=0):
            #                    if maxLength!=len(dic.values()):

            #                        print "problem"
            #                        exit(-1)
            #                maxLength=max(maxLength, len(dic.values()))
            if (query == ""):
                query = self.doInsertQueryMany(table, dic.keys(), dic.values())
            # queryList.append(self.doInsertQueryMany(table, dic.keys(), dic.values()))
            queryList.append(tuple(self.cleanFields(dic.values(), False)))

        self.doUpdateMany(query, queryList)

    def getValuesList(self, table, fields, condition="TRUE"):
        query = "SELECT "
        query += self.concatFields(fields)
        query += " FROM " + table;
        query += " WHERE " + condition
        # print query
        result = self.doQueryDic(query)
        valuesList = []
        for row in result:
            values = []
            for field in fields:
                values.append(row[field])
            valuesList.append(values)

        return valuesList

    def getValuesDicList(self, table, fields, condition="TRUE", orderBy=None):
        rows = []
        query = "SELECT "
        query += self.concatFields(fields)
        query += " FROM `" + table + "`"
        query += " WHERE " + condition
        if (orderBy != None):
            query += " ORDER BY " + orderBy
        # print query
        # print "QUERY:"+query
        if self.connect():
            try:
                cur = self.con.cursor(mdb.cursors.DictCursor)
                # print "CUR EXECUTE NEXT"
                cur.execute(query)
                # print "CUR fetchall NEXT"
                rows = cur.fetchall()
                cur.close()
                gc.collect()
            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0], e.args[1]))
                rows = False
            self.disconnect()
        return rows

    def getValuesAsColumns(self, table, fields, condition="TRUE", orderBy=None,
                           groupBy=None, no_comma_fields=None, theQuery=None):
        columns = {}
        for field in fields:
            columns[field] = []

        rows = []
        query = "SELECT "
        query += self.concatFields(fields, commas=True)
        if (no_comma_fields):
            if fields:
                query += ","
            query += self.concatFields(no_comma_fields, commas=False)
        query += " FROM " + table;
        if condition != None:
            query += " WHERE " + condition
        if (orderBy != None):
            query += " ORDER BY " + orderBy
        if (groupBy != None):
            query += " GROUP BY " + groupBy
        # print query
        if theQuery:
            query = theQuery
        # print "BIG QUERY:"+query
        if self.connect():
            try:
                cur = self.con.cursor(mdb.cursors.DictCursor)
                cur.execute(query)
                rows = cur.fetchall()
                for row in rows:
                    for field in fields:
                        columns[field].append(row[field])
                cur.close()
                gc.collect()
            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0], e.args[1]))
                rows = False
            self.disconnect()
        return columns

    def getValuesDicList_LowMem(self, table, fields, condition="TRUE", orderBy="None"):
        rows = []
        query = "SELECT "
        query += self.concatFields(fields)
        query += " FROM " + table;
        query += " WHERE " + condition
        if (orderBy != None):
            query += " ORDER BY " + orderBy
        # print query
        # print "QUERY:"+query
        if self.connect():
            try:
                cur = self.con.cursor(mdb.cursors.DictCursor)
                print
                "EXECUTE"
                cur.execute(query)
                print
                "AFTER EXECUTE"
                return cur
            # rows=cur.fetchall()
            # cur.close()
            # gc.collect()
            except mdb.Error as e:
                Log.log("Error %d: %s" % (e.args[0], e.args[1]))
                rows = False
        # self.disconnect()
        return None

    def close_LowMem(self, cur):
        print
        "CLOSING ALL SQL ELEMENTS"
        cur.close()
        self.disconnect()
        print
        "CLOSED ALL SQL ELEMENTS"

    #            gc.collect
    @classmethod
    # def copyTable(self, origDb, dstDb,table, fields, condition="TRUE"):
    #	valuesList=origDb.getValuesList(table, fields, condition)
    #	dstDb.insertListValues(table, fields, valuesList)

    def copyTable(self, origDb, dstDb, table, fields, condition="TRUE", extraFields=[], extraValues=[]):
        valuesList = origDb.getValuesList(table, fields, condition)
        if (extraFields != []):
            fields = fields + extraFields;
            temp = []
            for values in valuesList:
                values = values + extraValues
                temp.append(values)
            valuesList = temp
            print
            fields
            print
            values
        dstDb.insertListValues(table, fields, valuesList)

    def dumpFileOnDB(self, file, table, field, idField, id):
        content = ""
        file = openReadFile(file)
        content = file.read()
        file.close()
        query = "UPDATE " + table + " SET " + field + "= %s" + " WHERE " + idField + "=" + self.q(id)
        print
        query
        return self.doUpdateParams(query, [content])

    def setFieldOnTable(self, table, field, fieldValue, idField, idValue,
                        extra_cond="", no_commas=False):
        values_list = []
        if no_commas:
            query = ("UPDATE " + table + " SET " + field + "= {0}".format(fieldValue) +
                     " WHERE " + idField + "=" + self.q(idValue) + " " + extra_cond)
        else:
            query = ("UPDATE " + table + " SET " + field + "= %s" +
                     " WHERE " + idField + "=" + self.q(idValue) + " " + extra_cond)
            values_list.append(fieldValue)
        return self.doUpdateParams(query, values_list)

    def restoreFieldToFileFromDB(self, file, table, field, idField, id):
        content = self.retoreFieldToStringFromDB(table, field, idField, id)
        if content != "":
            file = openWriteFile(file)
            file.write(content)
            file.close()
            return True
        return False

    def retoreFieldToStringFromDB(self, table, field, idField, id):
        query = "SELECT " + field + " FROM " + table + " WHERE " + idField + "=" + self.q(id)
        rows = self.doQueryDic(query)
        for row in rows:
            return row[field]
        return ""
