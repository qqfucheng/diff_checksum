# /usr/bin/python
# -*- coding=UTF-8 -*-

import sys
import MySQLdb
import argparse
import re
import math
import threading
import numpy as np
import time


class Argparses(object):
    def __init__(self):
        pass

    @classmethod
    def getvalue(cls):
        # 获取源数据库参数
        parser = argparse.ArgumentParser()
        parser.add_argument("--s_hostname", type=str, help="input mysql s_hostname")
        parser.add_argument("--s_username", type=str, help="input mysql s_username")
        parser.add_argument("--s_password", type=str, help="input mysql s_password")
        parser.add_argument("--s_port", type=int, default=3306, help="input mysql s_port")
        parser.add_argument("--s_database", type=str, help="input mysql s_database")
        parser.add_argument("--s_charset", type=str, help="input mysql s_charset")

        # 获取目标数据库参数
        parser.add_argument("--d_hostname", type=str, help="input mysql d_hostname")
        parser.add_argument("--d_username", type=str, help="input mysql d_username")
        parser.add_argument("--d_password", type=str, help="input mysql d_password")
        parser.add_argument("--d_port", type=int, default=3306, help="input mysql d_port")
        parser.add_argument("--d_database", type=str, help="input mysql d_database")
        parser.add_argument("--d_charset", type=str, help="input mysql d_charset")
        args = parser.parse_args()

        return {'s_hostname': args.s_hostname, 's_username': args.s_username, 's_password': args.s_password, 's_port': args.s_port, 's_database': args.s_database,'s_charset': args.s_charset,'d_hostname': args.d_hostname, 'd_username': args.d_username, 'd_password': args.d_password, 'd_port': args.d_port, 'd_database': args.d_database,'d_charset': args.d_charset}


class DButils(object):

    def __init__(self,hostname,username,password,database,port,charset):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.charset = charset
        self.conn = MySQLdb.connect(self.hostname,self.username,self.password,self.database,self.port,self.charset)
        self.cur = self.conn.cursor()

    def fetchone(self,sql):
        try:
            self.cur.execute(sql)
            return self.cur.fetchone()
        except Exception as err:
            print(err)

    def fetchall(self,sql):
        try:
            self.cur.execute(sql)
            return self.cur.fetchall()
        except Exception as err:
            print(err)

    def create(self,sql):
        pass

    def close_db(self):
        self.cur.close()
        self.conn.close()


class DataChecker(object):
    def __init__(self):
        pass

    def get_table_list(self,hostname,username,password,database,port,charset):

        sdb = DButils(hostname,username,password,database,port,charset)
        sql = "select table_name from information_schema.tables where table_schema='%s'" % database
        # print sql
        table_list = sdb.fetchall(sql)
        sdb.close_db()
        return table_list

    def limit_generate(self,hostname,username,password,database,port,charset,table_name,factor=1):

        if factor == 1:
            check_limit = 100000
        elif factor == 5:
            check_limit = 500000

        sdb = DButils(hostname, username, password, database, port, charset)
        sql = "select count(*) from %s" % table_name
        table_rows = sdb.fetchone(sql)
        # 根据总行数以及比对因子factor，向上取整计算出此表校验所需分组数，如果为负数表示表行数很小，此时取1表示只分一个组
        page_numbers = int(math.ceil(float(table_rows[0])/check_limit)) if int(math.ceil(float(table_rows[0])/check_limit)) > 0 else 1
        pages_limit = []
        for x in range(page_numbers):
            if x == 0:
                pages_limit.append([check_limit*x,check_limit*(x+1)])
            else:
                pages_limit.append([check_limit*x+1,check_limit*(x+1)])

        return pages_limit

    def checksum_sql(self,hostname,username,password,database,port,charset,table_name,pages_limit=0,factor=1):

        sdb = DButils(hostname, username, password, database, port, charset)
        sql = "select column_name,data_type from information_schema.columns where table_schema='{database}' and table_name='{table_name}'".format(database=database, table_name=table_name)
        # print sql
        column_list = sdb.fetchall(sql)
        c_list = []
        d_list = []
        # column_list = self.cursor.fetchall()
        for i in column_list:
            if i[1].lower() == 'int':
                c_list.append(i[0])
            elif i[1].lower() == 'char' or i[1].lower() == 'varchar':
                c_list.append("CONVERT({s} using utf8mb4)".format(s=i[0]))
                d_list.append("ISNULL(%s)" % i[0])
            elif i[1].lower() == 'text':
                c_list.append("CRC32({s})".format(s=i[0]))
            else:
                c_list.append(i[0])
        if pages_limit == 0:
            pages_limit = self.limit_generate(hostname,username,password,database,port,charset,table_name, factor)

        for i in pages_limit:

            if len(d_list) != 0:
                check_sql = "select COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#'" +','+ ','.join(
                    c_list) + ',concat(' + ','.join(
                    d_list) + ')' + '))' + 'as unsigned)),10,16)),0)' + ' from ' + database + '.' + table_name + ' where id >= '+str(i[0])+' and id <='+str(i[1])+';'

                yield check_sql,i
                # return check_sql,i

            else:
                check_sql = "select COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#'" +","+','.join(
                    c_list) + ')' + 'as unsigned)),10,16)),0)' + ' from ' + database + '.' + table_name + ' where id >= '+str(i[0])+'and id <='+str(i[1])+';'

                yield check_sql,i
                # return check_sql, i

    def checksum_strc(self,hostname,username,password,database,port,charset,table_name):
        sdb = DButils(hostname, username, password, database, port, charset)
        strc_table = sdb.fetchone("show create table %s" % table_name)
        # print(strc_table)
        strc_table_hash = hash(re.sub('\n|\s|`', '', strc_table[1]).lower())
        return strc_table_hash

    def checksum_rows(self,hostname,username,password,database,port,charset,sql,limit_pages,table_name):

        sdb = DButils(hostname, username, password, database, port, charset)
        checksum_result = sdb.fetchone(sql)
        # return checksum_result,limit_pages,table_name
        return checksum_result,limit_pages


class Checkthread(threading.Thread):
    def __init__(self,checksum_func,hostname,username,password,database,port,charset,check_sql,limit_pages,table_name):
        # super(Checkthread,self).__init__()
        threading.Thread.__init__(self)
        self.checksum_func = checksum_func
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.charset = charset
        self.check_sql = check_sql
        self.limit_pages = limit_pages
        self.table_name = table_name
        # self.thread_number = thread_number

    def run(self):

        self.result = self.checksum_func(self.hostname,self.username,self.password,self.database,self.port,self.charset,self.check_sql,self.limit_pages,self.table_name)
        # threadlock = threading.Lock()
        # threadlock.acquire()
        # result = self.checksum_func(self.check_sql,self.table_name)
        # time.sleep(60)
        # threadlock.release()
        # print("检查结果:%s" % result[0])
        # print("线程号: %d ,执行结果: %s" % (self.thread_number,result))
        # print(time.ctime())
        # print self.result
        return self.result

    def get_result(self):
        # print self.result
        return self.result



def main():
    args = Argparses.getvalue()
    sdc = DataChecker()
    ddc = DataChecker()
    s_table_list = sdc.get_table_list(args['s_hostname'], args['s_username'], args['s_password'], args['s_database'],args['s_port'],args['s_charset'])
    d_table_list = ddc.get_table_list(args['d_hostname'], args['d_username'], args['d_password'], args['d_database'],
                                      args['d_port'], args['d_charset'])

    print("源表列表: "+str(s_table_list))
    print("目标表列表: "+str(d_table_list))
    for table_name in s_table_list:
        if table_name in d_table_list:
            # print("开始检查表: "+table_name[0])
            s_strc_table_hash = sdc.checksum_strc(args['s_hostname'], args['s_username'], args['s_password'], args['s_database'],args['s_port'],args['s_charset'],table_name[0])
            d_strc_table_hash = ddc.checksum_strc(args['s_hostname'], args['s_username'], args['s_password'], args['s_database'],args['s_port'],args['s_charset'],table_name[0])
            if s_strc_table_hash == d_strc_table_hash:
                print(table_name[0]+" 表结构检查通过!")

                checksum_sql = sdc.checksum_sql(args['s_hostname'], args['s_username'], args['s_password'], args['s_database'],args['s_port'],args['s_charset'],table_name[0], factor=1)
                # thread_number = 1
                threads = []
                for check_sql in checksum_sql:
                    # print check_sql[0],check_sql[1]
                    # threads= []
                    s_Checkthread = Checkthread(sdc.checksum_rows,args['s_hostname'], args['s_username'], args['s_password'], args['s_database'],args['s_port'],args['s_charset'],check_sql[0],check_sql[1],table_name[0])

                    d_Checkthread = Checkthread(ddc.checksum_rows,args['s_hostname'], args['s_username'], args['s_password'], args['s_database'],args['s_port'],args['s_charset'],check_sql[0],check_sql[1],table_name[0])

                    s_Checkthread.start()
                    d_Checkthread.start()
                    threads.append(s_Checkthread)
                    threads.append(d_Checkthread)

                result = []
                for t in threads:
                    t.join()
                    rs = t.get_result()
                    result.append(rs)
                rs = np.reshape(result,(len(result)/2,4))

                for i in range(0,len(rs)):
                    if rs[i][0] == rs[i][2]:
                        print("表" + table_name[0] + "范围" + str(rs[i][1]) + "校验通过")

                    else:
                        print("表" + table_name[0] + "范围" + str(rs[i][1]) + "校验未通过")




                # print(result)
                # for i in result:
                #     print i[0]
                # print(len(result)-1)
                # for i in range(len(result)-1):
                #     print(i,i+1)
                #     i += 1
                    # print result[i+1],result[i+2]

                # print result[0]
                # print result[1]

                # print(str(check_sql[1])+"检查线程执行完毕")

                    # print("表"+table_name[0]+str(check_sql[1])+"范围比较----"+"源表:"+str(s_checksum_result[0])+"目标表:"+str(d_checksum_result[0]))

            else:
                print(table_name[0] + " 表结构检查未通过!")

        else:
            print("源表：" + i + "  在目标实例不存！")

if __name__ == '__main__':

    main()







