# /usr/bin/python
# -*- coding=UTF-8 -*-


import sys
import MySQLdb
import argparse
import re


class argpar:
    def __init__(self):
        pass

    def getpar(self):
        # 获取源数据库参数
        parser = argparse.ArgumentParser()
        parser.add_argument("--s_hostname", type=str, help="input mysql s_hostname")
        parser.add_argument("--s_username", type=str, help="input mysql s_username")
        parser.add_argument("--s_password", type=str, help="input mysql s_password")
        parser.add_argument("--s_port", type=int, default=3306, help="input mysql s_port")
        parser.add_argument("--s_dbname", type=str, help="input mysql s_dbname")
        # 获取目标数据库参数
        parser.add_argument("--d_hostname", type=str, help="input mysql d_hostname")
        parser.add_argument("--d_username", type=str, help="input mysql d_username")
        parser.add_argument("--d_password", type=str, help="input mysql d_password")
        parser.add_argument("--d_port", type=int, default=3306, help="input mysql d_port")
        parser.add_argument("--d_dbname", type=str, help="input mysql d_dbname")

        args = parser.parse_args()
        print(type(args.s_password))
        return args.s_hostname, args.s_username, args.s_password, args.s_port, args.s_dbname, args.d_hostname, \
               args.d_username, args.d_password, args.d_port, args.d_dbname


class get_conn():
    def __init__(self):
        pass

    def conn_mysql(self, hostname, username, password, dbname, port, charset):
        # db = MySQLdb.connect(hostname,username,dbname,charset)
        self.hostname = hostname
        self.username = username
        self.password = password
        self.dbname = dbname
        self.port = port
        self.charset = charset
        # print(self.hostname)
        db_conn = MySQLdb.connect(self.hostname, self.username, self.password, self.dbname, self.port, self.charset)
        cursor = db_conn.cursor()
        # cursor.execute("SELECT VERSION()")
        # data = cursor.fetchone()
        # print "Database version : %s " % data
        # db.close()a
        return cursor, db_conn, self.dbname


class check_data:
    def __init__(self, s_cursor, s_db_conn, s_dbname, d_cursor, d_db_conn, d_dbname):
        self.s_cursor = s_cursor
        self.s_db_conn = s_db_conn
        self.s_dbname = s_dbname
        self.d_cursor = d_cursor
        self.d_db_conn = d_db_conn
        self.d_dbname = d_dbname

    def check_tables_num(self):

        # self.s_cursor.execute("select count(1) from information_schema.tables where table_schema='{s_dbname}'".format(
        self.s_cursor.execute("select table_name from information_schema.tables where table_schema='{s_dbname}'".format(
            s_dbname=self.s_dbname))
        s_data = self.s_cursor.fetchall()
        self.s_data_new = []
        for i in s_data:
            self.s_data_new.append(i[0].lower())

        print("源库所包含表列表："+str(self.s_data_new))
        # self.s_db_conn.close()


        self.d_cursor.execute("select table_name from information_schema.tables where table_schema='{d_dbname}'".format(
            d_dbname=self.d_dbname))
        d_data = self.d_cursor.fetchall()
        self.d_data_new = []
        for i in d_data:
            self.d_data_new.append(i[0].lower())

        print("目标库所包含表列表：" + str(self.d_data_new))
        # self.d_db_conn.close()


        if cmp(self.s_data_new,self.d_data_new) == 0:

            print("The tables of the database is equal")
        else:
            print("Attention,The tables of  database is not equal")


    def check_tables_structure(self):

        s_strc_hash=[]
        d_strc_hash=[]
        self.s_data_new.sort()
        self.d_data_new.sort()

        # print self.s_data_new
        # print self.d_data_new


        for i in self.s_data_new:

            self.s_cursor.execute("show create table %s" % i)
            strc_table = self.s_cursor.fetchone()
            strc_table_str = re.sub('\n|\s|`', '', strc_table[1]).lower()
            strc_table_str_hash = hash(strc_table_str)
            # print(strc_table_str_hash)
            s_strc_hash.append(strc_table_str_hash)

        for i in self.d_data_new:
            self.d_cursor.execute("show create table %s" % i)
            strc_table = self.d_cursor.fetchone()
            strc_table_str = re.sub('\n|\s|`', '', strc_table[1]).lower()
            strc_table_str_hash = hash(strc_table_str)
            # print(strc_table_str_hash)
            d_strc_hash.append(strc_table_str_hash)

        if cmp(s_strc_hash,d_strc_hash):
            print("The tables in the two databases has the same structrue")
        else:
            print("Attention,The tables in the two databases compared fail")


    def check_table_rows(self):
        pass





if __name__ == '__main__':
    args_m = argpar()
    s_hostname, s_username, s_password, s_port, s_dbname, d_hostname, d_username, d_password, d_port, d_dbname = args_m.getpar()
    # dbname=sys.argv[1]
    s = get_conn()
    s_cursor, s_db_conn, s_dbname = s.conn_mysql(s_hostname, s_username, s_password, s_dbname, s_port, 'utf-8')
    print("连接主库成功")

    d = get_conn()
    d_cursor, d_db_conn, d_dbname = d.conn_mysql(d_hostname, d_username, d_password, d_dbname, d_port, 'utf-8')
    print("连接从库成功")

    check_data = check_data(s_cursor, s_db_conn, s_dbname, d_cursor, d_db_conn, d_dbname)

    check_data.check_tables_num()
    check_data.check_tables_structure()



