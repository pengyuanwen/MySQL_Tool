#!/bin/env python
# _*_ coding:utf-8 _*_


import sys
import time
import datetime
import getopt
import calendar
import pymysql
import re

class MessageUint(object):

    help = \
"""mysqlpartition.py Tools are used to add and delete MySQL database partition
    table sub partitions

Usage: mysqlpartition.py [OPTIONS] [database]
    -H, --help      Display this help and exit.
    --host=name     Connect to host.
    --port=#        Port number to use for connection or 0 for default to, in
                    order of preference
    --user=name     User for login if not current user.
    --password=[name]
                    Password to use when connecting to server. If password is
                    not given it's asked from the tty.
    --database=name Database to use.
    --table=name    Table to use.
    --add-partition=#
                    Add the partition table, subpartition, and range of
                    values{number day | number month}, exmple : 30day, 1month
    --drop-partition=#
                    Keep partition tables partitioned in recent months, and
                    range of values {number day | number month}
    --start-partition=name
                    Specifies the time partition for the partition table to
start the partition
    --stop=partition=name
                    Specifies the time partition for the end of the partition
table
    --execute       Execute command and quit.
    --print         Print command results

Example:
    mysqlpartition.py --database DBNAME --table TABLENAME --start-partition
    "%Y-%m-%d" --stop-partition "%Y-%m-%d" --partition-type={ month | day }

    mysqlpartition.py --host IP --port PORT --user USER --password PASS
    --database DB --table TABLE --add-partition= NUM{ month | day }
    | --drop-partition= NUM{ month | day } { --print|--exec }"""

    doc_info = "Please run script \"\033[31mmysqlpartition.py --help\033[0m\", see help docuemnts."

    @staticmethod
    def maximum_partition(schema_name, table_name):
        result = "Warnings: The partition table: {0}.{1} maximum partition is greater than the current two month range," \
                 " \033[41;37mand this addition will be ignored.\033[0m".format(schema_name, table_name)
        return result

    print_add_statement = "\nPrint add partition statements :\n------------------------------\n"

    print_del_statement = "\nPrint drop partition statements :\n------------------------------\n"

    @staticmethod
    def color_print(cmds):
        result = '\033[32m{0}\033[0m'.format(cmds)
        return result

    @staticmethod
    def after_exec_info(cmds):
        result = "{0} \033[32mExec Success!\033[0m".format(cmds)
        print result

    @staticmethod
    def begin_add_exec_info(schema_name, table_name):
        result = '\nbegin exec add partition the {0}.{1} SQL statement:\n'.format(schema_name, table_name)
        print result

    @staticmethod
    def begin_del_exec_info(schema_name, table_name):
        result = '\nbegin exec drop partition the {0}.{1} SQL statement:\n'.format(schema_name, table_name)
        print result

    @staticmethod
    def partition_status_info(schema_name, table_name):
        result = 'Warnings: This {0}.{1} is not a partition table!'.format(schema_name, table_name)
        print result

class inlayParas(object):

    def __int__(self):
        pass

    def dict_range_parameter(self,get_dict):
        return get_dict["--database"], get_dict["--table"], get_dict["--partition-type"], get_dict["--start-partition"], \
               get_dict["--stop-partition"]

    def dict_add_extent_parameter(self, dicts):
        return dicts["--socket"], dicts["--user"], dicts["--password"], dicts["--database"], dicts["--table"], dicts[
            "--add-partition"]

    def dict_del_extent_parameter(self, dicts):
        return dicts["--socket"], dicts["--user"], dicts["--password"], dicts["--database"], dicts["--table"], dicts[
            "--drop-partition"]

    def dict_add_del_extent_parameter(self, dicts):
        return dicts["--socket"], dicts["--user"], dicts["--password"], dicts["--database"], dicts["--table"], dicts[
            "--add-partition"], dicts["--drop-partition"]

    parameters_list = ["host=", "port=", "socket=", "user=", "password=", "database=", "table=", "partition-type=",
                       "add-partition=", "drop-partition=", "start-partition=", "stop-partition=",
                       "partition-type=", "exec", "print", "help"]

    range_generator = sorted(["--database", "--table", "--partition-type", "--start-partition", "--stop-partition"])

    print_add_extent_generator = sorted(
        ["--host", "--port", "--user", "--password", "--database", "--table", "--add-partition", "--print"])

    print_del_extent_generator = sorted(
        ["--host", "--port", "--user", "--password", "--database", "--table", "--drop-partition", "--print"])

    exec_add_extent_generator = sorted(
        ["--host", "--port", "--user", "--password", "--database", "--table", "--add-partition", "--exec"])

    exec_del_extent_generator = sorted(
        ["--host", "--port", "--user", "--password", "--database", "--table", "--drop-partition", "--exec"])

    print_add_del_extent_generator = sorted(
        ["--host", "--port", "--user", "--password", "--database", "--table", "--add-partition", "--drop-partition",
         "--print"])

    exec_add_del_extent_generator = sorted(
        ["--host", "--port", "--user", "--password", "--database", "--table", "--add-partition", "--drop-partition",
         "--exec"])

class SqlUint(object):

    query_maximum_partition = "select max(PARTITION_DESCRIPTION) from INFORMATION_SCHEMA.PARTITIONS where " \
                              "TABLE_SCHEMA = %s and TABLE_NAME = %s and PARTITION_NAME is not null"

    query_expire_partition = "select PARTITION_NAME from INFORMATION_SCHEMA.PARTITIONS where TABLE_SCHEMA = %s " \
                             "and TABLE_NAME = %s and PARTITION_DESCRIPTION <= %s and PARTITION_NAME is not null"

    set_lock_timeout = "set lock_wait_timeout=3"

class DB(object):
    # 封装连接数据库对象
    def __init__(self,db_host,db_port,db_user,db_passwd,db_name):
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.db_name = db_name

    # 初始化数据库链接
    def get_conn(self):
        try:
            self.conn = pymysql.Connect(
                host = self.db_host,
                port = int(self.db_port),
                user = self.db_user,
                passwd = self.db_passwd,
                db = self.db_name,
                charset = 'utf8')
        except pymysql.Error as e:
            print e
            exit(11)

    def query(self, sql, val):
        cursor = self.conn.cursor()
        cursor.execute(sql, val)
        result = cursor.fetchall()
        cursor.close()
        return result

    def update(self, sql, val):
        cursor = self.conn.cursor()
        cursor.execute(sql, val)
        self.conn.commit()
        cursor.close()

    def __del__(self):
        if self.db_host is not None or self.db_port is not None:
        #if self.conn is not None:
            self.conn.close()

class DateUtil(object):

    @staticmethod
    def format_time_string(time_string):
        """
        :param time_string:
        :return: '%Y-%m-%d' timeString convert to "%Y%m%d"
        """
        result = time.strftime('%Y%m%d', time.strptime(time_string, '%Y-%m-%d'))
        return result

    @staticmethod
    def format_timestamp(time_string):
        result = time.mktime(time.strptime(time_string, '%Y-%m-%d'))
        return result

    @staticmethod
    def format_timestamp_string(timestamp):
        result =  time.strftime('%Y%m%d', time.localtime(int(timestamp)))
        return result

    @staticmethod
    def getBetweenDay(begin_date, end_date):
        date_list = []
        begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        while begin_date <= end_date:
            date_str = begin_date.strftime("%Y-%m-%d")
            date_list.append(date_str)
            begin_date += datetime.timedelta(days=1)
        return date_list

    @staticmethod
    def timestamp_datetime(timestamp):
        pass

        result = datetime.datetime.fromtimestamp(int(timestamp))
        return result

    @staticmethod
    def timestamp_datetime_1(timestamp):
        result = datetime.datetime.fromordinal(int(timestamp)-365)
        return result



    @staticmethod
    def time_stamp(str_time_stamp):
        result = time.mktime(time.strptime("{0}235959".format(str_time_stamp), '%Y%m%d%H%M%S')) + 1
        return result

    @staticmethod
    def date_time_stamp(str_time_stamp):
        #print "str_time_stamp",str_time_stamp
        str_time_stamp = str_time_stamp[0:4]+"-"+str_time_stamp[4:6]+'-'+str_time_stamp[-2:]
        result = datetime.datetime.strptime(str_time_stamp,'%Y-%m-%d').toordinal() + 365
        return result



    @staticmethod
    def add_month_datetime(timestamp,interval):
        """
        :param timestamp: timestamp type
        :return: datetime type
        """
        result = DateUtil.timestamp_datetime(timestamp) + datetime.timedelta(days=interval)
        return result

    @staticmethod
    def add_month_datetime_1(timestamp,interval):
        """
        :param timestamp: timestamp type
        :return: datetime type
        """
        result = DateUtil.timestamp_datetime_1(timestamp) + datetime.timedelta(days=interval)
        return result


    @staticmethod
    def get_local_times(flag, integer):
        now = datetime.datetime.now()
        delta = datetime.timedelta(days=integer)
        if flag == 'add':
            partition_timestamp = (now + delta)
        else:
            partition_timestamp = (now - delta)
        # datetime convert to timestamp
        result = int(time.mktime(partition_timestamp.timetuple()))
        # datetiem convert to string time
        # string_time = partition_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        return result

    @staticmethod
    def first_last_datetime_strftime(years, months, days):
        result = datetime.date(year=years, month=months, day=days)
        result = DateUtil.datetime_strftime(result)
        return result

    @staticmethod
    def datetime_strftime(date_time):
        return date_time.strftime('%Y-%m-%d')

    @staticmethod
    def cla(str_last_month):
        years = str_last_month.split('-')[0]
        months = str_last_month.split('-')[1]
        monthRange = calendar.monthrange(int(years), int(months))
        total_days = monthRange[1]
        return total_days

    @staticmethod
    def date_time_format(str_time_stamp):
        partition_name = str_time_stamp[:4] + '-' + str_time_stamp[4:6] + '-01'
        calendar_days = DateUtil.cla(partition_name)
        result = datetime.datetime.strptime(partition_name, '%Y-%m-%d').toordinal() + 365 + calendar_days
        return result

class Partition(object):

    def __init__(self, input_parameters):
        self.host = input_parameters.get("--host")
        self.port = input_parameters.get("--port")
        self.user = input_parameters.get("--user")
        self.password = input_parameters.get("--password")
        self.schema_name = input_parameters.get("--database")
        self.table_name = input_parameters.get("--table")
        self.add_partition_type = input_parameters.get("--partition-type")
        self.start_datetime = input_parameters.get("--start-partition")
        self.end_datetime = input_parameters.get("--stop-partition")
        self.add_partition = input_parameters.get("--add-partition")
        self.del_partition = input_parameters.get("--drop-partition")
        self.print_option = input_parameters.get("--print")
        self.exec_option = input_parameters.get("--exec")
        self.db_max_part_timestamp = None
        self.del_partition_type = None
        self.res_add_list = []
        self.res_del_list = []
        self.dicts = input_parameters
        self.mysql = DB(self.host, self.port, self.user, self.password, self.schema_name)

    @staticmethod
    def getParameter():
        receiveParameter = sys.argv[1:]
        program_parameters = inlayParas.parameters_list
        try:
            optList, args = getopt.getopt(receiveParameter, "HVo:", program_parameters)
            paras = dict(optList)
            return paras
        except getopt.GetoptError as e:
            print e
            print MessageUint.doc_info
            exit(1)

    def split_partiton_parameter(self, input_parameter):
        result = re.findall(r'(\d+)(\w+)', input_parameter)
        return result

    def get_first_last_month(self, date_time):
        years = date_time.year
        months = date_time.month
        first_day_weekday, month_range = calendar.monthrange(int(years), int(months))
        first_day = DateUtil.first_last_datetime_strftime(years, months, 1)
        last_day = DateUtil.first_last_datetime_strftime(years, months, month_range)
        return first_day, last_day

    def color_print(self, flag):
        if flag == 1:
            print MessageUint.print_add_statement
            for statements in self.res_add_list:
                print MessageUint.color_print(statements)
        elif flag == 2:
            print MessageUint.print_del_statement
            for del_statemetns in self.res_del_list:
                print MessageUint.color_print(del_statemetns)

    def exec_db_commands(self, cmds_list):
        for sql in cmds_list:
            result = self.mysql.update(sql, None)
            if result is None:
                MessageUint.after_exec_info(sql)
            else:
                # print Error information
                print result

    def add_partition_jude(self):
        if self.add_partition is not None:
            return True

    def del_partition_jude(self):
        if self.del_partition is not None:
            return True

    def exec_db_partition(self):
        if self.print_option == '':
            if self.add_partition_jude():
                self.color_print(1)
            if self.del_partition_jude():
                self.color_print(2)
        elif self.exec_option == '':
            if self.add_partition_jude():
                localtimes = int(DateUtil.get_local_times('add', 1000))
                MessageUint.begin_add_exec_info(self.schema_name, self.table_name)
                if int(self.db_max_part_timestamp) < localtimes:

                    self.exec_db_commands(self.res_add_list)
                else:
                    result = MessageUint.maximum_partition(self.schema_name, self.table_name)
                    print result
            if self.del_partition_jude():
                MessageUint.begin_del_exec_info(self.schema_name, self.table_name)
                self.exec_db_commands(self.res_del_list)

    def generator_del_cmds(self, partition_name):
        result = "alter table {0}.{1} drop partition {2};".format(self.schema_name, self.table_name, partition_name)
        self.res_del_list.append(result)

    def generator_del_db_partition(self):
        if self.del_partition is not None:
            results = self.split_partiton_parameter(self.del_partition)
            partition_interval = results[0][0]
            self.del_partition_type = results[0][1]
            if self.del_partition_type == 'day':
                partition_interval = int(partition_interval) * 1

            else:
                partition_interval = int(partition_interval) * 30
            local_timestamp = DateUtil.get_local_times('minus', partition_interval)
            val = (self.schema_name, self.table_name, local_timestamp)
            result = self.mysql.query(SqlUint.query_expire_partition, val)
            if len(result) > 0:
                for i in result:
                    self.generator_del_cmds(i[0])
            #self.db_partition_exec_type()
            self.exec_db_partition()

    def generator_add_cmds(self, partition_name, format_time_stamp):
        result = "alter table {0}.{1} add partition (partition p{2} VALUES LESS THAN ({3}));".format(self.schema_name,
                                                                                                     self.table_name,
                                                                                                     partition_name,
                                                                                                     int(
                                                                                                         format_time_stamp))
        self.res_add_list.append(result)

    def partition_filter(self, subpartition_datetime, partition_name, format_datetime):
        subpartition_datetime = int(DateUtil.format_time_string(subpartition_datetime))
        start_datetime = int(DateUtil.format_time_string(self.start_datetime))
        end_datetime = int(DateUtil.format_time_string(self.end_datetime))

        # print "subpartition_datetime:",subpartition_datetime
        # print "start_datetime:",start_datetime
        # print "end_datetime:",end_datetime

        if subpartition_datetime >= start_datetime and subpartition_datetime <= end_datetime:
            self.generator_add_cmds(partition_name, format_datetime)

    def partition_table_generator(self, begin_month, last_month):
        str_last_month = DateUtil.format_time_string(last_month)
        if self.add_partition_type == 'month':
            if len(self.db_max_part_timestamp) > 9:
                partition_name = str_last_month[:-2]

                format_datetime = DateUtil.time_stamp(str_last_month)
                self.partition_filter(begin_month, partition_name, format_datetime)
            else:
                partition_name = str_last_month[:4] + '-' + str_last_month[4:6] + '-01'
                partition_name = partition_name.replace('-', '')
                format_datetime = DateUtil.date_time_format(str_last_month)
                self.partition_filter(begin_month, partition_name, format_datetime)



        elif self.add_partition_type == 'day':
            result = DateUtil.getBetweenDay(begin_month, last_month)
            for i in result:
                if len(self.db_max_part_timestamp) > 9:
                    partition_name = DateUtil.format_time_string(i)
                    format_datetime = DateUtil.time_stamp(partition_name)

                    self.partition_filter(i, partition_name, format_datetime)
                else:
                    partition_name = DateUtil.format_time_string(i)
                    format_datetime = DateUtil.date_time_stamp(partition_name) + 1
                    self.partition_filter(i, partition_name, format_datetime)
        else:
            print MessageUint.help
            exit(11)

    def range_print(self):
        if self.res_add_list is not None:
            print MessageUint.print_add_statement
            for i in self.res_add_list:
                print MessageUint.color_print(i)

    def extended_subpartition(self):
        start_datetime = int(self.start_datetime.split('-')[0])
        end_datetime = int(self.end_datetime.split('-')[0])
        while start_datetime <= end_datetime:
            FORMAT = "%d-%d-%d"
            # inittalie start_datetime after start_datetime datetime
            for months in range(1, 13):
                lastDay = calendar.monthrange(start_datetime, months)
                start_month = FORMAT % (start_datetime, months, 1)
                end_month = FORMAT % (start_datetime, months, lastDay[1])

                self.partition_table_generator(start_month, end_month)
            start_datetime += 1
        if self.add_partition is None and self.del_partition is None:
            self.range_print()
        if len(self.res_add_list) == 1:
            return self.res_add_list
        else:
            self.res_add_list = self.res_add_list[:-1]
            return self.res_add_list

    def generator_add_db_partition(self):

        if self.add_partition is not None:
            results = self.split_partiton_parameter(self.add_partition)
            partition_interval = results[0][0]
            self.add_partition_type = results[0][1]

            if len(self.db_max_part_timestamp) > 9:
                start_datetime = DateUtil.timestamp_datetime(self.db_max_part_timestamp)
                self.start_datetime = DateUtil.datetime_strftime(start_datetime)
            else:
                start_datetime = datetime.datetime.fromordinal(int(self.db_max_part_timestamp)-365)
                self.start_datetime = start_datetime.strftime('%Y-%m-%d')

            if self.add_partition_type == 'month':
                if len(self.db_max_part_timestamp) > 9:
                    end_datetime_days = int(partition_interval) * 31
                    end_datetime = DateUtil.add_month_datetime(self.db_max_part_timestamp, end_datetime_days)
                    last_end_datetime = self.get_first_last_month(end_datetime)
                    self.end_datetime = last_end_datetime[1]
                    self.extended_subpartition()
                    if self.del_partition is None:
                        self.exec_db_partition()
                else:
                    end_datetime_days = int(partition_interval) * 31
                    end_datetime = DateUtil.add_month_datetime_1(self.db_max_part_timestamp, end_datetime_days)
                    last_end_datetime = self.get_first_last_month(end_datetime)

                    self.end_datetime = last_end_datetime[1]
                    self.extended_subpartition()
                    if self.del_partition is None:
                        self.exec_db_partition()
            else:
                if len(self.db_max_part_timestamp) > 9:
                    end_datetime = DateUtil.add_month_datetime(self.db_max_part_timestamp, int(partition_interval) - 1)
                    end_datetime = DateUtil.datetime_strftime(end_datetime)
                    self.end_datetime = end_datetime
                    self.extended_subpartition()
                    if self.del_partition is None:
                        self.exec_db_partition()
                else:
                    end_datetime = DateUtil.add_month_datetime_1(self.db_max_part_timestamp,int(partition_interval) - 1)
                    end_datetime = DateUtil.datetime_strftime(end_datetime)
                    self.end_datetime = end_datetime
                    self.extended_subpartition()
                    if self.del_partition is None:
                        self.exec_db_partition()





    def db_partition_status(self):
        #mysqld = DB(self.host, int(self.port), self.user, self.password, self.schema_name)
        self.mysql.get_conn()
        partition_time = self.mysql.query(SqlUint.query_maximum_partition, (self.schema_name, self.table_name))
        self.db_max_part_timestamp = partition_time[0][0]

        # Determine whether it is a partition table
        if self.db_max_part_timestamp is None:
            MessageUint.partition_status_info(self.schema_name, self.table_name)
            exit(2)
        else:
            self.generator_add_db_partition()
            self.generator_del_db_partition()

    def analysis_parameter(self, input_parameters):
        input_paras = sorted(input_parameters.keys())
        get_parameters = inlayParas()
        partition_generator_paras = get_parameters.range_generator
        print_add_extent_paras = get_parameters.print_add_extent_generator
        print_del_extent_paras = get_parameters.print_del_extent_generator
        print_add_del_extent_paras = get_parameters.print_add_del_extent_generator
        exec_add_extent_paras = get_parameters.exec_add_extent_generator
        exec_del_extent_paras = get_parameters.exec_del_extent_generator
        exec_add_del_extent_paras = get_parameters.exec_add_del_extent_generator

        # print inputParas
        if partition_generator_paras == input_paras:
            getattr(get_parameters, "dict_range_parameter")
            result = self.extended_subpartition()
            return result
        elif input_paras == print_add_extent_paras or input_paras == exec_add_extent_paras:
            getattr(get_parameters, "dict_add_extent_parameter")
            result = self.db_partition_status()
            return result
        elif input_paras == print_del_extent_paras or input_paras == exec_del_extent_paras:
            getattr(get_parameters, "dict_del_extent_parameter")
            result = self.db_partition_status()
            return result
        elif input_paras == print_add_del_extent_paras  or input_paras == exec_add_del_extent_paras:
            getattr(get_parameters, "dict_add_del_extent_parameter")
            result = self.db_partition_status()
            return result
        else:
            print MessageUint.help

#main Program
if __name__ == "__main__":
    paraRes = Partition.getParameter()
    partition = Partition(paraRes)
    partition.analysis_parameter(paraRes)