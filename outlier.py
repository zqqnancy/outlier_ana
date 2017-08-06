# !/usr/bin/python
# -*- coding:utf-8
# author: zuoqianqian
# create time:2017-07-07

"""分析流程周期"""

from __future__ import print_function
import sys
import os
import time
import datetime

#得到当前日期的前一个月
# return '2017-06'
def get_lastmonth(mytime):
    myday = datetime.datetime(int(mytime[0:4]),int(mytime[5:7]),1)
    delta = datetime.timedelta(days=1)
    my_yesterday = myday - delta
    # lastmonth = str(my_yesterday)[0:7].replace("-","")
    lastmonth = str(my_yesterday)[0:7]
    return lastmonth

#获取输入时间的上个月的第一天00:00:00点
# return '2017-06-01 00:00:00'
def get_firstday_lastmonth(input_time):
    # 以下测试
    # str = '2017-01-31 12:00:00'
    # date_time = datetime.datetime.strptime(str,'%Y-%m-%d %H:%M:%S')
    # input_time = date_time
    year = input_time.year
    month = input_time.month
    if month == 1:
        month = 12
        year -= 1
    else:
        month -= 1
    res = datetime.datetime(year,month,1)
    return res

#获取输入时间的上个月的最后一天23:59:59点
# return '2017-06-30 23:59:59'
def get_lastday_lastmonth(input_time):
    timespan = datetime.timedelta(seconds=1)
    yesterday =datetime.datetime(input_time.year,input_time.month,1)-timespan
    return yesterday

if __name__ == "__main__":
    '''1.获取上个月的时间'''
    mytime = time.strftime('%Y-%m-%d',time.localtime(time.time()))
    lastmonth = get_lastmonth(mytime);

    # '''2. 建立月度消费类指标表
    #     file:create_index_tables.sql
    # '''
    # sql_path = './hivesql/create_index_tables.sql' #'/home/hive/outlier/hivesql/create_index_tables.sql'
    # param_month = lastmonth+"%"
    # command = "hive --hivevar param_month='%s' -f %s" %(param_month,sql_path)
    # print(command)
    # status = os.system(command);
    # #hive --hivevar param_month='2017-06%' -f ./hivesql/create_index_tables.sql
    # if status == 0:
    #     print('[%s] The index tables for  specified  month data generated!' %lastmonth)
    # else:
    #     print('[%s] The index tables for  specified  month data failed to  generate!' %lastmonth)
    #     exit(1)

    '''3.指标计算
        file:real.params,fee_preprocess.pig, dataclean.jar
    '''
    #/home/hive/outlier/real.params
    params_path = "./real.params"
    #/home/hive/outlier/fee_preprocess.pig
    pig_path = "./fee_preprocess.pig"
    # 指标计算时间参数
    today = datetime.datetime.today()
    index1_start_time = get_firstday_lastmonth(today)
    index1_end_time = get_lastday_lastmonth(today)
    index2_start_time = index1_start_time
    index2_end_time = index1_end_time
    command = "pig -p index1_start_time='%s' -p index1_end_time='%s' -p index2_start_time='%s' -p index2_end_time='%s' " \
              "-param_file %s %s" %(index1_start_time,index1_end_time,index2_start_time,index2_end_time,params_path,pig_path)
    print(command)
    status = os.system(command)
    #pig -p index1_start_time='2017-06-01 00:00:00' -p index1_end_time='2017-06-30 23:59:59' -p index2_start_time='2017-06-01 00:00:00' -p index2_end_time='2017-06-30 23:59:59' -param_file real.params fee_preprocess.pig
    if status == 0:
        print('[%s] The index caculation for  specified  month data successed!' %lastmonth)
    else:
        print('[%s] The index caculation for  specified  month data failed!' %lastmonth)
        exit(1)

    '''4.汇总指标形成指标宽表
        file:recently_index_cal.py
    '''
    #sql中的python UDF路径
    #/home/hive/outlier/recently_index_cal.py
    python_udf_filepath = './recently_index_cal.py'
    python_udf_filename = 'recently_index_cal.py'
    sql_path = './hivesql/create_tmp_real_indexs.sql'
    history_start_time = get_firstday_lastmonth(get_firstday_lastmonth(today)).strftime('%Y-%m-%d %H:%M:%S')
    history_end_time = get_lastday_lastmonth(today).strftime('%Y-%m-%d %H:%M:%S')

    command = "hive --hivevar python_udf_filepath=%s --hivevar python_udf_filename=%s --hivevar history_start_time='%s' " \
              "--hivevar history_end_time='%s' -f %s" %(python_udf_filepath,python_udf_filename,history_start_time,history_end_time,sql_path)
    print(command)
    status = os.system(command)
    if status == 0:
        print('The summarized index tables for  specified  month data generated!')
    else:
        print('The summarized index tables for  specified  month data failed to  generate!')
        exit(1)

    '''5.模型预测
        file:DecisionTreePre.scala;RandomForestPre.scala;LRPre.scala
    '''









