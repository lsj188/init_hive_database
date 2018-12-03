#!/usr/bin/env python
# -*-coding:utf-8 -*-
##*************************************************************************************************************

##*************************************************************************************************************
## **  文件名称：  init_hive_database.py
## **  功能描述：  数据库初使化脚本 
## **  
## **  
## **  输入： 
## **  输出：
## **  
## **  
## **  创建者：骆仕军
## **  创建日期：2017-07-26
## **  修改日期：
## **  修改日志：
## **  
## **  
## **  
## **  
## **  
## **  
## ** ---------------------------------------------------------------------------------------
## ** 
## ** ---------------------------------------------------------------------------------------
## ** 
## ** 程序调用格式：pyspark init_hive_database.py $version
## ** eg:pyspark test_pyspark_log.py v2.0
## ** 
## ******************************************************************************************
## ** XXX金融资产交易所
## ** All Rights Reserved.
## ******************************************************************************************
## **
## **参数说明：
## **    1、version         版本
## **    2、start_time      开始时间
## **    3、bak_time        备份时间

#引用包
import os,sys
import datetime
import time
import math
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql.types import *
from pyspark.sql import HiveContext

# 校验输入参数
if len(sys.argv)<2 or sys.argv[1].strip()=="":
    print "输入参数错误，请输入需要初使化的版本目录！"
    sys.exit(1)

# 参数初使化
version=sys.argv[1]
startTime=datetime.datetime.now()
bakTime=str(int(time.time()))
bakDbName="tmp_db"
currentPath=sys.argv[0][0:sys.argv[0].rfind("/")+1]
addDdlFile=currentPath+version+"/ddl_scripts/ddl_add_tables.hql"
modifiedDdlFile=currentPath+version+"/ddl_scripts/ddl_modified_tables.hql"
dataPath=currentPath+version+"/data_files/"
dbValidationCfg=currentPath+version+"/validation_cfg/db_table_cfg.txt"
tabValidationCfg=currentPath+version+"/validation_cfg/table_record_cfg.txt"

#============================================================================================
# 初使化 Spark
#============================================================================================
def initSparkContext():
    print "初使化 SparkContext"
    conf=SparkConf().setMaster("yarn-client").setAppName("init_hive_database")
    sc = SparkContext(conf=conf)
    return HiveContext(sc),sc

  
#============================================================================================
# 释放资源
#============================================================================================
def closeSpackContext(sc,startTime):
    sc.stop()
    endTime=datetime.datetime.now()
    print '[' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']*************程序运行总时长为'+str((endTime-startTime).seconds) + '秒！*******************'
    print "======结束 释放资源====="

  
#============================================================================================
# 执行sql
#============================================================================================
def execSql(sqlText,flag):
    
    try:
        #打印log
        if flag==1:
            print "sql语句："+sqlText
            resultData = sqlContext.sql(sqlText)
            print '[' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']*************执行语句成功*******************'
            return 0,resultData
        else:
            resultData = sqlContext.sql(sqlText)
            return 0,resultData
    except Exception,e:
        print '[' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']*************执行语句失败*******************'
        print Exception,":",e
        return -1,-1
        
    
    
#============================================================================================
# 备份表
#============================================================================================
def backUp(dbName,tableName):
    print "备份表："+dbName+"."+tableName
    descTab="desc "+dbName+"."+tableName
    returnCode,resultData=execSql(descTab,1)
    
    #表不存在返回
    if returnCode==(-1):
        print dbName+"."+tableName,"表不存在！"
        return 1
    
    bakTabName=bakDbName+"."+tableName+bakTime
    descTab="desc "+bakTabName
    returnCode,resultData=execSql(descTab,1)  
    
    #本次备份存在返回
    if returnCode==0:
        return returnCode
    
    backUpSsql="create table "+bakTabName+" as select * from "+dbName+"."+tableName
    returnCode,resultData=execSql(backUpSsql,1)
    return returnCode

  
#============================================================================================
# 拆分表脚本——以单条语句执行
#============================================================================================
def splitSql(flag,fileName):
    print "执行DDL脚本"
    sqlText=""
    tmpStr=""
    tabList=[set(),set()]
    for line in open(fileName):
      
        #忽略注释行
        if line.find('--')<0:
            index=line.find(';')
            if index>=0:
                sqlText=sqlText+line[0:index]
                
                #处理一行写两条语句情况
                tmpStr=line[index+1:]
                returnCode=0
                
                #建表
                if flag==0:
                  
                    #识别、拼接、执行语句
                    if sqlText.find("create ")>=0 and sqlText.find(" table ")>=0:
                        tmpStr1=sqlText[0:sqlText.find("(")].replace("create ","").replace("table ","").strip()
                        dbName=tmpStr1[0:tmpStr1.find(".")]
                        tabName=tmpStr1[tmpStr1.find(".")+1:]
                        
                        if returnCode==-1:
                            print tmpStr1,"表备份失败，请手动执行备份及创建表操作！"
                                                    
                        returnCode,resultData=execSql(sqlText,1)
                        if returnCode ==0:
                            tabList[0].add(tmpStr1)
                        else:
                            tabList[1].add(tmpStr1)
                        
                    else:
                        tmpStr1=sqlText.replace("drop","").replace("table","").replace("if","").replace("exists","").strip()
                        dbName=tmpStr1[0:tmpStr1.find(".")]
                        tabName=tmpStr1[tmpStr1.find(".")+1:]
                        returnCode=backUp(dbName,tabName) 
                        
                        #备份成功执行删除表
                        if returnCode==0:
                            execSql(sqlText,1)
                #修改表
                else:
                    tmpStr1=sqlText.replace("alter","").replace("table","").strip()
                    tmpStr1=tmpStr1[0:tmpStr1.find(" ")]
                    dbName=tmpStr1[0:tmpStr1.find(".")]
                    tabName=tmpStr1[tmpStr1.find(".")+1:]
                    returnCode=backUp(dbName,tabName) 
                        
                    #备份成功执行修改表
                    if returnCode==0:
                        returnCode,resultData=execSql(sqlText,1)
                        if returnCode ==0:
                            tabList[0].add(tmpStr1)
                        else:
                            tabList[1].add(tmpStr1)
                    else:
                        print tmpStr1,"表备份失败，请手动执行备份及修改表操作！"
                              
                #将下一条语句代码友暂存              
                sqlText=tmpStr
            else:
                
                #拼接语句，一条语句写多行情况
                sqlText =sqlText+line
                
    return tabList


    
#============================================================================================
# 数据初使化
#============================================================================================
def loadData(tabCfgFile):
    print "数据导入"
    for line in open(tabCfgFile):
        tmpStr1=line[0:line.find(":")]
        dbName=tmpStr1[0:tmpStr1.find(".")]
        tabName=tmpStr1[tmpStr1.find(".")+1:]
        tabRecordCnt=line[line.find(":")+1:]
        returnCode=backUp(dbName,tabName)
        if returnCode==0:
            dataFile=dataPath+tabName+".txt"
            sqlText="load data local inpath '{dataFile}' overwrite into table {dbName}.{tabName}".format(dataFile=dataFile,tabName=tabName,dbName=dbName)
            execSql(sqlText,1)
        else:
            print tmpStr1,"表备份失败，请手动执行备份及初使化表操作！"
            

#============================================================================================
# 数据库表验证
#============================================================================================
def validationDbTab(dbCfgFile):
    print "数据库表个数验证"
    for line in open(dbCfgFile):
        dbName=line[0:line.find(":")]
        tabCnt=int(line[line.find(":")+1:])  
        execSql("use "+dbName,0)
        returnCode,resultData=execSql("show tables",0)
        if returnCode==0:
            curentTabCnt=len(resultData.collect())
            if curentTabCnt==tabCnt:
                print "--------",dbName,"库，验证通过，配置的表个数",tabCnt,"，当前库表个数",curentTabCnt,"--------"
            else:
                print "--------",dbName,"库，验证不通过，配置的表个数",tabCnt,"，当前库表个数",curentTabCnt,"--------"
                

#============================================================================================
# 表数据记录验证
#============================================================================================
def validationTabRecord(tabCfgFile):
    print "数据表记录验证"
    for line in open(tabCfgFile):
        tmpStr1=line[0:line.find(":")]
        dbName=tmpStr1[0:tmpStr1.find(".")]
        tabName=tmpStr1[tmpStr1.find(".")+1:]
        tabRecordCnt=int(line[line.find(":")+1:])
        sqlText="select count(*) as cnt from "+dbName+"."+tabName
        returnCode,resultData=execSql(sqlText,0)
        if returnCode==0:
            curentTabRecordCnt=resultData.collect()[0][0]    
            if curentTabRecordCnt==tabRecordCnt:
                print "--------",tmpStr1,"表，验证通过，配置的表记录数",tabRecordCnt,"，当前库表记录数",curentTabRecordCnt,"--------"
            else:
                print "--------",tmpStr1,"表，验证不通过，配置的表记录数",tabRecordCnt,"，当前库表记录数",curentTabRecordCnt,"--------"
            

#============================================================================================
# DDL脚本执行结果输出
#============================================================================================
def printResult(item,tabList):
    flag=1
    print item,"表"
    for tabSet in tabList:
        if flag==1:
            print "成功{0}个：".format(len(tabSet))
        else:
            print "失败{0}个：".format(len(tabSet))
                
        for tab in tabSet:
            print "--------",tab
                
        flag=0
    
    print "**********"

  
#============================================================================================
# 程序入口
#============================================================================================
if __name__=="__main__":
    sqlContext,sc=initSparkContext()
    try:
        print "version:",version,"\nstartTime:",startTime.strftime('%Y-%m-%d %H:%M:%S'),"\nbakTime",bakTime
        print "===================================================================================="
    
        #新增
        addTabList=splitSql(0,addDdlFile)
        print "===================================================================================="
        
        #修改
        modifiedTabList=splitSql(1,modifiedDdlFile)
        print "===================================================================================="

        #数据初使化
        loadData(tabValidationCfg)
        print "===================================================================================="
        
        #执行结果输出
        printResult("新增",addTabList)
        printResult("修改",modifiedTabList)
        print "===================================================================================="
        
        #数据校验
        #validationDbTab(dbValidationCfg)
        validationTabRecord(tabValidationCfg)
    
    except Exception,e:
        print Exception,":",e
        sys.exit(1)
    finally:
        closeSpackContext(sc,startTime)
    
    
    
    
    
