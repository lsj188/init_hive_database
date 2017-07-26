#!/usr/bin/env python
# -*-coding:utf-8 -*-
##*************************************************************************************************************

##*************************************************************************************************************
## **  文件名称：  test_pyspark_log.py
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
## ** 程序调用格式：test_pyspark_log.py $version
## ** eg:pyspark test_pyspark_log.py v2.0
## ** 
## ******************************************************************************************
## ** 重庆金融资产交易所
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
from pyspark.context import SparkConf
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
    conf=SparkConf().setMaster("yarn-client").setAppName("test_pyspark_log")
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
def execSql( sqlText ):
    print "sql语句："+sqlText
    resultData=DataFrame[]
    try:
        exc_result = sqlContext.sql(sqlText)
        print '[' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']*************执行语句成功*******************'
        return 0,resultData
    
    except Exception,e:
        print '[' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+']*************执行语句失败*******************'
        print Exception,":",e
        return -1,resultData
        
    
    
        

    
#============================================================================================
# 备份表
#============================================================================================
def backUp(dbName,tableName):
    print "备份表"
    descTab="desc "+dbName+"."+tableName
    returnCode,resultData=execSql(descTab)
    
    if returnCode==(-1):
    	  print dbName+"."+tableName,"表不存在！"
    	  return 1
    	  
    backUpSsql="create table "+dbName+"."+tableName+bakTime+" as select * from "+bakDbName+tableName
    returnCode,resultData=execSql(backUpSsql)
    return returnCode

  
#============================================================================================
# 拆分表脚本——以单条语句执行
#============================================================================================
def splitSql(flag,fileName):
    print "执行修改表脚本"
    sqlText=""
    tmpStr=""
    returnCode=
    tabList=[set(),set()]
    for line in open(fileName):
        index=line.find(';')
        if index>=0:
            sqlText=sqlText+line[0:index]
            tmpStr=line[index:]
            if sqlText.find("create ")>=0 and sqlText.find(" table ")>=0:
            	  tmpStr1=sqlText[0:sqlText.find("(")].strip("(").strip("create").strip("table").strip()
                dbName=tmpStr1[0:tmpStr1.find(".")]
                tabName=tmpStr1[tmpStr1.find("."):]
                if flag == 1:
                    returnCode=backUp(dbName,tabName)
                    if returnCode<>-1:
                        returnCode,resultData=execSql(sqlText)
                        if returnCode ==0:
                        	 tabList[0].add(tmpStr1)
                        else:
                        	 tabList[1].add(tmpStr1)
                    else:
                    	  tabList[1].add(tmpStr1)
                        print tmpStr1,"表备份失败，请手动执行备份及创建表操作！"
                else:
                     returnCode,resultData=execSql(sqlText)
                     if returnCode ==0:
                        	 tabList[0].add(tmpStr1)
                        else:
                        	 tabList[1].add(tmpStr1)
            
            sqlText=tmpStr
        else:
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
        tabName=tmpStr1[tmpStr1.find("."):]
        tabRecordCnt=line[line.find(":"):0]
        returnCode=backUp(dbName,tabName)
        if returnCode==0:
            execSql(sqlText)
        else:
            print tmpStr1,"表备份失败，请手动执行备份及创建表操作！"
            

#============================================================================================
# 数据库表验证
#============================================================================================
def validationDbTab(dbCfgFile):
    print "数据库表个数验证"
    for line in open(dbCfgFile):=
        dbName=line[0:line.find(":")]
        tabCnt=line[line.find(":"):]  
        execSql("use "+dbName)
        returnCode,resultData=execSql("show tables")
        curentTabCnt=len(resultData.collect())
        if curentTabCnt==tabCnt:
            print "--------",db_name,"库，验证通过，配置的表个数：",tabCnt,"，当前库表个数",curentTabCnt,"--------"
        else:
            print "--------",db_name,"库，验证不通过，配置的表个数：",tabCnt,"，当前库表个数",curentTabCnt,"--------"
            

#============================================================================================
# 表数据记录验证
#============================================================================================
def validationTabRecord(tabCfgFile):
    print "数据表记录验证"
    for line in open(tabCfgFile):
        tmpStr1=line[0:line.find(":")]
        dbName=tmpStr1[0:tmpStr1.find(".")]
        tabName=tmpStr1[tmpStr1.find("."):]
        tabRecordCnt=line[line.find(":"):]
        sqlText="select count(*) as cnt from "+dbName+"."+tabName
        returnCode,resultData=execSql(sqlText)
        curentTabRecordCnt=resultData.collect()[0][0]	
        if curentTabRecordCnt==tabRecordCnt:
            print "--------",tmpStr1,"表，验证通过，配置的表记录数："+tabRecordCnt+"，当前库表记录数"+curentTabRecordCnt+"--------"
        else:
            print "--------",tmpStr1,"表，验证不通过，配置的表记录数："+tabRecordCnt+"，当前库表记录数"+curentTabRecordCnt+"--------"
            

#============================================================================================
# DDL脚本执行结果输出
#============================================================================================
def printResult(item,tabList):
    print "DDL脚本执行结果:"
    flag=1
    print item,"表"
    for tabSet in tabList:
    	  if flag==1:
    	      print "成功{0}个：".format(len(tabSet))
    	  else:
    	  	  print "失败{0}个：".format(len(tabSet))
    	  	  
    	  for tab in tabSet:
    	  	  print tab
    	  	  
    	  flag=0


  
#============================================================================================
# 程序入口
#============================================================================================
if __name__=="__main__":
    sqlContext,sc=initSparkContext()
    try:
        print "version:",version,"\nstartTime:",strftime('%Y-%m-%d %H:%M:%S')
    
        #新增
        addTabList=splitSql(0,addDdlFile)
        
        #修改
        modifiedTabList=splitSql(1,modifiedDdlFile)

        #数据初使化
        loadData(tabCfgFile)
        
        #执行结果输出
        printResult("新增",addTabList)
        printResult("修改",modifiedTabList)
        
        #数据校验
        validationDbTab(dbCfgFile)
        validationTabRecord(tabCfgFile)
    
    except Exception,e:
        print Exception,":",e
        sys.exit(1)
    finally:
        closeSpackContext(sc,startTime)
    
    
    
    
    
