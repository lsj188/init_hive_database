#!/bin/bash
# -*-coding:utf-8 -*-
##*************************************************************************************************************

##*************************************************************************************************************
## **  文件名称：  01.exec_ddl_script.sh
## **  功能描述：  执行建库建表
## **  
## **  
## **  输入： 
## **  输出：
## **  
## **  
## **  创建者：骆仕军
## **  创建日期：2017-07-19
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
## ** 程序调用格式：bash 01.exec_ddl_script.sh
## ** eg:bash 01.exec_ddl_script.sh
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
## **    4、file_path       DDL脚本所在目录

#校验输入参数
if [ ! -n "$1" ] ;then  
    echo "未输入版本号"
	  exit 1
fi
version=$1


#参数初使化
start_time=$(date +'%Y-%m-%d %H:%M:%S')
bak_time=$(date +'%s')
bak_db_name="tmp_db"
file_path="${version}/ddl_scripts/"
hive_log_file="${version}/hive_sql.log"
modified_tabs=()

exec_add_ddl()
{
    echo "*******$1*******"
    echo "*******文件：${2}*******"
    #hive -f "${2}" 2>${hive_log_file}
    echo "test${2}"
    return_code=$?
    if [ ${return_code} -eq 0 ]; then
        printf "*****--成功--*****\n\n"
    else
        printf "*****--失败--*****\n\n"
    	exit 1
    fi
}

exec_modified_ddl()
{
   	cnt=0
    echo "*******读取配置文件循环导入初使化数据*******"
    cat "${2}" | grep "create" | grep "table" | while read line  
    do
		tmp_str=${line#*table}
		tmp_str=${tmp_str%(*}
		db_name=${tmp_str%.*}
        table_name=${tmp_str##*.}
		${modified_tabs[${cnt}]}="${tmp_str}"
		cnt+=1
        backup_sql="create table ${bak_db_name}.${table_name}${bak_time} as select * from ${db_name}.${table_name}" 
        #backUp "${backup_sql}"
	done
	
	#exec_add_ddl ${2}
	
}

backUp()
{
    echo "*******数据备份语句：${1}*******"
    #hive -e "${1}" 2>>${hive_log_file}
	echo "test:${1}"
	return_code=$?
    if [ ${return_code} -eq 0 ]; then
        printf "*****--成功--*****\n\n"
    else
        printf "*****--失败--*****\n\n"
    	exit 1
    fi
}

start()
{
    echo "***********DDL脚本——时间：${start_time}***********"
    echo "****Version:${version}"
    echo "****file_path:${file_path}"
    printf "****bak_time:${bak_time}\n\n"
    
    
    #新增脚本
    script_file="${file_path}ddl_add_tables.hql"
    #add_tabs=`exec_add_ddl "ADD DDL" "${script_file}"`
    
    #修改脚本
    script_file="${file_path}ddl_modified_tables.hql"
    exec_modified_ddl "Modified DDL" "${script_file}"
	
	#echo "新增表："
	#echo ${add_tabs[@]}
	echo "修改表：${#modified_tabs[@]}个"
	echo ${modified_tabs[@]}
    
    
    return 0
}

start



