#!/bin/bash
# -*-coding:utf-8 -*-
##*************************************************************************************************************

##*************************************************************************************************************
## **  文件名称：  start_all.sh
## **  功能描述：  数据库初使化入口脚本
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
## ** 程序调用格式：bash start_all.sh $version
## ** eg:bash start_all.sh v2.0
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
## **    4、file_path       文件根目录 

#校验输入参数
if [ ! -n "$1" ] ;then  
    echo "未输入版本号"
    exit 1
fi
version=$1

#参数初使化
start_time=$(date +'%Y-%m-%d %H:%M:%S')
bak_time=$(date +'%s')
#file_path="/mnt/hgfs/shared/init_database/"
file_path="$( cd "$( dirname "$0"  )" && pwd  )/"

exec_script()
{
    echo "*******${1}*******"
    printf "*******文件：${2}*******\n\n"
    bash ${2} ${3}
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
    echo "***********开始初使化数据——时间：${start_time}***********"
    echo "****Version:${version}"
    echo "****file_path:${file_path}"
    printf "****bak_time:${bak_time}\n\n"
     
    #初使化表结
    script_file="${file_path}01.exec_ddl_script.sh"
    exec_script "初使化DDL脚本" ${script_file} ${version}
    return_code=$?
    echo "test初使化DDL脚本:状态${return_code}"
    
    
    
    #初使化表数据
    script_file="${file_path}02.exec_init_dim_script.sh"
    #exec_script "初使化表数据" ${script_file} ${version}
    
    
    #数据校验
    script_file="${file_path}03.exec_validation_script.sh"
    #exec_script "数据校验" ${script_file} ${version}
    
    return 0
}

start




