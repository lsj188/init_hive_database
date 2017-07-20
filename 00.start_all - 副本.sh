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

#校验输入参数
if [ ! -n "$1" ] ;then  
    echo "未输入版本号"
	exit 1
fi
version=$1


#参数初使化
return_code=0
start_time=$(date +'%Y-%m-%d %H:%M:%S')
bak_time=$(date +'%s')
file_path="/mnt/hgfs/shared/init_database/"


echo "***********开始初使化数据——时间：${start_time}***********"
echo "****Version:${version}"
echo "****file_path:${file_path}"
echo "****bak_time:${bak_time}"


echo "*******初使化DDL脚本*******"
ddl_script_file="${file_path}01.exec_ddl_script.sh"
echo "*******文件：${ddl_script_file}*******"
bash ${ddl_script_file} ${version}
return_code=$?
if [ ${return_code} -eq 0 ]; then
    echo "*****--成功--*****"
else
    echo "*****--失败--*****"
	exit 1
fi


echo "*******初使化维表数据*******"
dim_script_file="${file_path}02.exec_init_dim_script.sh"
echo "*******文件：${dim_script_file}*******"
bash ${dim_script_file} ${version}
return_code=$?
if [ ${return_code} -eq 0 ]; then
    echo "*****--成功--*****"
else
    echo "*****--失败--*****"
	exit 1
fi

echo "*******数据校验*******"
validation_script_file="${file_path}03.exec_validation_script.sh"
echo "*******文件：${validation_script_file}*******"
sh ${validation_script_file} ${version}
return_code=$?
if [ ${return_code} -eq 0 ]; then
    echo "*****--成功--*****"
else
    echo "*****--失败--*****"
	exit 1
fi

exit 0





