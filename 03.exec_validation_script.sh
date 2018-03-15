#!/bin/bash
# -*-coding:utf-8 -*-
##*************************************************************************************************************

##*************************************************************************************************************
## **  文件名称：  03.validation_script.sh
## **  功能描述：  数据校验
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
## ** 程序调用格式：bash 03.validation_script.sh $version
## ** eg:bash 03.validation_script.sh v2.0
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
## **    4、db_cfg_file     库表个数验证配置文件
## **    5、tab_cfg_file    表数据验证配置文件

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
bak_db_name="tmp_db"
data_file_path="${version}/data_files/"
db_cfg_file="${version}/validation_cfg/db_table_cfg.txt"
tab_cfg_file="${version}/validation_cfg/table_record_cfg.txt"
hive_log_file="${version}/hive_sql.log"

validation_db_tables()
{
    echo "*******验证数据库表数量*******"  
    while read line  
    do   
        db_name=${line%:*}
        cfg_table_cnt=${line##*:}
        validation_sql="use ${db_name};show tables;"
        #current_tab_cnt=`hive -e "${validation_sql}" 2>>${hive_log_file} | wc -l`
        current_tab_cnt=${cfg_table_cnt}
        echo "test:${validation_sql}"
        
        if [ ${cfg_table_cnt} -eq ${current_tab_cnt} ]; then
            echo "--------${db_name}库，验证通过，配置的表个数：${cfg_table_cnt}，当前库表个数${current_tab_cnt}--------"
        else 
            echo "--------${db_name}库，验证不通过，配置的表个数：${cfg_table_cnt}，当前库表个数${current_tab_cnt}--------" 
        fi    
        
  done < "${1}"
}

validation_table_record()
{
    echo "*******验证数据库表记录数*******"  
    while read line  
    do   
        db_name=${line%.*}
        tmp_str=${line%:*}
        table_name=${tmp_str##*.}
        cfg_record_cnt=${line##*:}
        validation_sql="select count(*) from ${db_name}.${table_name};"
        #current_record_cnt=`hive -e "${validation_sql}" 2>${hive_log_file}`
        current_record_cnt=${cfg_record_cnt}
        echo "test:${validation_sql}"
        
        if [ ${cfg_record_cnt} -eq ${current_record_cnt} ]; then
            echo "--------${db_name}.${table_name}表，验证通过，配置的表记录数：${cfg_record_cnt}，当前库表记录数${current_record_cnt}--------"
        else 
            echo "--------${db_name}.${table_name}表，验证不通过，配置的表记录数：${cfg_record_cnt}，当前库表记录数${current_record_cnt}--------" 
        fi    
  done < "${1}"
}

start()
{
    echo "***********数据校验——时间：${start_time}***********"
    echo "****Version:${version}"
    echo "****file_path:${file_path}"
    printf "****bak_time:${bak_time}\n\n"
    
    #数据库表个数验证
    validation_db_tables  "${db_cfg_file}"
  
    #数据库表记录数校验
    validation_table_record  "${tab_cfg_file}"
    
    return 0
}

start



