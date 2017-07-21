#!/bin/bash
# -*-coding:utf-8 -*-
##*************************************************************************************************************

##*************************************************************************************************************
## **  文件名称：  02.exec_init_dim_script.sh
## **  功能描述：  维表数据初使化
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
## ** 程序调用格式：bash 02.exec_init_dim_script.sh $version
## ** eg:bash 02.exec_init_dim_script.sh v2.0
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
## **    4、data_file_path  导入数据文件存放目录
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
tab_cfg_file="${version}/validation_cfg/table_record_cfg.txt"
hive_log_file="${version}/hive_sql.log"

loadData()
{
    echo "*******数据导入语句：${1}*******"
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
    echo "***********表数据初使化——时间：${start_time}***********"
    echo "****Version:${version}"
    echo "****file_path:${file_path}"
    printf "****bak_time:${bak_time}\n\n"
    
    echo "*******读取配置文件循环导入初使化数据*******"  
    while read line  
    do   
        db_name=${line%.*}
        tmp_str=${line%:*}
        table_name=${tmp_str##*.}
        table_record_cnt=${line##*:}
        load_sql=""
        
        echo "*******${tmp_str}——表开始表的导入*******" 
        #desc_cnt=`hive -e "desc ${db_name}.${table_name}" 2>>${hive_log_file} | wc -l`
        desc_cnt=2
        echo "test:desc_cnt:${desc_cnt}"
        
        # 表存在
        if [ ${desc_cnt} -gt 1 ]; then
          
            #备份表
            backup_sql="create table ${bak_db_name}.${table_name}${bak_time} as select * from ${db_name}.${table_name}" 
            backUp "${backup_sql}"
        
            #导入数据
            data_file="${data_file_path}${table_name}"
            load_sql="load data local inpath '${data_file}' overwrite into table ${db_name}.${table_name}"
            loadData "${load_sql}"
         
        else
            echo "*******表存在*******"
        fi
    done < "${tab_cfg_file}"
    
    return 0
}

start



