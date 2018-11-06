#!/bin/bash
#set -x
echo "please input dbpass:"
read -s dbpass
if [ "x"$dbpass == "x" ]
then
        echo "dbpass is null, will exit"
        exit 1
fi

source /etc/profile
rundir=$(cd $(dirname $0);pwd)
cd ${rundir}
test -d ${rundir}/tmp || mkdir -p ${rundir}/tmp
mysqlbase=/usr/bin/





while read line
do
 slave_ip=$(echo $line|awk '{print $1}')
 slave_post=$(echo $line|awk '{print $2}') 
#  echo "#################$slave_ip,$slave_post#################"
  $mysqlbase/mysql -uyuanwen.peng -p$dbpass -h$slave_ip -P$slave_post -BN -e "select TABLE_SCHEMA,TABLE_NAME from information_schema.TABLES where CREATE_OPTIONS='partitioned';" > ./tmp/${slave_ip}_${slave_post}_partition_tables.log

   if [ -s "./tmp/${slave_ip}_${slave_post}_partition_tables.log" ]
   then 

   while read line
   do  
     db_name=$(echo $line |awk '{print $1}')
     table_name=$(echo $line |awk '{print $2}')

     max_partition=`$mysqlbase/mysql -uyuanwen.peng -p$dbpass -h$slave_ip -P$slave_post -BN -e "select max(partition_description) from information_schema.PARTITIONS where TABLE_SCHEMA='${db_name}' and TABLE_NAME='${table_name}';"`

     while read line 
     do 
       if [ "${max_partition}" == "MAXVALUE" ]
       then

 
         max_num=`$mysqlbase/mysql -uyuanwen.peng -p$dbpass -h$slave_ip -P$slave_post -BN -e "select max(PARTITION_ORDINAL_POSITION) from information_schema.PARTITIONS where TABLE_SCHEMA='${db_name}' and TABLE_NAME='${table_name}';"`
         now_num=`expr $max_num - 1 `
         now_partition_by=`$mysqlbase/mysql -uyuanwen.peng -p$dbpass -h$slave_ip -P$slave_post -BN -e "select PARTITION_DESCRIPTION from information_schema.PARTITIONS where TABLE_SCHEMA='${db_name}' and TABLE_NAME='${table_name}' and PARTITION_ORDINAL_POSITION=${now_num}"`
          length_now_partition_by=`echo ${#now_partition_by}`
          if [ $length_now_partition_by -le 7 ]
            then 
                max_time=`$mysqlbase/mysql -uyuanwen.peng -p$dbpass -h$slave_ip -P$slave_post -BN -e "select FROM_DAYS($now_partition_by);"`       
                echo -e "${slave_ip}_${slave_post}:DB名为:\033[31m ${db_name} \033[0m,表名为:\033[31m ${table_name} \033[0m,分区表最大的分区为:\033[31m ${max_partition} \033[0m,最大分区时间为:\033[31m ${max_time} \033[0m"
            else
                max_time=`$mysqlbase/mysql -uyuanwen.peng -p$dbpass -h$slave_ip -P$slave_post -BN -e "select FROM_UNIXTIME($now_partition_by);"`
                echo -e "${slave_ip}_${slave_post}:DB名为:\033[31m ${db_name} \033[0m,表名为:\033[31m ${table_name} \033[0m,分区表最大的分区为:\033[31m ${max_partition} \033[0m,最大分区时间为:\033[31m ${max_time} \033[0m"
           fi

       continue;
       else 
           length_max_partition=`echo ${#max_partition}`     
           if [ $length_max_partition -le 7 ]
		then 
                max_time=`$mysqlbase/mysql -uyuanwen.peng -p$dbpass -h$slave_ip -P$slave_post -BN -e "select FROM_DAYS($max_partition);"`	
                echo -e "${slave_ip}_${slave_post}:DB名为:\033[31m ${db_name} \033[0m,表名为:\033[31m ${table_name} \033[0m,分区表最大的分区为:\033[31m ${max_partition} \033[0m,最大分区时间为:\033[31m ${max_time} \033[0m"

  	 	else

      	        max_time=`$mysqlbase/mysql -uyuanwen.peng -p$dbpass -h$slave_ip -P$slave_post -BN -e "select FROM_UNIXTIME($max_partition);"`
                echo -e "${slave_ip}_${slave_post}:DB名为:\033[31m ${db_name} \033[0m,表名为:\033[31m ${table_name} \033[0m,分区表最大的分区为:\033[31m ${max_partition} \033[0m,最大分区时间为:\033[31m ${max_time} \033[0m"
          fi
      fi 
     done <<< ${max_partition}
    done < ./tmp/${slave_ip}_${slave_post}_partition_tables.log 
    
   else
     echo -e "${slave_ip}_${slave_post}:\033[31m 没有分区表 \033[0m"
     continue;
   fi
done < $1
   

