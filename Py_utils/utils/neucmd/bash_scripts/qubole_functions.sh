#!/bin/bash

#this_file_path=`readlink -f $BASH_SOURCE | rev | cut -d'/' -f2- | rev`
#platform_config_file="$this_file_path/platform_config.xml"

function _get_s3_location()
{
    echo `xmlstarlet sel -T -t -v "/configroot/qubole_settings/set[name='s3_location']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_arcadia_hdfs_location()
{
    echo `xmlstarlet sel -T -t -v "/configroot/qubole_settings/set[name='arcadia_hdfs_location']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}


function _get_arcadia_sql_hdfs_location()
{
    echo `xmlstarlet sel -T -t -v "/configroot/qubole_settings/set[name='arcadia_hdfs_sql_location']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_qubole_jar_location()
{
    echo `xmlstarlet sel -T -t -v "/configroot/qubole_settings/set[name='JARLOCATION']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}
 
function _get_qubole_config_arcadia_cluster() {
    echo `xmlstarlet sel -T -t -v "configroot/qubole_settings/set[name='arcadia_cluster']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_qubole_region()
{
    echo `xmlstarlet sel -T -t -v "/configroot/qubole_settings/set[name='region']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_qubole_access_key()
{
   echo `xmlstarlet sel -T -t -v "/configroot/qubole_settings/set[name='access_key']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}
 

function _get_qubole_secret_key()
{
   echo `xmlstarlet sel -T -t -v "/configroot/qubole_settings/set[name='secret_key']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}


 
function launch_qubole_cluster()
{      
    local path=$1
    local H2ONODEMEMORYINGB=$2
    local H2ONOOFNODES=$3
    local H2ODRIVERPORT=$4
    local H2OBASEPORT=$5
    local qds_cmd=`_get_qubole_config_qds_cmd` 
    local url=`_get_qubole_config_url`
    local token=`_get_qubole_config_token`
    local cluster=`_get_qubole_config_cluster`
    local h2odriver=`_get_qubole_jar_location`
    
    h2odriver="$h2odriver/h2odriver.jar"
    $qds_cmd --url=$url --token=$token  shellcmd run --cluster=$cluster -s "hadoop jar $h2odriver -Dmapred.jobclient.killjob.onexit=false  -Dqubole.command.id=-1 -Dmapreduce.job.tags= -gc -mapperXmx ${H2ONODEMEMORYINGB}g -nodes ${H2ONOOFNODES} -timeout 500 -driverport ${H2ODRIVERPORT} -baseport ${H2OBASEPORT} -output $path  -disown -notify $path"    
}

function copys3tohdfs()
{
    local src=$1
    local dst=$2
    local qds_cmd=`_get_qubole_config_qds_cmd`
    local url=`_get_qubole_config_url`
    local token=`_get_qubole_config_token`
    local cluster=`_get_qubole_config_arcadia_cluster`

    $qds_cmd --url=$url --token=$token hadoopcmd run --cluster=$cluster s3distcp --src "$src" --dest "$dst"
    local cmd_status=$?
    if [[ "$cmd_status" != "0" ]]; then
      echo "copy from src: $src to dst: $dst failed"
      exit 1
    fi    
}

function copyTableDatafroms3ToHDFS()
{
    local table_name=$1

    local tname=`echo $table_name|tr -d ''`
    IFS='.' read -r -a ar <<<$tname
    tab="${ar[0]}.db/${ar[1]}"
    local user=`whoami`
    local locations3=`_get_s3_location`
    local locationhdfs=`_get_arcadia_hdfs_location`
    local srcPath="$locations3$user/$tab"
    local dstPath="$locationhdfs$user/${ar[0]}/${ar[1]}"
    copys3tohdfs $srcPath $dstPath
}

function move_sql_file_to_hdfs_qubole()
{
    local input_file=$1 
    local hdfs_dir_path=$2  
    local out_file=$3
    local user=`whoami`
    ipath=`echo $input_file|tr -d ''`
    cp $input_file ./_external_script
    tr -d '' < ./_external_script >$ipath
    local dest=`_get_s3_location`
    NeuHadoop fs -put $ipath "$dest$user/" &> /dev/null 2>&1
    cmd_status=$?
    if [[ $cmd_status != 0 ]]; then
        echo "upload failed"
        exit 1
    fi
    IFS='/' read -a pathAr <<< "$ipath"
    fname=`echo ${pathAr[${#pathAr[@]} - 1]}`
    local src="$dest$user/$fname"
    local hdfsBase=`_get_arcadia_sql_hdfs_location`
    local dst="$hdfsBase$user/"
    copys3tohdfs $src $dst
}

function quboleSetjdbcURL()
{
    if [ "${platform}" == "qubole" ]; then
        local qds_cmd=`_get_qubole_config_qds_cmd`
        local url=`_get_qubole_config_url`
        local token=`_get_qubole_config_token`
        local cluster=`_get_qubole_config_cluster`
        local jdbc_url=`$qds_cmd --url=$url --token=$token shellcmd run --cluster=$cluster -s 'hdfs getconf -confKey fs.default.name'|tr -d '^M'`
        jdbc_url=`echo $jdbc_url| cut -f2 -d':'|cut -c 3-`
        
        if [ -d "${client_common_custom_input}" ]; then
            echo "directory ${client_common_custom_input} found" >>~/a1_jdbc_log.txt
            xmlstarlet edit --inplace -u "configroot/set[name='hive_hostname']/stringval" -v $jdbc_url ${client_common_custom_input}/hive_config.xml
            xmlstarlet edit --inplace -u "configroot/set[name='hive_port']/stringval" -v 10004 ${client_common_custom_input}/hive_config.xml
        fi

        if [ -d "${client_common_ddm_input}" ]; then
            echo "directory ${client_common_ddm_input} found" >>~/a1_jdbc_log.txt
            xmlstarlet edit --inplace -u "configroot/set[name='hive_hostname']/stringval" -v $jdbc_url ${client_common_ddm_input}/hive_config.xml
            xmlstarlet edit --inplace -u "configroot/set[name='hive_port']/stringval" -v 10004 ${client_common_ddm_input}/hive_config.xml
        fi

        if [ -d "${config_inputs_loc}" ]; then
            xmlstarlet edit --inplace -u "configroot/set[name='hive_hostname']/stringval" -v $jdbc_url ${config_inputs_loc}/hive_config.xml
            xmlstarlet edit --inplace -u "configroot/set[name='hive_port']/stringval" -v 10004 ${config_inputs_loc}/hive_config.xml
        fi
    fi
}

