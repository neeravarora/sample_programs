#!/bin/bash

this_file_path=`readlink -f $BASH_SOURCE | rev | cut -d'/' -f2- | rev`
source $this_file_path/qubole_functions.sh
source $this_file_path/emr_functions.sh


function check_platform_var() {
    if [ -z "$PLATFORM_CONFIG_FILE" ]; then
        return 1
    fi

    if [ ! -f "$PLATFORM_CONFIG_FILE" ]; then
        return 1
    fi

}

function _get_emr_config_cluster() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v "configroot/emr_settings/set[name='cluster']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_platform_name() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v  "/configroot/set[name='PLATFORM']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_qubole_config_qds_cmd() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v "configroot/qubole_settings/set[name='qds_cmd']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_qubole_config_url() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v "configroot/qubole_settings/set[name='url']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_qubole_config_token() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v "configroot/qubole_settings/set[name='token']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_qubole_config_cluster() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v "configroot/qubole_settings/set[name='cluster']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_config_s3_cmd() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v "configroot/qubole_settings/set[name='s3cmd']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_config_s3_access_key() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v "configroot/qubole_settings/set[name='access_key']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_config_s3_secret_key() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v "configroot/qubole_settings/set[name='secret_key']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_config_s3_region() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v "configroot/qubole_settings/set[name='region']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_altiscale_config_hive_cmd() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v "/configroot/altiscale_settings/set[name='hive_cmd']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_altiscale_config_hadoop_cmd() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v "/configroot/altiscale_settings/set[name='hadoop_cmd']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_client_type() {
    check_platform_var
    if [ "$?" -ne "0" ]; then
        return 1
    fi
    echo `xmlstarlet sel -T -t -v "configroot/set[name='CLIENT_TYPE']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

#TODO: pending
function setRunEnv()
{
    local action_backend_platform=`_get_platform_name`
    if [ "${action_backend_platform^^}" == "QUBOLE" ]; then
      export platform="qubole"
    elif [ "${action_backend_platform^^}" == "ALTISCALE" ]; then
      export platform="altiscale"
    elif [ "${action_backend_platform^^}" == "EMR" ]; then
      export platform="emr"
    fi
    
    local clientType=`_get_client_type`
    if [ "$clientType" == "facebook" ]; then
      export client_type="facebook"
    fi
}

function setjdbcURL()
{
    if [ "${platform}" == "qubole" ]; then
      quboleSetjdbcURL
    elif [ "${platform^^}" == "EMR" ]; then
      emrSetjdbcURL
    fi
}

function setJarLocation()
{

 if [ "${platform}" == "qubole" ]; then
     local qubole_jar_location=`_get_qubole_jar_location`
     set_config_param_in_temp_sysdef "JARLOCATION" "$qubole_jar_location"

 elif [ "${platform^^}" == "EMR" ]; then
    local emr_jar_location=`_get_qubole_jar_location`
     set_config_param_in_temp_sysdef "JARLOCATION" "$emr_jar_location"
 fi
}
#function hadoop() {
#    echo "use NeuHadoop instead"
#   _NeuHadoop_usage
#    return 1
#}

function _NeuHadoop_usage() {
    echo "NeuHadoop usage"
    echo "    1. NeuHadoop fs -put <source_file> <dest_file> [--access_key=<access_key> --secret_key=<secret_key> --region=<region>]"
    echo "    2. NeuHadoop fs -get <source_file> <dest_file>"
    echo "    3. NeuHadoop fs -rm <file>"
    echo "    4. NeuHadoop fs -mkdir <dir>"
    echo "    5. NeuHadoop fs -du <file>"

    echo "aws s3 cli commands usage"   
    echo "    1. NeuHadoop -awss3cli -ls <s3_location>"
    echo "    2. NeuHadoop -awss3cli -mb <bucket_name>"
    echo "    3. NeuHadoop -awss3cli -cp <source_location> <target_location> [--recursive --dryrun ]"
    echo "    4. NeuHadoop -awss3cli -mv <source_location> <target_location> [--recursive --dryrun ]"
    echo "    5. NeuHadoop -awss3cli -sync <source_location> <target_location> [ --dryrun --delete --exclude <exclude_dir>]"
    echo "    6. NeuHadoop -awss3cli -rm <s3_dir> [ --dryrun --recursive ]"
}


function NeuHadoop() {
    local access_key=""
    local secret_key=""
    local region=""
    local recursive_option=""
    local given_args=$@

    local awss3cli_option=""
    local awss3cli_dryrun_option=""
    local awss3cli_recursive_option=""
    local awss3cli_delete_option=""
    local awss3cli_exclude_val=""
    local awss3cli_exclude_args=""
    local awss3cli_cp_source_val=""
    local awss3cli_cp_target_val=""
    local awss3cli_rm_val=""
    local awss3cli_mb_val=""
    local awss3cli_ls_val=""
    local awss3cli_sync_source_val=""
    local awss3cli_sync_target_val=""
    local awss3cli_mv_source_val=""
    local awss3cli_mv_target_val=""

    if [[ "${given_args#*-awss3cli }" != "$given_args" ]]; then
        awss3cli_option="true"
    fi   

    if [[ "$awss3cli_option" == "true" ]]; then 

        while : ;
        do
            if [ "$1" == "" ]; then
                break;
            fi
            case $1 in
                "-awss3cli")
                    shift
                    ;;

                "-ls")
                    awss3cli_ls_val=$2
                    if [ -z "$awss3cli_ls_val" ]; then
                        echo "aws s3 cli: ls option value: $awss3cli_ls_val is null"
                        return 1
                    fi
                    shift 2
                    ;;
    
                "-mb")
                    awss3cli_mb_val=$2
                    if [ -z "$awss3cli_mb_val" ]; then
                        echo "aws s3 cli: mb option value: $awss3cli_mb_val is null"
                        return 1
                    fi
                    shift 2
                    ;;

                "-cp")
                    awss3cli_cp_source_val=$2
                    awss3cli_cp_target_val=$3
                    if [ -z "$awss3cli_cp_source_val" ]; then
                        echo "aws s3 cli: cp source location value: $awss3cli_cp_source_val is null"
                        return 1
                    fi
                    if [ -z "$awss3cli_cp_target_val" ]; then
                        echo "aws s3 cli: cp target location value: $awss3cli_cp_target_val is null"
                        return 1
                    fi
                    shift 3
                    ;;

                "-rm")
                    awss3cli_rm_val=$2
                    if [ -z "$awss3cli_rm_val" ]; then
                        echo "aws s3 cli: rm option value: $awss3cli_rm_val is null"
                        return 1
                    fi
                    shift 2
                    ;;

                "-mv")
                    awss3cli_mv_source_val=$2
                    awss3cli_mv_target_val=$3
                    if [ -z "$awss3cli_mv_source_val" ]; then
                        echo "aws s3 cli: mv source location value: $awss3cli_mv_source_val is null"
                        return 1
                    fi
                    if [ -z "$awss3cli_mv_target_val" ]; then
                        echo "aws s3 cli: mv target location value: $awss3cli_mv_target_val is null"
                        return 1
                    fi
                    shift 3
                    ;;

                "-sync")
                    awss3cli_sync_source_val=$2
                    awss3cli_sync_target_val=$3
                    if [ -z "$awss3cli_sync_source_val" ]; then
                        echo "aws s3 cli: sync source location value: $awss3cli_sync_source_val is null"
                        return 1
                    fi
                    if [ -z "$awss3cli_sync_target_val" ]; then
                        echo "aws s3 cli: sync target location value: $awss3cli_sync_target_val is null"
                        return 1
                    fi
                    shift 3
                    ;;

                "--recursive")
                    awss3cli_recursive_option=$1
                    shift
                    ;;

                "--delete")
                    awss3cli_delete_option=$1
                    shift
                    ;;

                "--dryrun")
                    awss3cli_dryrun_option=$1
                    shift 
                    ;;

                "--exclude")
                    awss3cli_exclude_val=$2
                    if [ -z $awss3cli_exclude_val ]; then
                        break
                    fi
                    awss3cli_exclude_val="$awss3cli_exclude_val/*"
                    awss3cli_exclude_args="$awss3cli_exclude_args --exclude $awss3cli_exclude_val"
                    shift 2
                    ;;

                *)
                    echo "option not supported"
                    return 1  
                    ;;
            esac
        done
    else

        while :
        do
            if [ "$1" == "" ]; then
                break;
            fi
            case $1 in
                "-r"|"--recursive")
                    recursive_option="-r" 
                    shift
                    ;;
                *)
                    if [ ${1#*--access_key} != $1 ]; then
                        access_key=`echo $1|cut -d'=' -f2|xargs`
                    elif [ ${1#*--secret_key} != $1 ]; then
                        secret_key=`echo $1|cut -d'=' -f2|xargs`
                    elif [ ${1#*--region} != $1 ]; then
                        region=`echo $1|cut -d'=' -f2|xargs`
                    fi
                    shift
                    ;;

            esac
        done
    fi

    action_backend_platform=`_get_platform_name`
    if [ "$?" != "0" ]; then
        echo "check if env PLATFORM_CONFIG_FILE is exported to absolute path of platform_confg.xml file and the file exists"        
        return 1
    fi
    if [ "${action_backend_platform^^}" == "QUBOLE" ] || [ "${action_backend_platform^^}" == "EMR" ]; then
        local session_token=""
        if [  "${action_backend_platform^^}" == "EMR" ]; then
            updateClusterId
            if [ -z "$access_key" ]; then
                access_key=`_get_emr_s3_access_key`
            fi
            if [ -z "$secret_key" ]; then
                secret_key=`_get_emr_s3_secret_key`
            fi
            if [ -z "$region" ]; then
                region=`_get_emr_s3_region`
            fi
        else
            if [ -z "$access_key" ]; then
                access_key=`_get_config_s3_access_key`
            fi
            if [ -z "$secret_key" ]; then
                secret_key=`_get_config_s3_secret_key`
            fi
            if [ -z "$region" ]; then 
                region=`_get_config_s3_region`
            fi
        fi
        if [[ "$access_key" == "" ||  "$secret_key" == "" || "$region" == "" ]]; then
            echo "fill in values for  aws_cmd, region in $PLATFORM_CONFIG_FILE"
            return 1
        fi

        local recursive_option=""
        if [[ "${given_args#*-r }" != "$given_args"  || "${given_args#*--recursive}" != "$given_args" ]]; then
            recursive_option=" -r "
        fi
        
        local trimmed_args=`echo $given_args | sed 's/^fs / /' | sed 's/ fs / /' | sed 's/\s-[-]*[^ ]*//g' | sed 's/^-[-]*[^ ]*//g'`

        if [[ "$awss3cli_option" == "true" ]]; then

            #configure aws
            aws configure set aws_access_key_id $access_key
            aws configure set aws_secret_access_key $secret_key
            aws configure set default.region $region

            if [[ "${given_args#*-mb}" != "$given_args" ]]; then
                # creates bukcet in s3 
                aws s3 mb $awss3cli_mb_val 
                return $?
            elif [[ "${given_args#*-cp}" != "$given_args" ]]; then
                # copies the directory from source to target loc recursively based on the recursive option given
                aws s3 cp $awss3cli_cp_source_val  $awss3cli_cp_target_val  $awss3cli_dryrun_option  $awss3cli_recursive_option
                return $?
            elif [[ "${given_args#*-rm}" != "$given_args" ]]; then
                # deleted the directory recursively based on the recursive option given
                aws s3 rm  $awss3cli_rm_val  $awss3cli_dryrun_option $awss3cli_recursive_option
                return $?
            elif [[ "${given_args#*-mv}" != "$given_args" ]]; then
                # moves the files from source directory to target directory recursively based on the recursive option provided
                aws s3 mv $awss3cli_mv_source_val  $awss3cli_mv_target_val  $awss3cli_dryrun_option  $awss3cli_recursive_option
                return $?
            elif [[ "${given_args#*-sync}" != "$given_args" ]]; then
                #syncs the data recursively from source path to the target path
                #--delete : deletes the files in target folder that do not exist in source during sync
                aws s3 sync $awss3cli_sync_source_val  $awss3cli_sync_target_val  $awss3cli_dryrun_option  $awss3cli_delete_option $awss3cli_exclude_args
                return $?
            elif [[ "${given_args#*-ls}" != "$given_args" ]]; then
                # lists the objects in a folder or checks if the folder exists
                aws s3 ls $awss3cli_ls_val $awss3cli_recursive_option
                return $?
            else
                # as of now -get, -put, -rm, -du, -mkdir are implemented.
                # rest of arguments are do-nothings and returns 0
                echo "option not supported"
                return 1
            fi
        else

            if [[ "${given_args#*-get}" != "$given_args" ]]; then
                # NOTE: We need the following --access_token option for EMR, but that needs s3cmd version 2.0.0.
                # When we move to that s3cmd version and when we need EMR, enable this
                # s3cmd get $recursive_option $trimmed_args --region  "$region" --access_key="$access_key" --secret_key="$secret_key" --access_token="$session_token" --force
                s3cmd get $recursive_option $trimmed_args --region  "$region" --access_key="$access_key" --secret_key="$secret_key"  --force
                return $?
            elif [[ "${given_args#*-put}" != "$given_args" ]]; then
                # NOTE: We need the following --access_token option for EMR, but that needs s3cmd version 2.0.0.
                # When we move to that s3cmd version and when we need EMR, enable this
                # s3cmd put $trimmed_args --region  "$region" --access_key="$access_key" --secret_key="$secret_key" --access_token="$session_token" --force
                s3cmd put $trimmed_args --region  "$region" --access_key="$access_key" --secret_key="$secret_key"  --force
                return $?
            elif [[ "${given_args#*-rm}" != "$given_args" ]]; then
                # NOTE: We need the following --access_token option for EMR, but that needs s3cmd version 2.0.0.
                # When we move to that s3cmd version and when we need EMR, enable this
                # s3cmd rm  $recursive_option $trimmed_args --region  "$region" --access_key="$access_key" --secret_key="$secret_key" --access_token="$session_token"
                s3cmd rm  $recursive_option $trimmed_args --region  "$region" --access_key="$access_key" --secret_key="$secret_key" --force
                return $?
            elif [[ "${given_args#*-mkdir}" != "$given_args" ]]; then
                # no implementation needed, s3 creates nested dirs while copying file
                return 0
            elif [[ "${given_args#*-du}" != "$given_args" ]]; then
                # NOTE: We need the following --access_token option for EMR, but that needs s3cmd version 2.0.0.
                # When we move to that s3cmd version and when we need EMR, enable this
                # s3cmd du $trimmed_args --region  "$region" --access_key="$access_key" --secret_key="$secret_key" --access_token="$session_token"
                s3cmd du $trimmed_args --region  "$region" --access_key="$access_key" --secret_key="$secret_key"
                return $?
            elif [[ "${given_args#*-cp}" != "$given_args" ]]; then
                # NOTE: We need the following --access_token option for EMR, but that needs s3cmd version 2.0.0.
                # When we move to that s3cmd version and when we need EMR, enable this
                # s3cmd cp $trimmed_args --region  "$region" --access_key="$access_key" --secret_key="$secret_key" --access_token="$session_token"
                s3cmd cp $recursive_option $trimmed_args --region  "$region" --access_key="$access_key" --secret_key="$secret_key"
                return $?
            elif [[ "${given_args#*-ls}" != "$given_args" ]]; then
#                s3cmd ls $trimmed_args --region  "$region" --access_key="$access_key" --secret_key="$secret_key" --access_token="$session_token" | sed 's/  */ /g' | rev | cut -d' ' -f1 | rev
#                return ${PIPESTATUS}
                 if [  "${action_backend_platform^^}" == "EMR" ]; then
                    updateClusterId
                    local cluster=`_get_emr_config_cluster`
                    local clusterId=`_get_emr_cluster_id`
                    local masterNodeIP=`aws emr describe-cluster --cluster-id $clusterId |jq .Cluster.MasterPublicDnsName|tr -d '"'`
                    local pemKeyPath=`_get_emr_pem_path`
                    _get_emr_hadoop_output $cluster "hadoop fs -ls $trimmed_args" $masterNodeIP $pemKeyPath
                    return $?
                else
                    local qds_cmd=`_get_qubole_config_qds_cmd`
                    local url=`_get_qubole_config_url`
                    local token=`_get_qubole_config_token`
                    local cluster=`_get_qubole_config_cluster`
                    if [[ "$qds_cmd" == "" || "$url" == "" || "$token" == "" || "$cluster" == "" ]]; then
                        echo "check if config values for qds_cmd, url, token, cluster are available in $PLATFORM_CONFIG_FILE"
                        return 1
                    fi
                    $qds_cmd --url=$url --token=$token shellcmd run --cluster=$cluster -s "hadoop fs -ls $trimmed_args"
                    return $?
                fi
            else
                # as of now -get, -put, -rm, -du, -mkdir are implemented. 
                # rest of arguments are do-nothings and returns 0
                echo "option not supported"
                return 1
            fi
        fi   
    elif [ "${action_backend_platform^^}" == "ALTISCALE" ]; then
        local hadoop_cmd=`_get_altiscale_config_hadoop_cmd`
        if [ ! -z "$hadoop_cmd" ]; then
            if [[ "${given_args#*-put}" != "$given_args" ]] || [[ "${given_args#*-get}" != "$given_args" ]] || [[ "${given_args#*-cp}" != "$given_args" ]] ;  then
                given_args=`echo $given_args | sed 's/-r / /' | sed 's/ --recursive / /'`
                $hadoop_cmd $given_args
                return $?
            elif [[ "${given_args#*-mkdir}" != "$given_args" ]] || [[ "${given_args#*-du}" != "$given_args" ]] || [[ "${given_args#*-ls}" != "$given_args" ]] ||
              [[ "${given_args#*-rm}" != "$given_args" ]]; then
              $hadoop_cmd $given_args
              return $?
            else
                echo "option not supported"
                return 1
            fi
        else
            echo "hadoop_cmd is not avalable in config $PLATFORM_CONFIG_FILE"
            return 1
        fi            
    else
        echo "unknown platform $action_backend_platform, supported platforms are ALTISCALE, QUBOLE"
        return 1
    fi
}


function _NeuSQL_usage() {
    echo "usage: 1. NeuSQL -e 'sql commands'"
    echo "          this will run the sql commands and prints the result of sql commands to stdout"
    echo "usage: 2. NeuSQL -f 'file with sql commands"
    echo "          this will run the sql commands file and prints the results to stdout and logs to stderr"
}

#function hive() {
#    echo "use NeuSQL instead"
#   _NeuSQL_usage 
#   return 1
#}

function NeuSQL() {
    local arg_type=""
    local string_arg=""

    action_backend_platform=`_get_platform_name`
    if [ "$?" != "0" ]; then
        echo "check if env PLATFORM_CONFIG_FILE is exported to absolute path of platform_confg.xml file and the file exists"
        return 1
    fi
    
    #  echo "platform $action_backend_platform"
    if [ "${action_backend_platform^^}" == "QUBOLE" ]; then
        local qds_cmd=`_get_qubole_config_qds_cmd`   
        local url=`_get_qubole_config_url`
        local token=`_get_qubole_config_token`
        local cluster=`_get_qubole_config_cluster`

        if [[ "$qds_cmd" == "" || "$url" == "" || "$token" == "" || "$cluster" == "" ]]; then
            echo "check if config values for qds_cmd, url, token, cluster are available in $PLATFORM_CONFIG_FILE"
            return 1
        fi

        while : ;
        do
            if [ "$1" == "" ]; then
                break;
            fi
            
            case $1 in
                "-e"|"-f")
                    arg_type="$1"
                    shift
                    string_arg="$1"
                    if [[ "$string_arg"  == -* ]]; then
                         echo "Need value for option $arg_type"
                         return 1
                    fi
                    shift
                    ;;
                "-S"|"-v")
                    # ignore this option for qubole
                    shift
                    ;;
                "-engine=spark")
                    arg_engine_type="$1"
                    shift
                    ;;
                 "-sc")
                    arg_sc="$1"
                    shift
                    spark_settings_location_arg="$1"
                    if [[ "$spark_settings_location_arg"  == -* ]]; then
                         echo "Need value for option $spark_settings_location_arg"
                         return 1
                    fi
                    shift
                    ;;

				"-cancel")
					shift
					jobid="$1"
					if [[ ! -z "$jobid" ]]; then
						cancelQuboleJob $qds_cmd $url $token $jobid
						if [[ "$?" == "0" ]]; then
							return 0
						else
							return 1
						fi
					else
						echo "invalid value given for qubole jobid, hence exiting.."
						return 1
					fi
					;;

                *)
                    echo "Unsupported option $1"
                    return 1
                    ;;
            esac
        done

         if [[ "$arg_engine_type" == "-engine=spark" ]]; then
            spark_cluster_status=`$qds_cmd --url=$url --token=$token cluster list --label=$cluster | jq .cluster.hadoop_settings.use_spark`

            if [[ "$spark_cluster_status" != "true" ]]; then
                echo "Spark is not enabled on the cluster" >&2
                return 1
            else
                echo "Spark is enabled on the cluster" >&2
            fi
         fi

        if [[ "$arg_engine_type" == "-engine=spark" &&  "$arg_type" == "-e" ]]; then
            #$qds_cmd --url=$url --token=$token sparkcmd run --cluster=$cluster --sql "$string_arg" --arguments '--jars s3://neustar-dsdk/Common/Jars/murmur-hash3-128-udf-1.0-with-dependencies.jar,s3://neustar-dsdk/Common/Jars/hiveudf_hash.jar --conf spark.driver.extraLibraryPath=/usr/lib/hadoop2/lib/native --conf spark.eventLog.compress=true --conf spark.eventLog.enabled=true --conf spark.executor.instances=90 --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.hadoop.parquet.enable.summary-metadata=false --conf spark.logConf=true --conf spark.shuffle.reduceLocality.enabled=false --conf spark.speculation=false --conf spark.sql.qubole.ignoreFNFExceptions=true --conf spark.sql.qubole.split.computation=false --conf spark.ui.retainedJobs=33 --conf spark.ui.retainedStages=100 --conf spark.yarn.maxAppAttempts=1 --conf spark.hadoop.fs.s3a.connection.establish.timeout=5000 --conf spark.hadoop.fs.s3a.connection.maximum=200 --conf spark.dynamicAllocation.enabled=true --conf spark.executor.cores=6 --conf spark.executor.memory=20945M --conf spark.executor.memoryOverhead=6389 --conf spark.yarn.executor.memoryOverhead=2070M --conf spark.master=yarn --conf spark.qubole.listenerbus.eventlog.queue.capacity=20000 --conf spark.scheduler.listenerbus.eventqueue.capacity=20000 --conf spark.shuffle.service.enabled=true --conf spark.submit.deployMode=client --conf spark.sql.inMemoryColumnarStorage.compressed=true --conf spark.sql.autoBroadcastJoinThreshold=20485760 --conf spark.sql.inMemorycolumnarStorage.compressed=trye --conf spark.sql.shuffle.partitions=500 --conf spark.default.parallelism=500 --conf spark.dynamicAllocation.executorIdleTimeout=600s --conf spark.network.timeout=420000 --conf spark.hadoop.hive.qubole.consistent.loadpartition=true'| sed 's/^M//g' | sed 's/\r//g'
          echo "The spark_settings_location_arg is ---> ${spark_settings_location_arg}"
          spark_settings=`sed -n '2p' $spark_settings_location_arg`
          $qds_cmd --url=$url --token=$token sparkcmd run --cluster=$cluster --sql "$string_arg" --arguments ''"$spark_settings"''| sed 's/^M//g' | sed 's/\r//g'

            return ${PIPESTATUS[0]}
        elif  [[ "$arg_type" == "-e" ]]; then
            $qds_cmd --url=$url --token=$token hivecmd run --cluster=$cluster --query "$string_arg" | sed 's/\^M//g' | sed 's/\r//g'
            return ${PIPESTATUS[0]}
        elif [[ "$arg_engine_type" == "-engine=spark" && "$arg_type" == "-f" ]]; then
            if [ ! -f "$string_arg" ]; then
                echo "$string_arg is not a file"
                return 1
            fi
#            jobid=`$qds_cmd --url=$url --token=$token sparkcmd submit --cluster=$cluster --script_location "$string_arg" --arguments '--jars s3://neustar-dsdk/Common/Jars/murmur-hash3-128-udf-1.0-with-dependencies.jar,s3://neustar-dsdk/Common/Jars/hiveudf_hash.jar --conf spark.driver.extraLibraryPath=/usr/lib/hadoop2/lib/native --conf spark.eventLog.compress=true --conf spark.eventLog.enabled=true --conf spark.executor.instances=90 --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.hadoop.parquet.enable.summary-metadata=false --conf spark.logConf=true --conf spark.shuffle.reduceLocality.enabled=false --conf spark.speculation=false --conf spark.sql.qubole.ignoreFNFExceptions=true --conf spark.sql.qubole.split.computation=false --conf spark.ui.retainedJobs=33 --conf spark.ui.retainedStages=100 --conf spark.yarn.maxAppAttempts=1 --conf spark.hadoop.fs.s3a.connection.establish.timeout=5000 --conf spark.hadoop.fs.s3a.connection.maximum=200 --conf spark.dynamicAllocation.enabled=true --conf spark.executor.cores=6 --conf spark.executor.memory=20945M --conf spark.executor.memoryOverhead=6389 --conf spark.yarn.executor.memoryOverhead=2070M --conf spark.master=yarn --conf spark.qubole.listenerbus.eventlog.queue.capacity=20000 --conf spark.scheduler.listenerbus.eventqueue.capacity=20000 --conf spark.shuffle.service.enabled=true --conf spark.submit.deployMode=client --conf spark.sql.inMemoryColumnarStorage.compressed=true --conf spark.sql.autoBroadcastJoinThreshold=20485760 --conf spark.sql.inMemorycolumnarStorage.compressed=trye --conf spark.sql.shuffle.partitions=500 --conf spark.default.parallelism=500 --conf spark.dynamicAllocation.executorIdleTimeout=600s --conf spark.network.timeout=420000 --conf spark.hadoop.hive.qubole.consistent.loadpartition=true'`
          echo "The spark_settings_location_arg is ---> ${spark_settings_location_arg}"
          spark_settings=`sed -n '2p' $spark_settings_location_arg`

          jobid=`$qds_cmd --url=$url --token=$token sparkcmd submit --cluster=$cluster --script_location "$string_arg" --arguments ''"$spark_settings"''`

            jobid=`echo $jobid|cut -d':' -f2|xargs`

            sats=`_pollQueryStatus $qds_cmd $url $token $jobid`
            #$qds_cmd --url=$url --token $token sparkcmd getlog $jobid >&2
            if [ "$sats" == "done" ]; then
                return 0
            else
                return 1
            fi

        elif [[ "$arg_type" == "-f" ]]; then
            if [ ! -f "$string_arg" ]; then
                echo "$string_arg is not a file"
                return 1
            fi
            jobid=`$qds_cmd --url=$url --token $token hivecmd submit --script_location "$string_arg" --cluster=$cluster`
            jobid=`echo $jobid|cut -d':' -f2|xargs`
			echo "COMMAND ID: $jobid has Started!!" >&1
            
			sats=`_pollQueryStatus $qds_cmd $url $token $jobid`
            $qds_cmd --url=$url --token $token hivecmd getlog $jobid >&2
			
			echo "JOB ID: $jobid" >&1
            echo "STATUS: $sats" >&1
            if [ "$sats" == "done" ]; then
                return 0
            else
                return 1
            fi
        fi
        elif [ "${action_backend_platform^^}" == "EMR" ]; then
          updateClusterId
          local cluster=`_get_emr_config_cluster`
          if [[ "$cluster" == "" ]]; then
              echo "fill in value for cluster in $PLATFORM_CONFIG_FILE"
              return 1
          fi

          while : ;
          do
              if [ "$1" == "" ]; then
                  break;
              fi

              case $1 in
                  "-e"|"-f")
                      arg_type="$1"
                      shift
                      string_arg="$1"
                      if [[ "$string_arg"  == -* ]]; then
                           echo "Need value for option $arg_type"
                           return 1
                      fi
                      shift
                      ;;

                  "-S"|"-v")
                      # ignore this option for qubole
                      shift
                      ;;

                  *)
                      echo "Unsupported option $1"
                      return 1
                      ;;
              esac
          done

          user=`whoami`
          local bucket_loc=`_get_emr_s3_location`
          TIME_STAMP=$(date +'%s_%N')
          fname_hql="hql_$TIME_STAMP.hql"
          local s3_script_loc="${bucket_loc}/scripts/${user}/"
          local cmd_type=""
          if [[ "$arg_type" == "-e" ]]; then
            local tmp_file_loc="/tmp/"
            echo "$string_arg" > ${tmp_file_loc}/$fname_hql
            cmd_type="E_OPTION"
            NeuHadoop fs -put "${tmp_file_loc}/$fname_hql" "$s3_script_loc" &> /dev/null 2>&1
            rm ${tmp_file_loc}/$fname_hql
          elif [[ "$arg_type" == "-f" ]]; then
            if [ ! -f "$string_arg" ]; then
                echo "$string_arg is not a file"
                return 1
            fi
            cmd_type="F_OPTION"
            NeuHadoop fs -put "$string_arg" "${s3_script_loc}${fname_hql}" &> /dev/null 2>&1
          fi
          jobid=`aws emr add-steps --cluster-id $cluster --steps Type=Hive,Name="Hive Program",ActionOnFailure=CONTINUE,Args=[-f,${s3_script_loc}${fname_hql},-d,INPUT=${bucket_loc}/,-d,OUTPUT=${bucket_loc}/]`
          jobid=`echo $jobid| jq '.StepIds'| jq .[0]|xargs`

          sats=`_pollStepStatus $cluster $jobid`
          local clusterId=`_get_emr_cluster_id`
          local masterNodeIP=`aws emr describe-cluster --cluster-id $clusterId |jq .Cluster.MasterPublicDnsName|tr -d '"'`
          local pemKeyPath=`_get_emr_pem_path`

          if [ "$sats" == "COMPLETED" ]; then
                _get_emr_logs_master  $cluster $jobid "COMPLETED" $cmd_type $masterNodeIP $pemKeyPath
              return 0
          else
              _get_emr_logs_master $cluster $jobid "FAILED" $cmd_type $masterNodeIP $pemKeyPath
              return 1
          fi
          elif [ "${action_backend_platform^^}" == "ALTISCALE" ]; then
          hive_cmd=`_get_altiscale_config_hive_cmd`
          if [ ! -z "$hive_cmd" ]; then
              $hive_cmd "$@"
              return $?
          else
              echo "check if config value for hive_cmd is available in $PLATFORM_CONFIG_FILE"
              return 1
          fi
    else
        echo "unknown platform $action_backend_platform, supported platforms are ALTISCALE, QUBOLE"
        return 1
    fi
}

function Neuh2o(){
    local platform=$1
    local H2ONODEMEMORYINGB=$2
    local H2ONOOFNODES=$3
    local H2O_DRIVER_PORT=$4
    local H2O_BASE_PORT=$5
    local OUT_FILE=$6
    local clientName=$7
    local projectname=$8
    local HADOOP_QUEUE_NAME="default"

    TIME_STAMP=$(date +'%s_%N')
    LOG_FILE="/tmp/startH2OServer$TIME_STAMP.log"
  if [ "$platform" == "altiscale" ]; then
    # Go to h2o location in repository
    cd $GIT_DWNLD_PATH/$GIT_REPOSITORY_NAME/common/BU3.0_core/util/h2o/hadoop
    # Remove files with same name
    hadoop fs -rm -r /tmp/h2o_temp_test_$TIME_STAMP
    rm -f /tmp/ip_h2o_temp_test_$TIME_STAMP.txt
    # Start the H2O hadoop server from shell script
    hadoop jar h2odriver.jar -Dmapreduce.job.queuename=$HADOOP_QUEUE_NAME -mapperXmx ${H2ONODEMEMORYINGB}g -nodes $H2ONOOFNODES -driverport $H2O_DRIVER_PORT -baseport $H2O_BASE_PORT -output /tmp/h2o_temp_test_$TIME_STAMP -disown -notify /tmp/ip_h2o_temp_test_$TIME_STAMP.txt 2>&1 > $LOG_FILE
  elif [ "${platform,,}" == "qubole" ]; then
    echo "Launching qubole cluster"
    outpath="/tmp/ip_h2o_temp_test_$TIME_STAMP.txt"
    launch_qubole_cluster $outpath $H2ONODEMEMORYINGB $H2ONOOFNODES $H2O_DRIVER_PORT $H2O_BASE_PORT > $LOG_FILE
    dos2unix $LOG_FILE
  elif [ "${platform^^}" == "EMR" ]; then
    outpath="/tmp/ip_h2o_temp_test_$TIME_STAMP.txt"A
    launch_emr_h2o_cluster_noclient $outpath $H2ONODEMEMORYINGB $H2ONOOFNODES $H2O_DRIVER_PORT $H2O_BASE_PORT $clientName $projectname > $LOG_FILE
    dos2unix $LOG_FILE
  fi

        if [ "$?" -ne "0" ];
        then
                # Error while starting H2O hadoop server
                cat $LOG_FILE
                echo "Unable to run H2O server."
                exit -1
        fi

  if [ "$platform" == "altiscale" ]; then
    # Extract application id, server ip and server port for later use
    APPLICATION_ID=$(cat $LOG_FILE | grep "application_*_*" | head -1 | awk -F"[ |']" '{print $11}')
    SERVER_IP=$(cat /tmp/ip_h2o_temp_test_$TIME_STAMP.txt | head -1 | awk -F"[:']" '{print $1}')
    SERVER_PORT=$(cat /tmp/ip_h2o_temp_test_$TIME_STAMP.txt | head -1 | awk -F"[:']" '{print $2}')
    rm $LOG_FILE
  elif [ "${platform^^}" == "QUBOLE" ]; then
    str_t=`cat $LOG_FILE |grep "Open H2O Flow"`
    str=`echo $str_t|tr -d '^M'`
    ar=(`echo $str|grep -aob ':'|grep -oE '[0-9]+'|tail -2`)
    ipBegin=$(expr ${ar[0]} + 3)
    ipLen=$(expr ${ar[1]} - ${ar[0]} - 3)
    portBegin=$(expr ${ar[1]} + 1)
    portLen=$(expr ${#str} - ${ar[1]})
    SERVER_PORT=${str:$portBegin:$portLen}
    SERVER_IP=${str:$ipBegin:$ipLen}
  elif [ "${platform^^}" == "EMR" ]; then
    local ip_port=`cat $LOG_FILE|head -1|tr -d '^M'`
    SERVER_IP=`echo $ip_port|cut -f1 -d ':'|xargs`
    SERVER_PORT=`echo $ip_port|cut -f2 -d ':'|xargs`
  fi

  echo "H2o Server IP is : $SERVER_IP" > $OUT_FILE
  echo "H2o Server Port Number is : $SERVER_PORT" >> $OUT_FILE
}

function _pollQueryStatus()
{
    local qds_cmd=$1
    local url=$2
    local token=$3
    local jobid=$4

    local sats="unknown"
	#echo "/n job_id: $jobid /n" >&
    while [[ "$sats" == "unknown" ]] || [[ "$sats" == "waiting" ]] || [[ "$sats" == "running" ]]
    do
        status=`$qds_cmd --url=$url --token $token hivecmd check $jobid`
        local cmd_status=`echo $?`
        if [ "$cmd_status" != "0" ]; then
          sats="unknown"
        else
          sats=`echo $status|${common_script_loc}jq '.status'|xargs`
		  #echo "/n sats: $sats /n" >&2
        fi
		#echo "/n status: $status /n" >&2
        sleep 60
    done
    echo $sats
}

# terminates qubole job using job id/command id
function cancelQuboleJob()
{
    local qds_cmd=$1
    local url=$2
    local token=$3
    local jobid=$4

    local jobrun_status="unknown"

    while [[ "$jobrun_status" == "unknown" ]] || [[ "$jobrun_status" == "waiting" ]] || [[ "$jobrun_status" == "running" ]]
    do
        status=`$qds_cmd --url=$url --token $token hivecmd check $jobid`
        local cmd_status=`echo $?`
        if [ "$cmd_status" != "0" ]; then
			echo "ERROR: query to check job run status for qubole job id: $jobid failed, hence exiting..!! "
			return 1
        else
         	jobrun_status=`echo $status|${common_script_loc}jq '.status'|xargs`
			if [[ "$jobrun_status" == "waiting" ]] || [[ "$jobrun_status" == "running" ]]; then
				`$qds_cmd --url=$url --token $token hivecmd cancel $jobid`
				if [[ "$?" == "0" ]]; then
					sleep 120
			        status=`$qds_cmd --url=$url --token $token hivecmd check $jobid`
			        local cmd_status=`echo $?`
			        if [ "$cmd_status" != "0" ]; then
			            echo "ERROR: query to check job run status for qubole job id: $jobid failed, hence exiting..!! "
				        return 1
			        else
						jobrun_status=`echo $status|${common_script_loc}jq '.status'|xargs`
						if [[ "$jobrun_status" != "waiting" ]] && [[ "$jobrun_status" != "running" ]]; then
							echo "qubole job with job id: $jobid is cancelled"
							jobrun_status="cancelled"
						fi
					fi
				fi
			fi
        fi
        sleep 60
    done
	return 0
}

