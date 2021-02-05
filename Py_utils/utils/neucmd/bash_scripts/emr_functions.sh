#!/bin/bash
this_file_path=`readlink -f $BASH_SOURCE | rev | cut -d'/' -f2- | rev`

function _get_emr_pem_path() {
    echo `xmlstarlet sel -T -t -v "configroot/emr_settings/set[name='pem_key_path']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_emr_s3_location()
{
    echo `xmlstarlet sel -T -t -v "/configroot/emr_settings/set[name='s3_location']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_emr_s3_aws_configuration_location()
{
   echo `xmlstarlet sel -T -t -v "/configroot/emr_settings/set[name='s3_aws_configuration_bucket']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_emr_release_label()
{
   echo `xmlstarlet sel -T -t -v "/configroot/emr_settings/set[name='emr_release_label']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_emr_jar_location()
{
    echo `xmlstarlet sel -T -t -v "/configroot/emr_settings/set[name='JARS']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_emr_s3_access_key() {
    echo `xmlstarlet sel -T -t -v "configroot/emr_settings/set[name='access_key']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_emr_s3_secret_key() {
    echo `xmlstarlet sel -T -t -v "configroot/emr_settings/set[name='secret_key']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_emr_cluster_id() {
    echo `xmlstarlet sel -T -t -v "configroot/emr_settings/set[name='cluster']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_emr_cluster_name() {
    echo `xmlstarlet sel -T -t -v "configroot/emr_settings/set[name='cluster_name']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_emr_logs_location()
{
        echo `xmlstarlet sel -T -t -v "configroot/emr_settings/set[name='cluster_logs']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_emr_service_role_path() {
    echo `xmlstarlet sel -T -t -v "configroot/emr_settings/set[name='service_role']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}

function _get_emr_s3_region() {
    echo `xmlstarlet sel -T -t -v "configroot/emr_settings/set[name='region']/stringval" $PLATFORM_CONFIG_FILE 2>/dev/null`
}


function updateClusterId()
{
  local auth=""
  local access_key=""
  local secret_key=""
  local cmd_status=""
  local region=`_get_emr_s3_region`
  access_key=`_get_emr_s3_access_key`
  secret_key=`_get_emr_s3_secret_key`

  export AWS_ACCESS_KEY_ID="${access_key}"
  export AWS_SECRET_ACCESS_KEY="${secret_key}"
  local clusterName=`_get_emr_cluster_name`
  local cnt=`aws emr list-clusters --cluster-states {STARTING,BOOTSTRAPPING,RUNNING,WAITING} --region "$region" |grep $clusterName|wc -l`
  cmd_status=`echo $?`
  checkCmdStatus $cmd_status "!!!!===Unable to list Clusters===!!!!"
  
  local clusterLogLoc=`_get_emr_logs_location`
  local service_role=`_get_emr_service_role_path`
  local s3_location=`_get_emr_s3_location`
  local s3_aws_configuration_location=`_get_emr_s3_aws_configuration_location`
  local emr_release_label=`_get_emr_release_label`

  local clusterId=""

        if [ "$cnt" == "0" ]; then
        clusterId=`aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --tags Product="EMR" Environment="Prod" 'Account Name'=$clusterName Project=$clusterName --applications Name=Hadoop Name=Hive Name=Hue Name=Tez Name=Spark --configurations https://s3.amazonaws.com/$s3_aws_configuration_location/Configuration.json --ec2-attributes https://s3.amazonaws.com/$s3_aws_configuration_location/ec2_attributes.json --service-role $service_role --enable-debugging --release-label $emr_release_label --log-uri $clusterLogLoc --name $clusterName --instance-group https://s3.amazonaws.com/$s3_aws_configuration_location/InstanceGroupConfig.json --bootstrap-actions Path=s3://$s3_aws_configuration_location/bootstrap_instances.sh --bootstrap-actions Path=s3://$s3_aws_configuration_location/bootstrap_instances_master.sh --emrfs https://s3.amazonaws.com/$s3_aws_configuration_location/emrfsconfig.json`
#       clusterId=`aws emr create-cluster --tags Product="EMR" Environment="Prod" 'Account Name'=$clusterName Project="MTA-EMR" --applications Name=Hadoop Name=Hive Name=Hue Name=Tez Name=Spark --configurations https://s3.amazonaws.com/$s3_aws_configuration_location/Configuration.json --ec2-attributes https://s3.amazonaws.com/$s3_aws_configuration_location/ec2_attributes.json --service-role $service_role --enable-debugging --release-label $emr_release_label --log-uri $clusterLogLoc --name $clusterName --instance-fleets https://s3.amazonaws.com/$s3_aws_configuration_location/InstanceFleetConfig.json --bootstrap-actions Path=s3://$s3_aws_configuration_location/bootstrap_instances.sh`


        cmd_status=`echo $?`
        checkCmdStatus $cmd_status "!!!!===Failed to launch EMR Cluster===!!!!"

        clusterId=`echo $clusterId |jq .ClusterId|tr -d '"'`
        local cluster_status=`aws emr describe-cluster --cluster-id $clusterId |jq .Cluster.Status.State|tr -d '"'`
        cmd_status=`echo $?`
        checkCmdStatus $cmd_status "Unable to descibe cluster"
        while [[ "$cluster_status" != "WAITING" ]] && [[ "$cluster_status" != "RUNNING" ]]
        do
          cluster_status=`aws emr describe-cluster --cluster-id $clusterId |jq .Cluster.Status.State|tr -d '"'`
          cmd_status=`echo $?`
          checkCmdStatus $cmd_status "Unable to descibe cluster"
          sleep 100
        done
        xmlstarlet edit --inplace -u "configroot/emr_settings/set[name='cluster']/stringval" -v $clusterId $PLATFORM_CONFIG_FILE

        else
                clusterId=$(aws emr list-clusters --active |jq  --arg clusterName "$clusterName" '.Clusters[]|select(.Name==$clusterName)|.Id')
                clusterId=`echo $clusterId|tr -d '"'`
    xmlstarlet edit --inplace -u "configroot/emr_settings/set[name='cluster']/stringval" -v $clusterId $PLATFORM_CONFIG_FILE
        fi
}

function emrSetjdbcURL()
{
    updateClusterId
    local clusterId=`_get_emr_cluster_id`
    local region=`_get_emr_s3_region`
    jdbc_url=`aws emr describe-cluster --cluster-id $clusterId |jq .Cluster.MasterPublicDnsName|tr -d '"'`
    local cmd_status=`echo $?`
    checkCmdStatus $cmd_status "Failed to get jdbc_url"
    if [ -d "${client_common_input}" ]; then
        xmlstarlet edit --inplace -u "configroot/set[name='hive_hostname']/stringval" -v $jdbc_url ${client_common_input}/custom/hive_config.xml
        xmlstarlet edit --inplace -u "configroot/set[name='hive_port']/stringval" -v 10000 ${client_common_input}/custom/hive_config.xml
    fi

    if [ -d "${config_inputs_loc}" ]; then
        xmlstarlet edit --inplace -u "configroot/set[name='hive_hostname']/stringval" -v $jdbc_url ${config_inputs_loc}/hive_config.xml
        xmlstarlet edit --inplace -u "configroot/set[name='hive_port']/stringval" -v 10000 ${config_inputs_loc}/hive_config.xml
    fi
}

function launch_emr_h2o_cluster()
{
    updateClusterId
    #generate script to run on master node
    local h2oLaunchScript="launchH2O.sh"
    local h2ologs="h2oLog.txt"
    local path=$1
    local H2ONODEMEMORYINGB=$2
    local H2ONOOFNODES=$3
    local H2ODRIVERPORT=$4
    local H2OBASEPORT=$5
    local access_key=`_get_emr_s3_access_key`
    local secret_key=`_get_emr_s3_secret_key`
    local region=`_get_emr_s3_region`
    local clusterLogLoc=`_get_emr_logs_location`
    local client_name=`xmlstarlet sel -T -t -v "configroot/set[name='client_name']/stringval" ${config_inputs_loc}/proj_setup_config_adv.xml 2>/dev/null`
    local project_name=`xmlstarlet sel -T -t -v "configroot/set[name='project_id']/stringval" ${config_inputs_loc}/proj_setup_config_adv.xml 2>/dev/null`
    local user=`whoami`
    local driver_location=`_get_emr_jar_location`
    local clusterId=`_get_emr_cluster_id`
    local cmd="hadoop jar /home/hadoop/h2odriver.jar -Dmapred.jobclient.killjob.onexit=false  -gc -mapperXmx ${H2ONODEMEMORYINGB}g -nodes $H2ONOOFNODES -timeout 500 -driverport $H2ODRIVERPORT -baseport $H2OBASEPORT -output $path  -disown -notify   $path"
    echo "#!/bin/bash" > $h2oLaunchScript
    echo "if [ ! -f /home/hadoop/h2odriver.jar ]; then" >> $h2oLaunchScript
    echo "  export AWS_ACCESS_KEY_ID=$access_key" >> $h2oLaunchScript
    echo "  export AWS_SECRET_ACCESS_KEY=$secret_key" >> $h2oLaunchScript
    echo "  aws s3 cp  $driver_location/h2odriver.jar  /home/hadoop/h2odriver.jar" >> $h2oLaunchScript
    echo "fi" >> $h2oLaunchScript
    echo "$cmd" >> $h2oLaunchScript
    local output_loc="$clusterLogLoc/$user/$client_name/$project_name"
    echo "aws s3 cp $path $output_loc/$h2ologs" >> $h2oLaunchScript
    #move h2oLaunchScript to s3
#s3cmd put $h2oLaunchScript $output_loc/ --region  "$region" --access_key="$access_key" --secret_key="$secret_key" --access_token="$session_token" --force &> /dev/null 2>&1
    s3cmd put $h2oLaunchScript $output_loc/ --region  "$region" --access_key="$access_key" --secret_key="$secret_key"  --force &> /dev/null 2>&1

    #check if log file already exists:
    s3cmd get $output_loc/$h2ologs ./ --region  "$region" --access_key="$access_key" --secret_key="$secret_key"  --force &> /dev/null 2>&1
    local cmd_status=`echo $?`
    if [ "$cmd_status" == "0" ]; then
      s3cmd del $output_loc/$h2ologs --region  "$region" --access_key="$access_key" --secret_key="$secret_key" --force &> /dev/null 2>&1
    fi


    #run h2oLaunchScript on master node
    local jobid=`aws emr add-steps --cluster-id $clusterId --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CONTINUE,Jar=s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar,Args=["$output_loc/$h2oLaunchScript"]`
    cmd_status=`echo $?`
    checkCmdStatus $cmd_status "Failed submit job:launch h2o cluster"

    jobid=`echo $jobid| jq '.StepIds'| jq .[0]|xargs`

    #wait for step to be complete
    local sats=`_pollStepStatus $clusterId $jobid`
    if [ "$sats" != "COMPLETED" ]; then
        checkCmdStatus "1" "Failed to get status of step: $jobid"
    fi

    #wait for h2o log file
    local cnt=`aws s3 ls $output_loc/$h2ologs|wc -l`

    while [[ "$cnt" != "1" ]]
    do
        sleep 100
        cnt=`aws s3 ls $output_loc/$h2ologs|wc -l`
    done
    
    if [ -f "$h2ologs" ];then
      rm $h2ologs
    fi
    #get result back to extract ip and port
    
    s3cmd get $output_loc/$h2ologs ./ --region  "$region" --access_key="$access_key" --secret_key="$secret_key" --access_token="$session_token" --force &> /dev/null 2>&1
    rm $h2oLaunchScript
    cmd_status=`echo $?`
    checkCmdStatus $cmd_status "Failed launch h2o cluster"
    cat $h2ologs
}

function launch_emr_h2o_cluster_ssh()
{
    #either copy h2odriver.jar to some location on master node as bootstrap
    #or figure out a way to call from s3
    path=$1
    local clusterId=`_get_emr_cluster_id`
    local pemKeyPath=`_get_emr_pem_path`
    aws emr ssh --cluster-id $clusterId --key-pair-file $pemKeyPath --command "hadoop jar /home/hadoop/h2odriver.jar -Dmapred.jobclient.killjob.onexit=false  -gc -mapperXmx 1g -nodes 2 -timeout 500 -driverport 54310 -baseport 64310 -output $path  -disown -notify   $path"
    local cmd_status=`echo $?`
    checkCmdStatus $cmd_status "Failed launch h2o cluster"
}

function checkCmdStatus()
{
    local cmd_status=$1
    local error_message=$2
    if [ "$cmd_status" != "0" ]; then
      echo "$error_message"
      exit 1
    fi
}

function _pollStepStatus()
{
    local clusterid=$1
    local stepid=$2
    local region=`_get_emr_s3_region`
    local sats="unknown"
       sats=`aws emr describe-step --cluster-id $clusterid --step-id $stepid | jq '.Step' | jq '.Status' | jq '.State'|xargs`
    while [[ "$sats" == "PENDING" ]] || [[ "$sats" == "WAITING" ]] || [[ "$sats" == "RUNNING" ]]
    do
        sats=`aws emr describe-step --cluster-id $clusterid --step-id $stepid | jq '.Step' | jq '.Status' | jq '.State'|xargs`
        sleep 150
    done
    echo $sats
}

function _get_emr_logs()
{
        local clusterLogLoc=`_get_emr_logs_location`
        local clusterid=$1
        local stepid=$2
        local stepStatus=$3
  local commandType=$4
  local path="${clusterLogLoc}${clusterid}/steps/${stepid}/"
        local logLoc="${HOME}/${cluster}/${jobid}/"
        mkdir -p $logLoc
        cnt=`aws s3 ls $path|wc -l`

        while [[ "$cnt" != "3" ]]
        do
                sleep 5
                cnt=`aws s3 ls $path|wc -l`
        done

        NeuHadoop fs -get $path $logLoc  --recursive &> /dev/null 2>&1

        if [ "$stepStatus" == "COMPLETED" ]; then
    if [ "$commandType" == "E_OPTION" ]; then
      gunzip "${logLoc}stdout.gz"
      cat ${logLoc}stdout|tail -n +2
    elif [ "$commandType" == "F_OPTION" ]; then
      gunzip "${logLoc}stderr.gz"
      cat ${logLoc}stderr
    fi
        else
                 gunzip "${logLoc}stderr.gz"
                 cat ${logLoc}stderr
        fi

}


function _get_emr_logs_master()
{
        local clusterLogLoc=`_get_emr_logs_location`
        local clusterid=$1
        local stepid=$2
        local stepStatus=$3
        local commandType=$4
        local masterNodeIP=$5
        local pemKey=$6

        local logLoc="/tmp/${clusterid}/${stepid}/"
        mkdir -p $logLoc
        chmod -R 777 $logLoc


        if [ "$stepStatus" == "COMPLETED" ]; then
           if [ "$commandType" == "E_OPTION" ]; then
                ssh -n -o "StrictHostKeyChecking no" hadoop@$masterNodeIP -i $pemKey '(cat /mnt/var/log/hadoop/steps/'$stepid'/stdout)' > $logLoc/stdout
                cat $logLoc/stdout|tail -n +2
           elif [ "$commandType" == "F_OPTION" ]; then
                ssh -n -o "StrictHostKeyChecking no" hadoop@$masterNodeIP -i $pemKey '(cat /mnt/var/log/hadoop/steps/'$stepid'/stderr)' > $logLoc/stderr
                cat $logLoc/stderr
           fi
        else
            ssh -n -o "StrictHostKeyChecking no" hadoop@$masterNodeIP -i $pemKey ' (cat /mnt/var/log/hadoop/steps/'$stepid'/stderr)' > $logLoc/stderr
            cat $logLoc/stderr
        fi
}

function _get_emr_hadoop_output()
{
        local clusterid=$1
        local hadoopCommand=$2
        local masterNodeIP=$3
        local pemKey=$4

        local timestamp=`date +%Y-%m-%d_%H%M%S`
        local fileName="/tmp/${timestamp}.log"

        ssh -n -o "StrictHostKeyChecking no" hadoop@$masterNodeIP -i $pemKey '('$hadoopCommand')' > $fileName
        cat $fileName
}


function _run_emrfssync()
{

    local s3Location=$1
    local clientName=$2
    local projectName=$3
    local output_loc=$4

     local  access_key=`_get_emr_s3_access_key`
      local  secret_key=`_get_emr_s3_secret_key`

    local region=`_get_emr_s3_region`
    local clusterLogLoc=`_get_emr_logs_location`
    local user=`whoami`
    local clusterId=`_get_emr_cluster_id`

    local output_loc="$clusterLogLoc/$user/$client_name/$project_name"
    local timestamp=`date +%Y-%m-%d_%H%M%S`
    local emrfssync="/tmp/emrfssync_${timestamp}.sh"

    echo "#!/bin/bash" > $emrfssync
    echo "emrfs sync $s3Location" >> $emrfssync
    s3cmd put $emrfssync $output_loc/emrfssync.sh --region  "$region" --access_key="$access_key" --secret_key="$secret_key"  --force &> /dev/null 2>&1
    export AWS_ACCESS_KEY_ID="${access_key}"
    export AWS_SECRET_ACCESS_KEY="${secret_key}"

    jobid=`aws emr add-steps --cluster-id $clusterId --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CONTINUE,Jar=s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar,Args=["$output_loc/emrfssync.sh"]`
    cmd_status=`echo $?`
    checkCmdStatus $cmd_status "Failed submit job:emr fs sync"
    jobid=`echo $jobid| jq '.StepIds'| jq .[0]|xargs`

    #wait for step to be complete
    sats=`_pollStepStatus $clusterId $jobid`
    if [ "$sats" != "COMPLETED" ]; then
        checkCmdStatus "1" "Failed to get status of step: $jobid"
    fi
    rm $emrfssync
}

function launch_emr_h2o_cluster_noclient()
{
    updateClusterId
    #generate script to run on master node
    local timestamp=`date +%Y-%m-%d_%H%M%S`
    local h2oLaunchScript="/tmp/launchH2O_{$timestamp}.sh"
    local h2ologs="h2oLog.txt"
    local path=$1
    local H2ONODEMEMORYINGB=$2
    local H2ONOOFNODES=$3
    local H2ODRIVERPORT=$4
    local H2OBASEPORT=$5
    local access_key=`_get_emr_s3_access_key`
    local secret_key=`_get_emr_s3_secret_key`
    local region=`_get_emr_s3_region`
    local clusterLogLoc=`_get_emr_logs_location`
    local client_name=$6
    local project_name=$7
    local user=`whoami`
    local driver_location=`_get_emr_jar_location`
    local clusterId=`_get_emr_cluster_id`
    local cmd="hadoop jar /home/hadoop/h2odriver.jar -Dmapred.jobclient.killjob.onexit=false  -gc -mapperXmx ${H2ONODEMEMORYINGB}g -nodes $H2ONOOFNODES -timeout 500 -driverport $H2ODRIVERPORT -baseport $H2OBASEPORT -output $path  -disown -notify   $path"
    echo "#!/bin/bash" > $h2oLaunchScript
    echo "if [ ! -f /home/hadoop/h2odriver.jar ]; then" >> $h2oLaunchScript
    echo "  export AWS_ACCESS_KEY_ID=$access_key" >> $h2oLaunchScript
    echo "  export AWS_SECRET_ACCESS_KEY=$secret_key" >> $h2oLaunchScript
    echo "  aws s3 cp  $driver_location/h2odriver.jar  /home/hadoop/h2odriver.jar" >> $h2oLaunchScript
    echo "fi" >> $h2oLaunchScript
    echo "$cmd" >> $h2oLaunchScript
    local output_loc="$clusterLogLoc/$user/$client_name/$project_name"
    echo "aws s3 cp $path $output_loc/$h2ologs" >> $h2oLaunchScript

    #move h2oLaunchScript to s3
#s3cmd put $h2oLaunchScript $output_loc/ --region  "$region" --access_key="$access_key" --secret_key="$secret_key" --access_token="$session_token" --force &> /dev/null 2>&1
    s3cmd put $h2oLaunchScript $output_loc/launchH2O.sh --region  "$region" --access_key="$access_key" --secret_key="$secret_key"  --force &> /dev/null 2>&1

    #check if log file already exists:
    s3cmd get $output_loc/$h2ologs ./ --region  "$region" --access_key="$access_key" --secret_key="$secret_key"  --force &> /dev/null 2>&1
    local cmd_status=`echo $?`
    if [ "$cmd_status" == "0" ]; then
      s3cmd del $output_loc/$h2ologs --region  "$region" --access_key="$access_key" --secret_key="$secret_key" --force &> /dev/null 2>&1
    fi

    #run h2oLaunchScript on master node
    local jobid=`aws emr add-steps --cluster-id $clusterId --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CONTINUE,Jar=s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar,Args=["$output_loc/launchH2O.sh"] --region  "$region"`
    cmd_status=`echo $?`
    checkCmdStatus $cmd_status "Failed submit job:launch h2o cluster"


    jobid=`echo $jobid| jq '.StepIds'| jq .[0]|xargs`

    #wait for step to be complete
    local sats=`_pollStepStatus $clusterId $jobid`
    if [ "$sats" != "COMPLETED" ]; then
        checkCmdStatus "1" "Failed to get status of step: $jobid"
    fi

    #wait for h2o log file
    local cnt=`aws s3 ls $output_loc/$h2ologs|wc -l`

    while [[ "$cnt" != "1" ]]
    do
        sleep 15
        cnt=`aws s3 ls $output_loc/$h2ologs|wc -l`
    done

    if [ -f "$h2ologs" ];then
      rm $h2ologs
    fi
    #get result back to extract ip and port

    s3cmd get $output_loc/$h2ologs ./ --region  "$region" --access_key="$access_key" --secret_key="$secret_key"  --force &> /dev/null 2>&1
    rm $h2oLaunchScript
    cmd_status=`echo $?`
    checkCmdStatus $cmd_status "Failed launch h2o cluster"
    cat $h2ologs
}

