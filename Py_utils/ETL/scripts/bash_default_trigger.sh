#!/bin/bash

cur_loc=`readlink -f $0 | rev | cut -d"/" -f2-| rev`

#tv_etl_script_path=/home/msptvmta/tv_etl/msaction_backend/common/BU3.0_core/util/Py_utils/ETL/scripts
tv_etl_script_path=$cur_loc

#log_dir=$cur_loc/logs
log_dir=/tmp/tv_etl/logs

now=$(date +"%Y-%m-%d-%H-%M-%S-%s")

echo "[$(date +"%Y-%m-%d %T")] job tv etl default trigger started on $(date +"%Y-%m-%d at %H:%M:%S (%s)")
" >> $log_dir/run_history.log

echo "========================================================================" >> $log_dir/bg_pre_$now.log
echo "=                                                                      =" >> $log_dir/bg_pre_$now.log
echo "=                        Last 50 ETL run states                        =" >> $log_dir/bg_pre_$now.log
echo "=                                                                      =" >> $log_dir/bg_pre_$now.log
echo "========================================================================" >> $log_dir/bg_pre_$now.log
echo "" >> $log_dir/bg_pre_$now.log
echo "" >> $log_dir/bg_pre_$now.log


cmd=$tv_etl_script_path'/tv_etl_audit.sh saved'
bash $cmd >> $log_dir/bg_pre_$now.log

echo "" >> $log_dir/bg_pre_$now.log
echo "" >> $log_dir/bg_pre_$now.log
echo "========================================================================" >> $log_dir/bg_pre_$now.log
echo "=                                                                      =" >> $log_dir/bg_pre_$now.log
echo "=                         Last 50 ETL COMPLETED runs                   =" >> $log_dir/bg_pre_$now.log
echo "=                                                                      =" >> $log_dir/bg_pre_$now.log
echo "========================================================================" >> $log_dir/bg_pre_$now.log
echo "" >> $log_dir/bg_pre_$now.log
echo "" >> $log_dir/bg_pre_$now.log

cmd=$tv_etl_script_path'/tv_etl_audit.sh --type=saved --status=COMPLETED' 
bash $cmd >> $log_dir/bg_pre_$now.log

# To trigger default run
cmd=$tv_etl_script_path'/tv_etl_run.sh'
bash $cmd > $log_dir/run_$now.log



echo "[$(date +"%Y-%m-%d %T")] job tv etl default trigger finished on $(date +"%Y-%m-%d at %H:%M:%S (%s)")
" >> $log_dir/run_history.log



echo "========================================================================" >> $log_dir/bg_post_$now.log
echo "=                                                                      =" >> $log_dir/bg_post_$now.log
echo "=                         Last 50 ETL run states                       =" >> $log_dir/bg_post_$now.log
echo "=                                                                      =" >> $log_dir/bg_post_$now.log
echo "========================================================================" >> $log_dir/bg_post_$now.log
echo "" >> $log_dir/bg_post_$now.log
echo "" >> $log_dir/bg_post_$now.log


bash $tv_etl_script_path'/tv_etl_audit.sh' >> $log_dir/bg_post_$now.log

echo "" >> $log_dir/bg_post_$now.log
echo "" >> $log_dir/bg_post_$now.log
echo "========================================================================" >> $log_dir/bg_post_$now.log
echo "=                                                                      =" >> $log_dir/bg_post_$now.log
echo "=                          Last 50 ETL COMPLETED runs                  =" >> $log_dir/bg_post_$now.log
echo "=                                                                      =" >> $log_dir/bg_post_$now.log
echo "========================================================================" >> $log_dir/bg_post_$now.log
echo "" >> $log_dir/bg_post_$now.log
echo "" >> $log_dir/bg_post_$now.log

cmd=$tv_etl_script_path'/tv_etl_audit.sh --status=COMPLETED'
bash $cmd >> $log_dir/bg_post_$now.log

