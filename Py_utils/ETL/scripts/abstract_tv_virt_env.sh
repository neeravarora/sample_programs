#!/bin/bash
export conda_path='/home/miniconda3/conda_setup/conda_install/bin/'
script_loc=`readlink -f $0 | rev | cut -d"/" -f2-| rev`
export tv_etl_path=$script_loc'/../'
source $conda_path'activate' /mnt/ephemeral0/cond_env/tvetl
