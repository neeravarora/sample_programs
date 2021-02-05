#!/bin/bash

script_loc=`readlink -f $0 | rev | cut -d"/" -f2-| rev`
source $script_loc/abstract_tv_virt_env.sh


python $tv_etl_path'main.py' --tv-etl DS_SYNC 

source $conda_path'deactivate'