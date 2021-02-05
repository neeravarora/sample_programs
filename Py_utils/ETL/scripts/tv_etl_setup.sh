#!/bin/bash

if [ $# != 1 ]; then
    echo >&2 "ERROR: Expecting command line parameters in the below format:
         $0 --start-date=2019-06-04
    "
    echo "failure"
    exit 1
fi

script_loc=`readlink -f $0 | rev | cut -d"/" -f2-| rev`
source $script_loc/abstract_tv_virt_env.sh

start_date=`echo $1 | sed 's/.*=//'`

python $tv_etl_path'main.py' --tv-etl SETUP --start-date $start_date --schema-change TRUE & 

source $conda_path'deactivate'