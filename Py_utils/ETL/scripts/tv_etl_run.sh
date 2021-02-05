#!/bin/bash
script_loc=`readlink -f $0 | rev | cut -d"/" -f2-| rev`
source $script_loc/abstract_tv_virt_env.sh
if [ "$#" -eq 0 ]
 then
	python $tv_etl_path'main.py' --tv-etl DEFAULT_TRIGGER --log INFO 
else
    
    python $tv_etl_path'main.py' --tv-etl DEFAULT_TRIGGER --log INFO &
fi
source $conda_path'deactivate'
