#!/bin/bash
script_loc=`readlink -f $0 | rev | cut -d"/" -f2-| rev`
source $script_loc/abstract_tv_virt_env.sh

if [ "$#" -eq 0 ]
then
   python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50
fi

if [ "$#" -eq 1 ] && [[ $1 == *"saved"* ]] || [[ $1 == *"local"* ]]; then
        if [[ $1 == *"saved"* ]]; then
            python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type SAVED
        else
                python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type LOCAL
        fi
elif [ "$#" -eq 1 ] && [[ $1 == *"status"* ]]; then
        if [[ $1 == *"SKIPPED"* ]]; then
            python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --status SKIPPED
        elif [[ $1 == *"COMPLETED"* ]]; then
            python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --status COMPLETED
        else
            python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --status FAILED
        fi
fi

function getSavedStatus(){
  if [[ $1 == *"SKIPPED"* ]]; then
	    python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type SAVED --status SKIPPED
  elif [[ $1 == *"COMPLETED"* ]]; then
		python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type SAVED --status COMPLETED
  elif [[ $1 == *"FAILED"* ]]; then
		python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type SAVED --status FAILED
  fi
}

function getlocalStatus(){
  if [[ $1 == *"SKIPPED"* ]]; then
		python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type LOCAL --status SKIPPED
  elif [[ $1 == *"COMPLETED"* ]]; then
		python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type LOCAL --status COMPLETED
  elif [[ $1 == *"FAILED"* ]]; then
		python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type LOCAL --status FAILED
  fi	
}

function getSkippedType(){
	if [[ $1 == *"saved"* ]]; then
		python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type SAVED --status SKIPPED
	else
		python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type LOCAL --status SKIPPED
	fi
}
function getCompletedType(){
	if [[ $1 == *"saved"* ]]; then
		python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type SAVED --status COMPLETED
	else
		python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type LOCAL --status COMPLETED
	fi
}
function getFailedType(){
	if [[ $1 == *"saved"* ]]; then
		python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type SAVED --status FAILED
	else
	    python $tv_etl_path'main.py' --tv-etl STAT --max-stat 50 --type LOCAL --status FAILED
	fi
}


if [ "$#" -eq 2 ]; then
	if [[ $1 == *"type"* ]] && [[ $2 == *"status"* ]]; then
		if [[ $1 == *"saved"* ]]; then
			getSavedStatus "$2"
		else
			getlocalStatus "$2"
		fi
	elif [[ $2 == *"type"* ]] && [[ $1 == *"status"* ]]; then
		if [[ $1 == *"SKIPPED"* ]]; then
			getSkippedType "$2"
		elif [[ $1 == *"COMPLETED"* ]]; then
			getCompletedType "$2"
		elif [[ $1 == *"FAILED"* ]]; then
			getFailedType "$2"
        fi
	else
		echo "please pass valid arguments"
		exit 1
	fi
fi
source $conda_path'deactivate'
