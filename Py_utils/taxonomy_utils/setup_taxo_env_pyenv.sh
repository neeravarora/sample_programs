#!/bin/bash

export conda_path='/home/miniconda3/conda_setup/conda_install/bin/'

export py_virt_env=/mnt/ephemeral0/cond_env/taxo_env

current_dir_loc=`readlink -f $0 | rev | cut -d"/" -f2-| rev`

echo -e  "   \n===Updating Conda python Environments===\n"
echo "$conda_path/conda env update --file ${current_dir_loc}/environment.yml --prefix ${py_virt_env}"
echo -e "   \n"



$conda_path/conda env update --file ${current_dir_loc}/environment.yml --prefix ${py_virt_env}


