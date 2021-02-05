# Facebook MTA integration

---------------

This repository contains routines for the integration of MTA to the Facebook environment, where FB user-level data will be processed and transfered to Neustar in an aggregate form.


## Setup

Prerequisites:

* The script is designed to run from a Linux workbench
* To setup and run the tools in this repo, you must have:
    * the `environment.yml` file from this repo
    * the `hive_query.py` tool from [this repo](https://git.nexgen.neustar.biz/jnatali/ds_utils/tree/master/hive_query) in your `$PYTHONPATH`

The report script requires a special Python 3 environment generated by the command line utility `conda`.

The steps to setup this environment are:

If in an environment without the `conda` package manager installed and setup:

* Download Miniconda 3: `wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh`
* Install conda: `bash -x Miniconda3-latest-Linux-x86_64.sh -b -f -p ~/miniconda3`
* Update PATH: `export PATH=~/miniconda3/bin:$PATH`
* Update conda: `conda update -y conda`
* Proceed to next step

If in an an environment with `conda`:

* Create Python environment: `conda create -n fb_mta -f environment.yml`
* Activate environment: `conda activate fb_mta`