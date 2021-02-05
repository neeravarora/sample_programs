
###### MTA TV ETL System

##### User manual guide

#### TO CREATE  TVETL PYTHON VIRTUAL ENV(tvetl) 
# Note this is one time job until TVETL PYTHON VIRTUAL ENV(tvetl)  is alive
../ETL> ./setup_tvetl_pyenv.sh

#### TO ACTIVATE PYTHON VIRTUAL ENV
../ETL> . activate_pyenv.sh

#### TO DEACTIVATE PYTHON VIRTUAL ENV
../ETL> . deactivate_pyenv.sh

#### ETL Setup
../ETL/scripts>  ./tv_etl_setup.sh 2020-05-01 &

#### ETL DataSource Sync
../ETL/scripts> ./tv_etl_ds_sync.sh &

#### ETL Default Trigger
../ETL/scripts> ./tv_etl_run.sh -bg

#### ETL Audit
../ETL/scripts> ./tv_etl_audit.sh







##### Developer manual guide

#### TO CREATE  PYTHON VIRTUAL ENV
#CONDA_PATH = /home/miniconda3/conda_setup/conda_install/bin/
${CONDA_PATH}/conda env update --file ${tpre_pf_loc}/environment.yml --prefix ${TPRE_VIRT_ENV}

#### TO CREATE  PYTHON VIRTUAL ENV (tvetl)
/home/miniconda3/conda_setup/conda_install/bin/conda env create --file ~/fb_v9.0_merged/msaction_backend/common/BU3.0_core/util/Py_utils/ETL/environment.yml --prefix /mnt/ephemeral0/cond_env/tvetl


#### TO ACTIVATE PYTHON VIRTUAL ENV
source /home/miniconda3/conda_setup/conda_install/bin/activate /mnt/ephemeral0/cond_env/tvetl

#### TO DEACTIVATE PYTHON VIRTUAL ENV
source /home/miniconda3/conda_setup/conda_install/bin/deactivate


#### Set UP
(/mnt/ephemeral0/cond_env/tpre) vbhargava@ip-10-0-0-58:~/fb_v9.0_merged/msaction_backend/common/BU3.0_core/util/Py_utils/ETL$

### With schema change
    python main.py     --tv-etl SETUP     --start-date 2019-06-04     --schema-change TRUE &
    
### With schema change And Staging directory
    python main.py  --tv-etl SETUP  --start-date 2019-06-04 --schema-change TRUE --tv-dir /home/msptvmta/tv_etl/etl_staging/ &

### Wthout schema change to set start date to automate etl
    python main.py     --tv-etl SETUP     --start-date 2019-06-04 --tv-dir /home/msptvmta/tv_etl/etl_staging/ &

### To Generate Setup Schema but not to execute(Mock Run)
    python main.py    --tv-etl SETUP     --start-date 2019-01-01    --dry-run True --mock-run True

### Trigger- e.g. -1
    python main.py  --tv-etl DEFAULT_TRIGGER  --log DEBUG &

### Trigger- e.g. -2
    python main.py --tv-etl DEFAULT_TRIGGER --tv-dir /home/msptvmta/tv_etl/etl_staging/ --log DEBUG &

### To Generate Script but not to execute(Mock Run)
    python main.py --tv-etl DEFAULT_TRIGGER --dry-run True --mock-run True

#### Manual Run

### To Generate or Re-generate impressions in given date range for limited default ultimateowner ids listed in S3 location
python main.py --tv-etl MANUAL_TRIGGER --start-date 2019-08-16 --end-date 2019-10-31 

### To Generate or Re-generate impressions in given date range for all ultimateowner ids
python main.py --tv-etl MANUAL_TRIGGER --start-date 2019-08-16 --end-date 2019-10-31 --filter ALL_ULTIMATEOWNER_ID

### To Generate or Re-generate impressions in given date range and given ultimateowner ids
python main.py --tv-etl MANUAL_TRIGGER --start-date 2019-08-16 --end-date 2019-10-31 --ultimate-owner-id-list 1395292 1395296

### To Generate or Re-generate impressions on given output location
python main.py --tv-etl MANUAL_TRIGGER --start-date 2019-08-16 --end-date 2019-10-31 --output s3://tv-mta/tv-results-dev/mta-tv-impressions-ford

### To Generate Script but not to execute(Mock Run) for Manual trigger
python main.py --tv-etl MANUAL_TRIGGER --start-date 2019-08-16 --end-date 2019-10-31 --ultimate-owner-id-list 1395292 1395296 --dry-run True --mock-run True --log DEBUG
    
### Audit ETL STAT
     python main.py --tv-etl STAT
     python main.py --tv-etl STAT  --type SAVED --status NA
     python main.py --tv-etl STAT  --type SAVED --status COMPLETED
     python main.py --tv-etl STAT  --max-stat 50 --status FAILED
     python main.py --tv-etl STAT  --type LOCAL
