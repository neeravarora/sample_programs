python ../taxonomy_utils/main.py \
       --src s3://qubole-ford/taxonomy_cs/test1/src/ \
       --data s3://qubole-ford/taxonomy_cs/test1/data/ \
       --config /home/vbhargava/feature_test0/msaction_backend/customers/raj_ford_test/common/config/inputs/platform_config.xml \
       --config-input-loc  /home/vbhargava/feature_test0/msaction_backend/customers/raj_ford_test/common/config/inputs/ \
       --config-file-name  audit_lmt_ds_schema_config.xml \
       --dn-version 12.1 \
      

or

python ../taxonomy_utils/main.py \
       --src s3://qubole-ford/taxonomy_cs/test1/src/ \
       --data s3://qubole-ford/taxonomy_cs/test1/data/ \
       --config /home/vbhargava/feature_test0/msaction_backend/customers/raj_ford_test/common/config/inputs/platform_config.xml \
       --config-input-loc  /home/vbhargava/feature_test0/msaction_backend/customers/raj_ford_test/common/config/inputs/ \
       --config-file-name  audit_lmt_ds_schema_config.xml \
       --dn-version 12.1 \
       --log DEBUG \
       --dry-run True \
       --mock-run True \