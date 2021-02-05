#!/usr/bin/env bash

python sql_attribution.py \
    --config /Users/jnatali/Drive/Action/Clients/ATT/platform_config.xml \
    --stack attrstack_att_2019feb15_experimental_fb_mspatt_att_v741.attrstack_modeldim \
    --dest-table jnatali.att_telesales_test \
    --db jnatali \
    --model-json "/Users/jnatali/Drive/Action/Clients/ATT/updated_models/Model_modeldate=1970-01-01,modeldim=Telesales.JSON" \
    --stack-filter "modeldate='1970-01-01' AND modeldim='Telesales'" \
    #--subtractive \
    #--dry-run \


