#!/usr/bin/env bash
python ../agg_attribution/gen_agg_attribution.py \
       --config /Users/jnatali/Drive/Action/Clients/ATT/platform_config.xml \
       --attr-stack attrstack_att_2019feb15_experimental_fb_mspatt_att_v741.attrstack_modelattribution \
       --ia-columns marattr_transvar_log_numcrads30_event_fb_0_w49 marattr_transvar_log_numcrads30_event_fb_1_w49 \
       --fa-columns relattr_transvar_log_numcrads30_event_fb_0_w49 relattr_transvar_log_numcrads30_event_fb_1_w49 \
       --fb-taxonomy fb_integration_2.fb_20190202_2_taxonomy \
       --request-mapping fb_integration_2.request_date_mapping_20190302 \
       --resolved-albis att_v741_lbis.albis_orig_n_resolved_cols \
       --conversion-dimensions activity_type channel_code act_source dlr_nm dlr_dist_sub_chnl_desc product_group conversion_channel \
       --fb-ff fb_integration_2.fb_ff_mod \
       --hive-settings "set hive.execution.engine=tez;" \
#       --dry-run

