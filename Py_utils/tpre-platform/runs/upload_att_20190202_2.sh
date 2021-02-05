#!/bin/bash

python ../conv_api/upload_conv.py --config att_platform_config.xml --table fb_integration_1.fb_stack_backbone_api \
                      --start-date 2018-12-30 --end-date 2019-02-02 \
                      --access-token EAAZAuVvcfbzIBAN7eEabFBtQglFmQZBXu3mdI6vveFfZBVQF7HkSp8Jq7FcekmODGZCEXZBcU3zsosT8JuTpReOcqOwcezuWuxXgAiPCi7d1b2nxZCR3QAjhxa2U7GRNuY5LvtkdpZBJBZB4EZB0dPBaKdPBIUNBAZBIYfTV4n8zhO6chwL6CI5LKnmkmukLaqWRUZD \
                      --business-id 1386462108046405 \
                      --event-set-id "2461135463917442" \
                      --tag-suffix "_attribution_mod_v2" \
                      --log DEBUG

