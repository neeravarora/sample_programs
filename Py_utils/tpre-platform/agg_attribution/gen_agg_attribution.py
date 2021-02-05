"""Generates FB FF from the FB taxonomy report and the Neustar attribution table"""

import sys
import argparse
import logging
from hive_query import hive_query


query_distr_attr = """
{hive_settings}

DROP TABLE IF EXISTS {fb_ff};
CREATE TABLE {fb_ff} AS
WITH
    total_attr AS (
    SELECT
        TO_DATE(from_unixtime(CAST(ta.ref_timestamp AS BIGINT))) AS act_date
      , {act_dims_tb}
      , SUM(ta.{ia_columns[0]}) AS ia_attr_IM
      , SUM(ta.{ia_columns[1]}) AS ia_attr_CL
      , SUM(ta.{fa_columns[0]}) AS fa_attr_IM
      , SUM(ta.{fa_columns[1]}) AS fa_attr_CL
      , COUNT(*) AS conversions
      , COUNT(DISTINCT ta.actuuid) AS conversions_check
    FROM
        {attr_stack} ta
    LEFT JOIN
        {resolved_albis} tb
    ON
        ta.actuuid = tb.actuuid
    GROUP BY
        TO_DATE(from_unixtime(CAST(ta.ref_timestamp AS BIGINT)))
      , {act_dims_tb}
    ),
    total_evts AS (
    SELECT
        request_id
      , click_or_imp
      , SUM(successful_events) AS tot_evts
    FROM
        {fb_taxonomy}
    GROUP BY
        request_id
      , click_or_imp
    ),
    total_convs AS (
    SELECT
        m.date_start
      , m.date_end
      , COUNT(*) AS tot_convs
    FROM
        {attr_stack} t
    LEFT JOIN
        {request_mapping} m
    ON
        TO_DATE(from_unixtime(CAST(t.ref_timestamp AS BIGINT))) BETWEEN m.date_start AND m.date_end
    GROUP BY
        m.date_start
      , m.date_end
    )
SELECT
    t1.pixel_id
  , t1.account_id
  , t1.account_name
  , t1.campaign_id
  , t1.campaign_name
  , t1.adset_id
  , t1.adset_name
  , t1.ad_id
  , t1.ad_name
  , t1.click_or_imp
  , t1.request_id
  , m.date_start AS date_start
  , m.date_end AS date_end
  , t2.act_date
  , {act_dims_t2}
  , CASE WHEN t1.click_or_imp = 'IM'
         --   1/taxonomy/week      1/act_type/date     1/week    =  1/taxonomy/act_type/date
         THEN t1.successful_events * t2.ia_attr_IM / t3.tot_evts
         WHEN t1.click_or_imp = 'CL'
         THEN t1.successful_events * t2.ia_attr_CL / t3.tot_evts
         ELSE NULL
         END AS ia
  , CASE WHEN t1.click_or_imp = 'IM'
         THEN t1.successful_events * t2.fa_attr_IM / t3.tot_evts
         WHEN t1.click_or_imp = 'CL'
         THEN t1.successful_events * t2.fa_attr_CL / t3.tot_evts
         ELSE NULL
         END AS fa
  -- Dimensionality analysis/balance
  -- 1/taxonomy/week       1/act_type/date     1/week    =  1/taxonomy/act_type/date
  , t1.successful_events * t2.conversions / t4.tot_convs AS successful_events
FROM
    {fb_taxonomy} t1
LEFT JOIN
    {request_mapping} m
ON
    t1.request_id = m.request_id
LEFT JOIN
    total_attr t2
ON
    t2.act_date BETWEEN m.date_start AND m.date_end
LEFT JOIN
    total_evts t3
ON
    t1.request_id = t3.request_id AND
    t1.click_or_imp = t3.click_or_imp
LEFT JOIN
    total_convs t4
ON
    t4.date_start = m.date_start AND
    t4.date_end = m.date_end
;"""


def main(args):
    query = query_distr_attr.format(
        fb_ff=args.fb_ff,
        attr_stack=args.attr_stack,
        fb_taxonomy=args.fb_taxonomy,
        resolved_albis=args.resolved_albis,
        request_mapping=args.request_mapping,
        ia_columns=args.ia_columns,
        fa_columns=args.fa_columns,
        act_dims_tb=', '.join(['tb.{}'.format(a) for a in args.act_dims]),
        act_dims_t2=', '.join(['t2.{}'.format(a) for a in args.act_dims]),
        hive_settings=args.hive_settings
    )
    logging.info("FB Attribution Distribution Query:\n{}".format(query))
    if not args.dry_run:
        hive_query(query, config=args.config)
        logging.info("Attribution Distribution complete")
    else:
        logging.info("Attribution Distribution not performed (dry-run)")
    return None


def create_parser():
    parser = argparse.ArgumentParser(
        description='Distribution of FB attribution to granular taxonomy'
    )
    parser.add_argument('--config',
                        dest='config',
                        action='store',
                        required=True,
                        type=str,
                        help='Path to the platform_config.xml file for the client')
    parser.add_argument('--attr-stack',
                        dest='attr_stack',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='Neustar feature-level attribution stack')
    parser.add_argument('--ia-columns',
                        dest='ia_columns',
                        metavar='feature',
                        nargs=2,
                        action='store',
                        type=str,
                        required=True,
                        help='Columns with FB iA in attribution stack. Must correspond to IM and CL, in this order')
    parser.add_argument('--fa-columns',
                        dest='fa_columns',
                        metavar='feature',
                        nargs=2,
                        action='store',
                        type=str,
                        required=True,
                        help='Columns with FB fA in attribution stack. Must correspond to IM and CL, in this order')
    parser.add_argument('--fb-taxonomy',
                        dest='fb_taxonomy',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='Name of the table containing the FB taxonomy report')
    parser.add_argument('--request-mapping',
                        dest='request_mapping',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='Table containing the mapping between request_id and start/end dates')
    parser.add_argument('--resolved-albis',
                        dest='resolved_albis',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='Resolved conversions table')
    parser.add_argument('--conversion-dimensions',
                        dest='act_dims',
                        metavar='dim',
                        nargs='+',
                        action='store',
                        required=True,
                        type=str,
                        help='Columns from resolved ALBIS to segment conversions by')
    parser.add_argument('--fb-ff',
                        dest='fb_ff',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='The FB Fact Funnel-like destination table (will be dropped and created)')
    parser.add_argument('--hive-settings',
                        dest='hive_settings',
                        action='store',
                        required=False,
                        default='',
                        type=str,
                        help='string containing hive settings to be prepended to each query')
    parser.add_argument('--dry-run',
                        dest='dry_run',
                        action='store_true',
                        help='If passed, no API calls are made')
    parser.add_argument('--log',
                        dest='loglevel',
                        action='store',
                        choices=['WARNING', 'INFO', 'DEBUG'],
                        required=False,
                        default='INFO',
                        type=str,
                        help='logging level')
    return parser


if __name__ == '__main__':

    parser = create_parser()
    args = parser.parse_args()

    # Setting up logging
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)

    stdout_handler = logging.StreamHandler(sys.stdout)

    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        handlers=[stdout_handler])

    logging.info('Starting... args:\n{}'.format(args))
    logging.debug('Package lookup path:\n{}'.format(sys.path))

    main(args)
