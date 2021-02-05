"""Merges the FB-generated features into the Neustar modeling stack"""

import sys
import argparse
import logging
from hive_query import hive_query


query_merge_stack = """
{hive_settings}

DROP TABLE IF EXISTS {stack}_orig;
ALTER TABLE {stack} RENAME TO {stack}_orig;  -- Probably not good practice, please improve

DROP TABLE IF EXISTS {stack};
CREATE TABLE IF NOT EXISTS {stack} STORED AS TEXTFILE AS
WITH
    fb_features AS (
    SELECT
        t1.cohort_number
      , t1.cohort_size
      , t1.request_id
      , t1.hh_id AS userid
      , t2.click_value
      , t2.imp_value
      , t3.week_start
      , t3.week_end
    FROM
        {fb_membership} t1
    LEFT JOIN
        {fb_metric} t2
    ON
        t1.cohort_number = t2.cohort_number AND
        t1.cohort_size = t2.cohort_size AND
        t1.request_id = t2.request_id
    LEFT JOIN
        {request_mapping} t3
    ON
        t1.request_id = t3.request_id
    WHERE
        t1.cohort_size = 100
    )
SELECT
    {features}
  , 0 AS stackmetric_numads10_event_fb_0_w49
  , 0 AS stackmetric_numads20_event_fb_0_w49
  , 0 AS stackmetric_numads30_event_fb_0_w49
  , 0 AS stackmetric_numads40_event_fb_0_w49
  , 0 AS stackmetric_numads50_event_fb_0_w49
  , 0 AS stackmetric_numads60_event_fb_0_w49
  , 0 AS stackmetric_numads10_event_fb_1_w49
  , 0 AS stackmetric_numads20_event_fb_1_w49
  , 0 AS stackmetric_numads30_event_fb_1_w49
  , 0 AS stackmetric_numads40_event_fb_1_w49
  , 0 AS stackmetric_numads50_event_fb_1_w49
  , 0 AS stackmetric_numads60_event_fb_1_w49
  , 0 AS stackmetric_numcrads10_event_fb_0_w49
  , 0 AS stackmetric_numcrads20_event_fb_0_w49
  , COALESCE(t2.imp_value, 0) AS stackmetric_numcrads30_event_fb_0_w49
  , 0 AS stackmetric_numcrads40_event_fb_0_w49
  , 0 AS stackmetric_numcrads50_event_fb_0_w49
  , 0 AS stackmetric_numcrads60_event_fb_0_w49
  , 0 AS stackmetric_numcrads10_event_fb_1_w49
  , 0 AS stackmetric_numcrads20_event_fb_1_w49
  , COALESCE(t2.click_value, 0) AS stackmetric_numcrads30_event_fb_1_w49
  , 0 AS stackmetric_numcrads40_event_fb_1_w49
  , 0 AS stackmetric_numcrads50_event_fb_1_w49
  , 0 AS stackmetric_numcrads60_event_fb_1_w49
FROM
    {stack}_orig t1  -- Neustar stack
LEFT JOIN
    fb_features t2
ON
    t1.userid = t2.userid AND
    TO_DATE(from_unixtime(CAST(t1.ref_timestamp AS BIGINT))) BETWEEN t2.week_start AND t2.week_end
;"""

query_stack_head = """
{hive_settings}

SELECT *
FROM {stack}
LIMIT 5
;"""


def main(args):
    query = query_stack_head.format(
        stack=args.stack,
        hive_settings=args.hive_settings
    )
    stack_head = hive_query(query, config=args.config)
    features = [c for c in stack_head.columns if '_fb_' not in c]

    query = query_merge_stack.format(
        features='\n  , '.join(['t1.{}'.format(f) for f in features]),
        stack=args.stack,
        fb_membership=args.fb_membership,
        fb_metric=args.fb_metric,
        request_mapping=args.request_mapping,
        hive_settings=args.hive_settings
    )
    logging.info("Merge Stack Query:\n{}".format(query))
    if not args.dry_run:
        hive_query(query, config=args.config)
        logging.info("Stack merge complete")
    else:
        logging.info("Stack merge not performed (dry-run)")
    return None


def create_parser():
    parser = argparse.ArgumentParser(
        description='Merges FB features into the Neustar modeling stack'
    )
    parser.add_argument('--config',
                        dest='config',
                        action='store',
                        required=True,
                        type=str,
                        help='Path to the platform_config.xml file for the client')
    parser.add_argument('--stack',
                        dest='stack',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='Neustar stack. Merged stack will keep same name')
    parser.add_argument('--fb-metric',
                        dest='fb_metric',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='Name of the table containing the FB-generated metrics')
    parser.add_argument('--fb-membership',
                        dest='fb_membership',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='Name of the table containing the FB-generated membership mapping')
    parser.add_argument('--request-mapping',
                        dest='request_mapping',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='Table containing the mapping between request_id and start/end dates')
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
