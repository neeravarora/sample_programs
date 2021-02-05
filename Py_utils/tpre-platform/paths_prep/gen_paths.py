"""Generates paths to be sent to FB through Conversions API"""

import sys
import argparse
import logging
import hashlib
from hive_query import hive_query


query_gen_paths = """
{hive_settings}

DROP TABLE IF EXISTS {api_table};
CREATE TABLE IF NOT EXISTS {api_table} STORED AS ORC AS
SELECT
    t1.userid AS match_keys_extern_id
  , t1.ref_timestamp AS event_time
  , t1.actuuid AS actuuid
  , COALESCE(concat_ws('|', COLLECT_SET(t2.order_id)), '') AS custom_data_cookie_ids
  , COALESCE(concat_ws('|', COLLECT_SET(t3.hash_ekey)), '') AS custom_data_ext_ids
  , CASE WHEN t1.response = 1 THEN 'conversion' ELSE 'non-conversion' END AS custom_data_type
FROM
    {stack_backbone} t1  -- This is the Neustar stack backbone
LEFT JOIN
    {cookie_mapping} t2  -- From key_collections
ON
    t1.userid = t2.userid
LEFT JOIN
    {offline_id_mapping} t3  -- From OneID FB upload
ON
    t1.userid = t3.hash_pid
GROUP BY
    t1.userid
  , t1.ref_timestamp
  , t1.actuuid
  , CASE WHEN t1.response = 1 THEN 'conversion' ELSE 'non-conversion' END
;"""

query_add_fake_conversions = """
{hive_settings}

DROP TABLE IF EXISTS {stack_backbone}_obfs;
CREATE TABLE {stack_backbone}_obfs STORED AS ORC AS
SELECT
    userid
  , actuuid
  , ref_timestamp
  , response
FROM {stack_backbone};

INSERT INTO TABLE {stack_backbone}_obfs
SELECT
    SHA1(CAST(rand({seed}) AS STRING)) AS userid
  , 'not_applicable' AS actuuid
  , CAST(
        unix_timestamp('{start_date}', 'yyyy-MM-dd') + 
        rand(34352) * (unix_timestamp('{end_date}', 'yyyy-MM-dd') - unix_timestamp('{start_date}', 'yyyy-MM-dd') )
        AS BIGINT
    ) AS ref_timestamp
  , 1 AS response
FROM (
    SELECT ones
    FROM (SELECT 'a') t1
    LATERAL VIEW explode(split(trim(repeat('1 ', {nrows})), ' ')) t2 AS ones
) t
;"""

query_date_range_from_bb = """
{hive_settings}

SELECT
    MIN(TO_DATE(from_unixtime(CAST(ref_timestamp AS BIGINT)))) AS start_date
  , MAX(TO_DATE(from_unixtime(CAST(ref_timestamp AS BIGINT)))) AS end_date
FROM {stack_backbone}
;"""


def main(args):

    # Find date range from stack_backbone
    logging.info("Finding date range from stack backbone")
    query = query_date_range_from_bb.format(
        hive_settings=args.hive_settings,
        stack_backbone=args.stack_backbone
    )
    date_range = hive_query(query, config=args.config)
    start_date = date_range.loc[0, 'start_date']
    end_date = date_range.loc[0, 'end_date']
    logging.info("Date range: {} - {}".format(start_date, end_date))

    # Add obfuscation
    if args.nrows > 0:
        logging.info("Adding {} fake conversions for obfuscation".format(args.nrows))
        query = query_add_fake_conversions.format(
            hive_settings=args.hive_settings,
            stack_backbone=args.stack_backbone,
            start_date=start_date,
            end_date=end_date,
            nrows=args.nrows,
            seed=int(hashlib.sha1(str(end_date).encode('utf-8')).hexdigest(), 16) % (10 ** 8)
        )
        logging.info("Obfuscation Query:\n{}".format(query))
        hive_query(query, config=args.config)

    # Generate API query
    query = query_gen_paths.format(
        hive_settings=args.hive_settings,
        stack_backbone=args.stack_backbone if args.nrows == 0 else args.stack_backbone + '_obfs',
        api_table=args.api_table,
        offline_id_mapping=args.offline_id_mapping,
        cookie_mapping=args.cookie_mapping,
    )
    logging.info("Generate Paths Query:\n{}".format(query))
    hive_query(query, config=args.config)

    return None


def create_parser():
    parser = argparse.ArgumentParser(
        description='Generates paths to be sent to FB through the Conversions API'
    )
    parser.add_argument('--config',
                        dest='config',
                        action='store',
                        required=True,
                        type=str,
                        help='Path to the platform_config.xml file for the client')
    parser.add_argument('--api-table',
                        dest='api_table',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='Name of the table in which to store the records for the API upload')
    parser.add_argument('--stack-backbone',
                        dest='stack_backbone',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='Name of the stack with records to be uploaded (can be just the backbone)')
    parser.add_argument('--offline-id-mapping',
                        dest='offline_id_mapping',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='Name of the table containing the mapping between offline IDs and hashed PIDs')
    parser.add_argument('--cookie-mapping',
                        dest='cookie_mapping',
                        metavar='db_name.table_name',
                        action='store',
                        type=str,
                        required=True,
                        help='Name of the table containing cookies mapping')
    parser.add_argument('--num-fake-conversions',
                        dest='nrows',
                        metavar='N',
                        action='store',
                        type=int,
                        required=False,
                        default=0,
                        help='Optional number of fake conversions to add for obfuscation. Default = 0')
    parser.add_argument('--hive-settings',
                        dest='hive_settings',
                        action='store',
                        required=False,
                        default='',
                        type=str,
                        help='string containing hive settings to be prepended to each query')
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
