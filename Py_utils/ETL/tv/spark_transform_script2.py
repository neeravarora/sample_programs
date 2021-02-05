
class Transform_Spark_Const:

	create_tivo_resolved_table = '''
		DROP TABLE IF EXISTS {db}.{table};
		CREATE TABLE IF NOT EXISTS {db}.{table} USING PARQUET AS
		SELECT 
			t1.household_id as household_id,
			t1.unit_id as unit_id,
			t1.event_type as event_type,
			t1.recording_offset_seconds as recording_offset_seconds,
			t1.program_name as program_name,
			t1.channel_call_sign as channel_call_sign,
			t1.channel_short_name as channel_short_name,
			t1.channel_long_name as channel_long_name,
			t1.network_affiliation as network_affiliation,
			t1.local_timezone as local_timezone,
			t1.utc_offset as utc_offset,
			t1.dt as dt,
			
			unix_timestamp(t1.start_event_timestamp_local)-CAST(t1.recording_offset_seconds AS BIGINT) AS start_event_timestamp_match,
			unix_timestamp(t1.end_event_timestamp_local)-CAST(t1.recording_offset_seconds AS BIGINT) AS end_event_timestamp_match,
			unix_timestamp(t1.program_start_local) as program_start_match,
			unix_timestamp(t1.program_end_local) as program_end_match,

			(UPPER(TRIM(t2.market_n))) as market_match,
			(UPPER(TRIM(t3.master_c))) as channel_match

		FROM 
			(SELECT household_id,
					unit_id,
					event_type,
					recording_offset_seconds,
					program_name,
					(UPPER(TRIM(channel_call_sign))) AS channel_call_sign,
					(UPPER(TRIM(channel_short_name))) AS channel_short_name,
					(UPPER(TRIM(channel_long_name))) AS channel_long_name,
					(UPPER(TRIM(network_affiliation))) AS network_affiliation,
					local_timezone,
					TO_DATE(program_start_local) as dt,
					start_event_timestamp_local,
					end_event_timestamp_local,
					program_start_local,
					program_end_local,
					utc_offset,
					dma_code

				FROM {db}.{raw_tivo}
				WHERE file_name >= cast({start_date_int} as int) and file_name<=cast({end_date_int} as int)
					AND dma_code is not Null AND upper(dma_code) <> "NULL"
				GROUP BY household_id,
					unit_id,
					event_type,
					recording_offset_seconds,
					program_name,
					channel_call_sign,
					channel_short_name,
					channel_long_name,
					network_affiliation,
					local_timezone,
					start_event_timestamp_local,
					end_event_timestamp_local,
					program_start_local,
					program_end_local,
					utc_offset,
					dma_code) t1

			INNER JOIN {db}.{kantar_tivo_dma_mapping} t2
			ON t1.dma_code = t2.dma_code 

			INNER JOIN 
			(SELECT 
					(UPPER(TRIM(channel_call_sign))) AS channel_call_sign,
					(UPPER(TRIM(channel_short_name))) AS channel_short_name,
					(UPPER(TRIM(channel_long_name))) AS channel_long_name,
					(UPPER(TRIM(network_affiliation))) AS network_affiliation,
					master_c as master_c
				FROM {db}.{kantar_tivo_channel_mapping} 
				WHERE master_c is not Null and UPPER(TRIM(master_c)) <> "NULL"
				GROUP BY
					channel_call_sign,
					channel_short_name,
					channel_long_name,
					network_affiliation,
					master_c
				) t3
				ON t1.channel_call_sign = t3.channel_call_sign
				AND t1.channel_short_name = t3.channel_short_name
				AND t1.channel_long_name = t3.channel_long_name
				AND t1.network_affiliation = t3.network_affiliation
				

				WHERE t1.dt >= TO_DATE('{start_date}', "yyyy-MM-dd") AND t1.dt <= TO_DATE('{end_date}', "yyyy-MM-dd") 
				GROUP BY t1.household_id,
						t1.unit_id,
						t1.event_type,
						t1.recording_offset_seconds,
						t1.program_name,
						t1.channel_call_sign,
						t1.channel_short_name,
						t1.channel_long_name,
						t1.network_affiliation,
						t1.local_timezone,
						t1.utc_offset,
						t1.dt,
						t1.start_event_timestamp_local,
						t1.end_event_timestamp_local,
						t1.program_start_local,
						t1.program_end_local,
						UPPER(TRIM(t2.market_n)),
						UPPER(TRIM(t3.master_c));

	'''


	create_kantar_resolved_table = '''
			DROP TABLE IF EXISTS {db}.{table};
			CREATE TABLE IF NOT EXISTS {db}.{table}  USING PARQUET  AS
			SELECT t1.oproduct_id as oproduct_id,
					t1.property_id as property_id,
					t1.media_id as media_id,  
					t1.market_id as market_id,
					t1.oprog_id as oprog_id, 
				
					t1.spot_length_kantar as spot_length_kantar, 
					t1.daypart_id as daypart_id, 
					t1.podnumber_id as podnumber_id, 
					t1.podposition_id as podposition_id, 
					t1.podbreak_id as podbreak_id, 
					t1.podsize_id as podsize_id, 
					t1.podplacement_a as podplacement_a, 
					t1.rep_month as rep_month, 
					t1.creative_id as creative_id,
					t1.creative_name as creative_name,
					t1.kantar_ts as kantar_ts,

					t2.name as market_name,
					t3.product_id as product_id,
					t4.ultimateowner_id as ultimateowner_id,
					t4.advertiser_id as advertiser_id,
					t5.name as product_name,
					t6.master_n as master_n,
					t6.master_c as master_c,
					t7.name as media_name,
					t8.name as advertiser_name,
					t9.prog_id as prog_id,
					t10.prog_name as prog_name,
					(UPPER(TRIM(t2.name))) as market_name_upper,
					(UPPER(TRIM(t6.master_c))) as master_c_upper
						
			FROM 
				(SELECT rkl.oproduct_id as oproduct_id,
						rkl.property_id as property_id,
						rkl.media_id as media_id,  
						rkl.market_id as market_id,
						rkl.oprog_id as oprog_id, 
						rkl.length_v as spot_length_kantar, 
						rkl.daypart_id as daypart_id, 
						rkl.podnumber_id as podnumber_id, 
						rkl.podposition_id as podposition_id, 
						rkl.podbreak_id as podbreak_id, 
						rkl.podsize_id as podsize_id, 
						rkl.podplacement_a as podplacement_a, 
						rkl.rep_month as rep_month, 
						rkl.creative_id as creative_id,
						unix_timestamp(concat(rkl.date_str,' ',rkl.time_str),'MM/dd/yyyy HH:mm:ss') AS kantar_ts,
						TO_DATE(from_unixtime(unix_timestamp(concat(rkl.date_str,' ',rkl.time_str),'MM/dd/yyyy HH:mm:ss'))) AS kantar_date_str,
						rc.creative_name as creative_name
				FROM {db}.{raw_kantar} rkl 
					LEFT JOIN
					{db}.{raw_creative} rc
					ON rkl.creative_id = rc.creative_id 
					and rkl.year = rc.year 
					and rkl.week = rc.week 
					and rkl.year = {year} 
					and rkl.week >= {start_week} 
					and rkl.week <= {end_week}
				) t1  
				INNER JOIN  {db}.{raw_market} t2 
				ON t1.market_id = t2.id
				INNER JOIN  {db}.{raw_oproduct} t3
				ON t1.oproduct_id = t3.oproduct_id
				INNER JOIN {db}.{raw_product}  t4
				ON t3.product_id = t4.product_id
				INNER JOIN {db}.{raw_product_name}  t5
				ON t4.product_name_id = t5.id
				INNER JOIN {db}.{raw_property} t6
				ON t1.property_id = t6.property_id
				INNER JOIN {db}.{raw_media} t7
				ON t1.media_id = t7.id
				INNER JOIN {db}.{raw_advertiser} t8
				ON t4.advertiser_id = t8.id
				INNER JOIN {db}.{raw_oprog} t9
				ON t1.oprog_id = t9.oprog_id
				INNER JOIN {db}.{raw_prog} t10
				ON t9.prog_id = t10.prog_id
				{ultimateowner_id_join_filter}
		WHERE
				t1.kantar_date_str >= TO_DATE('{start_date}') AND t1.kantar_date_str <= TO_DATE('{end_date}')
		GROUP BY
				t1.oproduct_id,
				t1.property_id,
				t1.media_id,  
				t1.market_id,
				t1.oprog_id, 
				t1.spot_length_kantar, 
				t1.daypart_id, 
				t1.podnumber_id, 
				t1.podposition_id, 
				t1.podbreak_id, 
				t1.podsize_id, 
				t1.podplacement_a, 
				t1.rep_month, 
				t1.creative_id,
				t1.creative_name,
				t1.kantar_ts,
				t2.name,
				t3.product_id,
				t4.ultimateowner_id,
				t4.advertiser_id,
				t5.name,
				t6.master_n,
				t6.master_c,
				t7.name,
				t8.name,
				t9.prog_id,
				t10.prog_name;

	'''

	create_experian_resolved_table = '''
			DROP TABLE IF EXISTS {db}.{table};
			CREATE TABLE IF NOT EXISTS {db}.{table}  USING PARQUET AS
			SELECT 
				ec.figl3id as user_id,
				em.tivo_id as tivo_id,
				em.experian_id as experian_id
			FROM
					(SELECT t1.tivo_id, t1.experian_id FROM {db}.{experian_mango} t1
					INNER JOIN 
					(SELECT  tivo_id , max(file_name) as file_name 
					FROM {db}.{experian_mango} 
					WHERE file_name >= {start_date_int} and file_name <= {end_date_int}
					GROUP BY tivo_id) t2
					ON t1.tivo_id=t2.tivo_id and t1.file_name = t2.file_name) em

				INNER JOIN 
				{db}.{experian_crosswalk} ec
				ON (UPPER(TRIM(em.experian_id)) = UPPER(TRIM(ec.tra_id)))
				GROUP BY em.tivo_id,
						em.experian_id,
						ec.figl3id;

	'''

	create_tivo_kantar_merged_table = '''


			DROP TABLE IF EXISTS {db}.{table};

			CREATE TABLE IF NOT EXISTS {db}.{table} USING PARQUET AS

			SELECT 	
				tivo.household_id as household_id,
				tivo.unit_id as unit_id,
				tivo.event_type as event_type,
				from_unixtime(tivo.program_start_match) as program_start_time_local,
				from_unixtime(tivo.program_end_match) as program_end_time_local,
				tivo.program_name as program_name_tivo,
				tivo.channel_call_sign as channel_call_sign_tivo,
				tivo.channel_short_name as channel_short_name_tivo,
				tivo.channel_long_name as channel_long_name_tivo,
				tivo.network_affiliation as network_affiliation_tivo,
				tivo.local_timezone as local_timezone,
				tivo.utc_offset as utc_offset,
				--tivo.start_event_timestamp_match as start_event_timestamp_match,
				--tivo.end_event_timestamp_match as end_event_timestamp_match,
				CASE WHEN kantar.market_name_upper = "* TOTAL US" 
					THEN kantar.kantar_ts+3600*(tivo.utc_offset+5.0)+tivo.recording_offset_seconds 
					ELSE kantar.kantar_ts+tivo.recording_offset_seconds end AS impression_timestamp,

				(CASE WHEN kantar.market_name_upper='* TOTAL US' AND (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))<kantar.spot_length_kantar
					THEN (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))/kantar.spot_length_kantar
					WHEN kantar.market_name_upper='* TOTAL US' AND (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))>=kantar.spot_length_kantar
					THEN 1
					WHEN kantar.market_name_upper<>'* TOTAL US' AND (tivo.end_event_timestamp_match-kantar.kantar_ts)<kantar.spot_length_kantar
					THEN (tivo.end_event_timestamp_match-kantar.kantar_ts)/kantar.spot_length_kantar
					WHEN kantar.market_name_upper<>'* TOTAL US' AND (tivo.end_event_timestamp_match-kantar.kantar_ts)>=kantar.spot_length_kantar
					THEN 1 
					ELSE NULL END
				) AS impression_fraction,

				kantar.spot_length_kantar AS spot_length_kantar,
				kantar.media_name AS media_name_kantar,
				kantar.market_name AS market_name_kantar,
				kantar.master_n AS channel_full_name_kantar,
				kantar.master_c AS channel_name_kantar,
				kantar.prog_name AS progam_name_kantar,
				kantar.advertiser_name AS advertiser_name_kantar,
				kantar.advertiser_id as advertiser_id,
				kantar.ultimateowner_id as ultimateowner_id,
				kantar.creative_name AS creative_name_kantar,
				kantar.product_name AS product_name_kantar,
				kantar.property_id AS property_id_kantar,
				kantar.product_id AS product_id_kantar,
				kantar.prog_id AS prog_id_kantar,
				kantar.daypart_id AS daypart_id_kantar,
				kantar.podnumber_id  AS podnumber_id_kantar,
				kantar.podposition_id AS podposition_id_kantar,
				kantar.podbreak_id AS podbreak_id_kantar,
				kantar.podsize_id AS podsize_id_kantar,
				kantar.podplacement_a AS podplacement_a_kantar
			FROM {db}.{tivo_resolved} tivo
				INNER JOIN (SELECT k.* FROM {db}.{kantar_resolved} k where k.media_id in (3,5,14)) kantar
			ON 
				tivo.network_affiliation = kantar.master_c_upper

			WHERE
			(kantar.kantar_ts 
				BETWEEN 
					tivo.start_event_timestamp_match-(CASE WHEN kantar.market_name_upper = "* TOTAL US" THEN (3600*(tivo.utc_offset+5.0)) else 0 end) 
				AND tivo.end_event_timestamp_match-(CASE WHEN kantar.market_name_upper = "* TOTAL US" THEN (3600*(tivo.utc_offset+5.0)) else 0 end))
			AND
			(kantar.market_name_upper = "* TOTAL US" OR tivo.market_match = kantar.market_name_upper)

			GROUP BY
				tivo.household_id,
				tivo.unit_id,
				tivo.event_type,
				from_unixtime(tivo.program_start_match),
				from_unixtime(tivo.program_end_match),
				tivo.program_name,
				tivo.channel_call_sign,
				tivo.channel_short_name,
				tivo.channel_long_name,
				tivo.network_affiliation,
				tivo.local_timezone,
				tivo.utc_offset,
				tivo.start_event_timestamp_match,
				tivo.end_event_timestamp_match,
				(CASE WHEN kantar.market_name_upper = "* TOTAL US" 
					THEN kantar.kantar_ts+3600*(tivo.utc_offset+5.0)+tivo.recording_offset_seconds 
					ELSE kantar.kantar_ts+tivo.recording_offset_seconds end),

				(CASE WHEN kantar.market_name_upper='* TOTAL US' AND (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))<kantar.spot_length_kantar
					THEN (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))/kantar.spot_length_kantar
					WHEN kantar.market_name_upper='* TOTAL US' AND (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))>=kantar.spot_length_kantar
					THEN 1
					WHEN kantar.market_name_upper<>'* TOTAL US' AND (tivo.end_event_timestamp_match-kantar.kantar_ts)<kantar.spot_length_kantar
					THEN (tivo.end_event_timestamp_match-kantar.kantar_ts)/kantar.spot_length_kantar
					WHEN kantar.market_name_upper<>'* TOTAL US' AND (tivo.end_event_timestamp_match-kantar.kantar_ts)>=kantar.spot_length_kantar
					THEN 1 
					ELSE NULL END
				),
				kantar.spot_length_kantar,
				kantar.media_name,
				kantar.market_name,
				kantar.master_n,
				kantar.master_c,
				kantar.prog_name,
				kantar.advertiser_name,
				kantar.advertiser_id,
				kantar.ultimateowner_id,
				kantar.creative_name,
				kantar.product_name,
				kantar.property_id,
				kantar.product_id,
				kantar.prog_id,
				kantar.daypart_id,
				kantar.podnumber_id,
				kantar.podposition_id,
				kantar.podbreak_id,
				kantar.podsize_id,
				kantar.podplacement_a


				UNION

				SELECT 	
				tivo.household_id as household_id,
				tivo.unit_id as unit_id,
				tivo.event_type as event_type,
				from_unixtime(tivo.program_start_match) as program_start_time_local,
				from_unixtime(tivo.program_end_match) as program_end_time_local,
				tivo.program_name as program_name_tivo,
				tivo.channel_call_sign as channel_call_sign_tivo,
				tivo.channel_short_name as channel_short_name_tivo,
				tivo.channel_long_name as channel_long_name_tivo,
				tivo.network_affiliation as network_affiliation_tivo,
				tivo.local_timezone as local_timezone,
				tivo.utc_offset as utc_offset,
				--tivo.start_event_timestamp_match as start_event_timestamp_match,
				--tivo.end_event_timestamp_match as end_event_timestamp_match,
				CASE WHEN kantar.market_name_upper = "* TOTAL US" 
					THEN kantar.kantar_ts+3600*(tivo.utc_offset+5.0)+tivo.recording_offset_seconds 
					ELSE kantar.kantar_ts+tivo.recording_offset_seconds end AS impression_timestamp,

				(CASE WHEN kantar.market_name_upper='* TOTAL US' AND (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))<kantar.spot_length_kantar
					THEN (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))/kantar.spot_length_kantar
					WHEN kantar.market_name_upper='* TOTAL US' AND (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))>=kantar.spot_length_kantar
					THEN 1
					WHEN kantar.market_name_upper<>'* TOTAL US' AND (tivo.end_event_timestamp_match-kantar.kantar_ts)<kantar.spot_length_kantar
					THEN (tivo.end_event_timestamp_match-kantar.kantar_ts)/kantar.spot_length_kantar
					WHEN kantar.market_name_upper<>'* TOTAL US' AND (tivo.end_event_timestamp_match-kantar.kantar_ts)>=kantar.spot_length_kantar
					THEN 1 
					ELSE NULL END
				) AS impression_fraction,

				kantar.spot_length_kantar AS spot_length_kantar,
				kantar.media_name AS media_name_kantar,
				kantar.market_name AS market_name_kantar,
				kantar.master_n AS channel_full_name_kantar,
				kantar.master_c AS channel_name_kantar,
				kantar.prog_name AS progam_name_kantar,
				kantar.advertiser_name AS advertiser_name_kantar,
				kantar.advertiser_id as advertiser_id,
				kantar.ultimateowner_id as ultimateowner_id,
				kantar.creative_name AS creative_name_kantar,
				kantar.product_name AS product_name_kantar,
				kantar.property_id AS property_id_kantar,
				kantar.product_id AS product_id_kantar,
				kantar.prog_id AS prog_id_kantar,
				kantar.daypart_id AS daypart_id_kantar,
				kantar.podnumber_id  AS podnumber_id_kantar,
				kantar.podposition_id AS podposition_id_kantar,
				kantar.podbreak_id AS podbreak_id_kantar,
				kantar.podsize_id AS podsize_id_kantar,
				kantar.podplacement_a AS podplacement_a_kantar
			FROM {db}.{tivo_resolved} tivo
				INNER JOIN 
				(SELECT k.* FROM {db}.{kantar_resolved} k where k.media_id not in (3,5,14)) kantar
			ON 
				tivo.channel_match = kantar.master_c_upper

			WHERE
			(kantar.kantar_ts 
				BETWEEN 
					tivo.start_event_timestamp_match-(CASE WHEN kantar.market_name_upper = "* TOTAL US" THEN (3600*(tivo.utc_offset+5.0)) else 0 end) 
				AND tivo.end_event_timestamp_match-(CASE WHEN kantar.market_name_upper = "* TOTAL US" THEN (3600*(tivo.utc_offset+5.0)) else 0 end))
			AND
			(kantar.market_name_upper = "* TOTAL US" OR tivo.market_match = kantar.market_name_upper)

			GROUP BY
				tivo.household_id,
				tivo.unit_id,
				tivo.event_type,
				from_unixtime(tivo.program_start_match),
				from_unixtime(tivo.program_end_match),
				tivo.program_name,
				tivo.channel_call_sign,
				tivo.channel_short_name,
				tivo.channel_long_name,
				tivo.network_affiliation,
				tivo.local_timezone,
				tivo.utc_offset,
				tivo.start_event_timestamp_match,
				tivo.end_event_timestamp_match,
				(CASE WHEN kantar.market_name_upper = "* TOTAL US" 
					THEN kantar.kantar_ts+3600*(tivo.utc_offset+5.0)+tivo.recording_offset_seconds 
					ELSE kantar.kantar_ts+tivo.recording_offset_seconds end),

				(CASE WHEN kantar.market_name_upper='* TOTAL US' AND (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))<kantar.spot_length_kantar
					THEN (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))/kantar.spot_length_kantar
					WHEN kantar.market_name_upper='* TOTAL US' AND (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))>=kantar.spot_length_kantar
					THEN 1
					WHEN kantar.market_name_upper<>'* TOTAL US' AND (tivo.end_event_timestamp_match-kantar.kantar_ts)<kantar.spot_length_kantar
					THEN (tivo.end_event_timestamp_match-kantar.kantar_ts)/kantar.spot_length_kantar
					WHEN kantar.market_name_upper<>'* TOTAL US' AND (tivo.end_event_timestamp_match-kantar.kantar_ts)>=kantar.spot_length_kantar
					THEN 1 
					ELSE NULL END
				),
				kantar.spot_length_kantar,
				kantar.media_name,
				kantar.market_name,
				kantar.master_n,
				kantar.master_c,
				kantar.prog_name,
				kantar.advertiser_name,
				kantar.advertiser_id,
				kantar.ultimateowner_id,
				kantar.creative_name,
				kantar.product_name,
				kantar.property_id,
				kantar.product_id,
				kantar.prog_id,
				kantar.daypart_id,
				kantar.podnumber_id,
				kantar.podposition_id,
				kantar.podbreak_id,
				kantar.podsize_id,
				kantar.podplacement_a
			;

			
	'''

	create_tv_impressions_table = '''
			DROP TABLE IF EXISTS {db}.{table};
			CREATE TABLE IF NOT EXISTS {db}.{table} USING PARQUET AS
			SELECT 
				exp.user_id as user_id,
				exp.experian_id as experian_id,
				tv_k.household_id as tivo_id,
				tv_k.unit_id as unit_id,
				tv_k.event_type as event_type,
				tv_k.program_start_time_local as program_start_time_local,
				tv_k.program_end_time_local as program_end_time_local,
				tv_k.program_name_tivo as program_name_tivo,
				tv_k.channel_call_sign_tivo as channel_call_sign_tivo,
				tv_k.channel_short_name_tivo as channel_short_name_tivo,
				tv_k.channel_long_name_tivo as channel_long_name_tivo,
				tv_k.network_affiliation_tivo as network_affiliation_tivo,
				tv_k.local_timezone as local_timezone,
				tv_k.utc_offset as utc_offset,
				tv_k.impression_timestamp as impression_timestamp,
				tv_k.impression_fraction AS impression_fraction,
				tv_k.spot_length_kantar AS spot_length_kantar,
				tv_k.media_name_kantar AS media_name_kantar,
				tv_k.market_name_kantar AS market_name_kantar,
				tv_k.channel_full_name_kantar AS channel_full_name_kantar,
				tv_k.channel_name_kantar AS channel_name_kantar,
				tv_k.progam_name_kantar AS progam_name_kantar,
				tv_k.advertiser_name_kantar AS advertiser_name_kantar,
				tv_k.advertiser_id as advertiser_id,
				tv_k.creative_name_kantar AS creative_name_kantar,
				tv_k.product_name_kantar AS product_name_kantar,
				tv_k.property_id_kantar AS property_id_kantar,
				tv_k.product_id_kantar AS product_id_kantar,
				tv_k.prog_id_kantar AS prog_id_kantar,
				tv_k.daypart_id_kantar AS daypart_id_kantar,
				tv_k.podnumber_id_kantar AS podnumber_id_kantar,
				tv_k.podposition_id_kantar AS podposition_id_kantar,
				tv_k.podbreak_id_kantar AS podbreak_id_kantar,
				tv_k.podsize_id_kantar AS podsize_id_kantar,
				tv_k.podplacement_a_kantar AS podplacement_a_kantar,
				tv_k.ultimateowner_id AS ultimateowner_id,
				TO_DATE(FROM_UNIXTIME(CAST(tv_k.impression_timestamp AS INT))) AS impression_date

			FROM 
				{db}.{tivo_kantar_merged} tv_k  
			LEFT JOIN
				{db}.{experian_resolved} exp
			ON (tv_k.household_id = exp.tivo_id)
			WHERE TO_DATE(FROM_UNIXTIME(CAST(tv_k.impression_timestamp AS INT))) BETWEEN '{start_date}' AND '{end_date}'
				
			GROUP BY
				exp.user_id,       
				exp.experian_id,    
			
				tv_k.household_id,       
				tv_k.unit_id,       
				tv_k.event_type,       
				tv_k.program_start_time_local,       
				tv_k.program_end_time_local,       
				tv_k.program_name_tivo,       
				tv_k.channel_call_sign_tivo,       
				tv_k.channel_short_name_tivo,       
				tv_k.channel_long_name_tivo,       
				tv_k.network_affiliation_tivo,       
				tv_k.local_timezone,       
				tv_k.utc_offset,       

				tv_k.impression_timestamp,
				tv_k.impression_fraction,
				
				tv_k.spot_length_kantar,       
				tv_k.media_name_kantar,       
				tv_k.market_name_kantar,       
				tv_k.channel_full_name_kantar,       
				tv_k.channel_name_kantar,       
				tv_k.progam_name_kantar,       
				tv_k.advertiser_name_kantar,       
				tv_k.advertiser_id,       
				tv_k.ultimateowner_id,       
				tv_k.creative_name_kantar,       
				tv_k.product_name_kantar,       
				tv_k.property_id_kantar,       
				tv_k.product_id_kantar,       
				tv_k.prog_id_kantar,       
				tv_k.daypart_id_kantar,
				tv_k.podnumber_id_kantar,
				tv_k.podposition_id_kantar,
				tv_k.podbreak_id_kantar,
				tv_k.podsize_id_kantar,
				tv_k.podplacement_a_kantar
				;

	'''


######################################################

######################################################

	create_tivo_resolved_view = '''
		DROP VIEW IF EXISTS {db}.{table};
		CREATE VIEW IF NOT EXISTS {db}.{table} AS
		SELECT 
			t1.household_id as household_id,
			t1.unit_id as unit_id,
			t1.event_type as event_type,
			t1.recording_offset_seconds as recording_offset_seconds,
			t1.program_name as program_name,
			t1.channel_call_sign as channel_call_sign,
			t1.channel_short_name as channel_short_name,
			t1.channel_long_name as channel_long_name,
			t1.network_affiliation as network_affiliation,
			t1.local_timezone as local_timezone,
			t1.utc_offset as utc_offset,
			t1.dt as dt,
			
			unix_timestamp(t1.start_event_timestamp_local)-CAST(t1.recording_offset_seconds AS BIGINT) AS start_event_timestamp_match,
			unix_timestamp(t1.end_event_timestamp_local)-CAST(t1.recording_offset_seconds AS BIGINT) AS end_event_timestamp_match,
			unix_timestamp(t1.program_start_local) as program_start_match,
			unix_timestamp(t1.program_end_local) as program_end_match,

			(UPPER(TRIM(t2.market_n))) as market_match,
			(UPPER(TRIM(t3.master_c))) as channel_match

		FROM 
			(SELECT household_id,
					unit_id,
					event_type,
					recording_offset_seconds,
					program_name,
					(UPPER(TRIM(channel_call_sign))) AS channel_call_sign,
					(UPPER(TRIM(channel_short_name))) AS channel_short_name,
					(UPPER(TRIM(channel_long_name))) AS channel_long_name,
					(UPPER(TRIM(network_affiliation))) AS network_affiliation,
					local_timezone,
					TO_DATE(program_start_local) as dt,
					start_event_timestamp_local,
					end_event_timestamp_local,
					program_start_local,
					program_end_local,
					utc_offset,
					dma_code

				FROM {db}.{raw_tivo}
				WHERE file_name >= {start_date_int} and file_name<={end_date_int}
					AND dma_code is not Null AND upper(dma_code) <> "NULL"
				GROUP BY household_id,
					unit_id,
					event_type,
					recording_offset_seconds,
					program_name,
					channel_call_sign,
					channel_short_name,
					channel_long_name,
					network_affiliation,
					local_timezone,
					start_event_timestamp_local,
					end_event_timestamp_local,
					program_start_local,
					program_end_local,
					utc_offset,
					dma_code) t1

			INNER JOIN {db}.{kantar_tivo_dma_mapping} t2
			ON t1.dma_code = t2.dma_code 

			INNER JOIN 
			(SELECT 
					(UPPER(TRIM(channel_call_sign))) AS channel_call_sign,
					(UPPER(TRIM(channel_short_name))) AS channel_short_name,
					(UPPER(TRIM(channel_long_name))) AS channel_long_name,
					(UPPER(TRIM(network_affiliation))) AS network_affiliation,
					master_c as master_c
				FROM {db}.{kantar_tivo_channel_mapping} 
				WHERE master_c is not Null and UPPER(TRIM(master_c)) <> "NULL"
				GROUP BY
					channel_call_sign,
					channel_short_name,
					channel_long_name,
					network_affiliation,
					master_c
				) t3
				ON t1.channel_call_sign = t3.channel_call_sign
				AND t1.channel_short_name = t3.channel_short_name
				AND t1.channel_long_name = t3.channel_long_name
				AND t1.network_affiliation = t3.network_affiliation
				

				WHERE t1.dt >= '{start_date}' AND t1.dt <= '{end_date}' 
				GROUP BY t1.household_id,
						t1.unit_id,
						t1.event_type,
						t1.recording_offset_seconds,
						t1.program_name,
						t1.channel_call_sign,
						t1.channel_short_name,
						t1.channel_long_name,
						t1.network_affiliation,
						t1.local_timezone,
						t1.utc_offset,
						t1.dt,
						t1.start_event_timestamp_local,
						t1.end_event_timestamp_local,
						t1.program_start_local,
						t1.program_end_local,
						UPPER(TRIM(t2.market_n)),
						UPPER(TRIM(t3.master_c));

	'''

	create_kantar_resolved_view = '''
			DROP VIEW IF EXISTS {db}.{table};
			CREATE VIEW IF NOT EXISTS {db}.{table} AS
			SELECT t1.oproduct_id as oproduct_id,
					t1.property_id as property_id,
					t1.media_id as media_id,  
					t1.market_id as market_id,
					t1.oprog_id as oprog_id, 
				
					t1.spot_length_kantar as spot_length_kantar, 
					t1.daypart_id as daypart_id, 
					t1.podnumber_id as podnumber_id, 
					t1.podposition_id as podposition_id, 
					t1.podbreak_id as podbreak_id, 
					t1.podsize_id as podsize_id, 
					t1.podplacement_a as podplacement_a, 
					t1.rep_month as rep_month, 
					t1.creative_id as creative_id,
					t1.creative_name as creative_name,
					t1.kantar_ts as kantar_ts,

					t2.name as market_name,
					t3.product_id as product_id,
					t4.ultimateowner_id as ultimateowner_id,
					t4.advertiser_id as advertiser_id,
					t5.name as product_name,
					t6.master_n as master_n,
					t6.master_c as master_c,
					t7.name as media_name,
					t8.name as advertiser_name,
					t9.prog_id as prog_id,
					t10.prog_name as prog_name,
					(UPPER(TRIM(t2.name))) as market_name_upper,
					(UPPER(TRIM(t6.master_c))) as master_c_upper
						
			FROM 
				(SELECT rkl.oproduct_id as oproduct_id,
						rkl.property_id as property_id,
						rkl.media_id as media_id,  
						rkl.market_id as market_id,
						rkl.oprog_id as oprog_id, 
						rkl.length_v as spot_length_kantar, 
						rkl.daypart_id as daypart_id, 
						rkl.podnumber_id as podnumber_id, 
						rkl.podposition_id as podposition_id, 
						rkl.podbreak_id as podbreak_id, 
						rkl.podsize_id as podsize_id, 
						rkl.podplacement_a as podplacement_a, 
						rkl.rep_month as rep_month, 
						rkl.creative_id as creative_id,
						unix_timestamp(concat(rkl.date_str,' ',rkl.time_str),'MM/dd/yyyy HH:mm:ss') AS kantar_ts,
						TO_DATE(from_unixtime(unix_timestamp(concat(rkl.date_str,' ',rkl.time_str),'MM/dd/yyyy HH:mm:ss'))) AS kantar_date_str,
						rc.creative_name as creative_name
				FROM {db}.{raw_kantar} rkl 
					LEFT JOIN
					{db}.{raw_creative} rc
					ON rkl.creative_id = rc.creative_id 
					and rkl.year = rc.year 
					and rkl.week = rc.week 
					and rkl.year = {year} 
					and rkl.week >= {start_week} 
					and rkl.week <= {end_week}
				) t1  
				INNER JOIN  {db}.{raw_market} t2 
				ON t1.market_id = t2.id
				INNER JOIN  {db}.{raw_oproduct} t3
				ON t1.oproduct_id = t3.oproduct_id
				INNER JOIN {db}.{raw_product}  t4
				ON t3.product_id = t4.product_id
				INNER JOIN {db}.{raw_product_name}  t5
				ON t4.product_name_id = t5.id
				INNER JOIN {db}.{raw_property} t6
				ON t1.property_id = t6.property_id
				INNER JOIN {db}.{raw_media} t7
				ON t1.media_id = t7.id
				INNER JOIN {db}.{raw_advertiser} t8
				ON t4.advertiser_id = t8.id
				INNER JOIN {db}.{raw_oprog} t9
				ON t1.oprog_id = t9.oprog_id
				INNER JOIN {db}.{raw_prog} t10
				ON t9.prog_id = t10.prog_id
				{ultimateowner_id_join_filter}
		WHERE
				t1.kantar_date_str >= '{start_date}' AND t1.kantar_date_str <= '{end_date}'
		GROUP BY
				t1.oproduct_id,
				t1.property_id,
				t1.media_id,  
				t1.market_id,
				t1.oprog_id, 
				t1.spot_length_kantar, 
				t1.daypart_id, 
				t1.podnumber_id, 
				t1.podposition_id, 
				t1.podbreak_id, 
				t1.podsize_id, 
				t1.podplacement_a, 
				t1.rep_month, 
				t1.creative_id,
				t1.creative_name,
				t1.kantar_ts,
				t2.name,
				t3.product_id,
				t4.ultimateowner_id,
				t4.advertiser_id,
				t5.name,
				t6.master_n,
				t6.master_c,
				t7.name,
				t8.name,
				t9.prog_id,
				t10.prog_name;

	'''

	create_experian_resolved_view = '''
			DROP VIEW IF EXISTS {db}.{table};
			CREATE VIEW IF NOT EXISTS {db}.{table} AS
			SELECT 
				ec.figl3id as user_id,
				em.tivo_id as tivo_id,
				em.experian_id as experian_id
			FROM
					(SELECT t1.tivo_id, t1.experian_id FROM {db}.{experian_mango} t1
					INNER JOIN 
					(SELECT  tivo_id , max(file_name) as file_name 
					FROM {db}.{experian_mango} 
					WHERE file_name >= {start_date_int} and file_name <= {end_date_int}
					GROUP BY tivo_id) t2
					ON t1.tivo_id=t2.tivo_id and t1.file_name = t2.file_name) em

				INNER JOIN 
				{db}.{experian_crosswalk} ec
				ON (UPPER(TRIM(em.experian_id)) = UPPER(TRIM(ec.tra_id)))
				GROUP BY em.tivo_id,
						em.experian_id,
						ec.figl3id;

	'''

	create_tivo_kantar_merged_view = '''
			set hive.optimize.skewjoin =true;
			set hive.skewjoin.key=500000;
			set hive.auto.convert.join=false;
			set hive.auto.convert.join.noconditionaltask=false;

			DROP VIEW IF EXISTS {db}.{table};

			CREATE VIEW IF NOT EXISTS {db}.{table} AS

			SELECT 	
				tivo.household_id as household_id,
				tivo.unit_id as unit_id,
				tivo.event_type as event_type,
				from_unixtime(tivo.program_start_match) as program_start_time_local,
				from_unixtime(tivo.program_end_match) as program_end_time_local,
				tivo.program_name as program_name_tivo,
				tivo.channel_call_sign as channel_call_sign_tivo,
				tivo.channel_short_name as channel_short_name_tivo,
				tivo.channel_long_name as channel_long_name_tivo,
				tivo.network_affiliation as network_affiliation_tivo,
				tivo.local_timezone as local_timezone,
				tivo.utc_offset as utc_offset,
				--tivo.start_event_timestamp_match as start_event_timestamp_match,
				--tivo.end_event_timestamp_match as end_event_timestamp_match,
				CASE WHEN kantar.market_name_upper = "* TOTAL US" 
					THEN kantar.kantar_ts+3600*(tivo.utc_offset+5.0)+tivo.recording_offset_seconds 
					ELSE kantar.kantar_ts+tivo.recording_offset_seconds end AS impression_timestamp,

				(CASE WHEN kantar.market_name_upper='* TOTAL US' AND (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))<kantar.spot_length_kantar
					THEN (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))/kantar.spot_length_kantar
					WHEN kantar.market_name_upper='* TOTAL US' AND (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))>=kantar.spot_length_kantar
					THEN 1
					WHEN kantar.market_name_upper<>'* TOTAL US' AND (tivo.end_event_timestamp_match-kantar.kantar_ts)<kantar.spot_length_kantar
					THEN (tivo.end_event_timestamp_match-kantar.kantar_ts)/kantar.spot_length_kantar
					WHEN kantar.market_name_upper<>'* TOTAL US' AND (tivo.end_event_timestamp_match-kantar.kantar_ts)>=kantar.spot_length_kantar
					THEN 1 
					ELSE NULL END
				) AS impression_fraction,

				kantar.spot_length_kantar AS spot_length_kantar,
				kantar.media_name AS media_name_kantar,
				kantar.market_name AS market_name_kantar,
				kantar.master_n AS channel_full_name_kantar,
				kantar.master_c AS channel_name_kantar,
				kantar.prog_name AS progam_name_kantar,
				kantar.advertiser_name AS advertiser_name_kantar,
				kantar.advertiser_id as advertiser_id,
				kantar.ultimateowner_id as ultimateowner_id,
				kantar.creative_name AS creative_name_kantar,
				kantar.product_name AS product_name_kantar,
				kantar.property_id AS property_id_kantar,
				kantar.product_id AS product_id_kantar,
				kantar.prog_id AS prog_id_kantar,
				kantar.daypart_id AS daypart_id_kantar,
				kantar.podnumber_id  AS podnumber_id_kantar,
				kantar.podposition_id AS podposition_id_kantar,
				kantar.podbreak_id AS podbreak_id_kantar,
				kantar.podsize_id AS podsize_id_kantar,
				kantar.podplacement_a AS podplacement_a_kantar
			FROM {db}.{tivo_resolved} tivo
				INNER JOIN {db}.{kantar_resolved} kantar
			ON 
				tivo.channel_match = kantar.master_c_upper

			WHERE
			(kantar.kantar_ts 
				BETWEEN 
					tivo.start_event_timestamp_match-(CASE WHEN kantar.market_name_upper = "* TOTAL US" THEN (3600*(tivo.utc_offset+5.0)) else 0 end) 
				AND tivo.end_event_timestamp_match-(CASE WHEN kantar.market_name_upper = "* TOTAL US" THEN (3600*(tivo.utc_offset+5.0)) else 0 end))
			AND
			(kantar.market_name_upper = "* TOTAL US" OR tivo.market_match = kantar.market_name_upper)

			GROUP BY
				tivo.household_id,
				tivo.unit_id,
				tivo.event_type,
				from_unixtime(tivo.program_start_match),
				from_unixtime(tivo.program_end_match),
				tivo.program_name,
				tivo.channel_call_sign,
				tivo.channel_short_name,
				tivo.channel_long_name,
				tivo.network_affiliation,
				tivo.local_timezone,
				tivo.utc_offset,
				tivo.start_event_timestamp_match,
				tivo.end_event_timestamp_match,
				(CASE WHEN kantar.market_name_upper = "* TOTAL US" 
					THEN kantar.kantar_ts+3600*(tivo.utc_offset+5.0)+tivo.recording_offset_seconds 
					ELSE kantar.kantar_ts+tivo.recording_offset_seconds end),

				(CASE WHEN kantar.market_name_upper='* TOTAL US' AND (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))<kantar.spot_length_kantar
					THEN (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))/kantar.spot_length_kantar
					WHEN kantar.market_name_upper='* TOTAL US' AND (tivo.end_event_timestamp_match-(kantar.kantar_ts+3600*(tivo.utc_offset+5.0)))>=kantar.spot_length_kantar
					THEN 1
					WHEN kantar.market_name_upper<>'* TOTAL US' AND (tivo.end_event_timestamp_match-kantar.kantar_ts)<kantar.spot_length_kantar
					THEN (tivo.end_event_timestamp_match-kantar.kantar_ts)/kantar.spot_length_kantar
					WHEN kantar.market_name_upper<>'* TOTAL US' AND (tivo.end_event_timestamp_match-kantar.kantar_ts)>=kantar.spot_length_kantar
					THEN 1 
					ELSE NULL END
				),
				kantar.spot_length_kantar,
				kantar.media_name,
				kantar.market_name,
				kantar.master_n,
				kantar.master_c,
				kantar.prog_name,
				kantar.advertiser_name,
				kantar.advertiser_id,
				kantar.ultimateowner_id,
				kantar.creative_name,
				kantar.product_name,
				kantar.property_id,
				kantar.product_id,
				kantar.prog_id,
				kantar.daypart_id,
				kantar.podnumber_id,
				kantar.podposition_id,
				kantar.podbreak_id,
				kantar.podsize_id,
				kantar.podplacement_a
			;


			set hive.auto.convert.join=true;
	'''

	create_tv_impressions_view = '''
			DROP VIEW IF EXISTS {db}.{table};
			CREATE VIEW IF NOT EXISTS {db}.{table} AS
			SELECT 
				exp.user_id as user_id,
				exp.experian_id as experian_id,
				tv_k.household_id as tivo_id,
				tv_k.unit_id as unit_id,
				tv_k.event_type as event_type,
				tv_k.program_start_time_local as program_start_time_local,
				tv_k.program_end_time_local as program_end_time_local,
				tv_k.program_name_tivo as program_name_tivo,
				tv_k.channel_call_sign_tivo as channel_call_sign_tivo,
				tv_k.channel_short_name_tivo as channel_short_name_tivo,
				tv_k.channel_long_name_tivo as channel_long_name_tivo,
				tv_k.network_affiliation_tivo as network_affiliation_tivo,
				tv_k.local_timezone as local_timezone,
				tv_k.utc_offset as utc_offset,
				tv_k.impression_timestamp as impression_timestamp,
				tv_k.impression_fraction AS impression_fraction,
				tv_k.spot_length_kantar AS spot_length_kantar,
				tv_k.media_name_kantar AS media_name_kantar,
				tv_k.market_name_kantar AS market_name_kantar,
				tv_k.channel_full_name_kantar AS channel_full_name_kantar,
				tv_k.channel_name_kantar AS channel_name_kantar,
				tv_k.progam_name_kantar AS progam_name_kantar,
				tv_k.advertiser_name_kantar AS advertiser_name_kantar,
				tv_k.advertiser_id as advertiser_id,
				tv_k.creative_name_kantar AS creative_name_kantar,
				tv_k.product_name_kantar AS product_name_kantar,
				tv_k.property_id_kantar AS property_id_kantar,
				tv_k.product_id_kantar AS product_id_kantar,
				tv_k.prog_id_kantar AS prog_id_kantar,
				tv_k.daypart_id_kantar AS daypart_id_kantar,
				tv_k.podnumber_id_kantar AS podnumber_id_kantar,
				tv_k.podposition_id_kantar AS podposition_id_kantar,
				tv_k.podbreak_id_kantar AS podbreak_id_kantar,
				tv_k.podsize_id_kantar AS podsize_id_kantar,
				tv_k.podplacement_a_kantar AS podplacement_a_kantar,
				tv_k.ultimateowner_id AS ultimateowner_id,
				TO_DATE(FROM_UNIXTIME(CAST(tv_k.impression_timestamp AS INT))) AS impression_date

			FROM 
				{db}.{tivo_kantar_merged} tv_k  
			LEFT JOIN
				{db}.{experian_resolved} exp
			ON (tv_k.household_id = exp.tivo_id)
			WHERE TO_DATE(FROM_UNIXTIME(CAST(tv_k.impression_timestamp AS INT))) BETWEEN '{start_date}' AND '{end_date}'
				
			GROUP BY
				exp.user_id,       
				exp.experian_id,
			
				tv_k.household_id,       
				tv_k.unit_id,       
				tv_k.event_type,       
				tv_k.program_start_time_local,       
				tv_k.program_end_time_local,       
				tv_k.program_name_tivo,       
				tv_k.channel_call_sign_tivo,       
				tv_k.channel_short_name_tivo,       
				tv_k.channel_long_name_tivo,       
				tv_k.network_affiliation_tivo,       
				tv_k.local_timezone,       
				tv_k.utc_offset,       

				tv_k.impression_timestamp,
				tv_k.impression_fraction,
				
				tv_k.spot_length_kantar,       
				tv_k.media_name_kantar,       
				tv_k.market_name_kantar,       
				tv_k.channel_full_name_kantar,       
				tv_k.channel_name_kantar,       
				tv_k.progam_name_kantar,       
				tv_k.advertiser_name_kantar,       
				tv_k.advertiser_id,       
				tv_k.ultimateowner_id,       
				tv_k.creative_name_kantar,       
				tv_k.product_name_kantar,       
				tv_k.property_id_kantar,       
				tv_k.product_id_kantar,       
				tv_k.prog_id_kantar,       
				tv_k.daypart_id_kantar,
				tv_k.podnumber_id_kantar,
				tv_k.podposition_id_kantar,
				tv_k.podbreak_id_kantar,
				tv_k.podsize_id_kantar,
				tv_k.podplacement_a_kantar
				;

	'''

	mta_tv_impression='''
			set hive.exec.dynamic.partition.mode=nonstrict;
			set hive.optimize.sort.dynamic.partition=true;
			set hive.exec.max.dynamic.partitions=10000000;
			
			
			INSERT OVERWRITE TABLE {db}.{table} PARTITION(ultimateowner_id, impression_date)
			SELECT 
				imp.user_id as user_id,
				imp.experian_id as experian_id,
				imp.tivo_id as tivo_id,
				imp.unit_id as unit_id,
				imp.event_type as event_type,
				imp.program_start_time_local as program_start_time_local,
				imp.program_end_time_local as program_end_time_local,
				imp.program_name_tivo as program_name_tivo,
				imp.channel_call_sign_tivo as channel_call_sign_tivo,
				imp.channel_short_name_tivo as channel_short_name_tivo,
				imp.channel_long_name_tivo as channel_long_name_tivo,
				imp.network_affiliation_tivo as network_affiliation_tivo,
				imp.local_timezone as local_timezone,
				imp.utc_offset as utc_offset,
				imp.impression_timestamp as impression_timestamp,
				imp.impression_fraction as impression_fraction,		
				imp.spot_length_kantar AS spot_length_kantar,
				imp.media_name_kantar AS media_name_kantar,
				imp.market_name_kantar AS market_name_kantar,
				imp.channel_full_name_kantar AS channel_full_name_kantar,
				imp.channel_name_kantar AS channel_name_kantar,
				imp.progam_name_kantar AS progam_name_kantar,
				imp.advertiser_name_kantar AS advertiser_name_kantar,
				imp.advertiser_id as advertiser_id,
				imp.creative_name_kantar AS creative_name_kantar,
				imp.product_name_kantar AS product_name_kantar,
				imp.property_id_kantar AS property_id_kantar,
				imp.product_id_kantar AS product_id_kantar,
				imp.prog_id_kantar AS prog_id_kantar,
				imp.daypart_id_kantar AS daypart_id_kantar,
				imp.podnumber_id_kantar AS podnumber_id_kantar,
				imp.podposition_id_kantar AS podposition_id_kantar,
				imp.podbreak_id_kantar AS podbreak_id_kantar,
				imp.podsize_id_kantar AS podsize_id_kantar,
				imp.podplacement_a_kantar AS podplacement_a_kantar,
				imp.ultimateowner_id as ultimateowner_id,
				imp.impression_date as impression_date

			FROM 
				{db}.{tv_impressions} imp;
	
	
	
	'''


	kantar_creative_transform = '''
		DROP TABLE IF EXISTS {db}.{staging_creative_lmt};
		CREATE TABLE {db}.{staging_creative_lmt} AS
		SELECT 
		(CASE WHEN new.creative_id is NULL THEN res.creative_id ELSE new.creative_id END) AS creative_id,
		(CASE WHEN new.creative_id is NULL THEN res.creative_name ELSE new.creative_name END) AS creative_name
		FROM
			(
				SELECT c.creative_id, c.year, c.week, max(d.creative_name) as creative_name
				FROM {db}.{raw_creative} d
				INNER JOIN
				(
					SELECT a.creative_id, a.year, max(week) as week
					FROM {db}.{raw_creative} b
					INNER JOIN
					(
						SELECT creative_id, max(year) as year FROM {db}.{raw_creative}
						WHERE year>={start_year} AND year<={end_year} AND week>={start_week} AND week<={end_week}
						GROUP BY  creative_id
					) a
					ON a.creative_id = b.creative_id and a.year = b.year
					GROUP BY  a.creative_id, a.year
				 ) c
					ON c.creative_id = d.creative_id and c.year = d.year  and c.week = d.week
				GROUP BY  c.creative_id, c.year, c.week
			) new
			
		FULL OUTER JOIN
		{db}.{creative_lmt} res
		ON res.creative_id= new.creative_id
		;

		INSERT OVERWRITE TABLE {db}.{creative_lmt}
		SELECT
			  creative_id,
			  creative_name
		FROM 
			  {db}.{staging_creative_lmt}
		;
			  
			  
			  
		DROP TABLE {db}.{staging_creative_lmt};

	'''


	mta_tv_impression_regenerate_for_date = '''
			set hive.exec.dynamic.partition.mode=nonstrict;
			set hive.optimize.sort.dynamic.partition=true;
			set hive.exec.max.dynamic.partitions=10000;

			INSERT OVERWRITE TABLE {db}.{table} PARTITION(impression_date='{date}')
			SELECT 
				imp.user_id as user_id,
				imp.experian_id as experian_id,
				imp.tivo_id as tivo_id,
				imp.unit_id as unit_id,
				imp.event_type as event_type,
				imp.program_start_time_local as program_start_time_local,
				imp.program_end_time_local as program_end_time_local,
				imp.program_name_tivo as program_name_tivo,
				imp.channel_call_sign_tivo as channel_call_sign_tivo,
				imp.channel_short_name_tivo as channel_short_name_tivo,
				imp.channel_long_name_tivo as channel_long_name_tivo,
				imp.network_affiliation_tivo as network_affiliation_tivo,
				imp.local_timezone as local_timezone,
				imp.utc_offset as utc_offset,
				imp.impression_timestamp as impression_timestamp,		
				imp.spot_length_kantar AS spot_length_kantar,
				imp.media_name_kantar AS media_name_kantar,
				imp.market_name_kantar AS market_name_kantar,
				imp.channel_full_name_kantar AS channel_full_name_kantar,
				imp.channel_name_kantar AS channel_name_kantar,
				imp.progam_name_kantar AS progam_name_kantar,
				imp.advertiser_name_kantar AS advertiser_name_kantar,
				imp.advertiser_id as advertiser_id,
				imp.creative_name_kantar AS creative_name_kantar,
				imp.product_name_kantar AS product_name_kantar,
				imp.property_id_kantar AS property_id_kantar,
				imp.product_id_kantar AS product_id_kantar,
				imp.prog_id_kantar AS prog_id_kantar,
				imp.daypart_id_kantar AS daypart_id_kantar,
				imp.ultimateowner_id as ultimateowner_id,
				imp.impression_date as impression_date

			FROM 
				{db}.{tv_impressions} imp;

	'''