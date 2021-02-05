
class Transform_HQL_Const:

	create_tivo_resolved_table = '''
		CREATE TABLE IF NOT EXISTS {db}.{table} AS
		SELECT t.household_id as household_id,
				t.unit_id as unit_id,
				t.event_type as event_type,
				t.recording_offset_seconds as recording_offset_seconds,
				t.program_name as program_name,
				t.channel_call_sign as channel_call_sign,
				t.channel_short_name as channel_short_name,
				t.channel_long_name as channel_long_name,
				t.network_affiliation as network_affiliation,
				t.local_timezone as local_timezone,
				t.utc_offset as utc_offset,
				t.dt as dt,
				t.start_event_timestamp_match as start_event_timestamp_match,
				t.end_event_timestamp_match as end_event_timestamp_match,
				t.program_start_match as program_start_match,
				t.program_end_match as program_end_match,
				t.market_match as market_match,
				t.channel_match as channel_match

		FROM
			(SELECT 
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
				(UPPER(TRIM(COALESCE(t3.master_c, t4.master_c, 'NO MATCH')))) as channel_match

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
					ON (t1.dma_code = t2.dma_code)

					LEFT JOIN 
						(SELECT 
							(UPPER(TRIM(channel_call_sign))) AS channel_call_sign,
							(UPPER(TRIM(channel_short_name))) AS channel_short_name,
							(UPPER(TRIM(channel_long_name))) AS channel_long_name,
							(UPPER(TRIM(network_affiliation))) AS network_affiliation,
							master_c as master_c
						FROM {db}.{kantar_tivo_channel_mapping_mz} ) t3
					ON t1.channel_call_sign = t3.channel_call_sign
					AND t1.channel_short_name = t3.channel_short_name
					AND t1.channel_long_name = t3.channel_long_name
					AND t1.network_affiliation = t3.network_affiliation

					LEFT JOIN 
						(SELECT 
							(UPPER(TRIM(channel_call_sign))) AS channel_call_sign,
							(UPPER(TRIM(channel_short_name))) AS channel_short_name,
							(UPPER(TRIM(channel_long_name))) AS channel_long_name,
							(UPPER(TRIM(network_affiliation))) AS network_affiliation,
							master_c as master_c
						FROM {db}.{kantar_tivo_channel_mapping_basic} ) t4
					ON t1.channel_call_sign = t4.channel_call_sign
					AND t1.channel_short_name = t4.channel_short_name
					AND t1.channel_long_name = t4.channel_long_name
					AND t1.network_affiliation = t4.network_affiliation

					WHERE dt >= '{start_date}' AND dt <= '{end_date}') t
				
				WHERE t.channel_match <> 'NO MATCH'

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
						utc_offset,
						dt,
						program_start_match,
						program_end_match,
						start_event_timestamp_match,
						end_event_timestamp_match,
						market_match,
						channel_match;
	'''


	create_tivo_resolved_table_2 = '''
		CREATE TABLE IF NOT EXISTS {db}.{table} AS
		SELECT t.household_id as household_id,
				t.unit_id as unit_id,
				t.event_type as event_type,
				t.recording_offset_seconds as recording_offset_seconds,
				t.program_name as program_name,
				t.channel_call_sign as channel_call_sign,
				t.channel_short_name as channel_short_name,
				t.channel_long_name as channel_long_name,
				t.network_affiliation as network_affiliation,
				t.local_timezone as local_timezone,
				t.utc_offset as utc_offset,
				t.dt as dt,
				t.start_event_timestamp_match as start_event_timestamp_match,
				t.end_event_timestamp_match as end_event_timestamp_match,
				t.program_start_match as program_start_match,
				t.program_end_match as program_end_match,
				t.market_match as market_match,
				t.channel_match as channel_match

		FROM
			(SELECT 
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

				t2.market_n as market_match,

				COALESCE(t3.master_c, 'NO MATCH') as channel_match

				FROM 
					(SELECT household_id,
							unit_id,
							event_type,
							recording_offset_seconds,
							program_name,
							channel_call_sign,
							channel_short_name,
							channel_long_name,
							network_affiliation,
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
					ON (t1.dma_code = t2.dma_code)

					LEFT JOIN {db}.{kantar_tivo_channel_mapping} t3
					ON (UPPER(TRIM(t1.channel_call_sign)) = UPPER(TRIM(t3.channel_call_sign))) 
					AND (UPPER(TRIM(t1.channel_short_name)) = UPPER(TRIM(t3.channel_short_name))) 
					AND (UPPER(TRIM(t1.channel_long_name)) = UPPER(TRIM(t3.channel_long_name)))
					AND (UPPER(TRIM(t1.network_affiliation)) = UPPER(TRIM(t3.network_affiliation)))

					WHERE dt >= '{start_date}' AND dt <= '{end_date}') t
				
				WHERE t.channel_match <> 'NO MATCH'

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
						utc_offset,
						dt,
						program_start_match,
						program_end_match,
						start_event_timestamp_match,
						end_event_timestamp_match,
						market_match,
						channel_match;
	'''

	create_kantar_resolved_table = '''
			CREATE TABLE IF NOT EXISTS {db}.{table} AS
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
				ON t4.product_id = t5.id
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

	create_experian_resolved_table = '''
			CREATE TABLE IF NOT EXISTS {db}.{table} AS
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
			CREATE TABLE IF NOT EXISTS {db}.{table}_part1 AS
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
				kantar.kantar_ts+tivo.recording_offset_seconds AS impression_timestamp,
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
				kantar.daypart_id AS daypart_id_kantar

			FROM {db}.{tivo_resolved} tivo
			INNER JOIN {db}.{kantar_resolved} kantar
			ON
				tivo.channel_match = kantar.master_c_upper AND
				tivo.market_match = kantar.market_name_upper AND 
				kantar.market_name <>"* TOTAL US"

			WHERE
				kantar.kantar_ts>=tivo.start_event_timestamp_match AND kantar.kantar_ts<=tivo.end_event_timestamp_match;


			CREATE TABLE IF NOT EXISTS {db}.{table}_part2 AS
			SELECT 	
				res.household_id as household_id,
				res.unit_id as unit_id,
				res.event_type as event_type,
				res.program_start_time_local as program_start_time_local,
				res.program_end_time_local as program_end_time_local,
				res.program_name_tivo as program_name_tivo,
				res.channel_call_sign_tivo as channel_call_sign_tivo,
				res.channel_short_name_tivo as channel_short_name_tivo,
				res.channel_long_name_tivo as channel_long_name_tivo,
				res.network_affiliation_tivo as network_affiliation_tivo,
				res.local_timezone as local_timezone,
				res.utc_offset as utc_offset,
				res.impression_timestamp as impression_timestamp,
				res.spot_length_kantar AS spot_length_kantar,
				res.media_name_kantar AS media_name_kantar,
				res.market_name_kantar AS market_name_kantar,
				res.channel_full_name_kantar AS channel_full_name_kantar,
				res.channel_name_kantar AS channel_name_kantar,
				res.progam_name_kantar AS progam_name_kantar,
				res.advertiser_name_kantar AS advertiser_name_kantar,
				res.advertiser_id as advertiser_id,
				res.advertiser_id as ultimateowner_id,
				res.creative_name_kantar AS creative_name_kantar,
				res.product_name_kantar AS product_name_kantar,
				res.property_id_kantar AS property_id_kantar,
				res.product_id_kantar AS product_id_kantar,
				res.prog_id_kantar AS prog_id_kantar,
				res.daypart_id_kantar AS daypart_id_kantar
			
			FROM (
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
					tivo.start_event_timestamp_match as start_event_timestamp_match,
					tivo.end_event_timestamp_match as end_event_timestamp_match,
					kantar.kantar_ts+3600*(tivo.utc_offset+5.0)+tivo.recording_offset_seconds  AS impression_timestamp,
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
					kantar.daypart_id AS daypart_id_kantar
					
				FROM {db}.{tivo_resolved} tivo
				INNER JOIN {db}.{kantar_resolved} kantar
				ON
					tivo.channel_match = kantar.master_c_upper AND
					kantar.market_name = "* TOTAL US"
				) res
			WHERE
				res.impression_timestamp>=res.start_event_timestamp_match AND
				res.impression_timestamp<=res.end_event_timestamp_match;


			CREATE TABLE IF NOT EXISTS {db}.{table} AS
			SELECT 
				impression.household_id as household_id,
				impression.unit_id as unit_id,
				impression.event_type as event_type,
				impression.program_start_time_local as program_start_time_local,
				impression.program_end_time_local as program_end_time_local,
				impression.program_name_tivo as program_name_tivo,
				impression.channel_call_sign_tivo as channel_call_sign_tivo,
				impression.channel_short_name_tivo as channel_short_name_tivo,
				impression.channel_long_name_tivo as channel_long_name_tivo,
				impression.network_affiliation_tivo as network_affiliation_tivo,
				impression.local_timezone as local_timezone,
				impression.utc_offset as utc_offset,

				CAST(impression.impression_timestamp AS INT) as impression_timestamp,
				TO_DATE(FROM_UNIXTIME(CAST(impression.impression_timestamp AS INT))) as impression_date,
				
				impression.spot_length_kantar AS spot_length_kantar,
				impression.media_name_kantar AS media_name_kantar,
				impression.market_name_kantar AS market_name_kantar,
				impression.channel_full_name_kantar AS channel_full_name_kantar,
				impression.channel_name_kantar AS channel_name_kantar,
				impression.progam_name_kantar AS progam_name_kantar,
				impression.advertiser_name_kantar AS advertiser_name_kantar,
				impression.advertiser_id as advertiser_id,
				impression.ultimateowner_id as ultimateowner_id,
				impression.creative_name_kantar AS creative_name_kantar,
				impression.product_name_kantar AS product_name_kantar,
				impression.property_id_kantar AS property_id_kantar,
				impression.product_id_kantar AS product_id_kantar,
				impression.prog_id_kantar AS prog_id_kantar,
				impression.daypart_id_kantar AS daypart_id_kantar
			FROM
				(
					SELECT * FROM {db}.{table}_part1
					UNION ALL
					SELECT * FROM {db}.{table}_part2
				) impression
			GROUP BY
				impression.household_id,
				impression.unit_id,
				impression.event_type,
				impression.program_start_time_local,
				impression.program_end_time_local,
				impression.program_name_tivo,
				impression.channel_call_sign_tivo,
				impression.channel_short_name_tivo,
				impression.channel_long_name_tivo,
				impression.network_affiliation_tivo,
				impression.local_timezone,
				impression.utc_offset,
				impression.impression_timestamp,
				impression.spot_length_kantar,
				impression.media_name_kantar,
				impression.market_name_kantar,
				impression.channel_full_name_kantar,
				impression.channel_name_kantar,
				impression.progam_name_kantar,
				impression.advertiser_name_kantar,
				impression.advertiser_id,
				impression.ultimateowner_id,
				impression.creative_name_kantar,
				impression.product_name_kantar,
				impression.property_id_kantar,
				impression.product_id_kantar,
				impression.prog_id_kantar,
				impression.daypart_id_kantar
				;
				
			DROP TABLE IF EXISTS {db}.{table}_part1;
			DROP TABLE IF EXISTS {db}.{table}_part2;

	'''

	create_tivo_kantar_merged_table2 = '''
		CREATE TABLE IF NOT EXISTS {db}.{table} AS
		SELECT 
			impression.household_id as household_id,
			impression.unit_id as unit_id,
			impression.event_type as event_type,
			impression.program_start_time_local as program_start_time_local,
			impression.program_end_time_local as program_end_time_local,
			impression.program_name_tivo as program_name_tivo,
			impression.channel_call_sign_tivo as channel_call_sign_tivo,
			impression.channel_short_name_tivo as channel_short_name_tivo,
			impression.channel_long_name_tivo as channel_long_name_tivo,
			impression.network_affiliation_tivo as network_affiliation_tivo,
			impression.local_timezone as local_timezone,
			impression.utc_offset as utc_offset,

			CAST(impression.impression_timestamp AS INT) as impression_timestamp,
			TO_DATE(FROM_UNIXTIME(CAST(impression.impression_timestamp AS INT))) as impression_date,
			
			impression.spot_length_kantar AS spot_length_kantar,
			impression.media_name_kantar AS media_name_kantar,
			impression.market_name_kantar AS market_name_kantar,
			impression.channel_full_name_kantar AS channel_full_name_kantar,
			impression.channel_name_kantar AS channel_name_kantar,
			impression.progam_name_kantar AS progam_name_kantar,
			impression.advertiser_name_kantar AS advertiser_name_kantar,
			impression.advertiser_id as advertiser_id,
			impression.ultimateowner_id as ultimateowner_id,
			impression.creative_name_kantar AS creative_name_kantar,
			impression.product_name_kantar AS product_name_kantar,
			impression.property_id_kantar AS property_id_kantar,
			impression.product_id_kantar AS product_id_kantar,
			impression.prog_id_kantar AS prog_id_kantar,
			impression.daypart_id_kantar AS daypart_id_kantar
			
		FROM
			(
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
					kantar.kantar_ts+tivo.recording_offset_seconds AS impression_timestamp,
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
					kantar.daypart_id AS daypart_id_kantar

				FROM {db}.{tivo_resolved} tivo
				INNER JOIN {db}.{kantar_resolved} kantar
				ON
					tivo.channel_match = kantar.master_c_upper AND
					tivo.market_match = kantar.market_name_upper AND 
					kantar.market_name <>"* TOTAL US"
		
				WHERE
					kantar.kantar_ts>=tivo.start_event_timestamp_match AND kantar.kantar_ts<=tivo.end_event_timestamp_match
						
						
				UNION ALL

				SELECT 	
					res.household_id as household_id,
					res.unit_id as unit_id,
					res.event_type as event_type,
					res.program_start_time_local as program_start_time_local,
					res.program_end_time_local as program_end_time_local,
					res.program_name_tivo as program_name_tivo,
					res.channel_call_sign_tivo as channel_call_sign_tivo,
					res.channel_short_name_tivo as channel_short_name_tivo,
					res.channel_long_name_tivo as channel_long_name_tivo,
					res.network_affiliation_tivo as network_affiliation_tivo,
					res.local_timezone as local_timezone,
					res.utc_offset as utc_offset,
					res.impression_timestamp as impression_timestamp,
					res.spot_length_kantar AS spot_length_kantar,
					res.media_name_kantar AS media_name_kantar,
					res.market_name_kantar AS market_name_kantar,
					res.channel_full_name_kantar AS channel_full_name_kantar,
					res.channel_name_kantar AS channel_name_kantar,
					res.progam_name_kantar AS progam_name_kantar,
					res.advertiser_name_kantar AS advertiser_name_kantar,
					res.advertiser_id as advertiser_id,
					res.advertiser_id as ultimateowner_id,
					res.creative_name_kantar AS creative_name_kantar,
					res.product_name_kantar AS product_name_kantar,
					res.property_id_kantar AS property_id_kantar,
					res.product_id_kantar AS product_id_kantar,
					res.prog_id_kantar AS prog_id_kantar,
					res.daypart_id_kantar AS daypart_id_kantar
				
				FROM (
				
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
						tivo.start_event_timestamp_match as start_event_timestamp_match,
						tivo.end_event_timestamp_match as end_event_timestamp_match,
						kantar.kantar_ts+3600*(tivo.utc_offset+5.0)+tivo.recording_offset_seconds  AS impression_timestamp,
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
						kantar.daypart_id AS daypart_id_kantar
						
					FROM {db}.{tivo_resolved} tivo
					INNER JOIN {db}.{kantar_resolved} kantar
					ON
						tivo.channel_match = kantar.master_c_upper AND
						kantar.market_name = "* TOTAL US"
					) res
						
					WHERE
						
						res.impression_timestamp>=res.start_event_timestamp_match AND
						res.impression_timestamp<=res.end_event_timestamp_match
						
			) impression
				
		GROUP BY
			impression.household_id,
			impression.unit_id,
			impression.event_type,
			impression.program_start_time_local,
			impression.program_end_time_local,
			impression.program_name_tivo,
			impression.channel_call_sign_tivo,
			impression.channel_short_name_tivo,
			impression.channel_long_name_tivo,
			impression.network_affiliation_tivo,
			impression.local_timezone,
			impression.utc_offset,
			impression.impression_timestamp,
			impression.spot_length_kantar,
			impression.media_name_kantar,
			impression.market_name_kantar,
			impression.channel_full_name_kantar,
			impression.channel_name_kantar,
			impression.progam_name_kantar,
			impression.advertiser_name_kantar,
			impression.advertiser_id,
			impression.ultimateowner_id,
			impression.creative_name_kantar,
			impression.product_name_kantar,
			impression.property_id_kantar,
			impression.product_id_kantar,
			impression.prog_id_kantar,
			impression.daypart_id_kantar
			;

	'''

	create_tivo_kantar_merged_table3 = '''
			CREATE TABLE IF NOT EXISTS {db}.{table} AS
			SELECT 
				impression.household_id as household_id,
				impression.unit_id as unit_id,
				impression.event_type as event_type,
				impression.program_start_time_local as program_start_time_local,
				impression.program_end_time_local as program_end_time_local,
				impression.program_name_tivo as program_name_tivo,
				impression.channel_call_sign_tivo as channel_call_sign_tivo,
				impression.channel_short_name_tivo as channel_short_name_tivo,
				impression.channel_long_name_tivo as channel_long_name_tivo,
				impression.network_affiliation_tivo as network_affiliation_tivo,
				impression.local_timezone as local_timezone,
				impression.utc_offset as utc_offset,

				CAST(impression.impression_timestamp AS INT) as impression_timestamp,
				TO_DATE(FROM_UNIXTIME(CAST(impression.impression_timestamp AS INT))) as impression_date,
				
				impression.spot_length_kantar AS spot_length_kantar,
				impression.media_name_kantar AS media_name_kantar,
				impression.market_name_kantar AS market_name_kantar,
				impression.channel_full_name_kantar AS channel_full_name_kantar,
				impression.channel_name_kantar AS channel_name_kantar,
				impression.progam_name_kantar AS progam_name_kantar,
				impression.advertiser_name_kantar AS advertiser_name_kantar,
				impression.advertiser_id as advertiser_id,
				impression.ultimateowner_id as ultimateowner_id,
				impression.creative_name_kantar AS creative_name_kantar,
				impression.product_name_kantar AS product_name_kantar,
				impression.property_id_kantar AS property_id_kantar,
				impression.product_id_kantar AS product_id_kantar,
				impression.prog_id_kantar AS prog_id_kantar,
				impression.daypart_id_kantar AS daypart_id_kantar
				
			FROM
				(
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
						kantar.kantar_ts+tivo.recording_offset_seconds AS impression_timestamp,
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
						kantar.daypart_id AS daypart_id_kantar

					FROM {db}.{tivo_resolved} tivo
					INNER JOIN {db}.{kantar_resolved} kantar
					ON
						UPPER(TRIM(tivo.channel_match)) = UPPER(TRIM(kantar.master_c)) AND
						UPPER(TRIM(tivo.market_match)) = UPPER(TRIM(kantar.market_name)) AND 
						kantar.market_name <>'* TOTAL US'
					WHERE
						kantar.kantar_ts>=tivo.start_event_timestamp_match AND kantar.kantar_ts<=tivo.end_event_timestamp_match
							
							
					UNION ALL

					SELECT 	
						res.household_id as household_id,
						res.unit_id as unit_id,
						res.event_type as event_type,
						res.program_start_time_local as program_start_time_local,
						res.program_end_time_local as program_end_time_local,
						res.program_name_tivo as program_name_tivo,
						res.channel_call_sign_tivo as channel_call_sign_tivo,
						res.channel_short_name_tivo as channel_short_name_tivo,
						res.channel_long_name_tivo as channel_long_name_tivo,
						res.network_affiliation_tivo as network_affiliation_tivo,
						res.local_timezone as local_timezone,
						res.utc_offset as utc_offset,
						res.impression_timestamp as impression_timestamp,
						res.spot_length_kantar AS spot_length_kantar,
						res.media_name_kantar AS media_name_kantar,
						res.market_name_kantar AS market_name_kantar,
						res.channel_full_name_kantar AS channel_full_name_kantar,
						res.channel_name_kantar AS channel_name_kantar,
						res.progam_name_kantar AS progam_name_kantar,
						res.advertiser_name_kantar AS advertiser_name_kantar,
						res.advertiser_id as advertiser_id,
						res.advertiser_id as ultimateowner_id,
						res.creative_name_kantar AS creative_name_kantar,
						res.product_name_kantar AS product_name_kantar,
						res.property_id_kantar AS property_id_kantar,
						res.product_id_kantar AS product_id_kantar,
						res.prog_id_kantar AS prog_id_kantar,
						res.daypart_id_kantar AS daypart_id_kantar
					
					FROM (
					
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
							tivo.start_event_timestamp_match as start_event_timestamp_match,
							tivo.end_event_timestamp_match as end_event_timestamp_match,
							kantar.kantar_ts+3600*(tivo.utc_offset+5.0)+tivo.recording_offset_seconds  AS impression_timestamp,
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
							kantar.daypart_id AS daypart_id_kantar
							
						FROM {db}.{tivo_resolved} tivo
						INNER JOIN {db}.{kantar_resolved} kantar
						ON
							UPPER(TRIM(tivo.channel_match)) = UPPER(TRIM(kantar.master_c)) AND
							kantar.market_name = '* TOTAL US'
						) res
							
						WHERE
							
							res.impression_timestamp>=res.start_event_timestamp_match AND
							res.impression_timestamp<=res.end_event_timestamp_match
							
				) impression
					
			GROUP BY
				impression.household_id,
				impression.unit_id,
				impression.event_type,
				impression.program_start_time_local,
				impression.program_end_time_local,
				impression.program_name_tivo,
				impression.channel_call_sign_tivo,
				impression.channel_short_name_tivo,
				impression.channel_long_name_tivo,
				impression.network_affiliation_tivo,
				impression.local_timezone,
				impression.utc_offset,

				impression.impression_timestamp,
				impression.spot_length_kantar,
				impression.media_name_kantar,
				impression.market_name_kantar,
				impression.channel_full_name_kantar,
				impression.channel_name_kantar,
				impression.progam_name_kantar,
				impression.advertiser_name_kantar,
				impression.advertiser_id,
				impression.ultimateowner_id,
				impression.creative_name_kantar,
				impression.product_name_kantar,
				impression.property_id_kantar,
				impression.product_id_kantar,
				impression.prog_id_kantar,
				impression.daypart_id_kantar
				;

	'''

	create_tivo_kantar_merged_table4 = '''
			CREATE TABLE IF NOT EXISTS {db}.{table} AS
			SELECT 
				impression.household_id as household_id,
				impression.unit_id as unit_id,
				impression.event_type as event_type,
				impression.program_start_time_local as program_start_time_local,
				impression.program_end_time_local as program_end_time_local,
				impression.program_name_tivo as program_name_tivo,
				impression.channel_call_sign_tivo as channel_call_sign_tivo,
				impression.channel_short_name_tivo as channel_short_name_tivo,
				impression.channel_long_name_tivo as channel_long_name_tivo,
				impression.network_affiliation_tivo as network_affiliation_tivo,
				impression.local_timezone as local_timezone,
				impression.utc_offset as utc_offset,

				CAST(impression.impression_timestamp AS INT) as impression_timestamp,
				TO_DATE(FROM_UNIXTIME(CAST(impression.impression_timestamp AS INT))) as impression_date,
				
				impression.spot_length_kantar AS spot_length_kantar,
				impression.media_name_kantar AS media_name_kantar,
				impression.market_name_kantar AS market_name_kantar,
				impression.channel_full_name_kantar AS channel_full_name_kantar,
				impression.channel_name_kantar AS channel_name_kantar,
				impression.progam_name_kantar AS progam_name_kantar,
				impression.advertiser_name_kantar AS advertiser_name_kantar,
				impression.advertiser_id as advertiser_id,
				impression.ultimateowner_id as ultimateowner_id,
				impression.creative_name_kantar AS creative_name_kantar,
				impression.product_name_kantar AS product_name_kantar,
				impression.property_id_kantar AS property_id_kantar,
				impression.product_id_kantar AS product_id_kantar,
				impression.prog_id_kantar AS prog_id_kantar,
				impression.daypart_id_kantar AS daypart_id_kantar
				
			FROM 
				(
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

					(CASE WHEN kantar.market_name='* TOTAL US' THEN kantar.kantar_ts+3600*(tivo.utc_offset+5.0)+tivo.recording_offset_seconds ELSE
					kantar.kantar_ts+tivo.recording_offset_seconds END) AS impression_timestamp,
					

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
					kantar.daypart_id AS daypart_id_kantar

				FROM {db}.{tivo_resolved} tivo
				INNER JOIN {db}.{kantar_resolved} kantar
				ON
					UPPER(TRIM(tivo.channel_match)) = UPPER(TRIM(kantar.master_c)) AND
					UPPER(TRIM(tivo.market_match)) = UPPER(TRIM(kantar.market_name))
		
				WHERE
					((kantar.market_name='* TOTAL US') AND (kantar.kantar_ts+3600*(tivo.utc_offset+5.0)>=tivo.start_event_timestamp_match AND
						kantar.kantar_ts+3600*(tivo.utc_offset+5.0)<=tivo.end_event_timestamp_match)) OR ((kantar.market_name<>'* TOTAL US') AND (kantar.kantar_ts>=tivo.start_event_timestamp_match AND
						kantar.kantar_ts<=tivo.end_event_timestamp_match))
					
				UNION ALL
			
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

					(CASE WHEN kantar.market_name='* TOTAL US' THEN kantar.kantar_ts+3600*(tivo.utc_offset+5.0)+tivo.recording_offset_seconds ELSE
					kantar.kantar_ts+tivo.recording_offset_seconds END) AS impression_timestamp,
					

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
					kantar.daypart_id AS daypart_id_kantar
					
				FROM {db}.{tivo_resolved} tivo
				INNER JOIN {db}.{kantar_resolved} kantar
				ON
					UPPER(TRIM(tivo.channel_match)) = UPPER(TRIM(kantar.master_c)) AND
					kantar.market_name = '* TOTAL US'
					
				WHERE
					((kantar.market_name='* TOTAL US') AND (kantar.kantar_ts+3600*(tivo.utc_offset+5.0)>=tivo.start_event_timestamp_match AND
					kantar.kantar_ts+3600*(tivo.utc_offset+5.0)<=tivo.end_event_timestamp_match)) OR ((kantar.market_name<>'* TOTAL US') AND (kantar.kantar_ts>=tivo.start_event_timestamp_match AND
					kantar.kantar_ts<=tivo.end_event_timestamp_match))
					
			) impression
			GROUP BY
				impression.household_id,
				impression.unit_id,
				impression.event_type,
				impression.program_start_time_local,
				impression.program_end_time_local,
				impression.program_name_tivo,
				impression.channel_call_sign_tivo,
				impression.channel_short_name_tivo,
				impression.channel_long_name_tivo,
				impression.network_affiliation_tivo,
				impression.local_timezone,
				impression.utc_offset,

				impression.impression_timestamp,
				impression.spot_length_kantar,
				impression.media_name_kantar,
				impression.market_name_kantar,
				impression.channel_full_name_kantar,
				impression.channel_name_kantar,
				impression.progam_name_kantar,
				impression.advertiser_name_kantar,
				impression.advertiser_id,
				impression.ultimateowner_id,
				impression.creative_name_kantar,
				impression.product_name_kantar,
				impression.property_id_kantar,
				impression.product_id_kantar,
				impression.prog_id_kantar,
				impression.daypart_id_kantar
				;
	'''

	create_tv_impressions_table = '''
			CREATE TABLE IF NOT EXISTS {db}.{table} AS
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
				tv_k.ultimateowner_id as ultimateowner_id,
				tv_k.impression_date as impression_date

			FROM 
				{db}.{tivo_kantar_merged} tv_k  
			LEFT JOIN
				{db}.{experian_resolved} exp
			ON (tv_k.household_id = exp.tivo_id)
				
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
				tv_k.impression_date,
				
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
				tv_k.daypart_id_kantar
				;

	'''

	mta_tv_impression='''
			set hive.exec.dynamic.partition.mode=nonstrict;
			set hive.optimize.sort.dynamic.partition=true;
			set hive.exec.max.dynamic.partitions=10000;
			
			
			INSERT INTO TABLE {db}.{table} PARTITION(ultimateowner_id, impression_date)
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