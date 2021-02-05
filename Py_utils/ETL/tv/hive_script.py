
class HQLScript_Builder:

	def __init__(self, hql:str = ""):
		self.hive_script_str = hql
	
	def append(self, query : str, **kwarg):
		#self.hive_script_str = "{}{}".format(self.hive_script_str, HiveQL.get_query(query, **kwarg))
		self.hive_script_str = "{}{}".format(self.hive_script_str, query.format(**kwarg))
		return self

	def clear(self):
		self.hive_script_str = ""


	# def append_tree_expanded_queries(self, query : str, ancestor_hierarchy: list, **kwargs):

	# 	iterable_size = 1
    #     for key, value in kwargs.items():
    #         if isinstance(value, list):
    #             if iterable_size == 1:
    #                 iterable_size = len(value)
    #             else:
    #                 assert len(value) == iterable_size, 'All Iterable parameters lengths should be equal.'
    #     for i in range(iterable_size):
    #         parameters: dict = dict()
    #         for key, value in kwargs.items():
    #             if isinstance(value, list):
    #                 parameters[key] = value[i]
    #             else:
    #                 parameters[key] = value
    #         print(parameters)
    #         self.hive_script_str = "{}{}".format(self.hive_script_str, query.format(**parameters))
    #     return self

	
	
	def append_expanded_queries(self, query : str, **kwargs):
		return self.append_queries_joiner(query, '','','',**kwargs)

	def append_queries_joiner(self, query : str, start_sym : str, end_sym : str, joiner : str, **kwargs):
		iterable_size = 1
		for key, value in kwargs.items():
			if isinstance(value, list):
				if iterable_size == 1:
					iterable_size = len(value)
				else:
					assert len(value) == iterable_size, 'All Iterable parameters lengths should be equal.'
		for i in range(iterable_size):
			
			parameters: dict = dict()
			for key, value in kwargs.items():
				if isinstance(value, list):
					parameters[key] = value[i]
				else:
					parameters[key] = value
			self.hive_script_str = "{}{}".format(self.hive_script_str, query.format(**parameters))
			if joiner != '' and i < iterable_size - 1:
				self.hive_script_str = "{}{}".format(self.hive_script_str, joiner)
		return self

	def value(self):
		return self.hive_script_str

	def str(self):
		return self.hive_script_str

	#Deprecated
	@classmethod
	def get_query(cls, query : str, **kwarg):
		return query.format(**kwarg)


class HQL_Const:
    drop_db = '''
		DROP DATABASE {db};
			
	'''

    drop_table = '''
		DROP TABLE {db}.{table};
			
	'''

    drop_table_if_exists = '''
		DROP TABLE IF EXISTS {db}.{table};
			
	'''

    drop_view_if_exists = '''
		DROP VIEW IF EXISTS {db}.{table};
			
	'''

    create_db= '''
		CREATE DATABASE IF NOT EXISTS {db} LOCATION '{location}/{db}.db';

	'''

    show_partitions= '''
		SHOW PARTITIONS {db}.{table};

	'''

    ext_table_repair= '''
		msck repair table {db}.{table};

	'''

    create_tivo_ext_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			household_id bigint, 
			unit_id bigint, 
			start_event_timestamp_utc timestamp, 
			end_event_timestamp_utc timestamp, 
			recording_offset_seconds int, 
			session_start_timestamp_utc timestamp, 
			session_end_timestamp_utc timestamp, 
			channel_id_1_1 bigint, 
			channel_id_2_0 bigint, 
			channel_call_sign string, 
			channel_short_name string, 
			channel_long_name string, 
			network_affiliation string, 
			program_start_utc timestamp, 
			program_end_utc timestamp, 
			program_id_1_1 bigint, 
			program_id_2_0 bigint, 
			program_name string, 
			event_type string, 
			postal_code int, 
			start_event_timestamp_local timestamp, 
			end_event_timestamp_local timestamp, 
			session_start_timestamp_local timestamp, 
			session_end_timestamp_local timestamp, 
			program_start_local timestamp, 
			program_end_local timestamp, 
			program_series_id_1_1 bigint, 
			program_series_id_2_0 bigint, 
			program_episode_title string, 
			dvr_time_shift string, 
			local_timezone string, 
			utc_offset double, 
			dma_code int, 
			dma_name string)
			PARTITIONED BY ( 
			file_name int)
			ROW FORMAT SERDE 
			'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
			LOCATION '{location}';

    '''
    
    create_kantar_log_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			oproduct_id string,
			property_id string, 
			media_id string, 
			market_id string, 
			oprog_id string, 
			date_str string, 
			time_str string, 
			length_v string, 
			skipped1 string, 
			daypart_id string, 
			podnumber_id string, 
			podposition_id string, 
			podbreak_id string, 
			podsize_id string, 
			podplacement_a string, 
			rep_month string, 
			creative_id string
			)
			PARTITIONED BY (year int, week int, file_name string)
			ROW FORMAT SERDE 
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

    
    '''

    create_creative_ext_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			creative_id string, 
			creative_name string
			)
			PARTITIONED BY (year int, week int)
			ROW FORMAT SERDE
			'com.bizo.hive.serde.csv.CSVSerde'
			STORED AS INPUTFORMAT
			'org.apache.hadoop.mapred.TextInputFormat'
			OUTPUTFORMAT
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


    '''

    # ALTER TABLE tv_mta_staging_dev.raw_kantar ADD PARTITION (year=2019, week=12, file_name='tv_locnet') LOCATION 's3://tv-mta/kantar/tvweekly/tvweekly_00/tv_locnet/';
    add_partition_for_kantar = '''
		ALTER TABLE {db}.{table} ADD IF NOT EXISTS PARTITION (year={year}, week={week}, file_name='{file_name}') 
		LOCATION '{partition_location}';

    '''

    # ALTER TABLE tv_mta_staging_dev.raw_kantar DROP IF EXISTS PARTITION(year=2019, week=12)
    drop_partition_for_kantar = '''
		ALTER TABLE {db}.{table} DROP IF EXISTS PARTITION (year={year}, week={week});
    '''

    

    # ALTER TABLE tv_mta_staging_dev.ext_raw_creative ADD PARTITION (week=12) LOCATION 's3://tv-mta/kantar/tvweekly/tvweekly_00/tv_locnet/';
    add_partition_for_creative = '''
		ALTER TABLE {db}.{table} ADD IF NOT EXISTS PARTITION (year={year}, week={week}) 
		LOCATION '{partition_location}';

    '''

    # ALTER TABLE tv_mta_staging_dev.ext_raw_creative DROP IF EXISTS PARTITION(year=2019, week=12)
    drop_partition_for_creative = '''
		ALTER TABLE {db}.{table} DROP IF EXISTS PARTITION (year={year}, week={week});
    '''

    create_ext_experian_crosswalk = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			tra_id string, 
			figl3id string
			) 
			ROW FORMAT SERDE 
			'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
			WITH SERDEPROPERTIES ( 'field.delim'=',', 'serialization.format'=',') 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
			LOCATION '{location}';


    '''

    create_experian_lu = '''
		CREATE TABLE IF NOT EXISTS {db}.{table}(
			tivo_id bigint, 
			experian_id string)
			ROW FORMAT SERDE 
			'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
			WITH SERDEPROPERTIES ( 
			'field.delim'=',', 
			'serialization.format'=',') 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

    '''

    create_ext_experian_mango = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			tivo_id bigint, 
			experian_id string
			)
			PARTITIONED BY (file_name int)
			ROW FORMAT SERDE 
			'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
			WITH SERDEPROPERTIES ( 
			'field.delim'=',', 
			'serialization.format'=',') 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';

    '''

    # # ALTER TABLE tv_mta_staging_dev.experian_lu ADD PARTITION (arrival_day_dir='0310') LOCATION 's3://tv-mta/experian/0310/';
    # add_partition_experian_lu = '''
	# 	ALTER TABLE {db}.{table} ADD PARTITION IF NOT EXISTS (arrival_day_dir='{day_dir}') LOCATION '{partition_location}';
    # '''

	# #ALTER TABLE tv_mta_staging_dev.raw_kantar DROP IF EXISTS PARTITION(week='00')
	# drop_partition_experian_lu = '''
	# 	ALTER TABLE {db}.{table} DROP IF EXISTS PARTITION (arrival_day_dir='{day_dir}');
    # '''

    create_ext_market_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			id string, 
			name string)
			ROW FORMAT SERDE 
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';
    
    '''

    create_ext_oproduct_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			oproduct_id string, 
			product_id string)
			ROW FORMAT SERDE 
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';
    
    '''


    create_ext_product_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			product_id string, 
			advertiser_id string, 
			brand_id string, 
			ultimateowner_id string, 
			parent_id string, 
			subsid_id string, 
			product_name_id string, 
			product_type_id string, 
			product_descriptor_id string, 
			category_id string, 
			subcategory_id string, 
			microcategory_id string)
			ROW FORMAT SERDE 
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';
    
    '''

    create_ext_media_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			id string, 
			name string)
			ROW FORMAT SERDE 
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';
    
    '''

    create_ext_product_name_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			id string, 
			name string)
			ROW FORMAT SERDE 
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';
    
    '''

    create_ext_property_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			property_id string, 
			media_id string, 
			market_id string, 
			master_id string, 
			master_n string, 
			master_a string, 
			master_c string, 
			aff_id string
			)
			ROW FORMAT SERDE  
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';
    
    '''
	

    create_ext_advertiser_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			id string, 
			name string)
			ROW FORMAT SERDE 
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';
    
    '''

    create_ext_ultimateowner_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			id string, 
			name string)
			ROW FORMAT SERDE 
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';
    
    '''

    create_ext_oprog_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			oprog_id string, 
			prog_id string)
			ROW FORMAT SERDE 
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';
    
    '''

    create_ext_prog_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			prog_id string, 
			prog_name string,
			progtype_id string)
			ROW FORMAT SERDE 
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';
    
    '''

    create_dma_mapping_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			market_id string, 
			market_n string, 
			dma_code string, 
			dma_name string
			) 
			ROW FORMAT SERDE 
			'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';
    
    '''

    create_channel_mapping_basic_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			channel_call_sign string, 
			channel_short_name string, 
			channel_long_name string, 
			network_affiliation string,
			master_c string, 
			kantar_property_id string) 
			ROW FORMAT SERDE 
			'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
			LOCATION '{location}';
	
	'''

    create_channel_mapping_table = '''
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			channel_call_sign string, 
			channel_short_name string, 
			channel_long_name string, 
			network_affiliation string,
			master_c string, 
			kantar_property_id string) 
			ROW FORMAT SERDE 
			'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
			LOCATION '{location}';
	
	'''

    create_kantar_creative_lmt_table = '''
		DROP TABLE IF EXISTS {db}.{table};
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			creative_id string, 
			creative_name string
			)
			ROW FORMAT SERDE 
			'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
			LOCATION '{location}';

    '''

    create_creative_stat_table = '''
		DROP TABLE IF EXISTS {db}.{table};
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			year int, 
			week int,
			completed_on int)
			ROW FORMAT SERDE
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';

    '''

    create_impression_stat_table = '''
		DROP TABLE IF EXISTS {db}.{table};
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			start_date string, 
			end_date string,
			completed_on int)
			ROW FORMAT SERDE
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION '{location}';

    '''


    create_mta_tv_impressions_table = '''
		DROP TABLE IF EXISTS {db}.{table};
		CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
			user_id string,
			experian_id string,
			tivo_id string,
			unit_id  string,        
			event_type  string,       
			program_start_time_local  string,        
			program_end_time_local  string,        
			program_name_tivo  string,        
			channel_call_sign_tivo  string,       
			channel_short_name_tivo  string,        
			channel_long_name_tivo  string,        
			network_affiliation_tivo  string,        
			local_timezone  string,        
			utc_offset  double,        

			impression_timestamp int,
			impression_fraction double,
			
			spot_length_kantar  string,        
			media_name_kantar  string,        
			market_name_kantar  string,        
			channel_full_name_kantar  string,        
			channel_name_kantar  string,        
			progam_name_kantar  string,        
			advertiser_name_kantar  string,       
			advertiser_id  string,    
			creative_name_kantar  string,      
			product_name_kantar  string,        
			property_id_kantar  string,
			product_id_kantar string,
			prog_id_kantar string,		
			daypart_id_kantar  string,
			podnumber_id_kantar  string,
			podposition_id_kantar  string,
			podbreak_id_kantar  string,
			podsize_id_kantar  string,
			podplacement_a_kantar  string

			
			)PARTITIONED BY (ultimateowner_id STRING, impression_date STRING)
			ROW FORMAT SERDE 
			'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
			LOCATION '{location}';
	
    
    '''

    alter_location = ''' 
		ALTER table {db}.{table} SET LOCATION '{location}';
                
    '''

    insert_creative_stat = '''
		INSERT OVERWRITE TABLE {db}.{table}
		SELECT * FROM (SELECT {year}, {week}, {timestamp}) a;
    '''

    insert_impression_stat = '''
		INSERT OVERWRITE TABLE {db}.{table}
		SELECT * FROM (SELECT '{start_date}', '{end_date}', {timestamp}) a;
    '''

    create_hosted_ultimateowner_id_table = '''
		CREATE EXTERNAL TABLE  IF NOT EXISTS  {db}.{table}  (
			ultimateowner_id   string ) 
			ROW FORMAT SERDE 
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION
  			'{location}';
	
	'''

    hive_settings = '''
			set hive.cli.print.header=true; 
			set hive.resultset.use.unique.column.names=false; 
			set hive.execution.engine=mr;
			set hive.mapjoin.smalltable.filesize=300000000;
			set hive.exec.dynamic.partition.mode=nonstrict;
			set hive.exec.max.dynamic.partitions.pernode=15000;
			set hive.exec.max.dynamic.partitions=10000000;
			set hive.optimize.sort.dynamic.partition=true;
			set hive.stats.autogather=true;
			set hive.compute.query.using.stats=true;
			set mapreduce.input.fileinputformat.input.dir.recursive=true;
			set hive.mapred.supports.subdirectories=true;
			set hive.enforce.bucketing=true;
			set hive.auto.convert.join=true;
			set hive.optimize.metadataonly=true;
			set hive.mapred.mode=nonstrict;
			set hive.strict.checks.cartesian.product=false;
			set mapreduce.map.output.compress=true;
			set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
			set hive.exec.compress.intermediate=true;
			set hive.exec.compress.output=true;
			set hive.cbo.enable=true;
			set mapreduce.input.fileinputformat.split.minsize=33554432;
			set mapreduce.input.fileinputformat.split.maxsize=500000000;
			set mapreduce.task.timeout=1200000;

	--		set hive.vectorized.execution.enabled=true;
 	--		set hive.vectorized.execution.reduce.enabled=true;
	--		set hive.vectorized.execution.reduce.groupby.enabled=true;

	--		set hive.exec.reducers.bytes.per.reducer=256000000;
	--		set hive.exec.reducers.max=1000;

	--		set hive.multigroupby.singlereducer=true;
	--		set mapreduce.map.memory.mb=8192;
	--		set mapreduce.reduce.memory.mb=8192;

    --      set mapreduce.map.java.opts=-Xmx5120M;
    --      set mapreduce.reduce.java.opts=-Xmx5120M;
			
			set hive.auto.convert.join.noconditionaltask.size=20000000;
	--		set mapreduce.task.io.sort.mb=2014;

    --      set mapreduce.reduce.cpu.vcores=2;
    --      set mapreduce.map.cpu.vcores=2;
      





    '''