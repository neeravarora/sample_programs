CREATE EXTERNAL TABLE IF NOT EXISTS tv_mta_staging_dev2.test212(
			year int, 
			week int
			)
			PARTITIONED BY (file int, file2 int)
			ROW FORMAT SERDE
			'com.bizo.hive.serde.csv.CSVSerde' 
			STORED AS INPUTFORMAT 
			'org.apache.hadoop.mapred.TextInputFormat' 
			OUTPUTFORMAT 
			'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
			LOCATION 's3://tv-mta/tv-staging-dev/tv_mta_staging_dev2.db/test212';
			
			
			
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE tv_mta_staging_dev2.test212 PARTITION(file, file2)
SELECT a.year as year, a.week as week, a.file as file, a.file2 as file2 FROM (SELECT 2019 as year, 12 as week , 1 as file, 2 as file2) a;

INSERT OVERWRITE TABLE tv_mta_staging_dev2.test212 PARTITION(file, file2)
SELECT a.year as year, a.week as week, a.file as file, a.file2 as file2 FROM (SELECT 2019 as year, 12 as week , 1 as file, 3 as file2) a;

INSERT OVERWRITE TABLE tv_mta_staging_dev2.test212 PARTITION(file, file2)
SELECT a.year as year, a.week as week, a.file as file, a.file2 as file2 FROM (SELECT 2019 as year, 12 as week , 1 as file, 4 as file2) a;

INSERT OVERWRITE TABLE tv_mta_staging_dev2.test212 PARTITION(file, file2)
SELECT a.year as year, a.week as week, a.file as file, a.file2 as file2 FROM (SELECT 2019 as year, 12 as week , 2 as file, 1 as file2) a;

INSERT OVERWRITE TABLE tv_mta_staging_dev2.test212 PARTITION(file, file2)
SELECT a.year as year, a.week as week, a.file as file, a.file2 as file2 FROM (SELECT 2019 as year, 12 as week , 2 as file, 2 as file2) a;

INSERT OVERWRITE TABLE tv_mta_staging_dev2.test212 PARTITION(file, file2)
SELECT a.year as year, a.week as week, a.file as file, a.file2 as file2 FROM (SELECT 2019 as year, 12 as week , 3 as file, 1 as file2) a;