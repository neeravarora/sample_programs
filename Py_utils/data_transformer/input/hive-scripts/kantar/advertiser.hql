CREATE EXTERNAL TABLE `tv_mta.ext_advertiser`(
  `id` string COMMENT 'from deserializer', 
  `name` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'com.bizo.hive.serde.csv.CSVSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://qubole-disneypark/warehouse/disneypark_v741/ETL/repository/tv_mta/mapping_files/kantar/advertiser'