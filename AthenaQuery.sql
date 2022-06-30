drop table `mgbooks`.`books`;

CREATE EXTERNAL TABLE IF NOT EXISTS `mgbooks`.`books` (
  `title` string,
  `author` string,
  `type` string,
  `language` string,
  `dateedition` string,
  `publication` string
)
ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        ESCAPED BY '"'
LOCATION 's3://devopslatam02mgbucket/publication/'

ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://devopslatam02mgbucket/publication/'
TBLPROPERTIES ('has_encrypted_data'='false');