CREATE EXTERNAL TABLE table_1 (id int, score int, ownerdisplayname string, title string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 
's3://aws-noha/tables';
