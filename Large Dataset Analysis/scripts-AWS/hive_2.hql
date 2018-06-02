CREATE EXTERNAL TABLE table_2 (body string, owneruserid int, title string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','LOCATION 
's3://aws-noha/tables2';
