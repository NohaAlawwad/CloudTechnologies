CREATE EXTERNAL TABLE table_tfidf (word string, owneruserid int, tfidf float) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://aws-noha/tables2';
