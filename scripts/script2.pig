A1 = load '/user/hive/warehouse/secondtable/part-m-00000' USING PigStorage(',') AS (Body:chararray, OwnerUserId:int, Title:chararray);
A2 = load '/user/hive/warehouse/secondtable/part-m-00001' USING PigStorage(',') AS (Body:chararray, OwnerUserId:int, Title:chararray);
A3 = load '/user/hive/warehouse/secondtable/part-m-00002' USING PigStorage(',') AS (Body:chararray, OwnerUserId:int, Title:chararray);
A4 = load '/user/hive/warehouse/secondtable/part-m-00003' USING PigStorage(',') AS (Body:chararray, OwnerUserId:int, Title:chararray);
A5 = load '/user/hive/warehouse/secondtable/part-m-00004' USING PigStorage(',') AS (Body:chararray, OwnerUserId:int, Title:chararray);


AA1 = FOREACH A1 GENERATE $1, CONCAT($0,' ',$2) As text;
AA2 = FOREACH A2 GENERATE $1, CONCAT($0,' ',$2) As text;
AA3 = FOREACH A3 GENERATE $1, CONCAT($0,' ',$2) As text;
AA4 = FOREACH A4 GENERATE $1, CONCAT($0,' ',$2) As text;
AA5 = FOREACH A5 GENERATE $1, CONCAT($0,' ',$2) As text;

AAA1 = FOREACH AA1 GENERATE $0,$1;
AAA2 = FOREACH AA2 GENERATE $0,$1;
AAA3 = FOREACH AA3 GENERATE $0,$1;
AAA4 = FOREACH AA4 GENERATE $0,$1;
AAA5 = FOREACH AA5 GENERATE $0,$1;

c1 = FOREACH AAA1 GENERATE $0, REPLACE($1,'[0-9]*','');
c2 = FOREACH AAA2 GENERATE $0, REPLACE($1,'[0-9]*','');
c3 = FOREACH AAA3 GENERATE $0, REPLACE($1,'[0-9]*','');
c4 = FOREACH AAA4 GENERATE $0, REPLACE($1,'[0-9]*','');
c5 = FOREACH AAA5 GENERATE $0, REPLACE($1,'[0-9]*','');

c6= UNION c1,c2,c3,c4,c5;
store c6 into '/user/cloudera/final/tfidf_input' using PigStorage(',');
