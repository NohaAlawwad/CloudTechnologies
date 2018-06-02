register file:/usr/lib/pig/lib/piggybank.jar

topfirst = LOAD 's3://aws-noha/data/preprocess1.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:datetime, DeletionDate:datetime, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:datetime, LastActivityDate:datetime, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:datetime);

topsecond = LOAD 's3://aws-noha/data/preprocess2.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:datetime, DeletionDate:datetime, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:datetime, LastActivityDate:datetime, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:datetime);

topthird = LOAD 's3://aws-noha/data/preprocess3.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:datetime, DeletionDate:datetime, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:datetime, LastActivityDate:datetime, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:datetime);

topfourth = LOAD 's3://aws-noha/data/preprocess4.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:datetime, DeletionDate:datetime, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:datetime, LastActivityDate:datetime, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:datetime);

topfifth = LOAD 's3://aws-noha/data/preprocess5.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:datetime, DeletionDate:datetime, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:datetime, LastActivityDate:datetime, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:datetime);

F = FOREACH topfirst GENERATE Id, PostTypeId, AcceptedAnswerId, ParentId, CreationDate, DeletionDate, Score, ViewCount, REPLACE(Body,',',' '), OwnerUserId, OwnerDisplayName, LastEditorUserId, LastEditorDisplayName, LastEditDate, LastActivityDate, REPLACE(Title,',',' '), Tags, AnswerCount, CommentCount, FavoriteCount, ClosedDate, CommunityOwnedDate;

S = FOREACH topsecond GENERATE Id, PostTypeId, AcceptedAnswerId, ParentId, CreationDate, DeletionDate, Score, ViewCount, REPLACE(Body,',',' '), OwnerUserId, OwnerDisplayName, LastEditorUserId, LastEditorDisplayName, LastEditDate, LastActivityDate, REPLACE(Title,',',' '), Tags, AnswerCount, CommentCount, FavoriteCount, ClosedDate, CommunityOwnedDate;

T = FOREACH topthird GENERATE Id, PostTypeId, AcceptedAnswerId, ParentId, CreationDate, DeletionDate, Score, ViewCount, REPLACE(Body,',',' '), OwnerUserId, OwnerDisplayName, LastEditorUserId, LastEditorDisplayName, LastEditDate, LastActivityDate, REPLACE(Title,',',' '), Tags, AnswerCount, CommentCount, FavoriteCount, ClosedDate, CommunityOwnedDate;

FO = FOREACH topfourth GENERATE Id, PostTypeId, AcceptedAnswerId, ParentId, CreationDate, DeletionDate, Score, ViewCount, REPLACE(Body,',',' '), OwnerUserId, OwnerDisplayName, LastEditorUserId, LastEditorDisplayName, LastEditDate, LastActivityDate, REPLACE(Title,',',' '), Tags, AnswerCount, CommentCount, FavoriteCount, ClosedDate, CommunityOwnedDate;

FI = FOREACH topfifth GENERATE Id, PostTypeId, AcceptedAnswerId, ParentId, CreationDate, DeletionDate, Score, ViewCount, REPLACE(Body,',',' '), OwnerUserId, OwnerDisplayName, LastEditorUserId, LastEditorDisplayName, LastEditDate, LastActivityDate, REPLACE(Title,',',' '), Tags, AnswerCount, CommentCount, FavoriteCount, ClosedDate, CommunityOwnedDate;

TOP = UNION F, S, T, FO, FI;

FILE1 = FOREACH TOP GENERATE Id AS id, Score AS score, OwnerDisplayName AS ownerdisplayname, $15 AS title;
FILE2 = FOREACH TOP GENERATE REPLACE($8,'"',' ') AS body, OwnerUserId AS owneruserid, REPLACE($15,'"',' ') AS title;

STORE FILE1 INTO  's3://aws-noha/results/res1' using PigStorage(',');
STORE FILE2 INTO 's3://aws-noha/results/res2' using PigStorage(',');
