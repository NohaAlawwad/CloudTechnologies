select owneruserid, tfidf, word
from table_tfidf
where owneruserid = 18107
sort by tfidf desc
limit 10;
