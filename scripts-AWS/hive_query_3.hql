select count(distinct(owneruserid))
from table_2
where (body REGEXP 'hadoop')
OR (title REGEXP 'hadoop');
