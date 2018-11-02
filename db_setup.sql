-- get familiarized with db 
select * from pg_namespace;
select distinct schemaname, tablename from pg_table_def;
select * from svl_qlog order by starttime desc limit 10;
select query, pid, elapsed, substring from svl_qlog;

-- create the litographs schema
create schema if not exists lito authorization icekid99;

-- find what's in litographs schema
select distinct schemaname, tablename from pg_table_def where schemaname = 'lito';

