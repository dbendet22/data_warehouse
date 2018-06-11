







drop table if exists tmp_variables;
create temp table tmp_variables as 
	select
   		to_char(CURRENT_DATE-1, 'YYYYMMDD') || '.csv' as yesterday_location
;

copy lito.orders from ('s3://lito-misc/data/orders/orders_' || (select yesterday_location FROM tmp_variables))
credentials 'aws_iam_role=arn:aws:iam::803205066366:role/myRedshiftRole' 
delimiter ',' 
region 'us-east-1'
IGNOREHEADER 1
CSV QUOTE AS '"'
-- removequotes 
emptyasnull 
blanksasnull
;

-- select userid, starttime, line_number, trim(colname) as colname, type, position, err_code, err_reason from stl_load_errors order by starttime desc limit 5;
