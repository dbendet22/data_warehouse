 
copy lito.orders from 's3://lito-misc/data/orders/orders_20150101_20150108.csv' 
credentials 'aws_iam_role=arn:aws:iam::803205066366:role/myRedshiftRole' 
delimiter ',' 
region 'us-east-1'
IGNOREHEADER 1
CSV QUOTE AS '"'
-- removequotes 
emptyasnull 
blanksasnull
; 
