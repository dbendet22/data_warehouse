 
copy lito.line_items from 's3://lito-misc/data/line_items/line_items_20140101.csv' 
credentials 'aws_iam_role=arn:aws:iam::803205066366:role/myRedshiftRole' 
delimiter ',' 
region 'us-east-1'
IGNOREHEADER 1
removequotes 
emptyasnull 
blanksasnull
; 
