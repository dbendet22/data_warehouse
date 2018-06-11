drop table lito.line_items;
create table if not exists lito.line_items (
	id varchar(64) primary key,
	order_id varchar(64),
	created_at timestamp,
	_sku varchar(128),
	name varchar(128), 
	price numeric(26,2), 
	quantity numeric(26,2), 
	title varchar(128), 
	variant_title varchar(128), 
	total_discount numeric(26,2), 
	fulfillable_quantity numeric(26,2), 
	fulfillment_status varchar(64), 
	gift_card varchar(64), 
	grams numeric(26,2), 
	product_exists varchar(64), 
	product_id varchar(64), 
	_inventory varchar(64), 
	requires_shipping varchar(64), 
	sku varchar(128), 
	tax_lines varchar(1024), 
	taxable varchar(64), 
	variant_id varchar(64),
	variant_inventory_management varchar(64), 
	vendor varchar(64))
;


copy lito.line_items from 's3://lito-misc/test_line_items.csv'
credentials 'aws_iam_role=arn:aws:iam::803205066366:role/myRedshiftRole' 
delimiter ',' region 'us-east-1'
IGNOREHEADER 1
removequotes 
emptyasnull 
blanksasnull
;


-- example query
-- select 
-- 	case 
-- 		when split_part(_sku, '_', 2) in ('tee', 'scarf', 'tote', 'grabbag', 'origtee', 'pack') then split_part(_sku, '_', 2) 
-- 		else 'poster' 
-- 	end as product_type, 
-- 	sum(price*quantity) as sales 
-- from 
-- 	lito.line_items 
-- group by 1 
-- order by 2 desc 
-- limit 25
-- ;
