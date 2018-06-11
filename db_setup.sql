-- select * from pg_namespace;
-- select distinct schemaname, tablename from pg_table_def;
-- select * from svl_qlog order by starttime desc limit 10;
-- select query, pid, elapsed, substring from svl_qlog;

--

-- create schema if not exists lito authorization icekid99;

-- select distinct schemaname, tablename from pg_table_def where schemaname = 'lito';



-- drop table lito.orders;
-- create table if not exists lito.orders (
-- 	id varchar(64) primary key, 
-- 	closed_at timestamp,
-- 	created_at timestamp,
-- 	number numeric(26,2),
-- 	test boolean, 
-- 	total_price numeric(26,2),
-- 	subtotal_price numeric(26,2),
-- 	total_weight numeric(26,2),
-- 	total_tax numeric(26,2),
-- 	taxes_included varchar(64),
-- 	currency varchar(64), 
-- 	financial_status varchar(64),
-- 	confirmed varchar(64),
-- 	total_discounts numeric(26,2),
-- 	total_line_items_price numeric(26,2),
-- 	cancelled_at timestamp,
-- 	cancel_reason varchar(64),
-- 	total_price_usd numeric(26,2),
-- 	reference varchar(64),
-- 	user_id varchar(64),
-- 	location_id varchar(64),
-- 	source_identifier varchar(64),
-- 	source_url varchar(1024),
-- 	processed_at timestamp,
-- 	device_id varchar(64),
-- 	customer_locale varchar(64),
-- 	app_id varchar(64),
-- 	browser_ip varchar(64),
-- 	landing_site_ref varchar(1024),
-- 	order_number bigint,
-- 	discount_code varchar(64),
-- 	discount_amount numeric(26,2),
-- 	discount_type varchar(64),
-- 	payment_gateway_names varchar(64),
-- 	processing_method varchar(64),
-- 	checkout_id varchar(64),
-- 	source_name varchar(64),
-- 	fulfillment_status varchar(64),
-- 	tags varchar(1024),
-- 	variant_id varchar(64),
-- 	title varchar(1024),
-- 	quantity numeric(26,2),
-- 	price numeric(26,2),
-- 	sku varchar(64),
-- 	variant_title varchar(64),
-- 	vendor varchar(64),
-- 	fulfillment_service varchar(64),
-- 	product_id varchar(64),
-- 	requires_shipping varchar(64),
-- 	taxable varchar(64),
-- 	gift_card varchar(64),
-- 	name varchar(1024),
-- 	product_exists boolean,
-- 	fulfillable_quantity int, 
-- 	customer_country varchar(64),
-- 	customer_state varchar(64),
-- 	customer_city varchar(1024),
-- 	customer_zip varchar(64),
-- 	shipping_code varchar(64),
-- 	orders_count varchar(64),
-- 	user_agent varchar(1024))
-- ;


