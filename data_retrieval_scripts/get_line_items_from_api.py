import requests
import json
import csv
import unicodecsv
import os
from datetime import date
import calendar
import xlrd
import xlwt
from collections import OrderedDict, Counter, defaultdict
import pprint
import re
from dateutil.relativedelta import relativedelta
from operator import itemgetter
import jinja2
import babel.numbers
import decimal
import pdfcrowd
import math
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta
import sys
import datetime
import pytz

# work_path = '/home/ec2-user/src/data_warehouse/'
work_path = '/Users/davidbendet/Work/coding/src/data_warehouse/'

# last day of data (or backfill)
# yesterday = sys.argv[1]
yesterday_date = sys.argv[1]
today_date = sys.argv[2] 
yesterday = yesterday_date.replace('-','')
today = today_date.replace('-','')
csv_name = 'line_items_' + yesterday + '_' + today

# # if i only wanted to backfill this table specifically, i could do this: 
# # yesterday = '20180710'
# yesterday_date = '2018-07-10'
# today_date = '2018-10-26'
# today_date = datetime.datetime.strftime(datetime.datetime.strptime(today_date, '%Y-%m-%d') + timedelta(1), '%Y-%m-%d')
# yesterday = yesterday_date.replace('-','')
# today = today_date.replace('-','')
# csv_name = 'line_items_' + yesterday + '_' + today

def get_api_login():
    admin_url = os.environ['RETAIL_ADMIN_URL']
    api_key = os.environ['RETAIL_API_KEY']
    api_password = os.environ['RETAIL_API_PASSWORD']     
    return admin_url, api_key, api_password

# credentials 
admin_url, api_key, api_password = get_api_login()

# get required page count 
count = json.loads(requests.get(admin_url + 'orders/count.json?status=any&created_at_min=' + yesterday_date + 'EST' + '&created_at_max=' + today_date + 'EST' + '&fields=id,line_items', 
				   auth=(api_key, api_password)).content)['count']

page_size = 250
pages = math.ceil(count / 250.0)

# initialize variables
current_page = 1
orders_w_line_items = []

# for all of the pages with records, add each record to the list and move to the next page
while current_page <= pages:

  r = requests.get(admin_url + 'orders.json?limit=250&page=' + str(current_page) + '&status=any&created_at_min=' + yesterday_date + 'EST' + '&created_at_max=' + today_date + 'EST' + '&fields=id,created_at,test,total_tax,financial_status,cancelled_at,discount_codes,customer,line_items', 
  				   auth=(api_key, api_password))  
  x = json.loads(r.content)['orders']
  orders_w_line_items.append(x) 
  current_page += 1

# initialize empty list
line_items_list = []

# for each order, append the order_id to the line_item dict, and add the dict of line_item info to the list of line_items
for page in orders_w_line_items:
	for order in page:
		order_id = order['id']
		created_at = pd.to_datetime(order['created_at'])
		test = order['test']
		total_tax = order['total_tax']
		financial_status = order['financial_status']
		cancelled_at = order['cancelled_at']
		order_discount_amount = order['discount_codes'][0]['amount'] if order['discount_codes'] else 0,
		country = order.get('customer')['default_address']['country'] if order.get('customer') else None,
		state = order.get('customer')['default_address']['province'] if order.get('customer') else None,
		city = order.get('customer')['default_address']['city'] if order.get('customer') else None,		
		zip_code = order.get('customer')['default_address']['zip'] if order.get('customer') else None,	
		email = order.get('customer')['email'] if order.get('customer') else None,
		first_name = order.get('customer')['first_name'] if order.get('customer') else None,
		last_name = order.get('customer')['last_name'] if order.get('customer') else None,
		orders_count = order.get('customer')['orders_count'] if order.get('customer') else 0,		
		for line_item in order['line_items']:
			# line_item = order['line_items'][0] # might be why every line item gets first element info
			item = line_item
			item.update({'order_id': order_id})
			item.update({'created_at': created_at})	
			item.update({'test': test})	
			item.update({'total_tax': total_tax})	
			item.update({'financial_status': financial_status})	
			item.update({'cancelled_at': cancelled_at})	
			item.update({'order_discount_amount': order_discount_amount[0] if order_discount_amount != 0 else 0})	
			item.update({'country': country[0]})	
			item.update({'state': state[0]})	
			item.update({'city': city[0]})			
			item.update({'zip_code': zip_code[0]})	
			item.update({'email': email[0]})	
			item.update({'first_name': first_name[0]})	
			item.update({'last_name': last_name[0]})	
			item.update({'orders_count': orders_count[0] if orders_count != 0 else 0})			
			line_items_list.append(item)			
			# print(item)
			# print('------------------------------------------------------------------------------------------------')			
			

# how i found the error of removing dupe keys in dict:
#
# test_list = []
#
# for thing in line_items_list:
# 	blah = (thing['order_id'])
# 	if blah == 545480867966:
# 		test_list.append(thing)
#
# len(test_list)
#
# for thing in test_list:
# 	print(thing['title'], thing['quantity'])
# the thing['quantity'] field vs thing['properties']['quantity'] field
# and bc of that diff, when i update the dictionary, the latter overrides the former
#
# solution is to add the if statment at the end of the for-loop list comprehension
# for item in test_list:
# 	formatted_property_dict = {}
# 	[formatted_property_dict.update({thing['name'] : thing['value']}) for thing in item['properties'] if thing['name'] != 'quantity']
# 	for attribute in formatted_property_dict:
# 		item.update({attribute : formatted_property_dict[attribute]})

# now look:
# for thing in test_list:
# 	print(thing['title'], thing['quantity'])


# this cleverly takes properties out of the nested list of dictionaries and addes each as it's own line
for item in line_items_list:
	formatted_property_dict = {}
	[formatted_property_dict.update({thing['name'] : thing['value']}) for thing in item['properties'] if thing['name'] != 'quantity']
	for attribute in formatted_property_dict:
		item.update({attribute : formatted_property_dict[attribute]})

# output the list (which is now a list of dictionaries) to a csv
keys = ['id', 'order_id', 'created_at', 'created_at_est', '_sku', 'name', 'price', 'quantity', 'title', 'variant_title', 'total_discount', 'fulfillable_quantity', 'fulfillment_status', 'gift_card', 'grams', 'product_exists', 'product_id', '_inventory', 'requires_shipping', 'sku', 'tax_lines', 'taxable', 'variant_id', 'variant_inventory_management', 'vendor', 'estimated_ship_date', '_inventory_source', 'test', 'total_tax', 'financial_status', 'cancelled_at', 'order_discount_amount', 'country', 'state', 'city', 'zip_code', 'email', 'first_name', 'last_name', 'orders_count']
with open(work_path + 'tmp_data/line_items/' + csv_name + '.csv', 'w') as output_file:
	dict_writer = csv.DictWriter(output_file, keys)
	dict_writer.writeheader()
	for line_items in line_items_list:	    
		dict_writer.writerow(dict(id = line_items['id'], 
								  order_id = line_items['order_id'], 
								  created_at = line_items['created_at'], 
								  created_at_est = pytz.utc.localize(line_items['created_at']).astimezone(pytz.timezone('America/New_York')), 
								  _sku = line_items.get('_sku') if line_items.get('_sku') else None,
								  name = line_items['name'], 
								  price = line_items['price'], 
								  quantity = line_items['quantity'], 
								  title = line_items['title'], 
								  variant_title = line_items['variant_title'], 
								  total_discount = line_items['total_discount'], 
								  fulfillable_quantity = line_items['fulfillable_quantity'], 
								  fulfillment_status = line_items['fulfillment_status'], 
								  gift_card = line_items['gift_card'], 
								  grams = line_items['grams'], 								 								 								 								  
								  product_exists = line_items['product_exists'], 
								  product_id = line_items['product_id'], 
								  _inventory = line_items.get('_inventory') if line_items.get('_inventory') else None,
								  requires_shipping = line_items['requires_shipping'], 
								  sku = line_items['sku'], 
								  tax_lines = line_items['tax_lines'], 
								  taxable = line_items['taxable'], 								  
								  variant_id = line_items['variant_id'], 
								  variant_inventory_management = line_items['variant_inventory_management'], 								  
								  vendor = line_items['vendor'],
								  estimated_ship_date = line_items.get('estimated ship date') if line_items.get('estimated ship date') else None,
								  _inventory_source = line_items.get('_inventory_source') if line_items.get('_inventory_source') else None,
								  test = line_items['test'],
								  total_tax = line_items['total_tax'],
								  financial_status = line_items['financial_status'],
								  cancelled_at = line_items['cancelled_at'],
								  order_discount_amount = line_items['order_discount_amount'],
								  country = line_items['country'],
								  state = line_items['state'],
								  city = line_items['city'],
								  zip_code = line_items['zip_code'],
								  email = line_items['email'],
								  first_name = line_items['first_name'],
								  last_name = line_items['last_name'],
								  orders_count = line_items['orders_count'])) 

# send csv to s3
s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
df = pd.read_csv(work_path + '/tmp_data/line_items/' + csv_name + '.csv')
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
s3_resource = boto3.resource('s3')
s3_resource.Object('lito-misc', 'data/line_items/' + csv_name + '.csv').put(Body=csv_buffer.getvalue())


# line_items_yesterday_file_path_s3 = 's3://lito-misc/data/line_items/line_items_' + yesterday + '_' + today + '.csv'
# 's3://lito-misc/data/line_items/line_items_20171101_20180101.csv'
copy lito.line_items from 's3://lito-misc/data/line_items/line_items_20170101_20180101.csv'
credentials 'aws_iam_role=arn:aws:iam::803205066366:role/myRedshiftRole' 
delimiter ',' 
region 'us-east-1'
IGNOREHEADER 1
CSV QUOTE AS '"'
-- removequotes 
emptyasnull 
blanksasnull
;