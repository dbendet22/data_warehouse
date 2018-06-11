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

def get_api_login():
    admin_url = os.environ['RETAIL_ADMIN_URL']
    api_key = os.environ['RETAIL_API_KEY']
    api_password = os.environ['RETAIL_API_PASSWORD'] 
    
    return admin_url, api_key, api_password

# credentials 
admin_url, api_key, api_password = get_api_login()

# get required page count 
count = json.loads(requests.get(admin_url + 'orders/count.json?status=any&created_at_min=2018-05-01T00:00:00-00:00&fields=id,line_items', 
				   auth=(api_key, api_password)).content)['count']
page_size = 250
pages = math.ceil(count / 250.0)

# initialize variables
current_page = 1
orders_w_line_items = []

# for all of the pages with records, add each record to the list and move to the next page
while current_page <= pages:
  r = requests.get(admin_url + 'orders.json?limit=250&page={0}&status=any&created_at_min=2018-05-01T00:00:00-00:00&fields=id,created_at,line_items'.format(current_page), 
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
		for line_item in order['line_items']:
			line_item = order['line_items'][0]
			line_item.update({'order_id': order_id})
			line_item.update({'created_at': created_at})
			# print(line_item)
			# print('------------------------------------------------------------------------------------------------')
			line_items_list.append(line_item)

# this cleverly takes properties out of the nested list of dictionaries and addes each as it's own line
for item in line_items_list:
	formatted_property_dict = {}
	[formatted_property_dict.update({thing['name'] : thing['value']}) for thing in item['properties']]
	for attribute in formatted_property_dict:
		item.update({attribute : formatted_property_dict[attribute]})

# output the list (which is now a list of dictionaries) to a csv
keys = ['id', 'order_id', 'created_at', '_sku', 'name', 'price', 'quantity', 'title', 'variant_title', 'total_discount', 'fulfillable_quantity', 'fulfillment_status', 'gift_card', 'grams', 'product_exists', 'product_id', '_inventory', 'requires_shipping', 'sku', 'tax_lines', 'taxable', 'variant_id', 'variant_inventory_management', 'vendor']
with open('/Users/davidbendet/Work/coding/src/data_warehouse/tmp_data/line_items.csv', 'w') as output_file:
	dict_writer = csv.DictWriter(output_file, keys)
	dict_writer.writeheader()
	for line_items in line_items_list:	    
		dict_writer.writerow(dict(id = line_items['id'], 
								  order_id = line_items['order_id'], 
								  created_at = line_items['created_at'], 
								  _sku = line_items['_sku'], 
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
								  _inventory = line_items['_inventory'], 
								  requires_shipping = line_items['requires_shipping'], 
								  sku = line_items['sku'], 
								  tax_lines = line_items['tax_lines'], 
								  taxable = line_items['taxable'], 								  
								  variant_id = line_items['variant_id'], 
								  variant_inventory_management = line_items['variant_inventory_management'], 								  
								  vendor = line_items['vendor']))

# send csv to s3
s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
df = pd.read_csv('~/work/coding/src/data_warehouse/tmp_data/line_items.csv')
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
s3_resource = boto3.resource('s3')
s3_resource.Object('lito-misc', 'data/line_items.csv').put(Body=csv_buffer.getvalue())
