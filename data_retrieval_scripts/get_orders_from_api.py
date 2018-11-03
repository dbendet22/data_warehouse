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

work_path = '/home/ec2-user/src/data_warehouse/'
# work_path = '/Users/davidbendet/Work/coding/src/data_warehouse/'

# last day of data (or backfill)
# yesterday = sys.argv[1]
yesterday_date = sys.argv[1]
today_date = sys.argv[2] 
yesterday = yesterday_date.replace('-','')
today = today_date.replace('-','')
csv_name = 'orders_' + yesterday + '_' + today

# # if i only wanted to backfill this table specifically, i could do this: 
# # yesterday = '20180710'
# yesterday_date = '2018-07-10'
# today_date = '2018-10-26'
# today_date = datetime.strftime(datetime.strptime(today_date, '%Y-%m-%d') + timedelta(1), '%Y-%m-%d')
# yesterday = yesterday_date.replace('-','')
# today = today_date.replace('-','')
# csv_name = 'orders_' + yesterday + '_' + today
  
def get_api_login():
    admin_url = os.environ['RETAIL_ADMIN_URL']
    api_key = os.environ['RETAIL_API_KEY']
    api_password = os.environ['RETAIL_API_PASSWORD']     
    return admin_url, api_key, api_password

# credentials 
admin_url, api_key, api_password = get_api_login()

# get required page count 
count = json.loads(requests.get(admin_url + 'orders/count.json?status=any&created_at_min=' + yesterday_date + 'EST' + '&created_at_max=' + today_date + 'EST', 
				   auth=(api_key, api_password)).content)['count']

page_size = 250
pages = math.ceil(count / 250.0)

# initialize variables
current_page = 1
orders = []

# for all of the pages with records, add each record to the list and move to the next page
while current_page <= pages:

  r = requests.get(admin_url + 'orders.json?limit=250&page=' + str(current_page) + '&status=any&created_at_min=' + yesterday_date + 'EST' + '&created_at_max=' + today_date + 'EST', 
  				   auth=(api_key, api_password))

  x = json.loads(r.content.decode("utf-8"))
  orders.append(x) 
  current_page += 1


# test_list = []

# for page in orders:
# 	for thing in page['orders']:
# 		test_list.append(thing['id'])

# test_list.sort() 
# test_list



# open csv to write into
csv_data = open(work_path + 'tmp_data/orders/' + csv_name + '.csv', 'w')
f = csv.writer(csv_data)

# write header row once
f.writerow(['id', 
			'closed_at', 
			'created_at',
			'created_at_est', 
			'number', 
			'test', 
			'total_price', 
			'subtotal_price', 
			'total_weight', 
			'total_tax', 
			'taxes_included',
			'currency',
			'financial_status',
			'confirmed',
			'total_discounts',
			'total_line_items_price',
			'cancelled_at',
			'cancel_reason',
			'total_price_usd',
			'reference',
			'user_id',
			'location_id',
			'source_identifier',
			'source_url',
			'processed_at',
			'device_id',
			'customer_locale',
			'app_id',
			'browser_ip',
			'landing_site_ref',
			'order_number',
			'discount_code',
			'discount_amount',
			'discount_type',
			'payment_gateway_names',
			'processing_method',
			'checkout_id',
			'source_name',
			'fulfillment_status',
			'tags',
			'variant_id',
			'title',
			'quantity',
			'price',
			'sku',
			'variant_title',
			'vendor',
			'fulfillment_service',
			'product_id',
			'requires_shipping',
			'taxable',
			'gift_card',
			'name',
			'product_exists', 
			'fulfillable_quantity', 
			'customer_country',
			'customer_state',
			'customer_city',
			'customer_zip',
			'email', 
			'first_name', 
			'last_name', 
			'shipping_code',
			'orders_count',
			'user_agent'])

# write each row of data for each page into a csv 
for page in orders:
	for thing in page['orders']:
		f.writerow([thing['id'], 
					pd.to_datetime(thing['closed_at']), 
					pd.to_datetime(thing['created_at']), 
					pytz.utc.localize( pd.to_datetime(thing['created_at'])).astimezone(pytz.timezone('America/New_York')), 
					thing['number'], 
					thing['test'], 
					thing['total_price'],
					thing['subtotal_price'],
					thing['total_weight'],
					thing['total_tax'], 
					thing['taxes_included'],
					thing['currency'],
					thing['financial_status'],
					thing['confirmed'],
					thing['total_discounts'],
					thing['total_line_items_price'],
					pd.to_datetime(thing['cancelled_at']),
					thing['cancel_reason'],
					thing['total_price_usd'],
					thing['reference'],
					thing['user_id'],
					thing['location_id'],
					thing['source_identifier'],
					thing['source_url'],
					pd.to_datetime(thing['processed_at']),
					thing['device_id'],
					thing['customer_locale'],
					thing['app_id'],
					thing['browser_ip'],
					thing['landing_site_ref'],
					thing['order_number'],
					thing['discount_codes'][0]['code'] if thing['discount_codes'] else 0,
					thing['discount_codes'][0]['amount'] if thing['discount_codes'] else 0,
					thing['discount_codes'][0]['type'] if thing['discount_codes'] else 'Blank',
					thing['payment_gateway_names'],
					thing['processing_method'],
					thing['checkout_id'],
					thing['source_name'],
					thing['fulfillment_status'],
					thing['tags'],
					thing['line_items'][0]['variant_id'],
					thing['line_items'][0]['title'],
					thing['line_items'][0]['quantity'],
					thing['line_items'][0]['price'],
					thing['line_items'][0]['sku'],
					thing['line_items'][0]['variant_title'],
					thing['line_items'][0]['vendor'],
					thing['line_items'][0]['fulfillment_service'],
					thing['line_items'][0]['product_id'],
					thing['line_items'][0]['requires_shipping'],
					thing['line_items'][0]['taxable'],
					thing['line_items'][0]['gift_card'],
					thing['line_items'][0]['name'],
					thing['line_items'][0]['product_exists'],
					thing['line_items'][0]['fulfillable_quantity'],					
					thing.get('customer')['default_address']['country'] if thing.get('customer') else None,
					thing.get('customer')['default_address']['province'] if thing.get('customer') else None,
					thing.get('customer')['default_address']['city'] if thing.get('customer') else None,
					thing.get('customer')['default_address']['zip'] if thing.get('customer') else None,	
					thing.get('customer')['email'] if thing.get('customer') else None,
					thing.get('customer')['first_name'] if thing.get('customer') else None,
					thing.get('customer')['last_name'] if thing.get('customer') else None,
					thing.get('shipping_lines')[0]['code'] if thing.get('shipping_lines') else None,
					thing.get('customer')['orders_count'] if thing.get('customer') else None,
					thing.get('client_details')['user_agent'] if thing.get('client_details') else None])

# close csv file
csv_data.close()

# send csv to s3
s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
df = pd.read_csv(work_path + 'tmp_data/orders/' + csv_name + '.csv')
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
s3_resource = boto3.resource('s3')
s3_resource.Object('lito-misc', 'data/orders/' + csv_name + '.csv').put(Body=csv_buffer.getvalue())
