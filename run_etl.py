# for backfill, run this script with params like this: python run_etl.py '2017-01-01' '2017-12-31'

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

# work_path = '/home/ec2-user/src/data_warehouse/'
work_path = '/Users/davidbendet/Work/coding/src/data_warehouse/'

start_time = datetime.now() #timing script 

redshift_user = os.environ['REDSHIFT_USER']
redshift_db = os.environ['REDSHIFT_DB']
redshift_host = os.environ['REDSHIFT_HOST']

# getting argument if they are passed in for backfill or specific dates, otherwise, get yesterday's data
if len(sys.argv) == 3:
	# yesterday = sys.argv[1]
	yesterday_date = sys.argv[1]
	today_date = sys.argv[2] 
	today_date = datetime.strftime(datetime.strptime(today_date, '%Y-%m-%d') + timedelta(1), '%Y-%m-%d')
	today = today_date.replace('-','')
	yesterday = yesterday_date.replace('-','')
else: 
	# yesterday = str(datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')) # e.g. '20180519' 
	yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d') # e.g. '2018-05-19'
	today_date = datetime.strftime(datetime.strptime(yesterday_date, '%Y-%m-%d') + timedelta(1), '%Y-%m-%d') # e.g. '2018-05-20'	
	today = today_date.replace('-','')
	yesterday = yesterday_date.replace('-','')

print('running a backfill script...\n')
print('number of arguments passed: ' + str(len(sys.argv)))
print('\narguments passed: ' + str(sys.argv))
print('\nusing:\n')
print(yesterday + ' for yesterday\n')
print(yesterday_date + ' for yesterday_date\n')
print(today_date + ' for today_date\n')

# last_entry_date = os.system('psql -h {0} -U {1} -d {2} -p 5439 -ec "select created_at::date from lito.orders order by 1 desc limit 1"'.format(redshift_host, redshift_user, redshift_db))


#################################################################################################################################################
# populate orders table
#################################################################################################################################################
orders_yesterday_file_path_local = work_path + 'table_scripts/orders/orders_table_' + yesterday + '_' + today + '.sql'

orders_yesterday_file_path_s3 = 's3://lito-misc/data/orders/orders_' + yesterday + '_' + today + '.csv'

f = open(orders_yesterday_file_path_local,'w')

f.write(""" 
copy lito.orders from \'%s\' 
credentials 'aws_iam_role=arn:aws:iam::803205066366:role/myRedshiftRole' 
delimiter ',' 
region 'us-east-1'
IGNOREHEADER 1
CSV QUOTE AS '"'
-- removequotes 
emptyasnull 
blanksasnull
; 
""" % (orders_yesterday_file_path_s3))

f.close() 

print('created local file:' + orders_yesterday_file_path_local)
print('\ncreated s3 file:' + orders_yesterday_file_path_s3)
#################################################################################################################################################
# populate line items table
#################################################################################################################################################
line_items_yesterday_file_path_local = work_path + 'table_scripts/line_items/line_items_table_' + yesterday + '_' + today + '.sql'

line_items_yesterday_file_path_s3 = 's3://lito-misc/data/line_items/line_items_' + yesterday + '_' + today + '.csv'

f = open(line_items_yesterday_file_path_local,'w')

f.write(""" 
copy lito.line_items from \'%s\' 
credentials 'aws_iam_role=arn:aws:iam::803205066366:role/myRedshiftRole' 
delimiter ',' 
region 'us-east-1'
IGNOREHEADER 1
removequotes 
emptyasnull 
blanksasnull
; 
""" % (line_items_yesterday_file_path_s3))

f.close() 

print('\ncreated local file: ' + str(line_items_yesterday_file_path_local))
print('\ncreated s3 file: ' + str(line_items_yesterday_file_path_s3))

#################################################################################################################################################
# run scripts to get new data in s3 and then redshift 
#################################################################################################################################################

populate_s3_files = ['get_orders_from_api.py', 'get_line_items_from_api.py']

for file in populate_s3_files:
	this_file = work_path + 'data_retrieval_scripts/' + file 
	arguments = yesterday_date + ' ' + today_date
	os.system('python' + ' ' + this_file + ' ' + arguments)

print('\nscripts run: ' + str(populate_s3_files))

# if that works, run table to go from s3 to redshift
populate_redshift_files = [orders_yesterday_file_path_local, line_items_yesterday_file_path_local]

for file in populate_redshift_files:
	this_file = file 
	os.system('psql -h {0} -U {1} -d {2} -p 5439 -f "{3}"'.format(redshift_host, redshift_user, redshift_db, this_file))

print('\nredshift population attempted (but not confirmed)')
print('\nscript run time: ' + str(datetime.now() - start_time))

# run some other check to make sure table is populated
