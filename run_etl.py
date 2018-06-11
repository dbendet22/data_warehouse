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

redshift_user = os.environ['REDSHIFT_USER']
redshift_db = os.environ['REDSHIFT_DB']
redshift_host = os.environ['REDSHIFT_HOST']

# last_entry_date = os.system('psql -h {0} -U {1} -d {2} -p 5439 -ec "select created_at::date from lito.orders order by 1 desc limit 1"'.format(redshift_host, redshift_user, redshift_db))
yesterday = str(datetime.strftime(datetime.now() - timedelta(5), '%Y%m%d')) # '20180519' 
yesterday_date = datetime.strftime(datetime.now() - timedelta(5), '%Y-%m-%d') # '2018-05-19'
today_date = datetime.strftime(datetime.strptime(yesterday_date, '%Y-%m-%d') + timedelta(1), '%Y-%m-%d') # '2018-05-20'

#################################################################################################################################################
# populate orders table
#################################################################################################################################################
orders_yesterday_file_path_local = '/Users/davidbendet/Work/coding/src/data_warehouse/table_scripts/orders/orders_table_' + yesterday + '.sql'

orders_yesterday_file_path_s3 = 's3://lito-misc/data/orders/orders_' + yesterday + '.csv'

f = open(orders_yesterday_file_path_local,'w+')

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
#################################################################################################################################################

# run scripts to get data in s3 
populate_s3_files = ['get_orders_from_api.py']

for file in populate_s3_files:
	this_file = '/Users/davidbendet/work/coding/src/data_warehouse/data_retrieval_scripts/' + file 
	arguments = yesterday + ' ' + yesterday_date + ' ' + today_date
	os.system('python' + ' ' + this_file + ' ' + arguments)

# if that works, run table to go from s3 to redshift
populate_redshift_files = [orders_yesterday_file_path_local]

for file in populate_redshift_files:
	this_file = file 
	os.system('psql -h {0} -U {1} -d {2} -p 5439 -f "{3}"'.format(redshift_host, redshift_user, redshift_db, this_file))

# run some other check to make sure table is populated
