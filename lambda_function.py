#!/usr/bin/python3
import argparse

import base64
import datetime
import json
import logging
import os
import sys
import time
import traceback
import urllib
import urllib.parse 
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import get_credentials
from botocore.endpoint import BotocoreHTTPSession
from botocore.session import Session
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Attr
import normalizer_dynamodb, general_config, general_storage

print("Updating DynamoDB")
DEBUG=True
logger = logging.getLogger()
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)

cf_dict={}

def get_cf(attr,val):
   ## get configuration based on attr and val
   if val in cf_dict:
      return cf_dict[val]
   else:
      items,LastEvaluatedKey = general_storage.scan_items(general_storage.get_dynamodb_table('client_configuration'),Attr(attr).eq(val))
      cf = general_config.create_configuration(items[0])
      cf_dict[val]=cf
      return cf


def _lambda_handler(event, context):
   logger.info('Event:'+json.dumps(event))
   records = event['data']
   now = datetime.datetime.utcnow()
   for r in records:   
      ## Notes: now the batch contains comments for different pages or clients, better to send batch with same page and clients for optimizing DB updates
      cf=get_cf('user_id',r['page_id'])
      ## Normalize RDS to DynamoDB and update DynamoDB
      normalizer_dynamodb.insert_mysql_items_into_dynamodb(cf,[r])           
         

# Global lambda handler - catches all exceptions to avoid dead letter in the DynamoDB Stream
def lambda_handler(event, context):
   try:
      return _lambda_handler(event, context)
   except Exception:
      logger.error(traceback.format_exc())

if __name__ == "__main__":
   with open('event-test-tw.json', 'r') as myfile:
      event = json.loads(myfile.read())
      lambda_handler(event, "")
      
