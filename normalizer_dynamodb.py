#!/usr/bin/python3
import json,argparse,logging,sys,os
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
import general_storage,utils,query,general_storage_mysql
from progress.bar import Bar
from normalizer import Normalizer

class Normalizer_comment_mysql_dynomodb(Normalizer):
    ## Normalizer class for comment from mysql to dynamodb
    name="comments"
    ## source(input) of normalization
    source={}
    ## target(output) of normalization
    target={}
    target_source_rule={'id':lambda x: get_id(x.source),
                        'tags':'tags',
                        'normalized_tags':'normalized_tags',
                        'events':'events'
             }

def get_id(item):
    ## make dynamodb id
    return "%s_%s" %(item['platform'],item['object_id'])

def insert_mysql_items_into_dynamodb(cf,items):
    ## Main function to call normalizer to normalize object from mysql processed object(json) to dynamodb format, and then insert normalized item to dynamodb
    normalized_item=[]
    nl = Normalizer_comment_mysql_dynomodb()
    for i in items:
        ## Normalize item from mysql json format to dynamodb format
        nl.normalize_source_to_target(cf,utils.fix_data(i))
        normalized_item.append(nl.target)

    ## Update normalized items to dynamodb
    table=general_storage.dynamodb.Table(cf.table_name)
    general_storage.update_items(table, normalized_item)
