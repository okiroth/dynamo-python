import boto3
import os
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import simplejson as json
import threading
import time
import numpy as np


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def float_to_decimal(x):
    return Decimal(str(x)) if isinstance(x, float) else x


class UploadThread (threading.Thread):
    def __init__(self, items, table):
        threading.Thread.__init__(self)
        self.items = items
        self.table = table
    def run(self):
        print('[dynamo thread] upload items:', len(self.items))
        with self.table.batch_writer() as batch:
            for item in self.items:
                _item = {k: float_to_decimal(item[k]) for k in item}
                batch.put_item(Item=_item)



class DynamoDB:

    def __init__(self, config):
        self.name = config['name']
        self.region = config['region']


    def clean_list(self, items):
        return [
            {k: float_to_decimal(item[k]) for k in item}
            for item in items
        ]


    def get_table(self):
        db = boto3.resource('dynamodb', self.region)
        return db.Table(self.name)


    def get_item(self, primary_key, sort_key):
        result = self.get_table().get_item(Key={
            'primary_key': primary_key, 
            'sort_key': sort_key
        }).get('Item', {})
        return json.loads(json.dumps(result))


    def put_item(self, item):
        _item = {k: float_to_decimal(item[k]) for k in item}
        return self.get_table().put_item(Item=_item)


    def wait_upload(self):
        while(threading.active_count() > 1):
            print('[dynamo] active uploads threads:', threading.active_count())
            time.sleep(10)


    def put_df(self, df):
        for split in chunks(df, 80000):
            items = json.loads(split.to_json(orient='records', date_format='iso'))
            UploadThread(items, self.get_table()).start()
        self.wait_upload()


    def put_items(self, items):
        for split in chunks(items, 80000):
            UploadThread(split, self.get_table()).start()
        self.wait_upload()
        

    def delete_item(self, primary_key, sort_key):
        return self.get_table().delete_item(Key={
            'primary_key': primary_key,
            'sort_key': sort_key
        })
    

    def delete_items(self, items):
        with table.batch_writer() as batch:
            for each in items:
                batch.delete_item(
                    Key={
                        'primary_key': each['primary_key'],
                        'sort_key': each['sort_key']
                    }
                )


    def get_items(self, primary_key, sort_key=None, filters=None, limit=None):
        key1 = Key('primary_key').eq(primary_key)
        key2 = Key('sort_key').eq(sort_key)
        _q = {
            'KeyConditionExpression': key1,
            'ScanIndexForward': False
        }
        if sort_key is not None:
            _q['KeyConditionExpression'] = key1 & key2
        if filters is not None:
            _q['FilterExpression'] = filters
        if limit is not None:
            _q['Limit'] = limit
        return self.run_query(_q)
    

        

    def get_items_between(self, primary_key, _from, _to, filters=None):
        key1 = Key('primary_key').eq(primary_key)
        key2 = Key('sort_key').between(str(_from), str(_to))
        _q = {
            'KeyConditionExpression': key1 & key2,
            'ScanIndexForward': False
        }
        if filters is not None:
            _q['FilterExpression'] = filters
        return self.run_query(_q)


    def run_query(self, _q):
        results = []
        response = self.get_table().query(**_q)
        results += response['Items']
        if len(response['Items']) < _q.get('Limit', float('inf')):
            while 'LastEvaluatedKey' in response:
                _q['ExclusiveStartKey'] = response['LastEvaluatedKey']
                response = self.get_table().query(**_q)
                results += response['Items']
        return json.loads(json.dumps(results))

