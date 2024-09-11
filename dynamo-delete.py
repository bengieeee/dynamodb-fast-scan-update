#!/usr/bin/env python

import boto3, threading
from boto3.dynamodb.conditions import Attr

# You'll need to update these variables below
aws_profile_name='prod'

aws_dynamodb_table_name='prod-member-details'
threads_count=20

dynamo_item_attribute_name = "firstname"
dynamo_item_attribute_values_to_match = [
    'ben',
    'jeremy',
    'raygun',
    'penelope',
    'etc...',
]

partition_key='memberId'
sort_key='createdTimestampUuid'

boto3.setup_default_session(profile_name=aws_profile_name)
table = boto3.resource('dynamodb').Table(aws_dynamodb_table_name)

def remove_items(thread_number, item_list):
    for item in item_list:
        response = table.delete_item(
            Key={
                partition_key: item[partition_key],
                sort_key: item[sort_key]
            }
        )
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            print(f"Thread {thread_number}: Delete didn't seem successful: {response}")
            continue
    if len(item_list) > 0:
        print(f"Thread {thread_number}: {len(item_list)} items removed")

def scan_and_delete(thread_number):
    response = table.scan(TotalSegments=threads_count, Segment=thread_number, FilterExpression=Attr(dynamo_item_attribute_name).is_in(dynamo_item_attribute_values_to_match))
    remove_items(thread_number, response['Items'])
    # While there's more keys to evaluate (scan through in pagination...)
    while 'LastEvaluatedKey' in response:
        print(f"Thread {thread_number}: Scanning DynamoDB with last_dynamo_key: {response['LastEvaluatedKey']}")
        response = table.scan(TotalSegments=threads_count, Segment=thread_number, ExclusiveStartKey=response['LastEvaluatedKey'], FilterExpression=Attr(dynamo_item_attribute_name).is_in(dynamo_item_attribute_values_to_match))
        remove_items(thread_number, response['Items'])

threads = []

for i in range(threads_count):
    thread = threading.Thread(target=scan_and_delete, args=(i,))
    thread.start()
    threads.append(thread)

# wait for all threads to complete before main program exits
for thread in threads:
    thread.join()
