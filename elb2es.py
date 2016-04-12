#!/usr/bin/env python
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth
import boto3
import csv
from datetime import date, datetime

#parameters
es_host = 'elb-logs-abc.us-west-2.es.amazonaws.com'
access_key = 'abc'
secret_key = 'def'
index_name = 'elb_logs-%s'%(date.today().year)
doc_type = 'elb_access_logs'
region = 'us-west-2'

#field names for ELB
field_names = ['timestamp', 'elb_name', 'client_ip', 'backend_ip', 'request_processing_time', 'backend_processing_time', 'response_processing_time', 'elb_status_code', 'backend_status_code', 'received_bytes', 'sent_bytes', 'request', 'user_agent', 'ssl_cipher', 'ssl_protocol']

#authentication helper
awsauth = AWS4Auth(access_key, secret_key, region, 'es')

#build up ES client
es = Elasticsearch(
    hosts=[{'host': es_host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def get_s3_file(s3_bucket, s3_key):
    client = boto3.client('s3')
    log_file = client.get_object(Bucket=s3_bucket,Key=s3_key)
    return log_file['Body'].read()

#This function converts the log entry list into a dictionary
#This function also does manipulation against existing fields for further data
def covert_to_dict(log_entry):
    log_entry_dict = zip(field_names, log_entry)
    log_entry_dict = dict(log_entry_dict)
    #split ip and port combinations
    try:
        log_entry_dict['client_port'] = log_entry_dict['client_ip'].split(':')[1]
        log_entry_dict['client_ip'] = log_entry_dict['client_ip'].split(':')[0]
    except:
        log_entry_dict['client_port'] = None
        log_entry_dict['client_ip'] = None
    try:
        log_entry_dict['backend_port'] = log_entry_dict['backend_ip'].split(':')[1]
        log_entry_dict['backend_ip'] = log_entry_dict['backend_ip'].split(':')[0]
    except:
        log_entry_dict['backend_port'] = None
        log_entry_dict['backend_ip'] = None
    #add in the elb_node_ip
    log_entry_dict['elb_node_ip'] = elb_node_ip
    #get the date for the log entry timestamp
    log_entry_dict['timestamp'] = datetime.strptime(log_entry_dict['timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ")
    return log_entry_dict

def lambda_handler(event, context):
    s3_key = event['Records'][0]['s3']['object']['key']
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    print("processing file from %s://%s"%(s3_bucket,s3_key))
    #try and obtain ELB node IP
    global elb_node_ip
    try:
        elb_node_ip = s3_key.split('_')[5]
    except:
        elb_node_ip = None
        print("are we sure this is an ELB access log file? ELB log files typically have IP addresses")
    #begin by obtaining the ELB log file
    file_string = get_s3_file(s3_bucket, s3_key)
    file_string = csv.reader(file_string.splitlines(), delimiter=' ')
    #this is the list to hold documents for BULK uploading
    entries_to_push = []
    for log_entry in file_string:
        log_entry_dict = covert_to_dict(log_entry)
        elb_name = log_entry_dict['elb_name']
        entries_to_push.append({"_index": index_name, "_type": elb_name, "_source": log_entry_dict})
        if len(entries_to_push) > 1000:
            helpers.bulk(es, entries_to_push)
            entries_to_push = []
    if len(entries_to_push) > 0:
        helpers.bulk(es, entries_to_push)
    print("completed process")
