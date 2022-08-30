import json
import boto3
import sys
# from DBConnection import DBConnection
from json_transformer import flatten
connection_details = ['PVD_JADER_MART_TEST_3','rxlogix','10.100.22.99',1521,'pvsdevdb']
s3 = boto3.client('s3')
# line_stream = codecs.getreader("utf-8")
def include_keys(dic, keys):
    key_set = set(keys) & set(dic.keys())
    return {key: dic[key] for key in key_set}
def flatten_list(d):
    try:
        key, lst = next((k, v) for k, v in d.items() if isinstance(v, list))
    except (StopIteration, AttributeError):
        return [flatten(d)]
    return [flatten({**d, **{key: v}}) for record in lst for v in flatten_list(record)]
def generate_inserts(table, data):
    jsons = flatten_list(data)
    statements = []
    for i in jsons:
        cols = i.keys()
        values = i.values()
        ins = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s']*len(cols))})"
        statements.append((ins,tuple(values)))
    return statements
def return_all_inserts(dictionary):
    generic_keys = ['tenantId','caseId','versionNum']
    inserts = {}
    for i in dictionary:
        if i not in generic_keys:
            t1 = include_keys(dictionary, generic_keys+[i])
            for j in t1:
                if isinstance(t1[j],dict):
                    if 'ArrayElem' in t1[j].keys():
                        t1[j] = t1[j]['ArrayElem']
            inserts[i] = generate_inserts(i,t1)
    return inserts
def lambda_handler(event, context):
    # TODO implement
    print(event)
    queue_records = event['Records']
    res = []
    for payload in queue_records:
        body = json.loads(payload['body'])
        content = json.loads(body['message']['Records'][0]['body'])['Records'][0]
        bucket = content['s3']['bucket']['name']
        key = content['s3']['object']['key']
        try:
            data = s3.get_object(Bucket=bucket, Key=key)
            # json_data = json.loads(data['Body'].read().decode('utf-8'))
            for line in data['Body'].read().decode('utf-8').splitlines():
                res.append(json.loads(line))
        except Exception as e:
            print(f"Failed to read : {e}")
    print(return_all_inserts(json.loads(res[0])))