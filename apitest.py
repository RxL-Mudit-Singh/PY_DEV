from timeit import default_timer as dt
import time
import json
from textwrap import indent
from flatten_json import flatten
from datetime import datetime
from functools import reduce
import operator

def include_keys(dic, keys):
    key_set = set(keys) & set(dic.keys())
    return {key: dic[key] for key in key_set}   

def pre_proc(data):
    blank_key_data = []
    for k,v in data.items():
        if isinstance(v,dict):
            if 'ArrayElem' in v.keys():
                data[k]=v['ArrayElem']
            if isinstance(data[k],list):
                if len(data[k][0]) == 0:
                    blank_key_data.append(k)
    for key_to_be_deleted in blank_key_data:
        del data[key_to_be_deleted]
    return data

def flatten_list(d):
    try:
        key, lst = next((k, v) for k, v in d.items() if isinstance(v, list))
    except (StopIteration, AttributeError):
        
        return [flatten(d,'.')]
    return [flatten({**d, **{key: v}},'.') for record in lst for v in flatten_list(record)]


def generate_inserts(table, data):
    jsons = flatten_list(data)
    # print(jsons)
    statements = []
    for i in jsons:
        cols = i.keys()
        values = i.values()
        ins = f"INSERT INTO {table} ({','.join(cols)}) VALUES {tuple(values)};".replace('None','null')
        statements.append((ins))
    return statements


def return_all_inserts(dictionary):
    generic_keys = ['TENANT_ID','CASE_ID','VERSION_NUM']
    inserts = {}
    lst = []
    dictionary2 = pre_proc(dictionary)
    for i in dictionary2:
        if i not in generic_keys:
            t1 = include_keys(dictionary, generic_keys+[i])
            for j in t1:
                if isinstance(t1[j],dict):
                    if 'ArrayElem' in t1[j].keys():
                        t1[j] = t1[j]['ArrayElem']
            # inserts[i] = generate_inserts(i,t1)
            lst.append(generate_inserts(i,t1))
    return reduce(operator.iconcat,lst,[])
    


with open('jsons/msg2.json','r',encoding='utf-8') as X:
    start = dt()
    string = X.read()
    D = json.loads(string)
    print(type(D))
    # print(return_all_inserts(D))
    with open('read.json','w') as T:
        json_inserts = return_all_inserts(D)
        g = json.dumps('\n'.join(["BEGIN"] + json_inserts + ["END;"]),indent=1)
        T.write(g)
    end = dt()
    print(end-start)