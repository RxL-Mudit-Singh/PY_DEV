from timeit import default_timer as dt
import time
import json
from textwrap import indent
from flatten_json import flatten
from datetime import datetime

def include_keys(dic, keys):
    key_set = set(keys) & set(dic.keys())
    return {key: dic[key] for key in key_set}
def pre_proc(data):
        print(type(data))
        for k,v in data.items():
            if isinstance(v,dict):
                if 'ArrayElem' in v.keys():
                # for k1,v1 in v.items():
                    data[k]=v['ArrayElem']
        return data
def flatten_list(d):
    try:
        key, lst = next((k, v) for k, v in d.items() if isinstance(v, list))
    except (StopIteration, AttributeError):
        return [flatten(d,'.')]
    return [flatten({**d, **{key: v}},'.') for record in lst for v in flatten_list(record)]

def generate_inserts(table, data):
    jsons = flatten_list(data)
    statements = []
    ins = jsons[0]
    ins = f"INSERT INTO {table} ({','.join(ins.keys())}) VALUES ({','.join(['%s']*len(ins.keys()))})"
    val_array = []
    for i in jsons:
        cols = i.keys()
        values = i.values()
        # ins = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s']*len(cols))})"
        val_array.append(tuple(values))
    statements.append(ins)
    statements.append(val_array)
    return statements

def return_all_inserts(dictionary)->dict:
    generic_keys = ['TENANT_ID','CASE_ID','VERSION_NUM']
    inserts = {}
    dictionary2 = pre_proc(dictionary)
    for i in dictionary2:
        if i not in generic_keys:
            t1 = include_keys(dictionary, generic_keys+[i])
            for j in t1:
                if isinstance(t1[j],dict):
                    if 'ArrayElem' in t1[j].keys():
                        t1[j] = t1[j]['ArrayElem']
            inserts[i] = tuple(generate_inserts(i,t1))
    return inserts


with open('jsons/msg1.json','r',encoding='utf-8') as X:
    start = dt()
    string = X.read()
    D = json.loads(string)
    print(type(D))
    # print(return_all_inserts(D))
    with open('read.json','w') as T:
        g = json.dumps(return_all_inserts(D),indent=1)
        T.write(g)
    end = dt()
    print(end-start)