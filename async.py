# import asyncio

# async def async_foo():
#     for i in range(5):
#         print("async_foo started")
#     await asyncio.sleep(5)
#     for i in range(5):
#         print("async_foo done")

# async def main():
#     asyncio.ensure_future(async_foo())  # fire and forget async_foo()
#     print('Do SOME ACTION 1')
#     await asyncio.sleep(5)
#     print('Do SOME ACTION 2')
# asyncio.run(main())
# loop = asyncio.get_event_loop()
# loop.run_until_complete(main())
# values = open('ouput.json', encoding="utf8").read()

# values = values.replace('\n',"")
# v = values.encode('utf-8')
# v= json.loads(v)
    
# from concurrent.futures import ThreadPoolExecutor, as_completed
# import json
# with open('ouput.json', encoding="utf8") as infile:
#   string = infile.read()
#   o = json.loads(string)
#   print(len(o))
#   chunkSize = 20
#   print(type(o))
#   for i in range(0, len(o), chunkSize):
#     with open('file_' + str(i//chunkSize) + '.json', 'w') as outfile:
# #       json.dump(o[i:i+chunkSize], outfile)
import json
def split(a,n):
    k,m = divmod(len(a),n)
    return list((a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)))

def load(file):
    with open(file,'r', encoding="utf8") as fp:
        string = fp.read()
        o = json.loads(string)
        line_chunk = split(o,10)
        print(type(line_chunk))
        
        for data in line_chunk:
            print(data)
            print(data[0]['id'])
            print("||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
        
load('MOCK_DATA_2.json')