import string
import random
import featurebase
import time

# intialize featurebase client for community or cloud featurebase server
client = featurebase.client(hostport="localhost:10101") #community
#client = client(hostport="query.featurebase.com/v2", database="", apikey="") #cloud


# generate random data
def get_random_string(length: int):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

# build a BULK INSERT sql and execute it using featurebase client 
def upload_data_bulk(key_from: int, key_to: int):
    # build bulk insert sql
    insertClause="BULK INSERT INTO demo_upload(_id, keycol, val1, val2) MAP (0 ID, 1 INT, 2 STRING, 3 STRING) FROM x"
    withClause=" WITH INPUT 'INLINE' FORMAT 'CSV' BATCHSIZE " + str((key_to-key_from)+1)
    records=""
    for i in range(key_from, key_to):
        val1 = get_random_string(3)
        val2 = get_random_string(12)
        if records!="":
            records+='\n'
        records+='%i, %i, "%s", "%s"'%(i, i, val1, val2)
    bulkInsertSql=insertClause + "'" + records + "'" + withClause
    stime=time.time()
    result=client.query(sql=bulkInsertSql)
    etime=time.time()
    if result.ok:
        print("inserted " + str(result.rows_affected) + " rows in " + str(etime-stime) + " seconds")
    else:
        print(result.error.description)
    return result.ok

# create a demo table and load million rows
def run(batchSize: int):
    # create demo table 
    result=client.query(sql="CREATE TABLE demo_upload(_id ID, keycol INT, val1 STRING, val2 STRING)")
    if not result.ok:
        print(result.error.description)
    # insert batchSize rows per insert for 1000 times
    n=int(1000000/batchSize)
    l=1
    h=batchSize
    for i in range(1, n):
        if not upload_data_bulk(l, h):
            break
        l=h+1
        h+=batchSize
        if h>1000000:
            h=1000000


run(10000)