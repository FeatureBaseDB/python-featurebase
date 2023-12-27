import string
import random
import featurebase
import time

# intialize featurebase client for community or cloud featurebase server
# local server running community
client = featurebase.client(hostport="localhost:10101")
# cloud server, using database and API key.
# client = featurebase.client(hostport="query.featurebase.com/v2", database="", apikey="") #cloud


# generate random data
def get_random_string(length: int):
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str


# build a BULK INSERT sql and execute it using featurebase client
def upload_data_bulk(key_from: int, count: int):
    # build bulk insert sql
    insert_clause = "BULK INSERT INTO demo_upload(_id, keycol, val1, val2) MAP (0 ID, 1 INT, 2 STRING, 3 STRING) FROM x"
    with_clause = " WITH INPUT 'INLINE' FORMAT 'CSV' BATCHSIZE " + str((count) + 1)
    records = ""
    for i in range(key_from, key_from + count):
        val1 = get_random_string(3)
        val2 = get_random_string(12)
        if records != "":
            records += "\n"
        records += '%i, %i, "%s", "%s"' % (i, i, val1, val2)
    bulk_insert_sql = insert_clause + "'" + records + "'" + with_clause
    stime = time.time()
    result = client.query(sql=bulk_insert_sql)
    etime = time.time()
    if result.ok:
        print(
            "inserted "
            + str(result.rows_affected)
            + " rows in "
            + str(etime - stime)
            + " seconds"
        )
    else:
        print(result.error)
    return result.ok


# create a demo table and load million rows
def run(batch_size: int):
    # create demo table
    result = client.query(sql="DROP TABLE IF EXISTS demo_upload")
    if not result.ok:
        print(result.error)
    result = client.query(
        sql="CREATE TABLE demo_upload(_id ID, keycol INT, val1 STRING, val2 STRING)"
    )
    if not result.ok:
        print(result.error)
    # insert batch_size rows per insert
    # (will not upload the full million if batch_size does not evenly divide 1M)
    n = int(1000000 / batch_size)
    l = 1
    for i in range(n):
        if not upload_data_bulk(l, batch_size):
            break
        l += batch_size


run(10000)
