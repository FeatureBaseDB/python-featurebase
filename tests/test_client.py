import json
import os
import unittest
import calendar
import time
from featurebase import client, result

client_hostport = os.getenv("FEATUREBASE_HOSTPORT", "localhost:10101")


class FeaturebaseClientTestCase(unittest.TestCase):
    # test client for default attributes
    def testDefaultClient(self):
        default_client = client()
        self.assertEqual(default_client.hostport, "localhost:10101")
        self.assertEqual(default_client.database, None)
        self.assertEqual(default_client.apikey, None)
        self.assertEqual(default_client.origin, None)
        self.assertEqual(default_client.capath, None)
        self.assertEqual(default_client.cafile, None)

    # test URL generation schemes
    def testURL(self):
        # default URL
        test_client = client(hostport="featurebase.com:2020")
        self.assertEqual(test_client._geturl(), "http://featurebase.com:2020/sql")
        # URL for context database
        test_client = client(hostport="featurebase.com:2020", database="db-1")
        self.assertEqual(
            test_client._geturl(),
            "http://featurebase.com:2020/databases/db-1/query/sql",
        )
        # https when CA attributes are defined
        test_client = client(
            hostport="featurebase.com:2020", database="db-1", capath="./pem/"
        )
        self.assertEqual(
            test_client._geturl(),
            "https://featurebase.com:2020/databases/db-1/query/sql",
        )
        test_client = client(
            hostport="featurebase.com:2020", database="db-1", cafile="./pem"
        )
        self.assertEqual(
            test_client._geturl(),
            "https://featurebase.com:2020/databases/db-1/query/sql",
        )
        # url for custom path
        self.assertEqual(
            test_client._geturl("/test"), "https://featurebase.com:2020/test"
        )

    # test request for method, origin and headers
    def testRequest(self):
        test_client = client(
            hostport="featurebase.com:2020", origin="gitlab.com", apikey="testapikey"
        )
        request = test_client._newrequest()
        self.assertEqual(request.full_url, "https://featurebase.com:2020/sql")
        # method must be POST
        self.assertEqual(request.method, "POST")
        # request origin must match origin supplied to the client
        self.assertEqual(request.origin_req_host, "gitlab.com")
        # headers should have specific entries including the api key supplied to the client
        expectedheader = {
            "Content-type": "text/plain",
            "Accept": "application/json",
            "X-api-key": "testapikey",
        }
        self.assertDictEqual(expectedheader, request.headers)

    # test client for post error scenarios
    def testPostExceptions(self):
        # domain exists but no /sql path defined
        result = None
        exec = None
        test_client = client(hostport="featurebase.com:2020", timeout=5)
        try:
            result = test_client._post("This is test data, has no meaning when posted.")
        except Exception as ex:
            exec = ex
        self.assertIsNotNone(exec)
        self.assertIsNone(result)
        # unknown domain
        result = None
        exec = None
        test_client = client(hostport="notarealhost.com", timeout=5)
        try:
            result = test_client._post("This is test data, has no meaning when posted.")
        except Exception as ex:
            exec = ex
        self.assertIsNotNone(exec)
        self.assertIsNone(result)
        # bad CA attributes
        result = None
        exec = None
        test_client = client(timeout=5, cafile="/nonexistingfile.pem")
        try:
            result = test_client._post("This is test data, has no meaning when posted.")
        except Exception as ex:
            exec = ex
        self.assertIsNotNone(exec)
        self.assertIsNone(result)


# test result data construction based on http response data
class FeaturebaseResultTestCase(unittest.TestCase):
    # test general HTTP failure
    def testGeneralFailure(self):
        with self.assertRaises(RuntimeError):
            res = result(
                sql="test sql",
                response="test raw response",
                code=500,
            )

    # test response with a bad JSON that fails to deserialize
    def testJSONParseFailure(self):
        with self.assertRaises(json.JSONDecodeError):
            res = result(sql="test sql", response="{'broken':{}", code=200)

    # test response with SQL error
    def testSQLError(self):
        with self.assertRaises(RuntimeError):
            resp = b'{"schema":{},"data":{}, "warnings":{}, "execution-time":10,"error":"test sql error"}'
            res = result(sql="test sql", response=resp, code=200)

    # test successful response
    def testSuccess(self):
        kv = {"k1": "v1"}
        res = result(
            sql="test sql",
            response=b'{"schema":{"k1":"v1"},"data":{"k1":"v1"}, "warnings":{"k1":"v1"}, "execution-time":10}',
            code=200,
        )
        self.assertEqual(res.sql, "test sql")
        self.assertDictEqual(res.schema, kv)
        self.assertDictEqual(res.data, kv)
        self.assertDictEqual(res.warnings, kv)
        self.assertEqual(res.execution_time, 10)


# test query interface
class FeaturebaseQueryTestCase(unittest.TestCase):
    # test SQL for error
    def testQueryError(self):
        test_client = client(client_hostport)
        with self.assertRaises(RuntimeError):
            result = test_client.query(
                "select non_existing_column from non_existing_table;"
            )

    # test SQL for success
    def testQuerySuccess(self):
        test_client = client(client_hostport)
        result = test_client.query("select toTimeStamp(0);")
        self.assertEqual(result.data[0][0], "1970-01-01T00:00:00Z")


# test query batch interface
class FeaturebaseQueryBatchTestCase(unittest.TestCase):
    # test SQL batch synchronous
    def testQueryBatchSync(self):
        test_client = client(client_hostport)
        # create a table and insert rows and query the rows before dropping the table.
        # all these SQLs to succeed they need to be run in a specific order
        # so they are run synchronously
        tablename = "pclt_" + str(calendar.timegm(time.gmtime()))
        sqllist = [
            "select * from {};",
            "create table {} (_id id, i1 int, s1 string) ;",
            "insert into {}(_id,i1,s1) values(1,1,'text1');",
            "insert into {}(_id,i1,s1) values(2,2,'text2');",
            "select count(*) from {};",
            "drop table {};",
        ]
        sqllist = [sql.format(tablename) for sql in sqllist]
        # if you try to run the full list, you should get an exception
        with self.assertRaises(RuntimeError):
            results = test_client.querybatch(sqllist)
        # if you skip the first one, you should get five back
        results = test_client.querybatch(sqllist[1:])
        self.assertEqual(len(results), 5)

    # test SQL batch Asynchronous
    def testQueryBatchAsync(self):
        # create 2 test tables and insert some rows
        # this need to be run synchronously because tables
        # should be created before inserts can be run
        sqllist = [
            "create table if not exists pclt_test_t1(_id id, i1 int, s1 string);",
            "create table if not exists pclt_test_t2(_id id, i1 int, s1 string);",
            "insert into pclt_test_t1(_id, i1, s1) values(1,1,'text1');",
            "insert into pclt_test_t1(_id, i1, s1) values(2,2,'text2');",
            "insert into pclt_test_t1(_id, i1, s1) values(3,3,'text3');",
            "insert into pclt_test_t1(_id, i1, s1) values(4,4,'text4');",
            "insert into pclt_test_t2(_id, i1, s1) values(1,1,'text1');",
            "insert into pclt_test_t2(_id, i1, s1) values(2,2,'text2');",
        ]

        test_client = client(client_hostport)
        results = test_client.querybatch(sqllist, asynchronous=False)

        self.assertEqual(len(results), 8)

        # run some select queries on the test tables
        # these queries will be run asynchronously
        sqlexpecting = {
            "select * from pclt_test_t1;": lambda x: len(x.data) == 4,
            "select * from pclt_test_t2;": lambda x: len(x.data) == 2,
            "select count(*) from pclt_test_t1;": lambda x: x.data[0][0] == 4,
            "select count(*) from pclt_test_t2;": lambda x: x.data[0][0] == 2,
        }
        sqllist = sqlexpecting.keys()

        results = test_client.querybatch(sqllist, asynchronous=True)
        self.assertEqual(len(results), 4)
        for result in results:
            self.assertEqual(sqlexpecting[result.sql](result), True)

        bad_client = client(hostport="bad-address")
        results = None
        exec = None
        try:
            results = bad_client.querybatch(sqllist, asynchronous=True)
        except Exception as ex:
            exec = ex
        self.assertIsNotNone(exec)
        self.assertIsNone(results)
        # cleanup by droping the test tables
        sqllist = [
            "drop table pclt_test_t1;",
            "drop table pclt_test_t2;",
        ]

        results = test_client.querybatch(sqllist, asynchronous=True)
        self.assertEqual(len(results), 2)


if __name__ == "__main__":
    unittest.main()
