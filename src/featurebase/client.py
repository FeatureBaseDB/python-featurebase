import json
import concurrent.futures
import urllib.request
import urllib.error
from collections import UserList

# convenience wrapper for list of results
class results(UserList):
    """results is just a handy wrapper for a list of results that caches a
    list of errors and an overall status so you don't have to write the
    list-iterating code every time if you just want to know whether things
    worked. it's basically just a normal list, but has two additional
    properties:

    ok -- did every request in this batch succeed (result.ok = True)
    errors -- None or a list of errors encountered"""

    def __init__(self, items):
        super().__init__(items)
        self.ok = true
        errors = []
        for item in items:
            if not item.ok:
                self.ok = false
            if item.error is not None:
                errors.append(item.error)
        self.errors = errors or None

# client represents a http connection to the FeatureBase sql endpoint.
class client:
    """Client represents a http connection to the FeatureBase sql endpoint.

    Keyword arguments:
    hostport -- hostname and port number of your featurebase instance, it should be passed in 'host:port' format (default localhost:10101)
    database -- database id of your featurebase cloud instance(default None)
    apikey -- api key (default None)
    cafile -- Fully qualified certificate file path (default None)
    capath -- Fully qualified certificate folder (default None)
    origin -- request origin, should be one of the allowed origins defined for your featurebase instance (default None)
    timeout -- seconds to wait before timing out on server connection attempts"""

    # client constructor initializes the client with key attributes needed to
    # make connection to the sql endpoint
    def __init__(
        self,
        hostport=None,
        database=None,
        apikey=None,
        cafile=None,
        capath=None,
        origin=None,
        timeout=None,
    ):
        if hostport is None:
            if apikey is not None:
                raise ValueError("when specifying API key, hostport is not optional")
            self.hostport = "localhost:10101"
        else:
            self.hostport = hostport
        if apikey is not None and apikey == "":
            raise ValueError("API key, if set, must not be empty string")
        if database is not None and database == "":
            raise ValueError("database ID, if set, must not be empty string")
        self.database = database
        self.apikey = apikey
        self.cafile = cafile
        self.capath = capath
        self.timeout = timeout
        self.origin = origin

    # private helper to create a new request/session object intialized with tls
    # attributes if any provided adds header entries as expected by the sql
    # endpoint
    def _newrequest(self):
        request = urllib.request.Request(self._geturl(), method="POST")
        if self.origin != None:
            request.origin_req_host = self.origin
        return self._addheaders(request)

    # private helper adds header entries to a request
    def _addheaders(self, request):
        request.add_header("Content-Type", "text/plain")
        request.add_header("Accept", "application/json")
        if self.apikey != None:
            request.add_header("X-API-Key", self.apikey)
        return request

    # private helper to build url for the request it determines http or https
    # default url points to sql endpoint, database is added to the path if
    # provided optionally it can point to other paths.
    def _geturl(self, path=None):
        scheme = "http"
        if self.cafile != None or self.capath != None or self.apikey != None:
            scheme = "https"
        if path == None:
            if self.database != None:
                path = "/databases/" + self.database + "/query/sql"
            else:
                path = "/sql"
        return scheme + "://" + self.hostport + path

    # helper method executes the http post request and returns a callable future
    def _post(self, sql):
        data = bytes(sql, "utf-8")
        # use context manager to ensure connection is promptly closed and released
        try:
            with urllib.request.urlopen(
                self._newrequest(),
                data=data,
                timeout=self.timeout,
                cafile=self.cafile,
                capath=self.capath,
            ) as conn:
                response = conn.read()
                return result(sql=sql, response=response, code=conn.code)
        except Exception as err:
            err.add_note(sql)
            return result(sql=sql, response="", code=500, err=err)

    # helper method executes the http post request and returns a callable future and handles exception
    def _postforasync(self, sql):
        try:
            response = self._post(sql)
        except Exception as err:
            err.add_note(sql)
            return result(sql=sql, response="", code=500, err=err)
        return response

    # helper method accepts a list of sql queries and executes them
    # asynchronously and returns the results as a list
    def _batchasync(self, sqllist):
        res = results()
        # use context manger to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Start the query execution and mark each future with its sql
            future_to_sql = {
                executor.submit(self._postforasync, sql): sql for sql in sqllist
            }
            for future in concurrent.futures.as_completed(future_to_sql, self.timeout):
                res.append(future.result())
        return res

    # public method accepts a sql query creates a new request object pointing to
    # sql endpoint attaches the sql query as payload and posts the request
    # returns a simple result object providing access to data, status and
    # warnings.
    def query(self, sql):
        """Executes a SQL query and returns a result object.

        Keyword arguments:
        sql -- the SQL query to be executed"""
        return self._post(sql)

    # public method accepts a list of sql queries and executes them
    # synchronously or asynchronously and returns the results as a list
    # if raiseexceptions is true, and exceptions are encountered, they
    # will be raised (after the batch is complete for an asynchronous
    # batch, upon hitting one for synchronous), otherwise they'll be
    # present in results.
    def querybatch(self, sqllist, asynchronous=False, stoponerror=False, raiseexceptions=False):
        """Executes a list of SQLs and returns a list of result objects.

        Keyword arguments:
        sqllist -- the list of SQL queries to be executed
        asynchronous -- a flag to indicate the SQLs should be run concurrently (default False)
        stoponerror -- a flag to indicate what to do when a SQL error happens. Passing True will stop executing remaining SQLs in the input list after the errored SQL item. This parameter is ignored when asynchronous=True (default False)"""
        if asynchronous:
            return results(self._batchasync(sqllist))
        else:
            res = []
            for sql in sqllist:
                result = self._post(sql)
                res.append(result)
                # during synchronous execution if a query fails and stoponerror is
                # true then stop executing remaining queries
                if not result.ok and stoponerror:
                    break
            return results(res)

# simple data object representing query result returned by the sql endpoint for
# successful requests, data returned by the service will be populated in the
# data, schema attributes along with any warnings. on a failure or exception,
# ok will be false and error will indicate the problem.
class result:
    """Result is a simple data object representing results of a SQL query.

    Keyword arguments:
    ok -- boolean indicating query execution status
    schema -- field definitions for the result data
    data -- data rows returned by the server
    error -- SQL error information or exception encountered during execution
    warnings -- warning information returned by the server
    execution_time -- amount of time (microseconds) it took for the server to execute the SQL
    rows_affected -- number of rows affected by the SQL statement
    exec -- exception captured during execution
    raw_response -- original request response
    """

    def __init__(self, sql, response, code, err=None):
        self.ok = False
        self.schema = None
        self.data = None
        self.error = None
        self.warnings = None
        self.execution_time = 0
        self.sql = sql
        # if we got an error/exception, the error wins over the code.
        if err is None:
            self.ok = code == 200
        self.rows_affected = 0
        self.raw_response = response
        if self.ok:
            try:
                result = json.loads(response)
                if "error" in result.keys():
                    self.ok = False
                    self.error = result["error"]
                else:
                    self.schema = result.get("schema")
                    self.data = result.get("data")
                    self.warnings = result.get("warnings")
                    self.execution_time = result.get("execution-time")
                    self.rows_affected = result.get("rows-affected")
            except json.JSONDecodeError as err:
                self.ok = False
                self.error = err
        else:
            self.error = err
