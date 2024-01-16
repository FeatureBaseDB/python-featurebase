import json
import concurrent.futures
import ssl
import urllib.request
import urllib.error


# client represents a http connection to the FeatureBase sql endpoint.
# the hostport parameter must be present when using an api key. the
# database parameter is optional, but if set must be a valid string.
# assumes http by default, but switches to https if certificate config
# is provided or an API key is present.
class client:
    """Client represents a http connection to the FeatureBase sql endpoint.

    Keyword arguments:
    hostport -- hostname and port number of your featurebase instance, it should be passed in 'host:port' format (default localhost:10101)
    database -- database id of your featurebase cloud instance (default None)
    apikey -- api key (default None) -- applicable only when specifying a host/port
    cafile -- Fully qualified certificate file path (default None)
    capath -- Fully qualified certificate folder (default None)
    origin -- request origin, should be one of the allowed origins defined for your featurebase instance (default None)
    timeout -- seconds to wait before timing out on server connection attempts

    When specifying API key, you should specify a host and port, and the
    client will expect HTTPS."""

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
        self.hostport = hostport
        self.database = database
        self.apikey = apikey
        self.timeout = timeout
        self.origin = origin
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
        scheme = "http"
        if cafile or capath or apikey:
            scheme = "https"
            # force https
            self.sslContext = ssl.create_default_context(cafile=cafile, capath=capath)
        else:
            self.sslContext = None
        path = "/sql"
        if self.database:
            path = "/databases/{}/query/sql".format(self.database)
        self.url = "{}://{}{}".format(scheme, self.hostport, path)

    # private helper to create a new request/session object intialized with tls
    # attributes if any provided adds header entries as expected by the sql
    # endpoint
    def _newrequest(self):
        request = urllib.request.Request(self.url, method="POST")
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

    # helper method executes the http post request and returns a callable future
    def _post(self, sql):
        data = bytes(sql, "utf-8")
        # use context manager to ensure connection is promptly closed and released
        with urllib.request.urlopen(
            self._newrequest(),
            data=data,
            timeout=self.timeout,
            context=self.sslContext,
        ) as conn:
            response = conn.read()
        return result(sql=sql, response=response, code=conn.code)

    # helper method accepts a list of sql queries and executes them
    # asynchronously and returns the results as a list
    def _batchasync(self, sqllist):
        results = []
        exceptions = []
        # use context manger to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Start the query execution and mark each future with its sql
            future_to_sql = {executor.submit(self._post, sql): sql for sql in sqllist}
            for future in concurrent.futures.as_completed(future_to_sql, self.timeout):
                try:
                    results.append(future.result())
                except Exception as e:
                    exceptions.append(e)
        if exceptions:
            raise ExceptionGroup("batch exception(s):", exceptions)
        return results

    # public method accepts a sql query creates a new request object pointing to
    # sql endpoint attaches the sql query as payload and posts the request
    # returns a simple result object providing access to data, status and
    # warnings. if the server returns an error, it will be raised as an exception.
    def query(self, sql):
        """Executes a SQL query and returns a result object.

        Keyword arguments:
        sql -- the SQL query to be executed"""
        return self._post(sql)

    # public method accepts a list of sql queries and executes them
    # synchronously or asynchronously and returns the results as a list
    # asynchronously, it runs all queries. if one or more queries hits
    # an exception, it raises an ExceptionGroup of the exceptions, otherwise
    # it returns a list of results.
    def querybatch(self, sqllist, asynchronous=False):
        """Executes a list of SQLs and returns a list of result objects.

        Keyword arguments:
        sqllist -- the list of SQL queries to be executed
        asynchronous -- a flag to indicate the SQLs should be run concurrently (default False)"""
        results = []
        if asynchronous:
            results = self._batchasync(sqllist)
        else:
            for sql in sqllist:
                results.append(self._post(sql))
        return results


# simple data object representing query result returned by the sql endpoint for
# successful requests, data returned by the service will be populated in the
# data, schema attributes along with any warnings. only successful requests
# generate results, server and communication errors are raised as exceptions.
class result:
    """Result is a simple data object representing results of a SQL query.

    Keyword arguments:
    sql -- the SQL which was executed
    schema -- field definitions for the result data
    data -- data rows returned by the server
    warnings -- warning information returned by the server
    execution_time -- amount of time (microseconds) it took for the server to execute the SQL
    rows_affected -- number of rows affected by the SQL statement
    raw_response -- original request response
    """

    def __init__(self, sql, response, code):
        self.sql = sql
        if code != 200:
            # HTTP error of some kind.
            raise RuntimeError("HTTP response code %d" % code)
        self.raw_response = response
        result = json.loads(response)
        if "error" in result:
            raise RuntimeError(result["error"])
        self.schema = result.get("schema")
        self.data = result.get("data")
        self.warnings = result.get("warnings", None)
        self.execution_time = result.get("execution-time", 0)
        self.rows_affected = result.get("rows-affected", 0)
