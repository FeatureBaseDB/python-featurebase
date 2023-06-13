import json
import concurrent.futures
import urllib.request
import urllib.error

# apply schema to list of dicts
def apply_schema(list_of_lists, schema):
    # build field names
    field_names = []

    # add them from the schema
    for field in schema.get('fields'):
        field_names.append(field.get('name'))

    # build the dicts
    result = []
    for row in list_of_lists:
        dict_row = {}
        for i, val in enumerate(row):
            dict_row[field_names[i]] = val
        result.append(dict_row)
    return result


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
    def __init__(self, hostport='localhost:10101', database=None, apikey=None, cafile=None, capath=None, origin=None, timeout=None):
        self.hostport=hostport
        self.database=database
        self.apikey=apikey
        self.cafile=cafile
        self.capath=capath
        self.timeout=timeout
        self.origin=origin

    # private helper to create a new request/session object intialized with tls
    # attributes if any provided adds header entries as expected by the sql
    # endpoint
    def _newrequest(self):
        request=urllib.request.Request(self._geturl(),method='POST')
        if self.origin!=None:
            request.origin_req_host=self.origin
        return self._addheaders(request)

    # private helper adds header entries to a request
    def _addheaders(self, request):
        request.add_header("Content-Type","text/plain")
        request.add_header("Accept","application/json")
        if self.apikey!=None:
            request.add_header("X-API-Key",self.apikey)
        return request

    # private helper to build url for the request it determines http or https
    # default url points to sql endpoint, database is added to the path if
    # provided optionally it can point to other paths.
    def _geturl(self, path=None):
        scheme='http'
        if self.cafile!=None or self.capath!=None or self.apikey!=None:
            scheme='https'
        if path==None:
            if self.database != None:
                path="/databases/" + self.database+ "/query/sql"
            else:
                path="/sql"           
        return scheme + "://" + self.hostport + path
    
    # helper method executes the http post request and returns a callable future 
    def _post(self, sql):
        data = bytes(sql, 'utf-8')
        try:
            # use context manager to ensure connection is promptly closed and released 
            with urllib.request.urlopen(self._newrequest(), data=data, timeout=self.timeout, cafile=self.cafile, capath=self.capath) as conn:
                response=conn.read()
        # handle exceptions in a decreasing specificity order 
        except urllib.error.HTTPError as exc:
            return result(sql=sql, response={}, code=exc.code, reason=exc.reason)
        except urllib.error.URLError as exc:
            return result(sql=sql, response={}, code=500, reason=str(exc.reason))        
        except Exception as exc:
            return result(sql=sql, response={}, code=500, reason=str(exc))                
        else:
            return result(sql=sql, response=response, code=conn.code, reason=conn.reason)

    # helper method accepts a list of sql queries and executes them
    # asynchronously and returns the results as a list
    def _batchasync(self, sqllist):
        results=[]        
        # use context manger to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Start the query execution and mark each future with its sql
            future_to_sql = {executor.submit(self._post, sql): sql for sql in sqllist}
            for future in concurrent.futures.as_completed(future_to_sql, self.timeout):
                sql = future_to_sql[future]
                try:
                    result =future.result()
                except Exception as exc:
                    results.append(result(sql=sql,response=None,code=500, reason=str(exc)))
                else:
                    results.append(result)
        return results

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
    def querybatch(self, sqllist, asynchronous=False, stoponerror=False):
        """Executes a list of SQLs and returns a list of result objects.

        Keyword arguments: 
        sqllist -- the list of SQL queries to be executed
        asynchronous -- a flag to indicate the SQLs should be run concurrently (default False) 
        stoponerror -- a flag to indicate what to do when a SQL error happens. Passing True will stop executing remaining SQLs in the input list after the errored SQL item. This parameter is ignored when asynchronous=True (default False)"""        
        results =[]
        if asynchronous:
            results=self._batchasync(sqllist)            
        else:
            for sql in sqllist:
                result=self._post(sql)
                results.append(result)
                # during synchronous execution if a query fails and stoponerror is
                # true then stop executing remaining queries  
                if not result.ok and stoponerror:
                    break
        return results


# simple data object representing query result returned by the sql endpoint for
# successful requests, data returned by the service will be populated in the
# data, schema attributes along with any warnings for failed requests, error
# info will be populated in the error attribute
class result:
    """Result is a simple data object representing results of a SQL query.

    Keyword arguments: 
    ok -- boolean indicating query execution status
    schema -- field definitions for the result data
    data -- data rows returned by the server
    error -- error information with a code and description
    warnings -- warning information returned by the server
    execution_time -- amount of time (microseconds) it took for the server to execute the SQL"""
    def __init__(self, sql, response, code, reason):
        self.ok=False
        self.schema=None
        self.data=None
        self.dict=None
        self.error=None
        self.warnings=None
        self.execution_time=0
        self.sql=sql
        self.ok=code==200 

        if self.ok:
            try:
                result=json.loads(response)
                if 'error' in result.keys():
                    self.ok=False
                    self.error=error(500, 'SQL error. ' + result['error'])
                else:
                    self.schema=result.get('schema')
                    self.data=result.get('data')
                    self.warnings=result.get('warnings')
                    self.execution_time=result.get('execution-time')

                    # Apply schema to the data
                    if self.schema and self.data:
                        self.dict = apply_schema(self.data, self.schema)

            except json.JSONDecodeError as exc:
                self.ok=False
                self.error=error(500, 'JSON error. ' + str(response))
        else:
            self.error=error(code, 'HTTP error. ' + reason)

# simple data object representing request error details
class error:
    def __init__(self, code, description):
        self.code=code
        self.description=description
