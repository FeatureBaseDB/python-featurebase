# python-featurebase

Python client for Featurebase SQL endpoint.

# Client Library Usage:

First install the python-featuebase package. Running `make` from project folder
will build and install the package. After installing the package you can try
executing queries as shown in the following examples.

    import featurebase

    # assuming featurebase runs at "localhost:10101"
    # for cloud, pass hostport="query.featurebase.com/v2", database="<database_id>", apikey="<APIKey_secret>"
    # create client
    client = featurebase.client()

    # query the endpoint with SQL
    result = client.query("SELECT * from demo;")
    if result.ok: 
        print(result.data)

    # query the endpoint with a batch of SQLs, running the SQLs synchronously
    # Synchronous run best suited for executing DDL and DMLs that need to follow specific run order
    # passing the optional parameter "stoponerror=True" will stop execution at the failed SQL and the remaining SQLs in the list will not be executed. 
        sqllist=[]
        sqllist.append("CREATE TABLE demo1(_id id, i1 int);")
        sqllist.append("INSERT INTO demo1(_id, i1) VALUES(1, 100);")
        sqllist.append("INSERT INTO demo1(_id, i1) VALUES(2, 200);")
        sqllist.append("select * from demo1;")
        results = client.querybatch(sqllist, stoponerror=True)
        for result in results:
            if result.ok: 
                print(result.data)
            
    # query the endpoint with a batch of SQLs, running the SQLs Asynchronously
    # Asynchronous run best suited for running SELECT queries that can be run concurrently.
        sqllist=[]
        sqllist.append("SELECT * from demo1;")
        sqllist.append("SELECT count(*) from demo1;")
        sqllist.append("SELECT max(i1) from demo1;")
        results = client.querybatch(sqllist, asynchronous=True)
        for result in results:
            if result.ok: 
                print(result.data)
