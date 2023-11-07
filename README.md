# Retrieving Data From Disparate Endpoints
## MSSQL to Databricks with Databricks Secrets

Here's a quick solve for a real-world scenario where I needed to connect MSSQL to Databricks to perform some data transformations and push to a separate database.
Any privelidged or otherwise sensitive information is replaced with general terms.

## Our goals in this project are:
 1. Retrieve our credentials from Databricks's native dbutils secrets functionality.
 2. Estalish a database connection to our Microsoft Services SQL Server.
 3. Perform some simple data operations and push to our historical database.

First, install the pymssql package and import. ([documentation](http://www.pymssql.org/))

    import pandas as pd
    import numpy as np
    import base64
    import pymssql
    import requests

> [!NOTE]
> There's always  **_!pip install pymssql_**

### Retrieve Credentials From Databricks Scope Secrets

Databricks Scope Secrets are a native Databricks utility that allows users to store sensitive variables (encoded certificates, passwords, usernames) in a secure environment. Here, we will use secrets to store our sensitive personal access information. Check [this]() notebook for basics on uploading Databricks Secrets.

Define the scope in which you saved your connection credentials and assign credentials to objects.

    secrets_scope = 'secrets_XXX_XXX'
    usr = 'XXX\\' _ dbutils.secrets.get(secrets_scope, 'username')
    pwd = dbutils.secrets.get(secrets_scope, 'password')

### Establish Server or API Connections

Define server information.

    server = 'ec2-1-23-4-567.us-generic-east-1.compute.xxx.xx.xxx'
    database = 'DATABASE_NAME'
    instance = 'COMPUTEINSTANCE'
    port = '123'

Write SQL queries. Here we write two queries. One query to retrieve field names from the table we are connecting to, and one query to retrieve the current day's data.

    q_fieldname = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Table_Name'"
    q_data = "SELECT [col1], [col2], [col3] FROM database.table"

Connect to the database with pymssql using server, port, instance, user, password, and database objects assigned previously. In this case, our credentials are obfuscated with base64 encoding prior to storage in Databricks Secrets, so we use _**base64.b64decode**_ to decode our information when assigning to our 'password' object.

    conn = pymssql.connect(host = f'{server}:{port}\\{instance}',
        user = usr,
        password = base64.b64decode(pwd).decode('utf-8'),
        database=database)
    c = conn.cursor()

Here is an alternate example of retrieving a json using a requests API. This endpoint requires that we retrieve an authentication cookie before performing our data call, necessitating a preliminary 'get' request.
 
    url = 'https://123.gov/services/rest-api/gis-tool/123'
    login_url = 'https://123.gov/portal/rest/login'

    def pull_gis(url, login_url):
        payload = {'f':'json',
                        'parameter1':'false',
                        'where':'clause',
                        'fields':'fields'}
        r_login = requests.get(login_url,
                        user = usr,
                        password = base64.b64decode(pwd))
        token = r_login.history[3].cookies.items()[0][1]
        token_dict = {'token':token}
        r = requests.get(url+'/query',
                        params={**payload, **token_dict},
                        user = usr,
                        password = base64.b64decode(pwd))
        return pd.json_normalize(r.json()['features']).reset_index(drop=True)

Execute each query, assign objects, then close the server connection.

    c.execute(q_fieldname)
    fields = c.fetchall()

    c.execute(q_data)
    data = c.fetchall()

    c.close()
    conn.close()

### Transform Data

Having a list of field names directly from the source table helps prevent data cleanliness issues if the table format changes server-side. Here, we remove spaces from each field name and create a pandas dataframe to perform any desired operations.

    fieldnames = [fields[i][3].replace(' ','_') for i in range(len(fields))]
    df_today = pd.DataFrame(result, columns = fieldnames)

An example of some operations we could do. Numpy is super useful for efficient manipulation of Pandas dataframes.

    df_today['col2'] = np.where(df_today['col2'] == df_today['col1'], None, df_today['col2'])
    
    col_list = ['col1', 'col2', 'col3']
    for i in col_list:
        df_today[i] = df_today[i].apply(lambda x: None if x < 100 else x)

Finally, we write our transformed data to a new table in Databricks. We use write.mode('append') because this script adds the current day's data to a historical database.

    try:
        spark_df = spark.createDataFrame(df_today, verifySchema=False)
        spark_df.write.mode('append').saveAsTable("database.tablename")
    except Exception as e:
        print(e)

### Summary

The core methodology behind this script is to ingest data from an MSSQL server or API endpoint, perform desired operations against data formatted as pandas dataframes, and output said data to one or more endpoints. In the real-world scenario, I performed many more data manipulation operations and finalized the project by scheduling a task in a Databricks workflow. This project was formatted with attention towards scalability. Following this project outline, we can add new endpoints at the beginning of our pipeline, add operations to the ingested data, and increase the number of output sources iteratively.
