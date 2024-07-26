# MSSQL to Databricks with Databricks Secrets

This project is based on a few real-world scenarios where I needed to connect MSSQL Server to Databricks to perform some data transformations and push to a separate database.

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

Databricks Scope Secrets are a native Databricks utility that allows users to store sensitive variables (encoded certificates, passwords, usernames) in a secure environment. Here, we will use secrets to store our sensitive personal access information.

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

Here we write two queries. One query to retrieve field names from the table we are connecting to, and one query to retrieve the table's data.

    q_fieldname = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Table_Name'"
    q_data = "SELECT [col1], [col2], [col3] FROM database.table"

Connect to the database with pymssql using server, database, instance, port, user and password objects assigned previously. In this case, our credentials are obfuscated with base64 encoding prior to storage in Databricks Secrets, so we use _**base64.b64decode**_ to decode our information when assigning our 'password' object.

    conn = pymssql.connect(host = f'{server}:{port}\\{instance}',
        user = usr,
        password = base64.b64decode(pwd).decode('utf-8'),
        database=database)
    c = conn.cursor()

Here is an alternate example of retrieving a json using requests API. This endpoint requires an authentication cookie before performing our data call, necessitating a preliminary 'get' request to retrieve said cookie :cookie:
 
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

Here, we remove spaces from each field name and create a pandas dataframe to perform any desired operations. Assigning column names to the list of field names directly from the source table prevents code failure or cleanliness issues if the table format changes server-side.

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

The core methodology behind this script is to ingest data from an MSSQL server or API endpoint, perform operations against the data using pandas dataframes, and return transformed data to one or more outputs. In the real-world scenarios this code is based on, I performed many more data manipulation operations and finalized the project by scheduling tasks in a Databricks workflow. This project was formatted with attention towards scalability. We can add new endpoints at the beginning of our pipeline, operations to the ingested data, or increase the number of output sources.
