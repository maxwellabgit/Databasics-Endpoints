# API Ingest
## MSSQL to Databricks with Databricks Secrets

Here's a quick solve for a real-world scenario where I needed to connect MSSQL to Databricks to perform some edits and push to a separate database.
Any privelidged or otherwise sensitive information is replaced with general terms

Our goals in this project are:

    <summary>Retrieve our credentials from Databricks's native dbutils secrets functionality</summary>
    <summary>Estalish a database connection to our Microsoft Services SQL Server</summary>
    <summary>Perform some simple operations on our single day of data and push to our historical database</summary>

Firstly, install the pymssql package and import. ([documentation](http://www.pymssql.org/))

    import pandas as pd
    import numpy as np
    import base64
    import pymssql

Note: There's always "!pip install pymssql"

Define the scope in which you saved your connection credentials and assign credentials to objects. Databricks Secrets are a native Databricks utility that allows users to store sensitive variables in an enccrypted environment. In our case, these credentials are obfuscated with a base64 encoding prior to storing in Databricks Secrets. Check [this] notebook for secrets upload review.

    secrets_scope = 'secrets_XXX_XXX'
    usr = 'XXX\\' _ dbutils.secrets.get(secrets_scope, 'username')
    pwd = dbutils.secrets.get(secrets_scope, 'password')

Define server information.

    server = 'ec2-1-23-4-567.us-generic-east-1.compute.xxx.xx.xxx'
    database = 'DATABASE_NAME'
    instance = 'COMPUTEINSTANCE'
    port = '123'

Write SQL queries. Here we write two queries. One query to retrieve field names from the table we are connecting to, and one query to retrieve the current day's data.

    q_fieldname = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Table_Name'"
    q_data = "SELECT [col1], [col2], [col3] FROM database.table"

Connect to the database with pymssql using server, port, instance, user, password, and database objects assigned previously.

    conn = pymssql.connect(host = f'{server}:{port}\\{instance}',
        user = usr,
        password = base64.b64decode(pwd).decode('utf-8'),
        database=database)
    c = conn.cursor()

Execute each query, assign objects, then close the server connection.

    c.execute(q_fieldname)
    fields = c.fetchall()

    c.execute(q_data)
    data = c.fetchall()

    c.close()
    conn.close()

Having a list of field names directly from the source table helps prevent data cleanliness issues if the table format changes server-side. Here, we remove spaces from each field name and create a pandas dataframe to perform any desired operations.

    fieldnames = [fields[i][3].replace(' ','_') for i in range(len(fields))]
    df_today = pd.DataFrame(result, columns = fieldnames)

An example of some operations we could do.

    col_list = ['col1', 'col2', 'col3']
    for i in col_list:
        df_today[i] = df_today[i].apply(lambda x: None if x < 100 else x)

    df_today['col2'] = np.where(df_today['col2'] == df_today['col1'], None, df_today['col2'])

Finally, we write our transformed data to a new table in Databricks.

    try:
        spark_df = spark.createDataFrame(df_today, verifySchema=False)
        spark_df.write.mode('append).saveAsTable("database.tablename")
    except Exception as e:
        print(e)

The real-world scenario this is based on ended with scheduling a task in a Databricks workflow.
