import pandas as pd
import numpy as np
import base64
import pymssql
import requests

#retrieve credentials from Databricks Secrets. 
secrets_scope = 'secrets_XXX_XXX'
usr = 'XXX\\' _ dbutils.secrets.get(secrets_scope, 'username')
pwd = dbutils.secrets.get(secrets_scope, 'password')

#establish server and database connection
server = 'ec2-1-23-4-567.us-generic-east-1.compute.xxx.xx.xxx'
database = 'DATABASE_NAME'
instance = 'COMPUTEINSTANCE'
port = '123'

#sql queries
q_fieldname = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Table_Name'"
q_data = "SELECT [col1], [col2], [col3] FROM database.table"

#connect to mssql server
conn = pymssql.connect(host = f'{server}:{port}\\{instance}',
    user = usr,
    password = base64.b64decode(pwd).decode('utf-8'),
    database=database)
c = conn.cursor()

#Alternate code for requests API call:
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

#retrieve field names
c.execute(q_fieldname)
fields = c.fetchall()

#retrieve data
c.execute(q_data)
data = c.fetchall()

c.close()
conn.close()

#format field names to an acceptable format for spark
fieldnames = [fields[i][3].replace(' ','_') for i in range(len(fields))]
df_today = pd.DataFrame(result, columns = fieldnames)

col_list = ['col1', 'col2', 'col3']
for i in col_list:
    df_today[i] = df_today[i].apply(lambda x: None if x < 100 else x)

df_today['col2'] = np.where(df_today['col2'] == df_today['col1'], None, df_today['col2'])


try:
    spark_df = spark.createDataFrame(df_today, verifySchema=False)
    spark_df.write.mode('append).saveAsTable("database.tablename")
except Exception as e:
    print(e)
