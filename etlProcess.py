#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import psycopg2
from sqlalchemy import create_engine
### 2. 데이터 불러오기

indata =     pd.read_csv("../dataset/kopo_decision_tree_all_new.csv")


indata.shape

### 3. 데이터 처리 (컬럼 소문자로 변환)

indata.columns = indata.columns.str.lower()


### 4. 데이터 저정하기

targetDbIp = "192.168.110.111"
targetDbPort = "5432"
targetDbId = "kopo"
targetDbPw = "kopo"
targetDbName = "kopodb"

targetDbPrefix = "postgresql://"

targetUrl = "{}{}:{}@{}:{}/{}".format(targetDbPrefix,
                                      targetDbId,
                                      targetDbPw,
                                      targetDbIp,
                                      targetDbPort,
                                     targetDbName)

pg_kopo_engine = create_engine(targetUrl)



tableName = "pgout_kopo_sc"


# In[ ]:


try:
    indata.to_sql(name=tableName, 
             con = pg_kopo_engine,
             if_exists="replace", index=False)
    print("{} unload 성공!".format(tableName))
except Exception as e:
    print(e)


# In[ ]:


import d6tstack


# In[ ]:


targetDbPrefix =  "postgresql+psycopg2://"

targetUrl = "{}{}:{}@{}:{}/{}".format(targetDbPrefix,
                                     targetDbId,
                                     targetDbPw,
                                     targetDbIp,
                                     targetDbPort,
                                     targetDbName)

pg_kopo_engine = create_engine(targetUrl)

tableName = "pg_result_sc"



d6tstack.utils.pd_to_psql(df=indata, uri=targetUrl, table_name=tableName,if_exists="replace")

