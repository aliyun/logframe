#!/usr/bin/env python
from dataframe import LogClient
import time
accessId  = ""
accessKey = ""
endpoint = 'cn-hangzhou.log.aliyuncs.com'
project = 'ali-cn-xxx'
endTime = (int)(time.time())
beginTime = endTime- 600

sls = LogClient(endpoint = endpoint, accessKeyId = accessId, accessKey= accessKey)
res = sls.get(project=project,logstore='presto_http',fromTime=beginTime,toTime=endTime,query='* | select count(1) as pv, (response_code) as response_code group by response_code')
print (res)
res = sls.get(project=project,logstore='presto_http',fromTime=beginTime,toTime=endTime,query='* | insert into tmp_table(b,a) select count(1) as pv, cast(response_code as varchar) as response_code group by response_code')
print (res)
