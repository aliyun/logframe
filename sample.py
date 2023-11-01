#!/usr/bin/env python
from dataframe import LogClient
import time
accessId  = ""
accessKey = ""
endpoint = 'cn-hangzhou.log.aliyuncs.com'
project = 'ali-cn-xxx'
logstore = 'http'
endTime = (int)(time.time())
beginTime = endTime- 1

sls = LogClient(endpoint = endpoint, accessKeyId = accessId, accessKey= accessKey)
res = sls.get(project=project,logstore=logstore,fromTime=beginTime,toTime=endTime,query='* | select count(1) as pv, (response_code) as response_code group by response_code')
print (res)
res = sls.get(project=project,logstore=logstore,fromTime=beginTime,toTime=endTime,query='* | insert into tmp_table(b,a) select count(1) as pv, cast(response_code as varchar) as response_code group by response_code')
print (res)
res = sls.get(project=project,logstore=logstore,fromTime=beginTime,toTime=endTime,query='response_code:200',keys=['client_address','method','uri','response_code'],fullData=True,timeoutSec=5)
print (res)
print (res.describe())
