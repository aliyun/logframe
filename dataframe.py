#!/usr/bin/env python

import pandas as pd  # type: ignore
from copy import copy
from util import Util, parse_timestamp, base64_encodestring as b64e, is_stats_query
from util import base64_encodestring as e64, base64_decodestring as d64
from logexception import LogException
from datetime import datetime
import six
import json
import locale
import requests
import time
API_VERSION ='0.6.0'
class LogClient(object):
    @staticmethod
    def checkNotNone(key):
        if key is None:
            raise key+" shouldnot be None"
        return key

    def __init__ ( self, endpoint=None, accessKeyId = None, accessKey = None, securityToken = None):
        self._endpoint = self.checkNotNone(endpoint)
        self._accessKeyId = self.checkNotNone(accessKeyId)
        self._accessKey = self.checkNotNone(accessKey)
        self._securityToken = securityToken

    def get(self,project=None, logstore=None,fromTime=None, toTime=None,query='',keys=[], timeoutSec=60, fullData=False,offset=0,lines=100):
        if query.lower().find("select") != -1:
            return self.getSql(project,logstore,fromTime,toTime,query,keys,timeoutSec)
        else:
            return self.getLogs(project,logstore,fromTime,toTime,query,keys,timeoutSec,fullData,offset,lines)



    def getSql(self,project=None, logstore=None,fromTime=None, toTime=None,query='',keys=[], timeoutSec=60):
        return self.doExecute(project,logstore,fromTime,toTime,query,keys,0,100)


    def getFullLogs(self,project=None, logstore=None,fromTime=None, toTime=None,query='',keys=[], timeoutSec=60):
        offset = 0
        lines = 100
        dfList = []
        startExeTime = (int)(time.time())
        while True:
            tmpRes = self.getLogs(project,logstore,fromTime,toTime,query,keys,timeoutSec, False, offset, lines)
            if len(tmpRes) ==0:
                break
            dfList.append(tmpRes)
            offset += lines
            if (int)(time.time()) - startExeTime > timeoutSec:
                break
        return pd.concat(dfList)
    def getLogs(self,project=None, logstore=None,fromTime=None, toTime=None,query='',keys=[], timeoutSec=60, fullData=False,offset=0,lines=100):
        if fullData:
            return self.getFullLogs(project,logstore,fromTime,toTime,query,keys,timeoutSec)

        return self.doExecute(project,logstore,fromTime,toTime,query,keys,offset,lines)

    def doExecute(self,project,logstore,fromTime,toTime,query,keys,offset,lines):
        self.checkNotNone(project);
        self.checkNotNone(logstore);
        self.checkNotNone(fromTime);
        self.checkNotNone(toTime);
        self.checkNotNone(query);
        if offset is None:
            offset =0
        if lines is None:
            lines = 100
        headers = {"x-log-bodyrawsize": '0',
                "Content-Type": "application/json",
                "Accept-Encoding": ""}
        body = {
                "offset": offset,
                "line": lines,
                "type": "log",
                "project": project,
                "logstore": logstore,
                "from": fromTime,
            "to": toTime,
            "topic": '',
            "query": query,
            "reverse": False,
            "powerSql": True
            }
        resource = "/logstores/" + logstore + '/logs'
        body_str = six.b(json.dumps(body))

        (resp, resp_headers) = self._send("POST", project, body_str, resource,
                None, headers, respons_body_type='json')

        request_id = Util.h_v_td(resp_headers, 'x-log-requestid', '')
        return self.decodeToFrame(resp,headers,keys)
    def decodeToFrame(self, resp,headers,keys):
        if keys is None or len(keys)==0:
            keys = resp['meta']['keys']
        data =  resp['data']
        mp = {}
        for k in keys:
            mp[k] = []
        for row in data:
            for k in keys:
                if k in row:
                    mp[k].append(row[k])
                else:
                    mp[k].append(None)
        return pd.DataFrame(mp)
    def _send(self, method, project, body, resource, params, headers, respons_body_type='json'):
        if body:
            headers['Content-Length'] = str(len(body))
            headers['Content-MD5'] = Util.cal_md5(body)
        else:
            headers['Content-Length'] = '0'
            headers["x-log-bodyrawsize"] = '0'

        headers['x-log-apiversion'] = API_VERSION
        headers['x-log-signaturemethod'] = 'hmac-sha1'
        url = "http://" + project + "." + self._endpoint

        headers['Host'] = project + "." + self._endpoint

        url = url + resource
        try:
            headers2 = copy(headers)
            params2 = copy(params)
            headers2['Date'] = self._getGMT()

            if self._securityToken:
                headers2["x-acs-security-token"] = self._securityToken

            signature = Util.get_request_authorization(method, resource,
                    self._accessKey, params2, headers2)

            headers2['Authorization'] = "LOG " + self._accessKeyId + ':' + signature
            headers2['x-log-date'] = headers2['Date']  # bypass some proxy doesn't allow "Date" in header issue.

            return self._sendRequest(method, url, params2, body, headers2, respons_body_type)
        except LogException as ex:
            raise

    def _sendRequest(self, method, url, params, body, headers, respons_body_type='json'):
        (resp_status, resp_body, resp_header) = self._getHttpResponse(method, url, params, body, headers)
        header = {}
        for key, value in resp_header.items():
            header[key] = value

        requestId = Util.h_v_td(header, 'x-log-requestid', '')

        if resp_status == 200:
            if respons_body_type == 'json':
                exJson = self._loadJson(resp_status, resp_header, resp_body, requestId)
                exJson = Util.convert_unicode_to_str(exJson)
                return exJson, header
            else:
                return resp_body, header

        exJson = self._loadJson(resp_status, resp_header, resp_body, requestId)
        exJson = Util.convert_unicode_to_str(exJson)

        if 'errorCode' in exJson and 'errorMessage' in exJson:
            raise LogException(exJson['errorCode'], exJson['errorMessage'], requestId,
                               resp_status, resp_header, resp_body)
        else:
            exJson = '. Return json is ' + str(exJson) if exJson else '.'
            raise LogException('LogRequestError',
                               'Request is failed. Http code is ' + str(resp_status) + exJson, requestId,
                               resp_status, resp_header, resp_body)
    def _getHttpResponse(self, method, url, params, body, headers):  # ensure method, url, body is str
        try:
            headers['User-Agent'] = 'logframe'
            r = getattr(requests, method.lower())(url, params=params, data=body, headers=headers, timeout=60)
            return r.status_code, r.content, r.headers
        except Exception as ex:
            raise LogException('LogRequestError', str(ex))


    @staticmethod
    def _loadJson(resp_status, resp_header, resp_body, requestId):
        if not resp_body:
            return None
        try:
            if isinstance(resp_body, six.binary_type):
                return json.loads(resp_body.decode('utf8', "ignore"))

            return json.loads(resp_body)
        except Exception as ex:
            raise LogException('BadResponse',
                               'Bad json format:\n"%s"' % b64e(resp_body) + '\n' + repr(ex),
                               requestId, resp_status, resp_header, resp_body)



    @staticmethod
    def _getGMT():
        try:
            locale.setlocale(locale.LC_TIME, "C")
        except Exception as ex:
            raise
        return datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')

