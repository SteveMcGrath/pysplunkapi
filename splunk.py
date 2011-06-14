import httplib
import base64
import json
from urllib import urlencode
from xml.dom import minidom


class SplunkBase(object):
  auth = None
  host = 'localhost'
  port = 8089
  conn = httplib.HTTPSConnection
  
  def _get(self, url, headers={}):
    if self.auth is not None:
      headers['Authorization'] = 'Splunk %s' % self.auth
    http = self.conn(self.host, port=self.port)
    http.request('GET', url, headers=headers)
    resp = http.getresponse()
    return resp.read()
  
  def _post(self, url, payload={}, headers={}):
    content = urlencode(payload)
    if self.auth is not None:
      headers['Authorization'] = 'Splunk %s' % self.auth
    headers['Content-Length'] = len(content)
    headers['Content-Type'] = 'application/x-www-form-urlencoded'
    http = self.conn(self.host, port=self.port)
    http.request('POST', url, body=content, headers=headers)
    resp = http.getresponse()
    return resp.read()
  
class SplunkSearch(SplunkBase):
  def __init__(self, api, sid):
    self.sid = sid
    self.host = api.host
    self.port = api.port
    self.auth = api.auth
    self.conn = api.conn
  
  def isdone(self):
    data = self._get('/services/search/jobs/%s' % self.sid)
    xml = minidom.parseString(data)
    keys = xml.getElementsByTagName('s:key')
    for key in keys:
      if key.getAttribute('name') == 'isDone':
        return bool(key.firstChild.nodeValue)
    return False

  def results(self, format='json'):
    data = self._get('/services/search/jobs/%s/results/?%s' % (self.sid,
                     urlencode({'output_mode': format})))
    if format == 'json':
      return json.loads(data)
    if format == 'xml':
      return minidom.parseString(data)
    if format == 'csv':
      return data

class SplunkAPI(SplunkBase):  
  def __init__(self, username, password, host, port=8089, ssl=True):
    self.host = host
    self.port = port
    if ssl:
      self.conn = httplib.HTTPSConnection
    else:
      self.conn = httplib.HTTPConnection
    data = self._post('/services/auth/login', 
                      payload={'username': username, 'password': password})
    xml = minidom.parseString(data)
    self.auth = xml.getElementsByTagName('sessionKey')[0].childNodes[0].nodeValue
    
  def search(self, search_string):
    data = self._post('/services/search/jobs', {'search': search_string})
    xml = minidom.parseString(data)
    sid = xml.getElementsByTagName('sid')[0].childNodes[0].nodeValue
    return SplunkSearch(self, sid)