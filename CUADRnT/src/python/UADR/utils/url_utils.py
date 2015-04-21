#!/usr/local/bin/python
"""
File       : url_utils.py
Author     : Bjorn Barrefors <bjorn dot peter dot barrefors AT cern dot ch>
Description: Useful functions to deal with internet services
"""

# system modules
import os
import json
import urllib
import urllib2
import httplib

# package modules
from UADR.utils.utils import get_key_cert

def fetch(target_url, api, params=dict(), name='', format_='json', instance='prod', debug=0):
    """
    Create http request for target_url, api and params of service name
    """
    url_data = urllib.urlencode(params)
    url = '%s/%s/%s/%s?' % (target_url, format_, instance, api)
    request = urllib2.Request(url, url_data)
    return get_data(request, name, debug)

def get_data(request, name, debug=0):
    """
    Data is json data returned as a string
    Use json.loads(data) to generate json structure
    """
    # FIXME: Better way of checking for error
    data = "{}"
    if debug:
        full_url = request.get_full_url() + request.get_data()
        print "Fetching %s data for url:\n%s" % (name, full_url)
    opener = urllib2.build_opener(HTTPSGridAuthHandler())
    try:
        return_data = opener.open(request)
        data = return_data.read()
        json.loads(data)
    except Exception as e:
        full_url = request.get_full_url() + request.get_data()
        print "Couldn't fetch %s data for url %s" % (name, full_url)
        print str(e)
        data = "{}"
    return data

class HTTPSGridAuthHandler(urllib2.HTTPSHandler):
    """
    Helper class to make http requests
    """
    def __init__(self):
        urllib2.HTTPSHandler.__init__(self)
        self.key, self.cert = get_key_cert()

    def https_open(self, req):
        return self.do_open(self.getConnection, req)

    def getProxy(self):
        proxy = os.environ.get('X509_USER_PROXY')
        if not proxy:
            proxy = "/tmp/x509up_u%d" % (os.geteuid(),)
        return proxy

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)
