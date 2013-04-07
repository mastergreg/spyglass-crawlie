#!/usr/bin/env python
# -*- coding: utf-8
#
#* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
# File Name : crawlie.py
# Creation Date : 06-01-2013
# Last Modified : Sun 07 Apr 2013 03:54:29 PM EEST
# Created By : Greg Liras <gregliras@gmail.com>
#_._._._._._._._._._._._._._._._._._._._._.*/

import serverconf
import userconf

import slumber
import requests

from urllib import quote_plus as urlquote
from urllib2 import urlopen

from hashlib import sha256

from lxml import etree

from time import sleep

class crawlie(object):
    def __init__(self, user=None, key=None, limit=serverconf.LIMIT):
        if user:
            self._user = user
        else:
            raise Exception('UserError')
        if key:
            self._key = key
        else:
            raise Exception('APIKeyError')
        self._params = {}
        self._params['format'] = 'json'
        self._params['username'] = self._user
        self._params['api_key'] = self._key
        self._limit = limit
        self._API_URI = "{0}/api/spyglass".format(serverconf.URL)
        self.API = slumber.API(self._API_URI)
        self._check_SSL()
        self._get_sites()
        self._get_siteXpaths()

        

    def _get_page(self, sid, params):
        searchURL = self._gen_searchlink(sid, params)
        return urlopen(searchURL).read()

    def _get_sites(self):
        params = dict(self._params)
        sites = self.API.site.get(**params)['objects']
        self._sites = {}
        for site in sites:
            self._sites[int(site['id'])] = site

    def _get_siteXpaths(self):
        params = dict(self._params)
        xpaths = self.API.paths.get(**params)['objects']
        for xpath in xpaths:
            sid = int(xpath['site'][-2])
            ns = self._sites[sid].get('xpaths', []) 
            ns.append((xpath['field_name'].lower(), xpath['xpath']))
            self._sites[sid]['xpaths'] = ns

    def _check_SSL(self):
        params=dict(self._params)
        params['limit'] = 1
        try:
            self._workload = self.API.query.get(**params)['objects']
        except requests.exceptions.SSLError:
            if self._API_URI.startswith("https"):
                print "security warning: https still throws SSLError, falling back to http"
                self._API_URI = "http{0}".format(self._API_URI[5:])
                self.API = slumber.API(self._API_URI, auth=(self._user, self._key))

    def _get_settings(self):
        pass


    def _gen_searchlink(self, sid, params):
        link = self._sites[sid]['url']
        if link.startswith("http://"):
            link = "http://{0}".format(urlquote(link[7:].format(*params.split("&")), '/'))
        elif link.startswith("https://"):
            link = "http://{0}".format(urlquote(link[8:].format(*params.split("&")), '/'))
        else:
            link = "http://{0}".format(urlquote(link.format(*params.split("&")), '/'))
        return link
    
    def _get_content_hash(self, mydata):
        hashable = ''.join(mydata.values()).encode('utf-8')
        return sha256(hashable).hexdigest()

    def _work(self, mywork):
        sid = int(mywork['site'][-2])
        doc = self._get_page(sid, mywork['params'])
        tree = etree.HTML(doc)
        for xpath in self._sites[sid]['xpaths']:
            r = tree.xpath(xpath[1])
            res = mywork.get('_data', {})
            res[xpath[0]] = r[0].text.strip()
            mywork['_data'] = res
        newhash = self._get_content_hash(mywork['_data'])

        if not newhash == mywork['content_hash']:
            self._send_data(mywork['_data'], mywork['id'], not mywork['persistent'], newhash)
        else:
            self._update_timestamp(mywork['id'])

    def _update_timestamp(self, qid):
        params=dict(self._params)
        work = {}
        self.API.query(qid).patch(work, **params)


    def _send_data(self, res, qid, completed, newhash):
        #params=dict(self._params.items() + res.items())
        params=dict(self._params)
        #self.API.meta.post(res, **params)
        work = {}
        work['completed'] = completed
        work['result'] = res
        work['content_hash'] = newhash
        print work
        self.API.meta.post(res, **params)
        self.API.query(qid).patch(work, **params)

    def work(self):
        for i in self._workload:
            self._work(i)



    def get_workload(self):
        params=dict(self._params)
        params['limit'] = min(self._limit, serverconf.LIMIT)
        try:
            self._workload = self.API.query.get(**params)['objects']
        except requests.exceptions.SSLError:
            if self._API_URI.startswith("https"):
                print "security warning: https still throws SSLError, falling back to http"
                self._API_URI = "http{0}".format(self._API_URI[5:])
                self.API = slumber.API(self._API_URI, auth=(self._user, self._key))
            self._workload = self.API.query.get(**params)['objects']
    

    def test(self):
        work = {}
        work['completed'] = True
        res = {}
        res['headline'] = "asdfassldfalsdkjfaskdjflaksdflkf"
        res['category'] = "asdfasdasldfkjalskjf"
        res['subtitle'] = "asdfasdfalskdjfl"

        work['result'] = res

        params = dict(self._params)
        self.API.meta.post(res, **params)
        self.API.query("1").patch(work, **params)


def main():
    cr = crawlie(userconf.username, userconf.api_key)
    while True:
        cr.get_workload()
        cr.work()
        sleep(1)

if __name__=="__main__":
    main()

