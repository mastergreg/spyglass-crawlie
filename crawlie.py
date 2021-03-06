#!/usr/bin/env python
# -*- coding: utf-8
#
#* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
# File Name : crawlie.py
# Creation Date : 06-01-2013
# Last Modified : Tue 23 Apr 2013 06:53:42 PM EEST
# Created By : Greg Liras <gregliras@gmail.com>
#_._._._._._._._._._._._._._._._._._._._._.*/

import serverconf
import userconf

import slumber
import requests

from urllib import quote_plus as urlquote
from urllib2 import urlopen
from urllib2 import URLError

from hashlib import sha256

from lxml import etree

from time import sleep


from fuzzywuzzy import fuzz

class Crawlie(object):
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
        self._ratio = serverconf.RESULT_RATIO
        self._wait_time = serverconf.WAIT_TIME
        self._srv_wait_time = serverconf.SRV_WAIT_TIME
        self._API_URI = "{0}/api/spyglass".format(serverconf.URL)
        self.API = slumber.API(self._API_URI)
        #self._check_SSL() get_workload checks for this too so we are ok
        self._get_sites()
        self._get_siteXpaths()

    def _get_page(self, sid):
        link = self._sites[sid]['url']
        searchURL = self._gen_link(link)
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


    def _gen_link(self, link):
        if link.startswith("http://"):
            link = "http://{0}".format(urlquote(link[7:]))
        elif link.startswith("https://"):
            link = "http://{0}".format(urlquote(link[8:]))
        else:
            link = "http://{0}".format(urlquote(link))
        return link
    
    def _get_content_hash(self, mydata):
        hashable = ''.join(mydata.values()).encode('utf-8')
        return sha256(hashable).hexdigest()

    def _work(self, mywork):
        sid = int(mywork['site'][-2])
        doc = self._get_page(sid)
        tree = etree.HTML(doc)
        xpaths = self._sites[sid]['xpaths']
        results = self._get_results(tree, xpaths)
        r = max(self._score_results(results, mywork['params']))
        
        for index, value in r[1].items():
            res = mywork.get('_data', {})
            res[index] = value
            mywork['_data'] = res

        newhash = self._get_content_hash(mywork['_data'])
        if r[0] >= self._ratio and not newhash == mywork['content_hash']:
            self._send_data(mywork['_data'], mywork['id'], not mywork['persistent'], newhash)
        else:
            self._update_timestamp(mywork['id'])


    def _get_text_lowercase_from_result(self, r):
        return r.text.strip().lower()


    def _get_results(self, tree, xpaths):
        results = {}
        final = []
        for xpath in xpaths:
            r = tree.xpath(xpath[1])
            results[xpath[0]] = map(self._get_text_lowercase_from_result, r)
        keys = results.keys()
        values = map(list, zip(*results.values()))
        for v in values:
            final.append(dict(zip(keys, v)))
        return final


    def _score_results(self, results, params):
        tokens = " ".join(params.split("&")).lower().encode('utf-8')
        data = map(self._get_text_from_values, results)
        ratios = [self._scoring(d, tokens) for d in data]
        return zip(ratios, results)


    def _scoring(self, encoded_data, encoded_tokens):
        tokens = encoded_tokens.split()
        ratios = map(lambda x: fuzz.partial_ratio(x , encoded_data), tokens)
        return sum(ratios)/len(ratios)

    def _get_text_from_values(self, dat):
        return " ".join(dat.values()).encode('utf-8')


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
        self.API.meta.post(res, **params)
        self.API.query(qid).patch(work, **params)

    def work(self):
        for i in self._workload:
            try:
                self._work(i)
            except URLError:
                self.net_srv_error()
            else:
                self.net_srv_restore()
            finally:
                self.net_srv_wait()



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

    def net_srv_restore(self):
        self._wait_time = serverconf.WAIT_TIME
    def net_srv_error(self):
        self._wait_time *= 2
    def net_srv_wait(self):
        sleep(self._wait_time)

    def spyglass_srv_restore(self):
        self._srv_wait_time = serverconf.SRV_WAIT_TIME
    def spyglass_srv_error(self):
        self._srv_wait_time *= 2
    def spyglass_srv_wait(self):
        sleep(self._srv_wait_time)


def main():
    cr = Crawlie(userconf.username, userconf.api_key)
    while True:
        try:
            cr.get_workload()
            cr.work()
        except requests.exceptions.ConnectionError:
            cr.spyglass_srv_error()
        else:
            cr.spyglass_srv_restore()
        finally:
            cr.spyglass_srv_wait()


if __name__=="__main__":
    main()

