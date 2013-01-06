#!/usr/bin/env python
# -*- coding: utf-8
#
#* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
# File Name : crawlie.py
# Creation Date : 06-01-2013
# Last Modified : Mon 07 Jan 2013 12:36:49 AM EET
# Created By : Greg Liras <gregliras@gmail.com>
#_._._._._._._._._._._._._._._._._._._._._.*/

import serverconf
import userconf

import slumber

class crawlie(object):
    def __init__(self, user=None, key=None, limit=serverconf.LIMIT):
        if user:
            self._user = user
        else:
            raise UserError
        if key:
            self._key = key
        else:
            raise APIKeyError
        self._limit = limit
        self._API_URI = "{0}/api/spyglass".format(serverconf.URL)
        self.API = slumber.API(self._API_URI)

    def work(self, index):
        pass

    def get_sites(self):
        pass

    def get_siteXpaths(self):
        pass

    def check_SSL(self):
        pass

    def get_settings(self):
        pass

    def send_data(self, index):
        pass

    def get_workload(self):
        params = {}
        params['format'] = 'json'
        params['username'] = self._user
        params['api_key'] = self._key
        params['limit'] = min(self._limit, serverconf.LIMIT)
        try:
            self._workload = self.API.query.get(**params)['objects']
        except requests.exceptions.SSLError:
            if self._API_URI.startswith("https"):
                print "security warning: https still throws SSLError, falling back to http"
                self._API_URI = "http{0}".format(self._API_URI[5:])
                self.API = slumber.API(self._API_URI, auth=(self._user, self._key))
            self._workload = self.API.query.get(**params)['objects']
        print self._workload
    

    def shit(self):
        work = {}
        work['completed'] = True
        res = {}
        res['headline'] = "asdfassldfalsdkjfaskdjflaksdflkf"
        res['category'] = "asdfasdasldfkjalskjf"
        res['subtitle'] = "asdfasdfalskdjfl"

        work['result'] = res

        params = {}
        params['format'] = 'json'

        print self.API.meta.get(**params)['objects']
        self.API.meta.post(res, **params)
        self.API.query("1").patch(work, **params)


def main():
    cr = crawlie(userconf.username, userconf.api_key)
    cr.get_workload()
    cr.shit()

if __name__=="__main__":
    main()

