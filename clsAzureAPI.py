##############################################
#### Written By: SATYAKI DE               ####
#### Written On: 07-Mar-2021              ####
#### Modified On 07-Mar-2021              ####
####                                      ####
#### Objective: Calling Azure dynamic API ####
##############################################

import json
from clsConfig import clsConfig as cf
import requests
import logging

class clsAzureAPI:
    def __init__(self):
        self.url = cf.conf['URL']
        self.azure_cache = cf.conf['CACHE']
        self.azure_con = cf.conf['conType']
        self.type = cf.conf['appType']
        self.typSel = cf.conf['typSel']
        self.typVal = cf.conf['colList']

    def searchQry(self):
        try:
            url = self.url
            api_cache = self.azure_cache
            api_con = self.azure_con
            type = self.type
            typSel = self.typSel
            typVal = self.typVal

            querystring = {"typeSel": typSel, "typeVal": typVal}

            print('Input JSON: ', str(querystring))

            headers = {
                'content-type': type,
                'Cache-Control': api_cache,
                'Connection': api_con
            }

            response = requests.request("GET", url, headers=headers, params=querystring)

            ResJson = response.text

            jdata = json.dumps(ResJson)
            ResJson = json.loads(jdata)

            return ResJson

        except Exception as e:
            ResJson = ''
            x = str(e)
            print(x)

            logging.info(x)
            ResJson = {'errorDetails': x}

            return ResJson
