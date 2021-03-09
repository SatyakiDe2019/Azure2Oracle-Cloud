###############################################
#### Written By: SATYAKI DE                ####
#### Written On: 04-Apr-2020               ####
####                                       ####
#### Objective: This script is a config    ####
#### file, contains all the keys for       ####
#### Azure 2 OCI API. Application will     ####
#### process these information & perform   ####
#### the call to our newly developed Azure ####
#### API in OCI.                           ####
###############################################

import os
import platform as pl

class clsConfig(object):
    Curr_Path = os.path.dirname(os.path.realpath(__file__))

    os_det = pl.system()
    if os_det == "Windows":
        sep = '\\'
    else:
        sep = '/'

    conf = {
        'APP_ID': 1,
        "comp": "ocid1.compartment.oc1..xxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyxxxxxx",
        "URL":"https://xxxxxxxxxx.yyyyyyyyyyyyyyyy.net/api/getDynamicCovidStats",
        "appType":"application/json",
        "conType":"keep-alive",
        "limRec":10,
        "CACHE":"no-cache",
        "colList": "date, state, positive, negative",
        "typSel": "Cols",
        "LOG_PATH":Curr_Path + sep + 'log' + sep,
        "STREAM_NAME":"Covid19-Stream",
        "PARTITIONS":1
    }
