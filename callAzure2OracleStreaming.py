#########################################################
#### Written By: SATYAKI DE                          ####
#### Written On: 06-Mar-2021                         ####
#### Modified On 07-Mar-2021                         ####
####                                                 ####
#### Objective: Main calling scripts -               ####
#### This Python script will consume an              ####
#### source API data from Azure-Cloud & publish the  ####
#### data into an Oracle Streaming platform,         ####
#### which is compatible with Kafka. Later, another  ####
#### consumer app will read the data from the stream.####
#########################################################

from clsConfig import clsConfig as cf
import clsL as cl
import logging
import datetime
import clsAzureAPI as ca
import clsOCIPublish as co
import clsOCIConsume as cc
import pandas as p
import json

# Disbling Warning
def warn(*args, **kwargs):
    pass

import warnings
warnings.warn = warn

# Lookup functions from
# Azure cloud SQL DB

var = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def main():
    try:
        # Declared Variable
        ret_1 = 0
        debug_ind = 'Y'
        res_2 = ''

        # Defining Generic Log File
        general_log_path = str(cf.conf['LOG_PATH'])

        # Enabling Logging Info
        logging.basicConfig(filename=general_log_path + 'Azure2OCIStream.log', level=logging.INFO)

        # Initiating Log Class
        l = cl.clsL()

        # Moving previous day log files to archive directory
        log_dir = cf.conf['LOG_PATH']

        tmpR0 = "*" * 157

        logging.info(tmpR0)
        tmpR9 = 'Start Time: ' + str(var)
        logging.info(tmpR9)
        logging.info(tmpR0)

        print()

        print("Log Directory::", log_dir)
        tmpR1 = 'Log Directory::' + log_dir
        logging.info(tmpR1)

        print('Welcome to Azure to Oracle Cloud Streaming(OCI) Calling Program: ')
        print('*' * 160)
        print('Reading dynamic Covid data from Azure API: ')
        print('https://xxxxxx.yyyyyyyyyy.net/api/getDynamicCovidStats')
        print()
        print('Selected Columns for this -> date, state, positive, negative')
        print()
        print('This will take few seconds depending upon the volume & network!')
        print('-' * 160)
        print()

        # Create the instance of the Mock Mulesoft API Class
        x1 = ca.clsAzureAPI()

        # Let's pass this to our map section
        retJson = x1.searchQry()

        # Converting JSon to Pandas Dataframe for better readability
        # Capturing the JSON Payload
        #res_1 = json.dumps(retJson)
        #res = json.loads(res_1)
        res = json.loads(retJson)

        # Converting dictionary to Pandas Dataframe
        # df_ret = p.read_json(ret_2, orient='records')
        df_ret = p.io.json.json_normalize(res)
        df_ret.columns = df_ret.columns.map(lambda x: x.split(".")[-1])

        # Removing any duplicate columns
        df_ret = df_ret.loc[:, ~df_ret.columns.duplicated()]

        print()
        print()
        print("-" * 160)

        print('Publishing Azure sample result: ')
        print(df_ret.head())

        # Logging Final Output
        l.logr('1.df_ret' + var + '.csv', debug_ind, df_ret, 'log')

        print("-" * 160)
        print()

        print('*' * 160)
        print('Calling Oracle Cloud Infrustructure Publisher Program!')
        print('Pushing Azure API to Oracle Kafka-Streaming using OCI!')
        print('-' * 160)
        # Create the instance of the Mock Mulesoft API Class
        x2 = co.clsOCIPublish()

        retVal = x2.publishStream(df_ret)

        if retVal == 0:
            print('Successfully streamed to Oracle Cloud!')
        else:
            print('Failed to stream!')

        print()

        print('*' * 160)
        print('Calling Oracle Cloud Infrustructure Consumer Program!')
        print('Getting Oracle Streaming captured in OCI!')
        print('-' * 160)
        # Create the instance of the Mock Mulesoft API Class
        x3 = cc.clsOCIConsume()

        retVal2 = x3.consumeStream()

        if retVal2 == 0:
            print('Successfully streamed captured from Oracle Cloud!')
        else:
            print('Failed to retrieve stream from OCI!')


        print('Finished Analysis points..')
        print("*" * 160)
        logging.info('Finished Analysis points..')
        logging.info(tmpR0)

        tmpR10 = 'End Time: ' + str(var)
        logging.info(tmpR10)
        logging.info(tmpR0)

    except ValueError as e:
        print(str(e))
        print("Invalid option!")
        logging.info("Invalid option!")

    except Exception as e:
        print("Top level Error: args:{0}, message{1}".format(e.args, e.message))

if __name__ == "__main__":
    main()
