##############################################
#### Enhancement By: SATYAKI DE           ####
#### Enhancement On: 07-Mar-2021          ####
#### Modified On 07-Mar-2021              ####
####                                      ####
#### Objective: Publishing stream at OCI  ####
##############################################
import oci
import sys
import time
import os
from base64 import b64encode, b64decode
import json
from clsConfig import clsConfig as cf
from oci.config import from_file
import pandas as p

class clsOCIPublish:
    def __init__(self):
        self.comp = str(cf.conf['comp'])
        self.STREAM_NAME = str(cf.conf['STREAM_NAME'])
        self.PARTITIONS = int(cf.conf['PARTITIONS'])
        self.limRec = int(cf.conf['limRec'])

    def get_stream(self, admin_client, stream_id):
        return admin_client.get_stream(stream_id)

    def publish_example_messages(self, client, stream_id, inputDF):
        try:
            limRec = self.limRec
            # Converting dataframe to json
            df = inputDF

            # Calculating total number of records
            cntRow = df.shape[0]
            print('Actual Record counts: ', str(cntRow))
            print('Supplied Record counts: ', str(limRec))

            # Build up a PutMessagesDetails and publish some messages to the stream
            message_list = []
            start_pos = 0
            end_pos = 0
            interval = 1

            for i in range(limRec):
                split_df = p.DataFrame()
                rJson = ''
                # Preparing Data

                # Getting Individual Element & convert them to Series
                if ((start_pos + interval) <= cntRow):
                    end_pos = start_pos + interval
                else:
                    end_pos = start_pos + (cntRow - start_pos)

                split_df = df.iloc[start_pos:end_pos]
                rJson = split_df.to_json(orient ='records')

                if ((start_pos > cntRow) | (start_pos == cntRow)):
                    pass
                else:
                    start_pos = start_pos + interval

                key = "key" + str(i)
                value = "value" + str(rJson)

                # End of data preparation

                encoded_key = b64encode(key.encode()).decode()
                encoded_value = b64encode(value.encode()).decode()
                message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value))

            print("Publishing {} messages to the stream {} ".format(len(message_list), stream_id))
            messages = oci.streaming.models.PutMessagesDetails(messages=message_list)
            put_message_result = client.put_messages(stream_id, messages)

            # The put_message_result can contain some useful metadata for handling failures
            for entry in put_message_result.data.entries:
                if entry.error:
                    print("Error ({}) : {}".format(entry.error, entry.error_message))
                else:
                    print("Published message to partition {} , offset {}".format(entry.partition, entry.offset))

            return 0
        except Exception as e:
            x = str(e)
            print('Error: ', x)
            return 1

    def get_or_create_stream(self, client, compartment_id, stream_name, partition, sac_composite):
        try:

            list_streams = client.list_streams(compartment_id=compartment_id, name=stream_name,
                                               lifecycle_state=oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE)

            if list_streams.data:
                # If we find an active stream with the correct name, we'll use it.
                print("An active stream {} has been found".format(stream_name))
                sid = list_streams.data[0].id
                return self.get_stream(sac_composite.client, sid)

            print(" No Active stream  {} has been found; Creating it now. ".format(stream_name))
            print(" Creating stream {} with {} partitions.".format(stream_name, partition))

            # Create stream_details object that need to be passed while creating stream.
            stream_details = oci.streaming.models.CreateStreamDetails(name=stream_name, partitions=partition,
                                                                      compartment_id=compartment, retention_in_hours=24)

            # Since stream creation is asynchronous; we need to wait for the stream to become active.
            response = sac_composite.create_stream_and_wait_for_state(stream_details, wait_for_states=[oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE])
            return response
        except Exception as e:
            print(str(e))

    def publishStream(self, inputDf):
        try:
            STREAM_NAME = self.STREAM_NAME
            PARTITIONS = self.PARTITIONS
            compartment = self.comp
            print('Publishing sream to Oracle Cloud!')
            # Load the default configuration
            config = from_file(file_location="~/.oci/config.poc")

            # Create a StreamAdminClientCompositeOperations for composite operations.
            stream_admin_client = oci.streaming.StreamAdminClient(config)
            stream_admin_client_composite = oci.streaming.StreamAdminClientCompositeOperations(stream_admin_client)

            # We  will reuse a stream if its already created.
            # This will utilize list_streams() to determine if a stream exists and return it, or create a new one.
            stream = self.get_or_create_stream(stream_admin_client, compartment, STREAM_NAME,
                                          PARTITIONS, stream_admin_client_composite).data

            print(" Created Stream {} with id : {}".format(stream.name, stream.id))

            # Streams are assigned a specific endpoint url based on where they are provisioned.
            # Create a stream client using the provided message endpoint.
            stream_client = oci.streaming.StreamClient(config, service_endpoint=stream.messages_endpoint)
            s_id = stream.id

            # Publish some messages to the stream
            self.publish_example_messages(stream_client, s_id, inputDf)

            return 0

        except Exception as e:
            x = str(e)
            print(x)

            logging.info(x)

            return 1
