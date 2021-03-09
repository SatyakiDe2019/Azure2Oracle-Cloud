##############################################
#### Enhancement By: SATYAKI DE           ####
#### Enhancement On: 07-Mar-2021          ####
#### Modified On 08-Mar-2021              ####
####                                      ####
#### Objective: Consuming stream from OCI ####
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

class clsOCIConsume:
    def __init__(self):
        self.comp = str(cf.conf['comp'])
        self.STREAM_NAME = str(cf.conf['STREAM_NAME'])
        self.PARTITIONS = int(cf.conf['PARTITIONS'])
        self.limRec = int(cf.conf['limRec'])

    def get_cursor_by_partition(self, client, stream_id, partition):
        print("Creating a cursor for partition {}".format(partition))
        cursor_details = oci.streaming.models.CreateCursorDetails(
            partition=partition,
            type=oci.streaming.models.CreateCursorDetails.TYPE_TRIM_HORIZON)
        response = client.create_cursor(stream_id, cursor_details)
        cursor = response.data.value
        return cursor

    def simple_message_loop(self, client, stream_id, initial_cursor):
        try:
            cursor = initial_cursor
            while True:
                get_response = client.get_messages(stream_id, cursor, limit=10)
                # No messages to process. return.
                if not get_response.data:
                    return

                # Process the messages
                print(" Read {} messages".format(len(get_response.data)))
                for message in get_response.data:
                    print("{}: {}".format(b64decode(message.key.encode()).decode(),
                                          b64decode(message.value.encode()).decode()))

                # get_messages is a throttled method; clients should retrieve sufficiently large message
                # batches, as to avoid too many http requests.
                time.sleep(1)
                # use the next-cursor for iteration
                cursor = get_response.headers["opc-next-cursor"]

            return 0
        except Exception as e:
            x = str(e)
            print('Error: ', x)

            return 1

    def get_stream(self, admin_client, stream_id):
        return admin_client.get_stream(stream_id)

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

    def consumeStream(self):
        try:
            STREAM_NAME = self.STREAM_NAME
            PARTITIONS = self.PARTITIONS
            compartment = self.comp
            print('Consuming sream from Oracle Cloud!')
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

            # Use a cursor for getting messages; each get_messages call will return a next-cursor for iteration.
            # There are a couple kinds of cursors.
            # A cursor can be created at a given partition/offset.
            # This gives explicit offset management control to the consumer.

            print("Starting a simple message loop with a partition cursor")
            partition_cursor = self.get_cursor_by_partition(stream_client, s_id, partition="0")
            self.simple_message_loop(stream_client, s_id, partition_cursor)

            return 0

        except Exception as e:
            x = str(e)
            print(x)

            logging.info(x)

            return 1
