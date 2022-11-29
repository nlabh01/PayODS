# from ensurepip import bootstrap
import sys
import time
import math
import random

from locust import User, TaskSet, task, between, events, run_single_user, tag
from locust.runners import MasterRunner

import gevent
_ = gevent.monkey.patch_all()

from kafka import KafkaProducer
from kafka import KafkaConsumer
import pymongo
from bson import json_util
import json
from schema2 import returnDocs

_send_rate = 0.0
_sent = 0
_start = 0.0
_mavg = 0.0

def get_time( ):
    return time.monotonic()

def delta_time( start ):
    return math.floor( (time.monotonic() - start) * 1000 ) #convert to millis

def ewma( start: float, sample:float, old_ewma:float, window:int ):
    delta = delta_time( start )
    sample_rate = sample / delta
    smooth = 2.0/(1.0 + window)
    ewma = sample_rate * smooth + old_ewma *( 1 - smooth )
    print( f"current rate: {sample_rate}, old: {old_ewma}, ewma: {ewma}")
    return ewma

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--test-file", type=str, env_var="LOCUST_TEST_FILE", default="pds-trans-10k.json", help="Test file of json documents to publish to kafka topic")
    # Set `include_in_web_ui` to False if you want to hide from the web UI
    parser.add_argument("--my-ui-invisible-argument", include_in_web_ui=False, default="I am invisible")

@events.init.add_listener
def _(environment, **kw):
    print("Custom argument supplied: %s" % environment.parsed_options.test_file)
    global _send_rate
    global _sent
    global _start
    global _mavg

##
# Simple Kafka Producer class
class KafkaClient:

    def __init__(self, kafka_brokers=None ):

        if kafka_brokers is None:
            kafka_brokers = 'localhost:9092'
        print("creating message sender with params: " + str(locals()))
        self.producer = KafkaProducer(bootstrap_servers=kafka_brokers, 
            value_serializer=lambda v: json_util.dumps(v).encode('utf-8'), acks=1, retries=0, batch_size=200000, linger_ms=50, compression_type='gzip' )

    def send(self, topic, key=None, message=None):
        if( _start == 0 ):
            _start = get_time()
            _sent = 0

        start_time = get_time()
        # print( f"Sending {message}" )
        future = self.producer.send(topic, key=key.encode() if key else None,
                                    value=message if message else None)
        _sent += 1
        if( _sent % 1000 == 0 ):
            _mavg = ewma( _start, _sent, _mavg, 1000 )
            _start = 0 # resets current sampling

        future.add_callback(self.__handle_success, start_time=start_time, future=future)
        future.add_errback(self.__handle_failure, start_time=start_time, topic=topic)
        # self.producer.flush()

    def __handle_success(self, *arguments, **kwargs):
        elapsed_time = delta_time( kwargs["start_time"] )
        # end_time = time.time()
        # elapsed_time = int((end_time - kwargs["start_time"]) * 1000)
        try:
            record_metadata = kwargs["future"].get(timeout=1)

            request_data = dict(request_type="ENQUEUE",
                                name=record_metadata.topic,
                                response_time=elapsed_time,
                                response_length=record_metadata.serialized_value_size)

            self.__fire_success(**request_data)
        except Exception as ex:
            print("Logging the exception : {0}".format(ex))
            raise  # ??

    def __handle_failure(self, *arguments, **kwargs):
        print("failure " + str(locals()))
        end_time = time.time()
        elapsed_time = int((end_time - kwargs["start_time"]) * 1000)

        request_data = dict(request_type="ENQUEUE", name=kwargs["topic"], response_time=elapsed_time,
                            exception=arguments[0])

        self.__fire_failure(**request_data)

    def __fire_failure(self, **kwargs):
        events.request_failure.fire(**kwargs)
        # print( kwargs )

    def __fire_success(self, **kwargs):
        events.request_success.fire(**kwargs)
        # print( kwargs )

    def finalize(self):
        print("flushing the messages")
        self.producer.flush(timeout=5)
        print("flushing finished")

client = None
filecontent = []

@events.init.add_listener
def on_locust_init( environment, **kwargs ):
    if isinstance(environment.runner, MasterRunner):
        print("I'm on master node")
    else:
        print("I'm on a worker or standalone node")
        # global client
        # client = KafkaClient()
        global filecontent
        with open( "./pds-trans-10k.json", 'r') as file:
            walker = iter( file )
            while True:
                try:
                    filecontent.append( json_util.loads( next( file ) ) )
                except StopIteration:
                    break

        if( len( filecontent ) ):
            print( f"loaded content: {filecontent[0]}" )
        else: print( "No file content loaded" )

class KafkaSendTest(User):

    def __init__(self, environment):
        super().__init__(environment)
        self.client = KafkaClient( kafka_brokers="localhost:9092,localhost:9093,localhost:9094" )

    @tag('GENERATE')
    @task(100)
    def sendMessages(self):
        type = "GENERATE"
        name = "schema2.returnDocs"
        start = time.time()
        docs = returnDocs( count=100 )
        delta = (time.time() - start) * 1000.0
        request_data = dict(request_type=name,
                            name=name,
                            response_time=delta,
                            response_length=len(docs))
        events.request_success.fire( **request_data)
        for doc in docs:
            key = doc.get( 'PDS_TRANS_ID' ) if 'PDS_TRANS_ID' in doc else doc.get('pmt_id')
            self.client.send( topic='TEST_TOPIC', key=key, message=doc )

    @tag('LOAD')
    @task(100)
    def sendMessageFromFile(self):
        type = "LOAD"
        name = "fileContent"
        start = time.time()
        doc = random.choice( filecontent )
        # delta = (time.time() - start) * 1000.0
        # request_data = dict(request_type=type,
        #                     name=name,
        #                     response_time=delta,
        #                     response_length=len(doc))
        # events.request_success.fire( **request_data)

        key = doc.get( 'PDS_TRANS_ID' ) if 'PDS_TRANS_ID' in doc else doc.get('pmt_id')
        self.client.send( topic='TEST_TOPIC', key=key, message=doc )

    def audit(self, type, msg):
        self._mongo.locust.audit.insert_one( {"type":type, "ts":time.time(), "msg":str(msg)} )

    def on_start(self):
        print( 'Running user on_start()')

    def on_stop(self):
        print( "User stopped." )
    
    def finalize(self):
#        self.file.close()
         pass

# if launched directly, e.g. "python debugging.py", not "locust -f debugging.py"
if __name__ == "__main__":
    topic = "TEST_TOPIC"
    KEYFIELD = 'PDS_TRANS_ID'
    doc = { '_id': "100001", 'name': 'MY NAME IS...', 'tags': [ 'TAG1', 'TAG2', 'TAG3'] }
    client = KafkaClient( kafka_brokers="localhost:9092,localhost:9093,localhost:9094" )
    key = doc.get( 'PDS_TRANS_ID' ) if 'PDS_TRANS_ID' in doc else doc.get('_id')
    client.send( topic=topic, key=key, message=doc )
