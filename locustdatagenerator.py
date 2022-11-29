from enum import Enum
# from datetime import datetime
from dateutil.parser import parse

import math
import traceback
import time

import pymongo
from locust import User, TaskSet, task, between, events, run_single_user
# from time import sleep
from pymongo import MongoClient, read_preferences
import random

import gevent
_ = gevent.monkey.patch_all()

class PAYMENT_STATUS(str, Enum):
    REQUEST="REQUEST"
    VALIDATE="VALIDATE"
    APPROVE="APPROVE"
    CLEARED="CLEARED"
    VOID="VOID"

_url = "mongodb+srv://charlielittle:mongodb@payments-ods.7x4ag.mongodb.net/ods?retryWrites=true&w=majority&readPreference=secondaryPreferred&compressors=snappy"
mongo = MongoClient(_url, compressors="snappy")

def fromisoformat( s ) :
    return parse(s)

def get_time( ):
    return time.monotonic()

def delta_time( start ):
    return math.floor( (time.monotonic() - start) * 1000 ) #convert to millis

class MongoTest(User):
    # wait_time = between( 0, 0 )
    _mongo = mongo
    _db = None
    _keys = []

    def audit(self, type, msg):
        self._mongo.locust.audit.insert_one( {"type":type, "ts":time.time(), "msg":str(msg)} )

    def on_start(self):
        print( list(self._mongo.list_databases()) )
        self._db = self._mongo.ods.payments
        doc = self._db.find_one()
        print( doc['clnt_acct_no'] )
        print( "User started..." )
        print( "Loading accounts..." )
        self._accounts = list(self._db.distinct('clnt_acct_no'))
        print( "Loading keys..." )
        self._keys = list( self._db.find({}, { '_id': 1 } ).limit(100) )
        print( 'keys: ', len(self._keys))
        item = random.randrange( 0, len(self._keys) )
        print( "key: " + str(self._keys[ item ]) )
        print( self._db.find_one( { '_id': self._keys[ item ]['_id'] } )['pmt_id'] )

    def on_stop(self):
        print( "User stopped." )

    @task(10)
    def doThis(self):
        name = "findOneByKey"
        key = random.choice( self._keys)
        tic = get_time()
        try:
            # print( "key: " + self._keys[ item ]["$oid"] )
            self._db.find_one( { '_id': key[ "_id" ] } )['pmt_id']
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0)
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)

    @task(2)
    def doThat(self):
        name = "findRecentByStatus"
        # item = random.randrange( 0, len(self._titles) )
        tic = get_time()
        try:
            cursor = self._db.find( {'payment_status.pmt_stat_id':'CLEARED', 'ods_rec_crtd_dt': {'$gte': fromisoformat('2022-06-20T07:16:54.000+00:00')} } ).sort('ods_rec_crtd_dt', pymongo.DESCENDING).limit(5)
            cursor.next()
            events.request_success.fire(request_type="MongoTest", name="%s.cursor" % (name), response_time=delta_time(tic), response_length=0)
            length = len(list(cursor))
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=length)
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)

    @task(1)
    def doMore(self):
        name = "findLargestByCounterPty"
        # idx = random.randrange( 0, len(self._directors)-10 )
        tic = get_time()
        try:
            # print( inarg )
            cursor = self._db.find( { 'cnt_pty_id': 'GB52JHZL94098319044363' } ).sort( 'pmt_am', pymongo.DESCENDING ).limit(5)
            cursor.next()
            events.request_success.fire(request_type="MongoTest", name="%s.cursor" % (name), response_time=delta_time(tic), response_length=0)
            result = list(cursor)
            # print( result[0] )
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=len(result) )
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)

    @task(50)
    def doKeyQuery(self):
        name = "findById"
        key = random.choice( self._keys )
        # print( key )
        rp = random.choice( [ ('primary', read_preferences.Primary()), ('secondary', read_preferences.Secondary()), ('nearest', read_preferences.Nearest()) ] )
        name += '.' + rp[0]
        rp_coll = self._mongo.ods.get_collection( name='payments', read_preference=rp[1] )
        tic =  get_time()
        try:
            doc = rp_coll.find_one( { '_id': key['_id'] } )
            # print( doc['_id'] )
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=1 )
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)
        
    @task(50)
    def doTxnKeyQuery(self):
        name = "findTxnById"
        key = random.choice( self._keys )
        # print( key )
        rc = random.choice( [ 'majority', 'local', 'available', 'linearizable', 'snapshot' ])
        name += '.' + rc
        tic =  get_time()
        try:
            with self._mongo.start_session( { 'readConcern': rc } ) as session:
                with session.start_transaction():
                    doc = self._db.find_one( { '_id': key['_id'] } )
                    # print( doc['_id'] )
                    events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=1 )
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)
            # session.abort_transaction()        

# if launched directly, e.g. "python3 debugging.py", not "locust -f debugging.py"
if __name__ == "__main__":
    run_single_user(MongoTest)
