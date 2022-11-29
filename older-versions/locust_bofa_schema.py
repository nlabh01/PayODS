
import os

os.environ["GEVENT_SUPPORT"] = "True"

from enum import Enum
from multiprocessing import Semaphore
from dateutil.parser import parse

import math
import traceback
import time

from locust import User, TaskSet, task, between, events, run_single_user
from locust.runners import MasterRunner

import gevent
_ = gevent.monkey.patch_all()

import pymongo
from pymongo import MongoClient, ReadPreference, read_preferences
import random

_url = "mongodb+srv://charlielittle:mongodb@payments-ods.7x4ag.mongodb.net/ods?retryWrites=true&w=majority&readPreference=secondaryPreferred&compressors=snappy"
# _url = "mongodb+srv://charlielittle:mongodb@payments-ods.7x4ag.mongodb.net/ods?retryWrites=true&w=majority&readPreference=primary&compressors=snappy"
# _url = "mongodb://charlielittle:mongodb@payments-ods-shard-00-00.7x4ag.mongodb.net:27016/ods?retryWrites=true&w=majority&readPreference=primary&compressors=snappy"

mongo = MongoClient(_url, compressors="snappy")
_db = mongo.ods.get_collection( 'poc', read_preference=ReadPreference.NEAREST )
_accounts = []
_keys = []
_cntrptys = []

def fromisoformat( s ) :
    return parse(s)

def get_time( ):
    return time.monotonic()

def delta_time( start ):
    return math.floor( (time.monotonic() - start) * 1000 ) #convert to millis

semaphore = Semaphore(1)

PRIMARY_KEY='_id'
# PRIMARY_KEY='PDS_TRANS_ID'
QUERY_LIMIT = 100

@events.init.add_listener
def on_locust_init( environment, **kwargs ):
    if isinstance(environment.runner, MasterRunner):
        print("I'm on master node")
    else:
        print("I'm on a worker or standalone node")
        print( "Acquiring semaphore to synchronize on_locust_init before clients are launched" )

        semaphore.acquire()

        # load some metadata saved in a collection called [acct_meta] to make it easy to get a list of 
        # TO_ACCT_NUM/FR_ACCT_NUM and PDS_TRANS_ID/_id values to test
        print( list( mongo.list_databases()) )
        doc = _db.find_one()
        print( doc['FR_ACCT_NUM'] )
        print( "User started..." )
        print( "Loading accounts..." )
        account_meta = list( mongo.ods.acct_meta.find( ) )
        print( account_meta[0].get( "_id" ) )
        global _accounts
        _accounts = list( map( lambda acct: acct['_id'], account_meta ) )

        global _cntrptys
        _cntrptys = account_meta[0]['counter']

        print( 'First counterparty: ' + str( _cntrptys[0] ))
        print( f'Counterparty count: {len( _cntrptys )}' )

        print( "Loading keys..." )
        global _keys
        _keys = list( _db.find( {}, { PRIMARY_KEY: 1 } ).limit(100000) )
        print( 'keys: ', len( _keys ) )
        acct = random.choice( _accounts )
        print( "acct to test: " + acct )
        print( f"ID : {_db.find_one( { 'FR_ACCT_NUM' : acct } )[PRIMARY_KEY]}" )

        semaphore.release();
        print( "COMPLETING Locust INIT event")

class MongoTest(User):
    # wait_time = between( 0, 0 )
    def __init__(self, environment):
        super().__init__(environment)

        print( "User aquiring semaphore" )
        semaphore.acquire()

        self._mongo = mongo #= MongoClient(_url, compressors="snappy")
        self._db = _db
        self._keys = _keys
        self._accounts = _accounts
        self._cntrptys = _cntrptys

        semaphore.release()
        print( "User initialized" )

    def audit(self, type, msg):
        self._mongo.locust.audit.insert_one( {"type":type, "ts":time.time(), "msg":str(msg)} )

    def on_start(self):
        print( 'Running user on_start()')

    def on_stop(self):
        print( "User stopped." )

    @task(10)
    def findOneByKey(self):
        name = "findOneByKey"
        key = random.choice( self._keys )
        tic = get_time()
        try:
            doc = self._db.find_one( key )
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=len(doc))
        except Exception as e:
            print( f"ERROR: key={key}" )
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)

    @task(20)
    def findRecentByStatus(self):
        name = "findRecentByStatus"
        acct = random.choice( self._accounts )
        tic = get_time()
        try:
            cursor = self._db.find( { 'FR_ACCT_NUM': acct, 'XFER_CD':'C', \
                'REQ_TS': {'$gte': fromisoformat('2022-06-20T00:00:00.000+00:00')} } ).sort('REQ_TS', pymongo.DESCENDING).limit( QUERY_LIMIT )
            doc = next( cursor, None )
            if( doc ):
                # events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=1)
                # tic = get_time()
                length = len(list(cursor))
                events.request_success.fire(request_type="MongoTest", name="%s.cursor" % (name), response_time=delta_time(tic), response_length=length)
            else:
                events.request_success.fire(request_type="MongoTest", name="%s.NoMatch" % (name), response_time=delta_time(tic), response_length=0)
        except Exception as e:
            print( f"ERROR: acct={acct}" )
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)

    @task(10)
    def findLargestByAccount(self):
        name = "findLargestByAccount"
        key = random.choice( self._accounts )
        tic = get_time()
        try:
            # print( inarg )
            cursor = self._db.find( { 'FR_ACCT_NUM': key, 'REQ_TS': { '$gt' : fromisoformat( '2022-03-03' )} } ).sort( 'XFER_AM', pymongo.DESCENDING ).limit( QUERY_LIMIT )
            # cursor.next()
            # events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=1)
            # tic = get_time()
            result = list(cursor)
            # print( result[0] )
            events.request_success.fire(request_type="MongoTest", name="%s.cursor" % (name), response_time=delta_time(tic), response_length=len(result) )
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)

    @task(50)
    def findById(self):
        name = "findById"
        key = random.choice( self._keys )
        # print( key )
        rp = random.choice( [ ('primary', read_preferences.Primary()), ('secondary', read_preferences.Secondary()), ('nearest', read_preferences.Nearest()) ] )
        name += '.' + rp[0]
        rp_coll = self._mongo.ods.get_collection( name=_db.name, read_preference=rp[1] )
        tic =  get_time()
        try:
            doc = rp_coll.find_one( key )
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=1 )
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)
        
    @task(50)
    def findTxnByAcct(self):
        name = "findTxnByAcct"
        key = random.choice( self._accounts )
        # print( key )
        rc = random.choice( [ 'majority', 'local', 'available', 'linearizable', 'snapshot' ])
        name += '.' + rc
        tic =  get_time()
        try:
            with self._mongo.start_session( { 'readConcern': rc } ) as session:
                with session.start_transaction():
                    doc = self._db.find_one( { 'FR_ACCT_NUM': key } )
                    events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=1 )
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)
            # session.abort_transaction()        

    @task(20)
    def findLast5ByCntPty(self):
        name = "findLast5ByCntPty"
        key = random.choice( self._cntrptys )
        tic = get_time()
        try:
            # print( inarg )
            cursor = self._db.find( { 'TO_ACCT_NUM' : key } ).sort( 'REQ_TS', pymongo.DESCENDING ).limit( QUERY_LIMIT )
            # cursor.next()
            # events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=1 )
            result = list(cursor)
            # print( result[0] )
            # tic = get_time()
            events.request_success.fire(request_type="MongoTest", name="%s.cursor" % (name), response_time=delta_time(tic), response_length=len(result))
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)

# if launched directly, e.g. "python3 debugging.py", not "locust -f debugging.py"
if __name__ == "__main__":
    run_single_user(MongoTest)
