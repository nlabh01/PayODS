from enum import Enum
from multiprocessing import Semaphore
# from datetime import datetime
from dateutil.parser import parse

import math
import traceback
import time

import pymongo
from locust import User, TaskSet, task, between, events, run_single_user
from locust.runners import MasterRunner
# from time import sleep
from pymongo import MongoClient, ReadPreference, read_preferences
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
_db = mongo.ods.get_collection( 'payments2', read_preference=ReadPreference.NEAREST )
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

@events.init.add_listener

def on_locust_init( environment, **kwargs ):
    if isinstance(environment.runner, MasterRunner):
        print("I'm on master node")
    else:
        print("I'm on a worker or standalone node")
        print( "Acquiring semaphore to synchronize on_locust_init before clients are launched" )

        semaphore.acquire()

        print( list( mongo.list_databases()) )
        doc = _db.find_one()
        print( doc['clnt_acct_no'] )
        print( "User started..." )
        print( "Loading accounts..." )
        # _accounts = list(self._db.distinct('clnt_acct_no'))
        doc = mongo.ods.accounts.find_one()
        print( doc['_id'] )
        global _accounts
        _accounts = list( map( lambda acct: acct['clnt_acct_no'], doc['accounts'] ) )
        print( "Loading keys..." )
        global _keys
        _keys = list( _db.find( {}, { '_id': 0, 'clnt_acct_no':1, 'pmt_id': 1 } ).limit(1000) )
        print( 'keys: ', len( _keys ) )
        acct = random.choice( _accounts )
        print( "acct to test: " + acct )
        print( f"pmt_id : {_db.find_one( { 'clnt_acct_no' : acct } )['pmt_id']}" )
        global _cntrptys
        _cntrptys = list(_db.aggregate( [ 
            {
                '$match' : { 'clnt_acct_no': acct }
            },
            {
                '$group' : {
                    '_id' : { 'clnt_acct_no':'$clnt_acct_no', 'cnt_pty_acct_no':'$cnt_pty_acct_no'}
                }
            },
            { '$replaceRoot' : { 'newRoot': '$_id' } }
        ] ))
        print( 'First counterparty: ' + str( _cntrptys[0] ))
        print( f'Counterparty count: {len( _cntrptys )}' )
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
    def doThis(self):
        name = "findOneByKey"
        key = random.choice( self._keys )
        tic = get_time()
        try:
            # print( "key: " + self._keys[ item ]["$oid"] )
            doc = self._db.find_one( key )
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=len(doc))
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)

    @task(20)
    def doThat(self):
        name = "findRecentByStatus"
        acct = random.choice( self._accounts )
        tic = get_time()
        try:
            cursor = self._db.find( { 'clnt_acct_no': acct, 'payment_status.pmt_stat_id':'CLEARED', 'tran_dt': {'$gte': fromisoformat('2022-06-20T07:16:54.000+00:00')} } ).sort('tran_dt', pymongo.DESCENDING).limit(5)
            doc = next( cursor, None )
            if( doc ):
                events.request_success.fire(request_type="MongoTest", name="%s.cursor" % (name), response_time=delta_time(tic), response_length=0)
                length = len(list(cursor))
                events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=length)
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)

    @task(10)
    def doMore(self):
        name = "findLargestByAccount"
        tic = get_time()
        try:
            # print( inarg )
            cursor = self._db.find( { 'clnt_acct_no': self._accounts[0] } ).sort( 'pmt_am', pymongo.DESCENDING ).limit(5)
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
    def doTxnKeyQuery(self):
        name = "findTxnByAcct"
        key = random.choice( self._accounts )
        # print( key )
        rc = random.choice( [ 'majority', 'local', 'available', 'linearizable', 'snapshot' ])
        name += '.' + rc
        tic =  get_time()
        try:
            with self._mongo.start_session( { 'readConcern': rc } ) as session:
                with session.start_transaction():
                    doc = self._db.find_one( key )
                    # print( doc['_id'] )
                    events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=1 )
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)
            # session.abort_transaction()        

    @task(20)
    def doEven(self):
        name = "findLast5ByCntPty"
        key = random.choice( self._cntrptys )
        tic = get_time()
        try:
            # print( inarg )
            cursor = self._db.find( key ).sort( 'tran_dt', pymongo.DESCENDING ).limit(5)
            cursor.next()
            events.request_success.fire(request_type="MongoTest", name="%s.cursor" % (name), response_time=delta_time(tic), response_length=0)
            result = list(cursor)
            # print( result[0] )
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=len(result) )
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)

# if launched directly, e.g. "python3 debugging.py", not "locust -f debugging.py"
if __name__ == "__main__":
    run_single_user(MongoTest)
