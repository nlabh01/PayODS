from datetime import datetime
import os
import sys
from urllib import response

os.environ["GEVENT_SUPPORT"] = "True"

from enum import Enum
from multiprocessing import Semaphore
from dateutil.parser import parse

import math
import traceback
import time

from locust import User, TaskSet, task, between, events, run_single_user, tag
from locust.runners import MasterRunner

import gevent
_ = gevent.monkey.patch_all()

import pymongo
from pymongo import MongoClient, ReadPreference, read_preferences
import random

# _url = "mongodb://vmpaymongoel01:27017,vmpaymongoel02:27017,vmpaymongoel03:27017/?replicaSet=PayOdsRs0"
_url = "mongodb+srv://charlielittle:mongodb@payments-ods.7x4ag.mongodb.net/ods?retryWrites=true&w=majority&readPreference=secondaryPreferred&compressors=snappy"
# _url = "mongodb://charlielittle:mongodb@payments-ods-shard-00-00.7x4ag.mongodb.net:27016,payments-ods-shard-00-01.7x4ag.mongodb.net:27016,payments-ods-shard-00-02.7x4ag.mongodb.net:27016,payments-ods-shard-00-03.7x4ag.mongodb.net:27016,payments-ods-shard-00-04.7x4ag.mongodb.net:27016,payments-ods-shard-01-00.7x4ag.mongodb.net:27016,payments-ods-shard-01-01.7x4ag.mongodb.net:27016,payments-ods-shard-01-02.7x4ag.mongodb.net:27016,payments-ods-shard-01-03.7x4ag.mongodb.net:27016,payments-ods-shard-01-04.7x4ag.mongodb.net:27016/?ssl=true&authSource=admin&readPreference=secondaryPreferred&compressors=snappy"


mongo = MongoClient(_url)
#_db = mongo.payodsdb.get_collection( 'poc', read_preference=ReadPreference.NEAREST )
_db = mongo.get_database("ods").get_collection( 'poc', read_preference=ReadPreference.NEAREST )
_fr_accounts = []
_to_accounts = []
_ptyid = []
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
        print (_db)
        doc = _db.find_one()
        print( doc )
        print( "User started..." )
        print( "Loading accounts..." )
#        account_meta = list( _db.find( ).limit(1000))
        # account_meta = _db.distinct( "FR_ACCT_NUM" )
        # account_meta = list( _db.aggregate( [
        #     { '$group' : 
        #       { '_id' : '$FR_ACCT_NUM',
        #         'FR_BOA_PTY_ID' : { '$first' : '$FR_BOA_PTY_ID' },
        #         'TO_ACCT_NUM' : { '$addToSet': '$TO_ACCT_NUM' }
        #       }
        #     },
        #     { '$project': { '_id':0, 'FR_ACCT_NUM':'$_id', 'FR_BOA_PTY_ID':1, 'TO_ACCT_NUM':1 } }
        # ] ) )
        account_meta = list( mongo.get_database("ods").get_collection( 'from_to' ).find() )
        print( f"First item in test accounts: {account_meta[0]['FR_ACCT_NUM']}" )
        global _fr_accounts
        # _fr_accounts = list( map( lambda fr_acct: fr_acct['FR_ACCT_NUM'], account_meta ) )
        _fr_accounts = account_meta

        # global _to_accounts
        # _to_accounts = list( map( lambda to_acct: to_acct['TO_ACCT_NUM'], account_meta ) )
        
        global _ptyid
        # _ptyid = list( map( lambda ptyid: ptyid['FR_BOA_PTY_ID'], account_meta ) )
        # _ptyid =  _db.distinct( "FR_BOA_PTY_ID" )
        _ptyid = list( mongo.get_database("ods").get_collection( 'parties' ).find() )
        
        print( f'_fr_accounts count: {len( _fr_accounts )}' )
        print( f'_to_accounts count: {len( _to_accounts )}' )
        print( f'_ptyid count: {len( _ptyid )}' )

        print( "Loading keys..." )
        
        global _keys
        _keys = list( _db.find( {}, { '_id':0, PRIMARY_KEY: 1 } ).skip(random.randint(0, 10000000)).limit(1000000) )
        print( 'keys: ', len( _keys ) )
        fr_acct = random.choice( _fr_accounts )[ 'FR_ACCT_NUM' ]
        print( "acct to test: " + fr_acct )
        print( f"ID : {_db.find_one( { 'FR_ACCT_NUM' : fr_acct }, { PRIMARY_KEY: 1 } )} " )
        to_acct = random.choice( random.choice( _fr_accounts )['TO_ACCT_NUM' ] )
        print( f"TO_ACCT_NUM : {to_acct}" )
        semaphore.release()
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
        self._fr_accounts = _fr_accounts
        self._to_accounts = _to_accounts
        self._ptyid = _ptyid

        semaphore.release()
        print( "User initialized" )

    def audit(self, type, msg):
        self._mongo.locust.audit.insert_one( {"type":type, "ts":time.time(), "msg":str(msg)} )

    def on_start(self):
        print( 'Running user on_start()')

    def on_stop(self):
        print( "User stopped." )

    @tag( "ID" )
    @task(50)
    def findOneByKey(self):
         name = "findOneByKey"
        #  print( "I am in @task(10)." )
         key = random.choice( self._keys )
         #print( f"findOneByKey {key}" )
         tic = get_time()
         try:
             # print( "key: " + self._keys[ item ]["$oid"] )
             doc = self._db.find_one( key )
            #  print(doc)
             response_time = delta_time(tic)
             events.request_success.fire(request_type="MongoTest", name=name, response_time=response_time, response_length=len(doc))
             if response_time >= 500:
                print(f"Request {name} {response_time} ms -- {key}", file=sys.stderr)
         except Exception as e:
             traceback.print_exc()
             events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
             self.audit("exception", e)

    @tag( "ACCOUNT" )
    @task(20)
    def findRecentByStatus(self):
        name = "findRecentByStatus"
        acct =  random.choice(_fr_accounts)['FR_ACCT_NUM']
        #print( f"findRecentByStatus {acct}" )
        tic = get_time()
        try:
            query = { 'FR_ACCT_NUM': acct, 'REQ_TS': { '$gte': fromisoformat( "2022-08-01" ) }, 'XFER_CD':'E' }
            cursor = self._db.find( query ).limit( QUERY_LIMIT )
            doc = next( cursor, None )
            #print(doc)
            if( doc ):
                # events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=1)
                # tic = get_time()
                length = len(list(cursor))
                #print("number of rec returned = ", length)
                response_time=delta_time(tic)
                events.request_success.fire(request_type="MongoTest", name="%s.cursor" % (name), response_time=response_time, response_length=length)
                if response_time >= 500:
                    print(f"Request {name} {response_time} ms -- {query}", file=sys.stderr)
            else:
                events.request_success.fire(request_type="MongoTest", name="%s.NoMatch" % (name), response_time=delta_time(tic), response_length=0)
        except Exception as e:
            print( f"ERROR: acct={acct}" )
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)

    @tag( "ACCOUNT" )
    @task(20)
    def findLargestByAccount(self):
        name = "findLargestByAccount"
        acct =  random.choice(_fr_accounts)['FR_ACCT_NUM']
        #print( f"findLargestByAccount {acct}" )
        tic = get_time()
        try:
            # print( inarg )
            query = { 'FR_ACCT_NUM': acct, 'REQ_TS' : { '$gt' : fromisoformat( '2022-08-14' ) } }
            sortFields = [ ('XFER_AM', pymongo.DESCENDING ) ]
            cursor = self._db.find( query ).sort( sortFields ).limit(5)
            # cursor.next()
            result = list(cursor)
            length = len(result)
            response_time=delta_time( tic )
            events.request_success.fire(request_type="MongoTest", name="%s.cursor" % (name), response_time=response_time, response_length=length)
            if response_time >= 500:
                print(f"Request {name} {response_time} ms -- query: {query} sort: {sortFields}", file=sys.stderr)
            #print("number of rec returned = ", length)
            # events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=len(result) )
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)
    
    @tag( "ACCOUNT" )
    @task(20)
    def findByChannel(self):
        name = "findByChannel"
        acct =  random.choice(_fr_accounts)['FR_ACCT_NUM']
        channel_cd = random.choices( population=[ 'BKA', 'CCC', 'IVR', 'Mobile', 'Online' ], k=3 )
        #print( f"findbychannel {channel_cd}" )
        tic = get_time()
        try:
            query = { "FR_ACCT_NUM": acct, "CHN_CD": { '$in' : channel_cd }, 'REQ_TS': { '$gte': fromisoformat( "2022-08-01" ) } }
            cursor = self._db.find( query ).limit( QUERY_LIMIT )
            length = len(list(cursor))
            response_time = delta_time(tic)
            if( length > 0 ):
                # events.request_success.fire(request_type="MongoTest", name=name, response_time=response_time, response_length=1)
                if response_time >= 500:
                    print(f"Request {name} {response_time} ms -- query: {query}", file=sys.stderr)
                #print("number of rec returned = ", length)
                events.request_success.fire(request_type="MongoTest", name="%s.cursor" % (name), response_time=response_time, response_length=length)
            else:
                events.request_success.fire(request_type="MongoTest", name="%s.NoMatch" % (name), response_time=delta_time(tic), response_length=0)
        except Exception as e:
            print( f"ERROR: acct={acct}" )
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)
         
    @tag( "PARTY" )
    @task(10)
    def findByPartyAndChannel(self):
          name = "findByPartyAndChannel"
          ptyid =  random.choice(_ptyid)
          channel_cd = random.choice([ 'BKA', 'CCC', 'IVR', 'Mobile', 'Online' ])
          #print( f"findbyparty {ptyid} {channel_cd}" )
          tic = get_time()
          try:
              query = { "FR_BOA_PTY_ID": ptyid, "CHN_CD": channel_cd, 'REQ_TS': { '$gte': fromisoformat( "2022-08-01" ) } }
              cursor = self._db.find( query ).limit( QUERY_LIMIT )
             
              doc = next( cursor, None )
              #print(doc)
              if( doc ):
                  length = len(list(cursor))
                  response_time = delta_time(tic)
                #   events.request_success.fire(request_type="MongoTest", name=name, response_time=response_time, response_length=1)
                  if response_time >= 500:
                      print(f"Request {name} {response_time} ms -- query: {query}", file=sys.stderr)
                  #print("number of rec returned = ", length)
                  events.request_success.fire(request_type="MongoTest", name="%s.cursor" % (name), response_time=response_time, response_length=length)
              else:
                  events.request_success.fire(request_type="MongoTest", name="%s.NoMatch" % (name), response_time=delta_time(tic), response_length=0)
          except Exception as e:
              print( f"ERROR: ptyid={ptyid}" )
              traceback.print_exc()
              events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
              self.audit("exception", e)     

    @tag( "ACCOUNT" )
    @task(30)
    def findRecentByAccount(self):
          name = "findRecentByAccount"
          acct =  random.choice(_fr_accounts)['FR_ACCT_NUM']
          tic = get_time()
          try:
              query = { "FR_ACCT_NUM": acct, "REQ_TS": { "$gte" : fromisoformat( "2022-08-01" ) } }
              cursor = self._db.find( query ).sort( "REQ_TS", pymongo.DESCENDING ).limit( QUERY_LIMIT )
             
              doc = next( cursor, None )
              #print(doc)
              if( doc ):
                  length = len(list(cursor))
                  response_time = delta_time(tic)
                #   events.request_success.fire(request_type="MongoTest", name=name, response_time=response_time, response_length=1)
                  if response_time >= 500:
                      print(f"Request {name} {response_time} ms -- query: {query}", file=sys.stderr)
                  #print("number of rec returned = ", length)
                  events.request_success.fire(request_type="MongoTest", name="%s.cursor" % (name), response_time=response_time, response_length=length)
              else:
                  events.request_success.fire(request_type="MongoTest", name="%s.NoMatch" % (name), response_time=delta_time(tic), response_length=0)
          except Exception as e:
              print( f"ERROR: acct={acct}" )
              traceback.print_exc()
              events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
              self.audit("exception", e)     

# if launched directly, e.g. "python3 debugging.py", not "locust -f debugging.py"
if __name__ == "__main__":
    run_single_user(MongoTest)