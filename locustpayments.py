import os

os.environ["GEVENT_SUPPORT"] = "True"

from enum import Enum
from multiprocessing import Semaphore
from dateutil.parser import parse

import math
import traceback
import time
import datetime
from datetime import date, datetime, timedelta
import csv

from locust import User, TaskSet, task, between, events, run_single_user
from locust.runners import MasterRunner

import gevent
_ = gevent.monkey.patch_all()

import pymongo
from pymongo import MongoClient, ReadPreference, read_preferences
import random

_url = "mongodb+srv://charlielittle:mongodb@payments-ods.7x4ag.mongodb.net/ods?retryWrites=true&w=majority&readPreference=nearest&compressors=snappy"

# DATE_FIELD = "TRAN_DT"
DATE_FIELD = "REQ_TS"
# STAT_CD_FIELD = "STAT_CD"
STAT_CD_FIELD = "XFER_CD"

# STAT_CD_VALUES = ["Completed", "Suspended", "Failed"]
STAT_CD_VALUES = [ 'A', 'B', 'C', 'D', 'E', 'F' ]


START_DATE = "2022-05-15T11:02:17.789743"
END_DATE =   "2022-06-15T11:02:17.789743"

USE_ISODATE = True

def get_date( date=datetime.now() ) :
    if( USE_ISODATE ): return parse( date )
    else: return date

# DB_NAME = "payodsdb"
DB_NAME = "ods"

COLLECTION_NAME = "poc"

LIMIT = 100

mongo = MongoClient(_url,)
_db = mongo.get_database( DB_NAME ).get_collection( COLLECTION_NAME, read_preference=ReadPreference.NEAREST )
_fr_accounts = []
_to_accounts = []
_ptyids = []
# _cntrptys = []

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
        
        # filepath ="/home/"
        # partytoaccountfile = filepath + "party_account.csv"
        # with open(partytoaccountfile ,"r") as acctptyfile:
        #     reader = csv.reader(acctptyfile, delimiter = ",")
        #     next(reader)
        #     l_list = list(reader)
        #     lmax, lmin = [],[] 
        #     lmin = l_list[0]
        #     lmax = l_list[len(l_list) - 1]
        #     maxAcctnum = int(lmax[1])
        #     maxpartyid = int(lmax[2])
        #     minAcctnum = int(lmin[1])
        #     minpartyid = int(lmin[2])
       
        # acctptyfile.close()
        global _fr_accounts
        _fr_accounts = list( mongo.get_database( DB_NAME ).get_collection( "accounts" ).find() )
        global _to_accounts
        _to_accounts = list( mongo.get_database( DB_NAME ).get_collection( "to_accounts" ).find() )
        global _ptyids
        _ptyids = list( mongo.get_database( DB_NAME ).get_collection( "parties" ).find() )
        minAcctnum = 0 # position zero in the _fr_accounts array
        maxAcctnum = len( _fr_accounts ) - 1

        minpartyid = 0
        maxpartyid = len( _ptyids ) - 1
        print( f"account list sizes: {len(_fr_accounts)} / {len(_to_accounts)} / {len(_ptyids)}" )

        dt_array = []
        end_date =  datetime.now() - timedelta(days =1)
        start_date = datetime.now() - timedelta(days =180)

        while start_date < end_date:
            trandt = start_date.isoformat()
            dt_array.append(trandt)
            start_date += timedelta(days=1)
        
        global _maxAcctnum
        _maxAcctnum = maxAcctnum
        
        global _minAcctnum 
        _minAcctnum = minAcctnum
        
        global _minpartyid
        _minpartyid = minpartyid
        
        global _maxpartyid
        _maxpartyid = maxpartyid
        
        global _dt_array
        _dt_array = dt_array

        semaphore.release();
        print( "COMPLETING Locust INIT event")

class MongoTest(User):
    # wait_time = between( 0, 0 )
    def __init__(self, environment):
        super().__init__(environment)

        print( "User aquiring semaphore" )
        semaphore.acquire()

        self._mongo = mongo
        self._db = _db
        self._maxAcctnum = _maxAcctnum
        self._minAcctnum = _minAcctnum
        self._maxpartyid = _maxpartyid
        self._minpartyid = _minpartyid
        #self._ptyid = _ptyid

        semaphore.release()
        print( "User initialized" )

    def audit(self, type, msg):
        self._mongo.locust.audit.insert_one( {"type":type, "ts":time.time(), "msg":str(msg)} )

    def on_start(self):
        print( 'Running user on_start()')

    def on_stop(self):
        print( "User stopped." )
        
  
    @task(20)
    def doThis(self):
        name = "findByAcctNumTrandt"
        # print (name)
        # Acc_no = str(random.randint(self._minAcctnum, self._maxAcctnum))
        Acc_no = _fr_accounts[ random.randint(self._minAcctnum, self._maxAcctnum) ]['_id']
        # print ("Selected Account Number is - ", Acc_no)
        
        try:
            tic = get_time()
            i=0
            query = {
                "$and":[{ 
                        "FR_ACCT_NUM": { "$eq": Acc_no}
                         }, 
                        {DATE_FIELD:
                            {
                               "$gte": get_date(START_DATE),
                               "$lte": get_date(END_DATE)
                             }
                            } ]
                    }
            cursor = self._db.find( query ).limit( LIMIT )
                
            i = len( list( cursor ) )

            # print ('number of records returned = ', i)
            # if( i == 0 ) :
            #     print( query )
            
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=i)

        except Exception as e:
             traceback.print_exc()
             events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
             self.audit("exception", e)
    
    @task(5)
    def doThat(self):
        name = "findByCNTRPartyidTrandt"
        # print (name)
        
        # Cnt_Pty_Acc_no =  str(random.randint(self._minAcctnum, self._maxAcctnum))
        Cnt_Pty_Acc_no =  _to_accounts[ random.randint(0, len( _to_accounts )-1) ]['_id']
        
        # print ("Selected Counter Party Account Number is - ", Cnt_Pty_Acc_no)
        
        try:
            tic = get_time()
            i=0
            cursor = self._db.find({
                "$and":[{ 
                        "TO_ACCT_NUM": { "$eq": Cnt_Pty_Acc_no}
                         }, 
                        {DATE_FIELD:
                            {
                               "$gte": get_date(START_DATE),
                               "$lte": get_date(END_DATE)
                             }
                         }]
                    }).limit( LIMIT )
                
            i = len( list( cursor ) )

            # print ('number of records returned = ', i)
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=i)

        except Exception as e:
             traceback.print_exc()
             events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
             self.audit("exception", e)

    @task(15)
    def doMore(self):
        name = "findByPartyIDTrandt"
        # print (name)

        # Pty_ID =   str(random.randint(self._minpartyid, self._maxpartyid))
        Pty_ID =   _ptyids[ random.randint(self._minpartyid, self._maxpartyid) ]['_id']
        # print ("Selected Party ID Number is - ", Pty_ID)
        
        try:
            tic = get_time()
            i=0
            cursor = self._db.find({
                "$and":[{ 
                        "FR_BOA_PTY_ID": { "$eq": Pty_ID}
                         }, 
                        {DATE_FIELD:
                            {
                               "$gte": get_date(START_DATE),
                               "$lte": get_date(END_DATE)
                             }
                         }]
                    }).limit( LIMIT )
                
            i = len( list( cursor ) )
               
            # print ('number of records returned = ', i)
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=i)

        except Exception as e:
             traceback.print_exc()
             events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
             self.audit("exception", e)
    
    @task(10)
    def doThatalso(self):
        name = "findbyAcctNumChannel"
        # print (name)
        
        # Acc_no = str(random.randint(self._minAcctnum, self._maxAcctnum))
        Acc_no = _fr_accounts[ random.randint(self._minAcctnum, self._maxAcctnum) ]['_id']
        # print ("Selected Account Number is - ", Acc_no)
        chn_cd = random.choice(["CCC","Mobile" , "Online", "BKA", "IVR" ])
        # print ("Selected delivery channel Code is - ", chn_cd)
        
        try:
            tic = get_time()
            i=0
            cursor = self._db.find({
                "$and":[{ 
                        "FR_ACCT_NUM": { "$eq": Acc_no}
                         }, 
                        {DATE_FIELD:
                            {
                               "$gte": get_date(START_DATE),
                               "$lte": get_date(END_DATE)
                             }
                         },
                        {"CHN_CD": { "$eq": chn_cd} }
                    ]
                    }).limit( LIMIT )
                
            i = len( list( cursor ) )
                
            # print ('number of records returned = ', i)
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=i)

        except Exception as e:
             traceback.print_exc()
             events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
             self.audit("exception", e)            
      
    @task(5)
    def doThisalso(self):
        name = "findbyPartyidStatus"
        # print (name)
        
        # Pty_ID =   str(random.randint(self._minpartyid, self._maxpartyid))
        Pty_ID =   _ptyids[ random.randint(self._minpartyid, self._maxpartyid) ]['_id']
        # print ("Selected Party ID Number is - ", Pty_ID)
        stat_cd = random.choice( STAT_CD_VALUES )
        # print ("Selected Status Code is - ", stat_cd)
        
        try:
            tic = get_time()
            i=0
            cursor = self._db.find({
                "$and":[{ 
                        "FR_BOA_PTY_ID": { "$eq": Pty_ID}
                         },
                        {DATE_FIELD:
                            {
                               "$gte": get_date(START_DATE),
                               "$lte": get_date(END_DATE)
                             }
                         },
                        { STAT_CD_FIELD: { "$eq": stat_cd} }
                    ]
                    }).limit( LIMIT )
                
            i = len( list( cursor ) )
                
            # print ('number of records returned = ', i)
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=i)

        except Exception as e:
             traceback.print_exc()
             events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
             self.audit("exception", e)  
      

# if launched directly, e.g. "python3 debugging.py", not "locust -f debugging.py"
if __name__ == "__main__":
    run_single_user(MongoTest)
