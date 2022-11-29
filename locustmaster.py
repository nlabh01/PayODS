import traceback
import time
from locust import User, TaskSet, task, between, events, run_single_user
from time import sleep
from pymongo import MongoClient, client_session, read_preferences
import random

import gevent
_ = gevent.monkey.patch_all()

def get_time( ):
    return time.perf_counter_ns()

def delta_time( start ):
    return (time.perf_counter_ns() - start)/1E06

class MongoTest(User):
    # wait_time = between( 0, 0 )
    _mongo = None
    _db = None
    _titles = []
    _release_years = []
    _keys = []

    def audit(self, type, msg):
        self._mongo.locust.audit.insert_one( {"type":type, "ts":time.time(), "msg":str(msg)} )

    def on_start(self):
        self._mongo = MongoClient("mongodb+srv://charlielittle:mongodb@sa-shared-demo.lbvlu.mongodb.net/myFirstDatabase?retryWrites=true&w=majority", compressors="snappy")
        print( list(self._mongo.list_databases())[:1] )
        self._db = self._mongo.sample_mflix.movies
        print( self._db.find_one()['title'] )
        print( "User started..." )
        self._titles = list(self._db.distinct('title'))
        self._release_years = list(self._db.distinct('year'))
        aggregation = [
            { '$unwind': '$directors' }
            ,{ '$limit': 100 }
            ,{ '$group': { '_id': '$directors'} }
        ]
        self._directors = list(self._db.aggregate( [{ '$unwind' : '$directors' }, { '$group': { '_id': '$directors'} }, 
        { '$project' : { '_id':1} } ]))
        self._directors = list( map( lambda x: x['_id'], self._directors ))
        print( len(self._directors), ', '.join( self._directors[1:5] ) )
        self._keys = list( self._db.find({}, { '_id': 1 } ) )
        print( 'keys: ', len(self._keys))
        # return super().on_start()

    def on_stop(self):
        print( "User stopped." )
        # return super().on_stop()

    @task(10)
    def doThis(self):
        name = "findOneByTitle"
        item = random.randrange( 0, len(self._titles) )
        tic = get_time()
        try:
            self._db.find_one( { 'title': self._titles[ item ] } )['title']
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0)
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)

    @task(2)
    def doThat(self):
        name = "findByPartialTitle"
        item = random.randrange( 0, len(self._titles) )
        tic = get_time()
        try:
            cursor = self._db.find( { 'title': { '$regex': self._titles[ item ].split(' ')[0] } } )
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
        name = "findSomeDirectors"
        idx = random.randrange( 0, len(self._directors)-10 )
        tic = get_time()
        try:
            inarg = self._directors[ idx : idx + 3 ]
            # print( inarg )
            cursor = self._db.find( { 'directors': { '$in': inarg } }, { 'title':1, 'directors':1, 'year':1 } )
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
        rp_coll = self._mongo.sample_mflix.get_collection( name='movies', read_preference=rp[1] )
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
            session = self._mongo.start_session( { 'readConcern': rc } )
            session.start_transaction()
            doc = self._db.find_one( { '_id': key['_id'] } )
            # print( doc['_id'] )
            events.request_success.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=1 )
            session.commit_transaction()
        except Exception as e:
            traceback.print_exc()
            events.request_failure.fire(request_type="MongoTest", name=name, response_time=delta_time(tic), response_length=0, exception=e)
            self.audit("exception", e)
            session.abort_transaction()        

# if launched directly, e.g. "python3 debugging.py", not "locust -f debugging.py"
if __name__ == "__main__":
    run_single_user(MongoTest)