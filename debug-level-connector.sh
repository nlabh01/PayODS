#/bin/bash
curl -s -X PUT -H "Content-Type:application/json" http://localhost:8083/admin/loggers/org.mongodb.driver -d '{"level":"INFO"}' | jq '.'
curl -s -X PUT -H "Content-Type:application/json" http://localhost:8083/admin/loggers/org.mongodb.driver.operation -d '{"level":"DEBUG"}' | jq '.'

curl -s -X PUT -H "Content-Type:application/json" http://localhost:8083/admin/loggers/com.mongodb.kafka.connect.sink.MongoSinkTask -d '{"level":"INFO"}' | jq '.'
