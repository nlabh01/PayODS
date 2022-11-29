#/bin/bash
curl -s -X PUT -H "Content-Type:application/json" http://localhost:8083/admin/loggers/org.mongodb.driver -d '{"level":"INFO"}' | jq '.'

