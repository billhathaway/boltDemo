#!/bin/sh

echo "Building the demo"
go build

echo "Running the server in background"
./boltDemo -db data/demo.db -e  &

sleep 1

echo "listing queues (none should be present)"
curl "http://localhost:9999/queues"
echo

echo "creating two queues"
curl -X POST "http://localhost:9999/queue/q1"
curl -X POST "http://localhost:9999/queue/q2"
echo

echo "listing queues (should be two now)"
curl "http://localhost:9999/queues"
echo

echo "sending two messages to queue q1"
curl -X POST --data message=hello1 "http://localhost:9999/queue/q1/messages"
curl -X POST --data message=hello2 "http://localhost:9999/queue/q1/messages"
echo

echo "receiving the messages"
curl "http://localhost:9999/queue/q1/messages"
echo

echo "sending one more message"
curl -X POST --data message=hello3 "http://localhost:9999/queue/q1/messages"
echo

echo "listing the queues to show the stats"
curl "http://localhost:9999/queues"
echo

echo "deleting queue q1"
curl -X DELETE  "http://localhost:9999/queue/q1"
echo

echo "listing queues (should now only be q2)"
curl "http://localhost:9999/queues"
echo

echo "killing the server"
pkill -f boltDemo
