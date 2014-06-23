A demo program used to learn how to work with Bolt key/value store.  
[![Build Status](https://travis-ci.org/billhathaway/boltDemo.svg?branch=master)](https://travis-ci.org/billhathaway/boltDemo)  

This implements a very simple message queue.  Operations are:
* list queues
* create queue 
* delete queue 
* send message 
* receive message(s)
* get queue info

The data is represented in Bolt by two categories of buckets: 
* "queues" - holds an object for each queue 
* "$queuename-messages" - holds messages for each queue (one of these buckets per queue)
 
  
Paths served:
--
```
  /queues                GET lists all queues and their stats
 
  /queue/$name           POST creates the queue 
  /queue/$name           DELETE deletes the queue
  /queue/$name           GET retrieves statistics for the queue
  /queue/$name/messages  GET retrieves up to 10 messages
  /queue/$name/messages  POST sends a message with the content from the "message" parameter

```

Example session
--
```
# list queues (none should exist if using a fresh db)
curl "http://localhost:9999/queues"

# create two queues
curl -X POST "http://localhost:9999/queue/q1"
curl -X POST "http://localhost:9999/queue/q2"

# list queues, should now see two
curl "http://localhost:9999/queues"

# send some messages
curl -X POST --data message=hello1 "http://localhost:9999/queue/q1/messages"
curl -X POST --data message=hello2 "http://localhost:9999/queue/q1/messages"

# receive the messages
curl "http://localhost:9999/queue/q1/messages"

# send one more message
curl -X POST --data message=hello3 "http://localhost:9999/queue/q1/messages"

# list queues to show the stats are updated
curl "http://localhost:9999/queues"

# delete a queue 
curl -X DELETE "http://localhost:9999/queues/q1"

# list queues to show only q2 is remaining
curl "http://localhost:9999/queues"

```

