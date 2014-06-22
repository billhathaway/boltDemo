/*
demo program for boltdb

One bucket "queues" is used to store metadata about queues
Each queue has another bucket $queue-messsages that store messages for the each queue

Paths served:
  /queues               GET lists all queues and their stats
  /queues               POST creates a queue with the name provided in the "queue"" parameter


  /queue/$queuename      GET retrieves up to 10 messages
  /queue/$queuename      POST sends a message with the content from the "message" parameter
  /queue/$queuename      DELETE deletes the queue
  /queue/$queuename/info GET retrieves statistics for the queue

*/

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"

	"github.com/boltdb/bolt"
)

const (
	dbPermission  = 0666
	defaultPort   = "9999"
	defaultDBPath = "data/queues.db"
	maxMessages   = 10
)

type (
	Response struct {
		Status  string `json:"status"`
		Message string `json:"message,omitempty"`
	}

	InfoResponse struct {
		Status string `json:"status"`
		Queue  *Queue `json:"queue"`
	}

	ListResponse struct {
		Status string            `json:"status"`
		Queues map[string]*Queue `json:"queues"`
	}

	ReceiveResponse struct {
		Status   string `json:"status"`
		Message  string `json:"message,omitempty"`
		Messages []Message
	}

	Message struct {
		Id   string `json:"id"`
		Data string `json:"data"`
	}

	Queue struct {
		sync.Mutex
		Created          time.Time
		MessageCount     int
		MessagesSent     int
		MessagesReceived int
	}
)

var (
	queueDB           *bolt.DB
	messageDB         *bolt.DB
	queueBucketName   = []byte("queues")
	queues            = make(map[string]*Queue)
	limitReachedError = errors.New("limitReached")
)

func queueInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	vars := mux.Vars(r)
	queueName := vars["queue"]
	queue, exists := queues[queueName]
	if !exists {
		w.WriteHeader(http.StatusBadRequest)
		encoder.Encode(Response{Status: "error", Message: "queue does not exist"})
		return
	}
	encoder.Encode(InfoResponse{Status: "ok", Queue: queue})
}

func receiveMessage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	vars := mux.Vars(r)
	queueName := vars["queue"]
	queue, exists := queues[queueName]
	if !exists {
		encoder.Encode(Response{Status: "error", Message: "queue does not exist"})
		return
	}
	messages := make([]Message, 0)
	queue.Lock()
	defer queue.Unlock()
	err := queueDB.Update(func(tx *bolt.Tx) error {
		messageBucket, err := tx.CreateBucketIfNotExists([]byte(queueName + "-messages"))
		if err != nil {
			return err
		}
		count := 0
		err = messageBucket.ForEach(func(k, v []byte) error {
			messages = append(messages, Message{string(k), string(v)})
			err := messageBucket.Delete(k)
			if err != nil {
				return err
			}
			count++
			if count >= maxMessages {
				return limitReachedError
			}
			return nil
		})
		return err
	})
	if len(messages) > 0 {
		queue.MessageCount -= len(messages)
		queue.MessagesReceived += len(messages)
		updateErr := queueDB.Update(func(tx *bolt.Tx) error {
			queueBucket, err := tx.CreateBucketIfNotExists(queueBucketName)
			if err != nil {
				return err
			}
			data, err := json.Marshal(queue)
			if err != nil {
				return err
			}
			return queueBucket.Put([]byte(queueName), data)
		})
		if updateErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			encoder.Encode(ReceiveResponse{Status: "error", Message: err.Error(), Messages: messages})
			log.Fatalf("unable to update queue record in DB queue=%s error=%s\n", queueName, updateErr.Error())
		}
	}

	if err != nil && err != limitReachedError {
		w.WriteHeader(http.StatusInternalServerError)
		encoder.Encode(ReceiveResponse{Status: "error", Message: err.Error(), Messages: messages})
		return
	}
	encoder.Encode(ReceiveResponse{Status: "ok", Messages: messages})

}
func sendMessage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	vars := mux.Vars(r)
	queueName := vars["queue"]
	queue, exists := queues[queueName]
	if !exists {
		w.WriteHeader(http.StatusBadRequest)
		encoder.Encode(Response{Status: "error", Message: "queue does not exist"})
		return
	}
	message := r.FormValue("message")
	if message == "" {
		w.WriteHeader(http.StatusBadRequest)
		encoder.Encode(Response{Status: "error", Message: "message must not be empty"})
		return
	}
	msgId := strconv.FormatInt(time.Now().UnixNano(), 10)
	err := queueDB.Update(func(tx *bolt.Tx) error {

		messageBucket, err := tx.CreateBucketIfNotExists([]byte(queueName + "-messages"))
		if err != nil {
			return err
		}
		err = messageBucket.Put([]byte(msgId), []byte(message))
		if err != nil {
			return err
		}
		queue.Lock()
		queue.MessageCount++
		queue.MessagesSent++
		queue.Unlock()
		return nil

	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		encoder.Encode(Response{Status: "error", Message: err.Error()})
		return
	}
	encoder.Encode(Response{Status: "ok", Message: fmt.Sprintf("received msgId=%s", msgId)})

}

func createQueue(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	vars := mux.Vars(r)
	queueName := vars["queue"]
	if queueName == "" {
		w.WriteHeader(http.StatusBadRequest)
		encoder.Encode(Response{Status: "error", Message: "queue parameter must not be empty"})
		return
	}
	if _, exists := queues[queueName]; exists {
		w.WriteHeader(http.StatusBadRequest)
		encoder.Encode(Response{Status: "error", Message: "a queue with that name already exists"})
		return
	}
	queue := Queue{Created: time.Now()}
	queues[queueName] = &queue
	err := queueDB.Update(func(tx *bolt.Tx) error {
		queueBucket, err := tx.CreateBucketIfNotExists(queueBucketName)
		if err != nil {
			return err
		}
		data, err := json.Marshal(queue)
		if err != nil {
			return err
		}
		return queueBucket.Put([]byte(queueName), data)
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		encoder.Encode(Response{Status: "error", Message: err.Error()})
		return
	}
	encoder.Encode(Response{Status: "ok", Message: "created queue " + queueName})
}

func deleteQueue(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	vars := mux.Vars(r)
	queueName := vars["queue"]
	if queueName == "" {
		w.WriteHeader(http.StatusBadRequest)
		encoder.Encode(Response{Status: "error", Message: "queue parameter must not be empty"})
		return
	}
	if _, exists := queues[queueName]; !exists {
		w.WriteHeader(http.StatusInternalServerError)
		encoder.Encode(Response{Status: "error", Message: "queue does not exists"})
		return
	}
	delete(queues, queueName)
	err := queueDB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(queueBucketName).Delete([]byte(queueName))

	})
	// TODO: should not return here, should also try to delete the message bucket as well
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		encoder.Encode(Response{Status: "error", Message: "deleting queue " + err.Error()})
		return
	}

	err = queueDB.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(queueName + "-messages"))
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		encoder.Encode(Response{Status: "error", Message: "deleting message bucket " + err.Error()})
		return
	}

	encoder.Encode(Response{Status: "ok"})
}
func listQueues(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.Encode(ListResponse{Status: "ok", Queues: queues})
}

func index(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, `
<html>
<head><title><Simple BoltDB example></title></head>
<body>
<h3>Simple BoltDB example</h3>
<li> /queues               GET lists all queues and their statsistics


<p>
<li> /queue/$queuename      POST creates the queue
<li> /queue/$queuename      DELETE deletes the queue
<li> /queue/$queuename/messages      GET retrieves up to 10 messages
<li> /queue/$queuename/messages      POST sends a message with the content from the "message" parameter
<li> /queue/$queuename/info GET retrieves statistics for the queue

`)
}

func signalHandler() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan)
	<-sigChan
	if queueDB != nil {
		log.Println("closing DB")
		queueDB.Close()
	}
	os.Exit(0)
}

func startServer(dbPath string, port string, ephemeral bool) error {
	var err error

	queueDB, err = bolt.Open(dbPath, dbPermission)
	if err != nil {
		log.Fatalf("Problem opening %s - %s\n", dbPath, err.Error())
	}
	go signalHandler()

	if ephemeral {
		os.Remove(dbPath)
	}

	err = queueDB.Update(func(tx *bolt.Tx) error {
		queueBucket, err := tx.CreateBucketIfNotExists([]byte(queueBucketName))
		if err != nil {
			panic(err)
		}
		return queueBucket.ForEach(func(k, v []byte) error {
			var queue Queue
			err = json.Unmarshal(v, &queue)
			if err != nil {
				return err
			}
			fmt.Printf("Loading queue %s\n", string(k))
			queues[string(k)] = &queue
			return nil
		})
	})
	if err != nil {
		return err
	}

	r := mux.NewRouter()

	r.HandleFunc("/queues", listQueues).Methods("GET")

	r.HandleFunc("/queue/{queue}", createQueue).Methods("POST")
	r.HandleFunc("/queue/{queue}", deleteQueue).Methods("DELETE")
	r.HandleFunc("/queue/{queue}", queueInfo).Methods("GET")
	r.HandleFunc("/queue/{queue}/messages", sendMessage).Methods("POST", "PUT")
	r.HandleFunc("/queue/{queue}/messages", receiveMessage).Methods("GET")

	r.HandleFunc("/", index)
	log.Printf("Listening on port %s\n", port)
	return http.ListenAndServe(":"+port, r)

}

func main() {
	dbPath := flag.String("db", defaultDBPath, "database file path")
	port := flag.String("p", defaultPort, "http port")
	ephemeral := flag.Bool("e", false, "remove DB on exit")
	flag.Parse()
	err := startServer(*dbPath, *port, *ephemeral)
	if err != nil {
		log.Fatal(err)
	}
}
