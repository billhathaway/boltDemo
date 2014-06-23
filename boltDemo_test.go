package main

import (
	//"encoding/json"
	"encoding/gob"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	baseURL = "http://localhost:9999/"
)

func init() {
	go startServer(os.TempDir()+"/test.db", "9999", true)
	time.Sleep(time.Second)
}

// validateResponse validates the following conditions: err is nil, statusCode is 200, the lookFor string is found in the response body
// if any of the conditions are not true, we fail the test
func validateResponse(t *testing.T, response *http.Response, err error, lookFor string, message string) {
	if err != nil {
		t.Fatalf("%s - %s\n", message, err.Error())
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		t.Fatalf("%s - received invalid status code %d\n", message, response.StatusCode)
	}
	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("%s - error reading response body %s\n", message, err.Error())
	}
	if !strings.Contains(string(content), lookFor) {
		t.Fatalf("%s - looking for %q but did not find it response=%q\n", message, lookFor, content)
	}
	t.Logf("completed %s", message)

}

func TestQueueOperations(t *testing.T) {

	client := &http.Client{}
	httpResponse, err := client.PostForm(baseURL+"queue/q1", nil)
	validateResponse(t, httpResponse, err, "created queue", "creating queue")

	messageAttr := url.Values{"message": []string{"content"}}
	httpResponse, err = client.PostForm(baseURL+"queue/q1/messages", messageAttr)
	validateResponse(t, httpResponse, err, "msgId", "sending message")

	httpResponse, err = client.Get(baseURL + "queue/q1")
	validateResponse(t, httpResponse, err, `"MessageCount":1`, "get queue info - 1 message")

	httpResponse, err = client.Get(baseURL + "queue/q1/messages")
	validateResponse(t, httpResponse, err, `"data":"content`, "receive message")

	httpResponse, err = client.Get(baseURL + "queue/q1")
	validateResponse(t, httpResponse, err, `"MessageCount":0`, "get queue info - 0 messages")

}

func BenchmarkGobMessage(b *testing.B) {
	m := Message{Id: strconv.FormatInt(time.Now().UnixNano(), 10), Data: "small value"}
	encoder := gob.NewEncoder(ioutil.Discard)
	var err error
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = encoder.Encode(m); err != nil {
			b.Error("problem encoding", err)
		}
	}
}

func BenchmarkJSONMessage(b *testing.B) {
	m := Message{Id: strconv.FormatInt(time.Now().UnixNano(), 10), Data: "small value"}
	encoder := json.NewEncoder(ioutil.Discard)
	var err error
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = encoder.Encode(m); err != nil {
			b.Error("problem encoding", err)
		}
	}
}
