// Package glumelogger provides support for Flume NG in Go
//
// It communicates to a Flume NG agent in Thrift via TFramedTranport with TCompactProtocol
// thrift bindings are taken from Flume 1.7[1] (however they should be compatible with 1.6, 1.5, 1.4)
//
// [1] https://raw.githubusercontent.com/apache/flume/flume-1.7/flume-ng-sdk/src/main/thrift/flume.thrift
package glumelogger

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/ceocoder/glumelogger/flume"
)

// GlumeLogger holds thrift client and headers
// use NewGlumeLogger to create new one
type GlumeLogger struct {
	client  *flume.ThriftSourceProtocolClient
	headers *map[string]string
	lm      *sync.Mutex
	log     *log.Logger
}

// NewGlumeLogger create a new GlumeLogger client, it requires a host, port and
// map of headers
func NewGlumeLogger(host string, port int, headers *map[string]string) *GlumeLogger {
	var trans thrift.TTransport
	trans, err := thrift.NewTSocket(net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		fmt.Fprintln(os.Stderr, "error resolving address:", err)
		os.Exit(1)
	}
	log.New(os.Stdout, "[StatsdClient] ", log.Ldate|log.Ltime)
	trans = thrift.NewTFramedTransport(trans)
	client := flume.NewThriftSourceProtocolClientFactory(trans, thrift.NewTCompactProtocolFactory())
	log.New(os.Stdout, "[StatsdClient] ", log.Ldate|log.Ltime)
	return &GlumeLogger{client, headers, &sync.Mutex{}, log.New(os.Stdout, "[StatsdClient] ", log.Ldate|log.Ltime)}
}

// Log forwards message to be logged as array of bytes
// returns status of write and error
//
// append operation is wrapped in a mutex making it thread-safe.
func (l *GlumeLogger) Log(body []byte) (flume.Status, error) {

	event := &flume.ThriftFlumeEvent{*l.headers, body}

	l.lm.Lock()
	defer l.lm.Unlock()
	if !l.client.Transport.IsOpen() {
		l.client.Transport.Open()
	}
	status, err := l.client.Append(event)
	if err != nil {
		// close bad transport proactively for the next write
		l.client.Transport.Close()
		return status, err
	}

	return status, nil
}
