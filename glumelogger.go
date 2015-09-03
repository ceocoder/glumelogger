package glumelogger

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"git.apache.org/thrift.git/lib/go/thrift"
)

//holder for thrift client and headers
//use NewGlumeLogger to create new one
type GlumeLogger struct {
	client  *ThriftSourceProtocolClient
	headers *map[string]string
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
	trans = thrift.NewTFramedTransport(trans)
	client := NewThriftSourceProtocolClientFactory(trans, thrift.NewTCompactProtocolFactory())
	return &GlumeLogger{client, headers}
}

// message to be logged as array of bytes
// returns status of write and error
func (l *GlumeLogger) Log(body []byte) (Status, error) {

	event := &ThriftFlumeEvent{*l.headers, body}
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
