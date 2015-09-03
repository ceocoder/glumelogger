package glumelogger

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"testing"
	"time"
)

const (
	testHostAndPort = "localhost:51515"
)

type thriftSourceProtocolHandler struct {
	t *testing.T
}

func (ts thriftSourceProtocolHandler) AppendBatch(events []*ThriftFlumeEvent) (Status, error) {
	for _, event := range events {
		ts.Append(event)
	}
	return Status_OK, nil
}

func (ts thriftSourceProtocolHandler) Append(event *ThriftFlumeEvent) (Status, error) {
	ts.t.Log(string(event.GetBody()))
	return Status_OK, nil
}

func runDummyFlumeAgent(t *testing.T) {
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTCompactProtocolFactory()
	transport, _ := thrift.NewTServerSocket("localhost:51515")

	handler := thriftSourceProtocolHandler{t}
	processor := NewThriftSourceProtocolProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)
	server.Serve()
}

func TestLogToFlumeViaThrift(t *testing.T) {
	// this call should return a new glumelogger instance
	go runDummyFlumeAgent(t)

	time.Sleep(1 * time.Millisecond)

	logger := NewGlumeLogger("localhost", 51515, &map[string]string{})
	status, err := logger.Log([]byte("foo"))
	if err != nil {
		t.Errorf("Write failed with %s, %s", status.String(), err)
		t.Fail()
	}
}
