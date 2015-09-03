package glumelogger

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/ceocoder/glumelogger/flume"
	"testing"
	"time"
)

const (
	testHostAndPort = "localhost:51515"
)

type thriftSourceProtocolHandler struct {
	t *testing.T
	//TODO: start the handler with expected event values - fail if writes are different from expected
}

func (ts thriftSourceProtocolHandler) AppendBatch(events []*flume.ThriftFlumeEvent) (flume.Status, error) {
	for _, event := range events {
		ts.Append(event)
	}
	return flume.Status_OK, nil
}

func (ts thriftSourceProtocolHandler) Append(event *flume.ThriftFlumeEvent) (flume.Status, error) {
	ts.t.Log(string(event.GetBody()))
	return flume.Status_OK, nil
}

// run a test flume agent
func runDummyFlumeAgent(t *testing.T) {
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTCompactProtocolFactory()
	transport, _ := thrift.NewTServerSocket("localhost:51515")

	handler := thriftSourceProtocolHandler{t}
	processor := flume.NewThriftSourceProtocolProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)
	server.Serve()
}

func TestLogToFlumeViaThrift(t *testing.T) {
	go runDummyFlumeAgent(t)

	time.Sleep(1 * time.Millisecond)

	logger := NewGlumeLogger("localhost", 51515, &map[string]string{})
	status, err := logger.Log([]byte("foo"))
	if err != nil {
		t.Errorf("Write failed with %s, %s", status.String(), err)
		t.Fail()
	}
}
