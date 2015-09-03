

test:
	go test -v

gen: glumelogger.thrift
	thrift -r --gen go -o $(PWD) glumelogger.thrift

glumelogger.thirft:
	wget 'https://raw.githubusercontent.com/apache/flume/trunk/flume-ng-sdk/src/main/thrift/flume.thrift' -O glumelogger.thrift
