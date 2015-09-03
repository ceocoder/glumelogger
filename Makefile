

test:
	go test -v

gen: flume.thrift
	# a hack since --package_prefix is not working for me
	@rm -r flume && thrift -r --gen go -o $(PWD) flume.thrift && mv gen-go/flume . && rm -r gen-go flume/thrift_source_protocol-remote

flume.thrift:
	@wget 'https://raw.githubusercontent.com/apache/flume/flume-1.7/flume-ng-sdk/src/main/thrift/flume.thrift' -O flume.thrift
