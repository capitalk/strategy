PROTOC=protoc
PROTO=../../common/proto
PROTO_OUT=./proto_objs

all: protos


protos: tick_subscribe.py
	mkdir -p $(PROTO_OUT)
	$(PROTOC) -I$(PROTO) --python_out=$(PROTO_OUT) $(PROTO)/*.proto
	touch $(PROTO_OUT)/__init__.py

clean :
	rm -f $(PROTO_OUT)/*.py $(PROTO_OUT)/*.pyc

.phony: tick_subscribe.py

